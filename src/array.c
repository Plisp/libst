/*
 * simple array layout
 */

#include <assert.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>

#include "st.h"

size_t st_slices_moved = 0;

// slices >= HIGH_WATER must be immutable
#define HIGH_WATER 4

enum blktype { LARGE, LARGE_MMAP, SMALL };

struct block {
	enum blktype type;
	int refc; // only for LARGE blocks
	char *data;
	size_t size;
};

struct slice {
	struct block *block; // data block
	size_t offset; // offset into block
	size_t bytes; // length
};

struct slicetable {
	size_t bytes; // byte count
	size_t size, capacity; // no. of slices in array
	struct slice *vec; // an array of slices
};

/* simple */

size_t st_size(const SliceTable *st)
{
	return st->bytes;
}

int st_depth(const SliceTable *st)
{
	return st->size;
}

static size_t count_lfs(const char *s, size_t len)
{
	size_t count = 0;
	for(size_t i = 0; i < len; i++)
		if(*s++ == '\n')
			count++;
	return count;
}

static void st_pprint_slice(struct slice *slice)
{
	fprintf(stderr, "┃slice with %7ld bytes ┃ data: %5.*s...┃\n",
		slice->bytes, (int)MIN(5, slice->bytes),
		slice->block->data + slice->offset);
}

void st_pprint(const SliceTable *st)
{
	fprintf(stderr, "PieceTable with %ld/%ld slices, %ld bytes\n", st->size,
		st->capacity, st_size(st));
	fputs("┏━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━┓\n",
	      stderr);

	for(size_t i = 0; i < st->size; i++)
		st_pprint_slice(&st->vec[i]);

	fputs("┗━━━━━━━━━━━━━━━━━━━━━━━━━┻━━━━━━━━━━━━━┻━━━━━━━━━━━━━━━┛\n",
	      stderr);
}

bool st_check_invariants(const SliceTable *st)
{
	size_t len = 0;
	for(size_t i = 0; i < st->size; i++)
		len += st->vec[i].bytes;
	if(len != st->bytes) {
		st_pprint(st);
		return false;
	} else
		return true;
}

void st_print_struct_sizes(void)
{
	fprintf(stderr,
		"Implementation: \e[38;5;1marray\e[0m\n"
		"sizeof(struct slice): %zd\n"
		"sizeof(PieceTable): %zd\n",
		sizeof(struct slice), sizeof(SliceTable));
}

/* the fun stuff */

static struct block *new_block(const char *data, size_t len)
{
	struct block *new = malloc(sizeof *new);
	new->size = len;
	new->data = malloc(MAX(HIGH_WATER, len));
	memcpy(new->data, data, len);

	if(len > HIGH_WATER) {
		new->type = LARGE;
		new->refc = 1;
	} else
		new->type = SMALL;
	return new;
}

static void free_block(struct block *block)
{
	switch(block->type) {
	case SMALL:
		free(block->data);
		free(block);
		break;
	case LARGE_MMAP:
		if(--block->refc == 0) {
			munmap(block->data, block->size);
			free(block);
		}
		break;
	case LARGE:
		if(--block->refc == 0) {
			free(block->data);
			free(block);
		}
	}
}

static void block_insert(struct block *block, size_t offset, const char *data,
			 size_t len)
{
	assert(block->type == SMALL);
	if(block->size + len > HIGH_WATER)
		block->data = realloc(block->data, block->size + len);
	char *start = block->data + offset;
	memmove(start + len, start, block->size - offset);
	memcpy(block->data + offset, data, len);
	block->size += len;
	if(block->size > HIGH_WATER) {
		block->type = LARGE;
		block->refc = 1;
	}
}

static void block_delete(struct block *block, size_t offset, size_t len)
{
	assert(block->type == SMALL);
	char *start = block->data + offset;
	memmove(start, start + len, block->size - offset - len);
	block->size -= len;
}

SliceTable *st_new(void)
{
	SliceTable *st = malloc(sizeof *st);
	st->vec = malloc(sizeof(struct slice));
	st->size = st->capacity = 1;
	st->bytes = 0;
	// ensures we do not ever try mutating the sentinel block
	struct block *init = malloc(sizeof(struct block));
	*init = (struct block){ .type = LARGE, .refc = 1, .data = malloc(1) };
	st->vec[0] = (struct slice){ .bytes = 0, .offset = 0, .block = init };
	return st;
}

SliceTable *st_new_from_file(const char *path)
{
	int fd = open(path, O_RDONLY);
	if(!fd)
		return NULL;
	size_t len = lseek(fd, 0, SEEK_END);
	if(!len)
		return st_new(); // mmap cannot handle 0-length mappings

	enum blktype type;
	void *data;
	if(len <= HIGH_WATER) {
		data = malloc(HIGH_WATER);
		if(read(fd, data, len) != len) {
			free(data);
			return NULL;
		}
		type = SMALL;
	} else {
		data = mmap(NULL, len, PROT_READ, MAP_SHARED, fd, 0);
		close(fd);
		if(data == MAP_FAILED)
			return NULL;
		type = LARGE_MMAP;
	}

	SliceTable *st = malloc(sizeof *st);
	st->vec = malloc(2 * sizeof(struct slice));
	st->size = st->capacity = 2;
	st->bytes = len;

	struct block *dummy = malloc(sizeof(struct block));
	*dummy = (struct block){ .type = LARGE, .refc = 1, .data = malloc(1) };
	st->vec[0] = (struct slice){ .bytes = 0, .offset = 0, .block = dummy };

	struct block *init = malloc(sizeof(struct block));
	*init = (struct block){ type, .refc = 1, data, len, };
	st->vec[1] = (struct slice){ .bytes = len, .offset = 0, .block = init };
	return st;
}

SliceTable *st_clone(const SliceTable *st)
{
	SliceTable *clone = malloc(sizeof *clone);
	clone->bytes = st->bytes;
	clone->capacity = st->capacity;
	clone->size = st->size;
	clone->vec = malloc(st->size * sizeof(struct slice));
	memcpy(clone->vec, st->vec, st->size * sizeof(struct slice));
	// copy SMALL blocks, increment reference counts for LARGE blocks
	for(size_t i = 0; i < clone->size; i++) {
		struct slice *s = &clone->vec[i];
		if(s->block->type == SMALL)
			s->block = new_block(s->block->data + s->offset, s->block->size);
		else
			s->block->refc++;
	}
	return clone;
}

void st_free(SliceTable *st)
{
	for(size_t i = 0; i < st->size; i++)
		free_block(st->vec[i].block);
	free(st->vec);
	free(st);
}

void st_dump(const SliceTable *st, FILE *file)
{
	for(size_t i = 1; i < st->size; i++) {
		fprintf(file, "%.*s", (int)st->vec[i].bytes,
				st->vec[i].block->data + st->vec[i].offset);
	}
}

struct slice *st_search(const SliceTable *st, size_t pos, size_t *off)
{
	struct slice *slice = st->vec;

	while(pos > slice->bytes) {
		pos -= slice->bytes;
		slice++;
	}
	*off = pos;
	assert(pos > 0 || slice == &st->vec[0]);
	assert((size_t)(slice - st->vec) < st->size);
	return slice;
}

static void maybe_expand_vec(SliceTable *st)
{
	if(st->size > st->capacity) {
		st->capacity = MAX(st->capacity * 2, st->size);
		st->vec = realloc(st->vec, st->capacity * sizeof(struct slice));
	}
}

static void maybe_shrink_vec(SliceTable *st)
{
	if(st->size < st->capacity / 2) {
		st->capacity /= 2;
		st->vec = realloc(st->vec, st->capacity * sizeof(struct slice));
	}
}

static struct slice new_slice(const char *data, size_t len)
{
	struct block *block = new_block(data, len);
	struct slice p = (struct slice){ .block = block, .offset = 0, .bytes = len };
	return p;
}

static void slice_insert(struct slice *slice, size_t off,
						const char *data, size_t len)
{
	block_insert(slice->block, slice->offset + off, data, len);
	slice->bytes += len;
}

static void slice_delete(struct slice *slice, size_t off, size_t len)
{
	block_delete(slice->block, off, len);
	slice->bytes -= len;
}

size_t st_insert(SliceTable *st, size_t pos, const char *data, size_t len)
{
	if(len == 0)
		return 0;

	st->bytes += len;
	struct slice *old = st_search(st, pos, &pos);

	if(old->block->type == SMALL)
		slice_insert(old, pos, data, len);
	else if(pos == old->bytes) {
		size_t index = old - st->vec + 1;
		size_t count = st->size - index;
		struct slice *start;

		st->size++;
		maybe_expand_vec(st);
		start = &st->vec[index];
		memmove(start + 1, start, count * sizeof(struct slice));
		st_slices_moved += count;
		*start = new_slice(data, len);
	} else {
		struct slice new_left = {
			.block = old->block,
			.bytes = pos,
			.offset = old->offset,
		};
		struct slice new_right = {
			.block = old->block,
			.bytes = old->bytes - pos,
			.offset = old->offset + pos,
		};
		if(old->block->type == LARGE || old->block->type == LARGE_MMAP)
			old->block->refc++;

		size_t index = old - st->vec + 1;
		size_t count = st->size - index;
		struct slice *start;

		st->size += 2;
		maybe_expand_vec(st);
		start = &st->vec[index];
		memmove(start + 2, start, count * sizeof(struct slice));
		st_slices_moved += count;
		start[-1] = new_left;
		start[0] = new_slice(data, len);
		start[1] = new_right;
	}
	return count_lfs(data, len);
}

size_t st_delete(SliceTable *st, size_t pos, size_t len)
{
	len = MIN(len, st_size(st) - pos);
	if(len == 0)
		return 0;

	st->bytes -= len; // do it here as we mutate len
	struct slice *slice = st_search(st, pos, &pos);
	size_t lf_delta = 0;

	if(pos + len < slice->bytes) {
		lf_delta += count_lfs(slice->block->data + slice->offset + pos, len);
		if(slice->block->type == SMALL)
			slice_delete(slice, pos, len);
		else {
			struct slice new_right = {
				.block = slice->block,
				.bytes = slice->bytes - pos - len,
				.offset = slice->offset + pos + len
			};
			slice->bytes = pos;
			slice->block->refc++;

			size_t index = slice + 1 - st->vec;
			size_t count = st->vec + st->size - (slice + 1);
			struct slice *start;

			st->size++;
			maybe_expand_vec(st);
			start = &st->vec[index];
			memmove(start + 1, start, count * sizeof(struct slice));
			st_slices_moved += count;
			*start = new_right;
		}
	} else {
		struct slice *start, *end;

		len -= slice->bytes - pos;
		lf_delta += count_lfs(slice->block->data + slice->offset + pos,
							slice->bytes - pos);
		// truncate slice
		slice->bytes = pos;

		if(len > 0) {
			start = ++slice;
			while(len > 0 && len >= slice->bytes) {
				lf_delta += count_lfs(slice->block->data + slice->offset,
									slice->bytes);
				free_block(slice->block);
				len -= slice->bytes;
				slice++;
			}
		} else
			start = slice; // end boundary: start = end, don't look further

		end = slice;
		if(len > 0) { // we could use another sentinel but that's a bit far
			lf_delta += count_lfs(slice->block->data + slice->offset, len);
			if(slice->block->type == SMALL)
				slice_delete(slice, 0, len);
			else {
				slice->offset += len;
				slice->bytes -= len;
			}
		}
		size_t count = st->vec + st->size - end;
		memmove(start, end, count * sizeof(struct slice));
		st->size -= end - start;
		maybe_shrink_vec(st);
		st_slices_moved += count;
	}
	return lf_delta;
}

/* TODO iterator */

struct sliceiter {
	SliceTable *st;
	struct slice *slice;
	char *data; // points directly
};

SliceIter *st_iter_new(SliceTable *st, size_t pos)
{
	SliceIter *it = malloc(sizeof *it);
	size_t off;
	it->slice = st_search(st, pos, &off);
	it->data = it->slice->block->data + it->slice->offset + off;
	it->st = st;
	return it;
}

/* dot output */

#include "dot.h"

static void st_array_to_dot(const SliceTable *st, FILE *file)
{
	graph_link(file, st, "vec", st->vec, "body");
	graph_table_begin(file, st->vec, "aquamarine3");
	char *tmp = NULL, *port = NULL;

	for(size_t i = 0; i < st->size; i++) {
		FSTR(port, "%ld", i);
		FSTR(tmp, "len: %ld", st->vec[i].bytes);
		graph_table_entry(file, tmp, port);
	}
	free(tmp);
	free(port);
	graph_table_end(file);
	// output links
	for(size_t i = 1; i < st->size; i++) {
		struct slice *p = &st->vec[i];
		fprintf(file, "\n  x%ld:%ld -> \"%.*s\" [style=dashed];\n",
			(long)st->vec, i, MIN((int)p->bytes, 60),
			p->block->data + p->offset);
	}
}

bool st_to_dot(const SliceTable *st, const char *path)
{
	char *tmp = NULL;
	FILE *file = fopen(path, "w");
	if(!file)
		goto fail;
	graph_begin(file);

	graph_table_begin(file, st, NULL);
	FSTR(tmp, "size: %ld", st->size);
	graph_table_entry(file, tmp, NULL);
	FSTR(tmp, "capacity: %ld", st->capacity);
	graph_table_entry(file, tmp, NULL);
	graph_table_entry(file, "vec", "vec");
	graph_table_end(file);

	st_array_to_dot(st, file);

	graph_end(file);
	free(tmp);
	if(fclose(file) < 0)
		goto fail;
	return true;

fail:
	perror("st_to_dot");
	return false;
}
