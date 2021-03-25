/*
 * simple array layout. No merging optimization
 */

#include <assert.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>

#include "pt.h"

long pt_pieces_moved = 0;

// TODO what are optimal values?
// pieces below LOW_WATER should be merged
// pieces above HIGH_WATER must be immutable
#define LOW_WATER 256
#define HIGH_WATER 4096

enum blktype { IMMUT, IMMUT_MMAP, HEAP };

struct block {
	enum blktype type;
	int refs; // only for IMMUT blocks
	char *data;
	long size;
};

struct piece {
	struct block *block; /* data block */
	long offset; /* offset into block */
	long bytes; /* length */
};

struct PieceTable {
	long bytes; /* byte count */
	struct block *init; /* initial block */
	long size, capacity; /* no. of pieces in array */
	struct piece *vec; /* an array of pieces */
};

/* simple */

long pt_size(PieceTable *pt)
{
	return pt->bytes;
}

static long count_lfs(struct piece *piece)
{
	long count = 0;
	for(long i = piece->offset; i < piece->offset + piece->bytes; i++)
		if(piece->block->data[i] == '\n')
			count++;
	return count;
}

long pt_lfs(PieceTable *pt)
{
	long lfs = 0;
	for(long i = 0; i < pt->size; i++)
		lfs += count_lfs(&pt->vec[i]);
	return lfs;
}

static void pt_pprint_piece(struct piece *piece)
{
	fprintf(stderr, "┃piece with %7ld bytes ┃ %7ld lfs ┃ data: %5.*s...┃\n",
		piece->bytes, count_lfs(piece), (int)MIN(5, piece->bytes),
		piece->block->data + piece->offset);
}

void pt_pprint(PieceTable *pt)
{
	fprintf(stderr, "PieceTable with %ld/%ld pieces, %ld bytes\n", pt->size,
		pt->capacity, pt_size(pt));
	fputs("┏━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━┓\n",
	      stderr);

	for(long i = 0; i < pt->size; i++)
		pt_pprint_piece(&pt->vec[i]);

	fputs("┗━━━━━━━━━━━━━━━━━━━━━━━━━┻━━━━━━━━━━━━━┻━━━━━━━━━━━━━━━┛\n",
	      stderr);
}

//void check(PieceTable *pt, int val)
//{
//	long len = 0;
//	for(long i = 0; i < pt->size; i++)
//		len += pt->vec[i].bytes;
//	if(len != pt->bytes) {
//		pt_pprint(pt);
//		pt_dbg("val %d", val);
//		assert(false);
//	}
//}

void pt_print_struct_sizes(void)
{
	fprintf(stderr,
		"Implementation: \e[38;5;1marray\e[0m\n"
		"sizeof(struct piece): %zd\n"
		"sizeof(PieceTable): %zd\n",
		sizeof(struct piece), sizeof(PieceTable));
}

/* the fun stuff */

static struct block *new_block(const char *data, long len)
{
	struct block *new = malloc(sizeof *new);
	new->size = len;
	new->data = malloc(MAX(HIGH_WATER, len));
	memcpy(new->data, data, len);

	if(len > HIGH_WATER) {
		new->type = IMMUT;
		new->refs = 1;
	} else
		new->type = HEAP;
	return new;
}

static void free_block(struct block *block)
{
	switch(block->type) {
	case IMMUT_MMAP: return; // pt->init handled specially by pt_free
	case HEAP: free(block->data); break;
	case IMMUT:
		if(--block->refs == 0)
			free(block->data);
	}
	free(block);
}

static void block_insert(struct block *block, long offset, const char *data,
			 long len)
{
	assert(block->type == HEAP);
	char *start = block->data + offset;
	memmove(start + len, start, block->size - offset);
	memcpy(block->data + offset, data, len);
	block->size += len;
	if(block->size > HIGH_WATER) {
		block->type = IMMUT;
		block->refs = 1;
	}
}

static void block_delete(struct block *block, long offset, long len)
{
	assert(block->type == HEAP);
	char *start = block->data + offset;
	memmove(start, start + len, block->size - offset - len);
	block->size -= len;
}

PieceTable *pt_new(void)
{
	PieceTable *pt = malloc(sizeof *pt);
	pt->vec = malloc(sizeof(struct piece));
	pt->size = pt->capacity = 1;
	pt->bytes = 0;
	pt->init = malloc(sizeof(struct block));
	*pt->init = (struct block){ .type = IMMUT, .refs = 1 };
	pt->vec[0] = (struct piece){ .bytes = 0, .block = pt->init };
	return pt;
}

PieceTable *pt_new_from_file(const char *path, long len, long off)
{
	int fd = open(path, O_RDONLY);
	if(!fd)
		return NULL;
	len = len ? len : lseek(fd, off, SEEK_END);
	if(!len)
		return pt_new(); // mmap cannot handle 0-length mappings
	void *data = mmap(NULL, len, PROT_READ, MAP_SHARED, fd, off);
	close(fd);
	if(data == MAP_FAILED)
		return NULL;

	PieceTable *pt = malloc(sizeof *pt);
	pt->vec = malloc(2 * sizeof(struct piece));
	pt->size = pt->capacity = 2;
	pt->bytes = len;
	pt->init = malloc(sizeof(struct block));
	*pt->init = (struct block){
		.type = IMMUT_MMAP,
		.size = len,
		.data = data,
	};
	pt->vec[0] = (struct piece){ .bytes = 0, .block = pt->init };
	pt->vec[1] = (struct piece){
		.bytes = len,
		.offset = 0,
		.block = pt->init,
	};
	return pt;
}

void pt_free(PieceTable *pt)
{
	for(long i = 0; i < pt->size; i++)
		free_block(pt->vec[i].block);
	// goes after
	if(pt->init->type == IMMUT_MMAP)
		munmap(pt->init->data, pt->init->size);
	free(pt->init);
	free(pt->vec);
	free(pt);
}

struct piece *pt_search(PieceTable *pt, long pos, long *off)
{
	struct piece *piece = pt->vec;
	while(pos > piece->bytes) {
		pos -= piece->bytes;
		piece++;
	}
	*off = pos;
	assert(pos > 0 || piece == &pt->vec[0]);
	assert(piece - pt->vec < pt->size);
	return piece;
}

static void maybe_expand_vec(PieceTable *pt)
{
	if(pt->size > pt->capacity) {
		pt->capacity = MAX(pt->capacity * 2, pt->size);
		pt->vec = realloc(pt->vec, pt->capacity * sizeof(struct piece));
	}
}

static void maybe_shrink_vec(PieceTable *pt)
{
	if(pt->size < pt->capacity / 2) {
		pt->capacity /= 2;
		pt->vec = realloc(pt->vec, pt->capacity * sizeof(struct piece));
	}
}

static struct piece new_piece(char *data, long len)
{
	struct block *block = new_block(data, len);
	struct piece new =
		(struct piece){ .block = block, .offset = 0, .bytes = len };
	return new;
}

static void piece_insert(struct piece *piece, long off, char *data, long len)
{
	block_insert(piece->block, piece->offset + off, data, len);
	piece->bytes += len;
}

static void piece_delete(struct piece *piece, long off, long len)
{
	block_delete(piece->block, off, len);
	piece->bytes -= len;
}

void pt_insert(PieceTable *pt, long pos, char *data, long len)
{
	if(len == 0)
		return;
	struct piece *old = pt_search(pt, pos, &pos);
	if(old->block->type == HEAP)
		piece_insert(old, pos, data, len);
	else if(pos == old->bytes) {
		long index = old - pt->vec + 1;
		long count = pt->size - index;
		pt->size++;
		maybe_expand_vec(pt);
		struct piece *start = &pt->vec[index];
		memmove(start + 1, start, count * sizeof(struct piece));
		pt_pieces_moved += count;

		*start = new_piece(data, len);
	} else {
		struct piece new_left = {
			.block = old->block,
			.bytes = pos,
			.offset = old->offset,
		};
		struct piece new_right = {
			.block = old->block,
			.bytes = old->bytes - pos,
			.offset = old->offset + pos,
		};
		if(old->block->type == IMMUT)
			old->block->refs++;
		long index = old - pt->vec + 1;
		long count = pt->size - index;
		pt->size += 2;
		maybe_expand_vec(pt);
		struct piece *start = &pt->vec[index];
		memmove(start + 2, start, count * sizeof(struct piece));
		pt_pieces_moved += count;
		start[-1] = new_left;
		start[0] = new_piece(data, len);
		start[1] = new_right;
	}
	pt->bytes += len;
}

void pt_erase(PieceTable *pt, long pos, long len)
{
	if(len == 0)
		return;
	len = MIN(len, pt_size(pt) - pos);
	pt->bytes -= len;
	struct piece *piece = pt_search(pt, pos, &pos);
	if(len < piece->bytes - pos) {
		if(piece->block->type == HEAP)
			piece_delete(piece, pos, len);
		else {
			struct piece new_right = {
				.block = piece->block,
				.bytes = piece->bytes - pos - len,
				.offset = piece->offset + pos + len
			};
			piece->block->refs++;
			piece->bytes = pos;
			long index = piece + 1 - pt->vec;
			long count = pt->vec + pt->size - (piece + 1);
			pt->size++;
			maybe_expand_vec(pt);
			struct piece *start = &pt->vec[index];
			memmove(start + 1, start, count * sizeof(struct piece));
			pt_pieces_moved += count;

			*start = new_right;
		}
	} else {
		struct piece *start, *end;
		len -= piece->bytes - pos;
		piece->bytes = pos;
		if(len > 0) {
			start = ++piece;
			while(len > piece->bytes) {
				free_block(piece->block);
				len -= piece->bytes;
				piece++;
			}
		} else {
			start = piece; // end boundary: start = end, don't look further
		}
		if(len == piece->bytes) {
			end = piece + 1;
			assert(piece->bytes > 0); // not sentinel
			free_block(piece->block);
		} else { // len = 0 from above is ok
			end = piece;
			if(piece->block->type == HEAP)
				piece_delete(piece, 0, len);
			else {
				piece->offset += len;
				piece->bytes -= len;
			}
		}
		long count = pt->vec + pt->size - end;
		memmove(start, end, count * sizeof(struct piece));
		pt->size -= end - start;
		maybe_shrink_vec(pt);
		pt_pieces_moved += count;
	}
}

/* iterator */

//PieceIterator *pt_iter_get

/* dot output */

#include "dot.h"

#define FSTR(dest, str, ...)                                                   \
	do {                                                                   \
		int len = snprintf(NULL, 0, str, __VA_ARGS__);                 \
		dest = realloc(dest, len + 1);                                 \
		sprintf(dest, str, __VA_ARGS__);                               \
	} while(0)

static void pt_array_to_dot(PieceTable *pt, FILE *file)
{
	graph_link(file, pt, "vec", pt->vec, "body");
	graph_table_begin(file, pt->vec, "aquamarine3");
	char *tmp = NULL;
	char *port = NULL;
	for(long i = 0; i < pt->size; i++) {
		FSTR(port, "%ld", i);
		FSTR(tmp, "len: %ld", pt->vec[i].bytes);
		graph_table_entry(file, tmp, port);
	}
	free(tmp);
	free(port);
	graph_table_end(file);
	// output links
	for(long i = 1; i < pt->size; i++) {
		struct piece *p = &pt->vec[i];
		fprintf(file, "\n  x%ld:%ld -> \"%.*s\" [style=dashed];\n",
			(long)pt->vec, i, MIN((int)p->bytes, 60),
			p->block->data + p->offset);
	}
}

bool pt_to_dot(PieceTable *pt)
{
	FILE *file = fopen("array.dot", "w");
	char *tmp = NULL; // realloc
	if(!file)
		goto fail;
	graph_begin(file);
	graph_table_begin(file, pt, NULL);
	FSTR(tmp, "size: %ld", pt->size);
	graph_table_entry(file, tmp, NULL);
	FSTR(tmp, "capacity: %ld", pt->capacity);
	graph_table_entry(file, tmp, NULL);
	graph_table_entry(file, "vec", "vec");
	graph_table_end(file);
	pt_array_to_dot(pt, file);
	graph_end(file);
	free(tmp);
	if(fclose(file) < 0)
		goto fail;
	return true;

fail:
	perror("pt_to_dot");
	return false;
}
