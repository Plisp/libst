//
// simple array layout
//

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

struct piece {
	long bytes;  /* length */
	char *data; /* pointer into corresponding block */
};

#define BLKSIZE (1<<20)
enum blktype { DATA, HEAP, MMAP };

struct block {
	enum blktype type;
	long size, capacity;
	char *data;
	struct block *next;
};

struct PieceTable {
	long bytes;            /* byte count */
	struct block *blocks; /* list of text blocks */
	long size, capacity; /* no. of pieces in array */
	struct piece *vec;  /* an array of pieces */
};

/* simple */

long pt_size(PieceTable *pt)
{
	return pt->bytes;
}

static long count_lfs(struct piece *piece, long off, long bytes)
{
	long count = 0;
	for(long i = off; i < bytes; i++)
		if(piece->data[i] == '\n')
			count++;
	return count;
}

long pt_lfs(PieceTable *pt)
{
	long lfs = 0;
	for(long i = 0; i < pt->size; i++)
		lfs += count_lfs(&pt->vec[i], 0, pt->vec[i].bytes);
	return lfs;
}

void pt_pprint_piece(struct piece *piece)
{
	printf("┃piece with %7ld bytes ┃ %7ld lfs ┃ data: %5.*s...┃\n",
			piece->bytes, count_lfs(piece, 0, piece->bytes),
			(int)MIN(5, piece->bytes), piece->data);
}

void pt_pprint(PieceTable *pt)
{
	printf("PieceTable with %ld/%ld pieces, %ld bytes\n",
			pt->size, pt->capacity, pt_size(pt));
	puts("┏━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━┓");
	for(long i = 0; i < pt->size; i++)
		pt_pprint_piece(&pt->vec[i]);
	puts("┗━━━━━━━━━━━━━━━━━━━━━━━━━┻━━━━━━━━━━━━━┻━━━━━━━━━━━━━━━┛");
}

void pt_print_struct_sizes(void)
{
	printf(
		"Implementation: \e[38;5;1marray\e[0m\n"
		"sizeof(struct piece): %ld\n"
		"sizeof(PieceTable): %ld\n",
		sizeof(struct piece),
		sizeof(PieceTable)
	);
}

/* the fun stuff */

static struct block *block_append(struct block *b, char *data, long len)
{
	// should never hold for MMAP and DATA blocks
	if(b->size + len <= b->capacity) {
		assert(b->type == HEAP);
		memcpy(b->data + b->size, data, len);
		b->size += len;
		return NULL;
	} else {
		struct block *new = malloc(sizeof *new);
		*new = (struct block) {
			.type = HEAP,
			.size = len, .capacity = MAX(BLKSIZE, len),
		};
		new->data = malloc(new->capacity);
		memcpy(new->data, data, len);
		pt_dbg("new block of size %ld allocated\n", new->capacity);
		return new;
	}
}

PieceTable *pt_new_from_data(const char *data, long len)
{
	PieceTable *pt = malloc(sizeof *pt);
	pt->blocks = malloc(sizeof(struct block));
	pt->vec = malloc(2*sizeof(struct piece));
	pt->size = pt->capacity = 2;

	struct block *init = pt->blocks;
	*init = (struct block) {
		.next = NULL,
		.type = DATA,
		.size = len, .capacity = len,
		.data = (char *)data
	};
	// we take ownership of data directly
	//memcpy(init->data, data, len);

	pt->bytes = len;
	pt->vec[0] = (struct piece) {0};
	pt->vec[1] = (struct piece) { .bytes = len, .data = init->data };
	return pt;
}

PieceTable *pt_new_from_file(const char *path, long len, long off)
{
	PieceTable *pt = malloc(sizeof *pt);
	pt->blocks = malloc(sizeof(struct block));
	pt->vec = malloc(2*sizeof(struct piece));
	pt->size = pt->capacity = 2;

	int fd = open(path, O_RDONLY);
	if(!fd)
		goto fail;
	// mmap manpage asserts len > 0
	len = len ? len : lseek(fd, off, SEEK_END);
	void *data = mmap(NULL, len, PROT_READ, MAP_SHARED, fd, off);
	close(fd);
	if(data == MAP_FAILED)
		goto fail;
	struct block *init = pt->blocks;
	*init = (struct block) {
		.next = NULL,
		.type = MMAP,
		.size = len, .capacity = len,
		.data = data
	};

	pt->bytes = len;
	pt->vec[0] = (struct piece) {0};
	pt->vec[1] = (struct piece) { .bytes = len, .data = init->data };
  return pt;

fail:
	free(pt->vec);
	free(pt->blocks);
	free(pt);
	return NULL;
}

static void free_block(struct block *b)
{
	switch(b->type) {
		case DATA: break;
		case HEAP: free(b->data); break;
		case MMAP: munmap(b->data, b->size); break;
	}
	free(b);
}

void pt_free(PieceTable *pt)
{
	for(struct block *b = pt->blocks, *next; b; b = next) {
		pt_dbg("freeing block of size %ld\n", b->size);
		next = b->next;
		free_block(b);
	}
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
	return piece;
}

void maybe_expand_vec(PieceTable *pt)
{
	// this still works worst case:
	// the faliure case: c+2 > 2c <=> c <= 1
	// However capacity >= 2 from the moment of table creation
	// invariant is preserved after maybe_shrink as c >= 2 && s >= 1 (sentinel)
	// which means we cannot have s < c/2 as necessary for shrinking here.
	if(pt->size > pt->capacity) {
		pt->capacity *= 2;
		pt->vec = realloc(pt->vec, pt->capacity * sizeof(struct piece));
	}
}

void maybe_shrink_vec(PieceTable *pt)
{
	if(pt->size < pt->capacity/2) {
		pt->capacity /= 2;
		pt->vec = realloc(pt->vec, pt->capacity * sizeof(struct piece));
	}
}

void pt_insert(PieceTable *pt, long pos, char *data, long len)
{
	if(len == 0) return;
	struct block *new_block = block_append(pt->blocks, data, len);
	if(new_block) {
		new_block->next = pt->blocks;
		pt->blocks = new_block;
	}
	char *inserted_data = pt->blocks->data + pt->blocks->size - len;
	struct piece new = { .bytes = len, .data = inserted_data };
	struct piece *old = pt_search(pt, pos, &pos);
	if(pos == old->bytes) {
		long index = old - pt->vec + 1;
		long count = pt->size - index;
		pt->size++;
		maybe_expand_vec(pt);
		struct piece *start = &pt->vec[index];
		memmove(start + 1, start, count*sizeof(struct piece));
		*start = new;
		pt_pieces_moved += count;
	} else {
		struct piece new_left = { .bytes = pos, .data = old->data };
		struct piece new_right = { old->bytes - pos, old->data + pos };
		long index = old - pt->vec;
		long count = pt->size - index;
		pt->size += 2;
		maybe_expand_vec(pt);
		struct piece *start = &pt->vec[index];
		memmove(start + 2, start, count*sizeof(struct piece));
		start[0] = new_left;
		start[1] = new;
		start[2] = new_right;
		pt_pieces_moved += count;
	}
	pt->bytes += len;
}

void pt_erase(PieceTable *pt, long pos, long len)
{
	if(len == 0) return;
	len = MIN(len, pt_size(pt) - pos);
	pt->bytes -= len;
	struct piece *piece = pt_search(pt, pos, &pos);
	if(len < piece->bytes - pos) { // common case: piece split
		struct piece new_right = {
			.bytes = piece->bytes - pos - len,
			.data = piece->data + pos + len
		};
		piece->bytes = pos;
		long index = piece+1 - pt->vec;
		long count = pt->vec + pt->size - (piece+1);
		pt->size++;
		maybe_expand_vec(pt);
		struct piece *start = &pt->vec[index];
		memmove(start + 1, start, count*sizeof(struct piece));
		*start = new_right;
		pt_pieces_moved += count;
	} else {
		struct piece *start, *end;
		len -= piece->bytes - pos;
		piece->bytes = pos; // pos > 0
		if(len > 0) {
			start = ++piece;
			while(len > piece->bytes) {
				len -= piece->bytes;
				piece++;
			}
		} else {
			start = piece; // end boundary: start = end, don't look further
		}
		if(len == piece->bytes) { // len != 0 esp. when piece is the sentinel
			end = piece + 1;
		} else { // len = 0 from above is ok
			end = piece;
			piece->bytes -= len;
			piece->data += len;
		}
		long count = pt->vec + pt->size - end;
		memmove(start, end, count*sizeof(struct piece));
		pt->size -= end - start;
		maybe_shrink_vec(pt);
		pt_pieces_moved += count;
	}
}

/* iterator */

//PieceIterator *pt_iter_get

/* dot output */

#include "dot.h"

#define FSTR(dest, str, ...) \
do { \
	int len = snprintf(NULL, 0, str, __VA_ARGS__); \
	dest = realloc(dest, len+1); \
	sprintf(dest, str, __VA_ARGS__); \
} while (0)              \

void pt_array_to_dot(PieceTable *pt, FILE *file)
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
	for(long i = 1; i < pt->size; i++)
		fprintf(file, "\n  x%ld:%ld -> \"%.*s\" [style=dashed];\n",
				(long)pt->vec, i,
				MIN((int)pt->vec[i].bytes, 15), pt->vec[i].data);
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
