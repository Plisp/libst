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

long pt_nodes_moved = 0;

struct piece {
	long bytes;  /* length */
	char *data; /* pointer into corresponding block */
};

#define BLKSIZE (1<<20)
enum blktype { HEAP, MMAP };

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

long pt_size(PieceTable *pt) {
	return pt->bytes;
}

static long count_lfs(struct piece *node, long off, long bytes)
{
	long count = 0;
	for(long i = off; i < bytes; i++)
		if(node->data[i] == '\n')
			count++;
	return count;
}

void pt_print_node(struct piece *node)
{
	printf("┃piece with %7ld bytes ┃ %7ld lfs ┃ data: %5.*s...┃\n",
			node->bytes, count_lfs(node, 0, node->bytes),
			(int)MIN(5, node->bytes), node->data);
}

void pt_pprint(PieceTable *pt)
{
	printf("PieceTable with %ld/%ld nodes\n", pt->size, pt->capacity);
	puts("┏━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━┓");
	for(long i = 0; i < pt->size; i++)
		pt_print_node(&pt->vec[i]);
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
	if(b->size + len <= b->capacity) { // should never hold for MMAP blocks
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
		.type = HEAP,
		.size = len, .capacity = len,
		.data = (char *)data
	};
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
	if(b->type == HEAP)
		free(b->data);
	else // if(pt->init.type == MMAP)
		munmap(b->data, b->size);
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

bool pt_insert(PieceTable *pt, long pos, char *data, long len)
{
	struct block *new_block = block_append(pt->blocks, data, len);
	if(new_block) {
		new_block->next = pt->blocks;
		pt->blocks = new_block;
	}
	pt->bytes += len;
	char *inserted_data = pt->blocks->data + pt->blocks->size - len;
	struct piece new = { .bytes = len, .data = inserted_data };
	struct piece *old = pt->vec;
	long off = pos;
	for(long i = 0; i < pt->size; i++) {
		if(off <= old->bytes)
			break;
		off -= old->bytes;
		old++;
	}
	if(off == old->bytes) {
		pt_dbg("insertion at boundary\n");
		long index = old - pt->vec;
		if(pt->size == pt->capacity) {
			pt->capacity *= 2;
			pt->vec = realloc(pt->vec, pt->capacity*sizeof(struct piece));
		}
		long count = pt->size - index;
		pt_nodes_moved += count;
		memmove(&pt->vec[index+1], &pt->vec[index], count*sizeof(struct piece));
		pt->vec[index] = new;
		pt->size++;
	} else {
		struct piece new_left = { .bytes = off, .data = old->data, };
		struct piece new_right = {
			.bytes = old->bytes - off,
			.data = old->data + off,
		};
		long index = old - pt->vec;
		if(pt->size + 2 > pt->capacity) {
			pt->capacity *= 2;
			pt->vec = realloc(pt->vec, pt->capacity*sizeof(struct piece));
		}
		long count = pt->size - index;
		pt_nodes_moved += count;
		memmove(&pt->vec[index+2], &pt->vec[index], count*sizeof(struct piece));
		pt->vec[index] = new_left;
		pt->vec[index+1] = new;
		pt->vec[index+2] = new_right;
		pt->size += 2;
	}
	return true;
}

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
	for(long i = 0; i < pt->size; i++) {
		char *tmp = NULL;
		char *port = NULL;
		FSTR(port, "%ld", i);
		FSTR(tmp, "len: %ld", pt->vec[i].bytes);
		graph_table_entry(file, tmp, port);
	}
	graph_table_end(file);
	// output links
	for(long i = 1; i < pt->size; i++)
		fprintf(file, "\n  x%ld:%ld -> %.*s [style=dashed];\n",
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
	if(fclose(file) < 0)
		goto fail;
	return true;

fail:
	perror("pt_to_dot");
	return false;
}
