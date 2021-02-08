//
// simple array-based layout
//

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>

#include "pt.h"

long pt_nodes_moved = 0;

struct pnode {
	long bytes;  /* length */
	char *data; /* pointer into corresponding block */
};

#define PT_BLKSIZE (1<<20)
enum blktype { HEAP, MMAP };

struct block {
	enum blktype type;
	long size, capacity;
	char *data;
	struct block *next;
};

struct piecetable {
	long size;                       /* byte count */
	struct block *blocks;           /* list of text blocks */
	long tree_size, tree_capacity; /* no. of nodes in tree */
	struct pnode *tree;           /* an inorder array of nodes */
};

struct undo {
	union {
		PieceTable clone;
		struct {
			struct pnode *old;
			long old_len;
			struct pnode *new;
			long new_len;
			long off;
		} record;
	};
	bool is_clone;
	bool is_undone;
};

/* basic utilities */

long pt_size(PieceTable *pt) {
	return pt->size;
}

/*
long pt_lfs(PieceTable *pt) {
	return pt->lfs;
}
*/

/* constructors */

static struct block *block_append(struct block *b, char *data, long len)
{
	if(b->size + len <= b->capacity) { // should never hold for MMAP blocks
		assert(b->type == HEAP);
		memcpy(b->data + b->size, data, len);
		b->size += len;
		return NULL;
	} else {
		struct block *new = malloc(sizeof *new);
		new->type = HEAP;
		new->size = len;
		new->capacity = MAX(PT_BLKSIZE, len);
		new->data = malloc(new->capacity);
		memcpy(new->data, data, len);
		pt_dbg("new block of size %ld allocated\n", new->capacity);
		return new;
	}
}

static long count_lfs(struct pnode *node, long off, long bytes)
{
	long count = 0;
	for(long i = off; i < bytes; i++)
		if(node->data[i] == '\n')
			count++;
	return count;
}

PieceTable *pt_new_from_data(const char *data, long len)
{
	PieceTable *pt = malloc(sizeof *pt);
	pt->blocks = malloc(sizeof(struct block));
	pt->tree_size = pt->tree_capacity = 2;
	pt->tree = malloc(2 * sizeof(struct pnode));

	struct block *init = pt->blocks;
	init->next = NULL;
	init->type = HEAP;
	init->size = init->capacity = len;
	init->data = malloc(len);
	memcpy(init->data, data, len);

	pt->size = len;
	pt->tree[0] = (struct pnode) {0};
	pt->tree[1] = (struct pnode) {
		.bytes = len,
		.data = init->data,
	};
	return pt;
}

PieceTable *pt_new_from_file(const char *path, long len, long off)
{
	PieceTable *pt = malloc(sizeof *pt);
	pt->blocks = malloc(sizeof(struct block));
	pt->tree_size = pt->tree_capacity = 2;
	pt->tree = malloc(2 * sizeof(struct pnode));

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
	init->next = NULL;
	init->type = MMAP;
	init->size = init->capacity = len;
	init->data = data;

	pt->size = len;
	pt->tree[0] = (struct pnode) {0};
	pt->tree[1] = (struct pnode) {
		.bytes = len,
		.data = init->data,
	};
	return pt;

fail:
	free(pt->tree);
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

	free(pt->tree);
	free(pt);
}

/* the fun stuff */

void pt_insert_piece(PieceTable *pt, struct pnode new, struct pnode *at)
{
}

bool pt_insert(PieceTable *pt, long pos, char *data, long len, struct undo *undo)
{
	struct block *b = block_append(pt->blocks, data, len);
	if(b) {
		b->next = pt->blocks;
		pt->blocks = b;
	}
	pt->size += len;
	char *inserted_data = pt->blocks->data + pt->blocks->size - len;
	struct pnode new = { .bytes = len, .data = inserted_data };
	struct pnode *old = pt->tree;
	long off = pos;
	for(long i = 0; i < pt->tree_size; i++) {
		if(off <= old->bytes)
			break;
		off -= old->bytes;
		old++;
	}
	if(off == old->bytes) {
		pt_dbg("insertion at boundary\n");
		if(undo) {
			struct pnode *new_nodes = malloc(sizeof(struct pnode));
			*new_nodes = new;
			*undo = (struct undo) {
				.record = {
					.old = NULL,
					.old_len = 0,
					.new = new_nodes,
					.new_len = 1,
					.off = pos
				},
				.is_clone = false,
				.is_undone = false
			};
		}
		long index = old - pt->tree;
		if(pt->tree_size == pt->tree_capacity) {
			pt->tree_capacity = pt->tree_size * 2;
			pt->tree = realloc(pt->tree, pt->tree_capacity * sizeof(struct pnode));
		}
		long count = pt->tree_size - index;
		pt_nodes_moved += count;
		memmove(&pt->tree[index + 1], &pt->tree[index], count * sizeof(struct pnode));
		pt->tree[index] = new;
		pt->tree_size++;
	} else {
		struct pnode new_left = { .bytes = off, .data = old->data, };
		struct pnode new_right = {
			.bytes = old->bytes - off,
			.data = old->data + off,
		};
		if(undo) {
			struct pnode *old_nodes = malloc(sizeof(struct pnode));
			*old_nodes = *old;
			struct pnode *new_nodes = malloc(3*sizeof(struct pnode));
			new_nodes[0] = new_left;
			new_nodes[1] = new;
			new_nodes[2] = new_right;
			*undo = (struct undo) {
				.record = {
					.old = old,
					.old_len = 1,
					.new = new_nodes,
					.new_len = 3,
					.off = pos
				},
				.is_clone = false,
				.is_undone = false
			};
		}
		long index = old - pt->tree;
		if(pt->tree_size + 2 > pt->tree_capacity) {
			pt->tree_capacity = pt->tree_size * 2;
			pt->tree = realloc(pt->tree, pt->tree_capacity * sizeof(struct pnode));
		}
		long count = pt->tree_size - index;
		pt_nodes_moved += count;
		memmove(&pt->tree[index + 2], &pt->tree[index], count * sizeof(struct pnode));
		pt->tree[index] = new_left;
		pt->tree[index + 1] = new;
		pt->tree[index + 2] = new_right;
		pt->tree_size += 2;
	}
	return true;
}

bool pt_undo(PieceTable *pt, struct undo *undo)
{
	assert(!undo->is_undone);
	undo->is_undone = true;
	return pt;
}

bool pt_redo(PieceTable *pt, struct undo *undo)
{
	assert(undo->is_undone);
	undo->is_undone = false;
	return pt;
}

/* printing */

void pt_print_node(struct pnode *node)
{
	printf("┃piece with %7ld bytes ┃ %7ld lfs ┃ data: %5.*s...┃\n",
			node->bytes, count_lfs(node, 0, node->bytes),
			(int)MIN(5, node->bytes), node->data);
}

void pt_print_tree(PieceTable *pt)
{
	printf("PieceTable with %ld/%ld nodes\n", pt->tree_size, pt->tree_capacity);
	puts("┏━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━┓");
	for(long i = 0; i < pt->tree_size; i++) {
		pt_print_node(&pt->tree[i]);
	}
	puts("┗━━━━━━━━━━━━━━━━━━━━━━━━━┻━━━━━━━━━━━━━┻━━━━━━━━━━━━━━━┛");
}

void pt_print_struct_sizes()
{
	printf(
		"sizeof(struct pnode): %ld, "
		"sizeof(struct block): %ld, "
		"sizeof(struct undo): %ld, "
		"sizeof(PieceTable): %ld\n",
		sizeof(struct pnode),
		sizeof(struct block),
		sizeof(struct undo),
		sizeof(PieceTable)
	);
}
