#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>

#include "pt.h"

size_t pt_nodes_moved = 0;

struct text_info {
	size_t bytes, lfs;
};

struct pnode {
	struct text_info info; /* info of this piece */
	char *data;           /* pointer into corresponding block */
	struct block *buf;   /* only sane way to maintain/resize lfs */
	size_t lf;          /* index into corresponding linefeed buffer */
};

struct location {
	struct pnode *piece;
	size_t off;
};

#define PT_BLKSIZE (1<<20)
enum blktype { HEAP, MMAP };

struct block {
	enum blktype type;
	size_t size, capacity;
	char *data;
	size_t lf_size, lf_capacity;
	size_t *lfs;
	struct block *next;
};

struct piecetable {
	size_t size, lfs;
	struct block *blocks;             /* list of text blocks */
	size_t tree_size, tree_capacity; /* no. of nodes in tree */
	struct pnode *tree;             /* an inorder array of nodes */
};

struct undo {
	union {
		PieceTable clone;
		struct {
			struct pnode *old;
			size_t old_len;
			struct pnode *new;
			size_t new_len;
			size_t off;
		} record;
	};
	bool is_clone;
	bool is_undone;
};

/* basic utilities */

size_t pt_size(PieceTable *pt) {
	return pt->size;
}

size_t pt_lfs(PieceTable *pt) {
	return pt->lfs;
}

/* constructors */

void pt_print_lfs(struct block *b)
{
	for(size_t i = 0; i < b->lf_size; i++)
		printf("%zd\n", b->lfs[i]);
}

static size_t buffer_append_lfs(struct block *b, size_t len)
{
	char *data = b->data + b->size - len;
	size_t orig = b->lf_size;
	for(size_t i = b->size - len; i < b->size; i++) {
		if(*data++ == '\n') { // found a newline, 'push' back
			if (b->lf_size++ == b->lf_capacity) {
				b->lf_capacity = b->lf_size * 2;
				b->lfs = realloc(b->lfs, b->lf_capacity * sizeof(size_t));
			}
			b->lfs[b->lf_size - 1] = i;
		}
	}
	return b->lf_size - orig;
}

static struct block *buffer_append(struct block *b, char *data, size_t len)
{
	if (b->size + len <= b->capacity) { // should never hold for MMAP blocks
		assert(b->type == HEAP);
		memcpy(b->data + b->size, data, len);
		b->size += len;
		return NULL;
	} else {
		struct block *new = malloc(sizeof *new);
		new->type = HEAP;
		new->size = len;
		new->capacity = max(PT_BLKSIZE, len);
		new->data = malloc(new->capacity);
		memcpy(new->data, data, len);
		new->lf_size = new->lf_capacity = 0;
		new->lfs = NULL;
		pt_dbg("new buffer of size %zd allocated\n", new->capacity);
		return new;
	}
}
 
static size_t count_lfs(struct pnode *node, size_t off, size_t bytes)
{
	pt_dbg("searching for lfs: off %zd, bytes: %zd, %zd lfs in node\n",
			off, bytes, node->info.lfs);
	struct block *buf = node->buf;
	size_t lf = node->lf;
	while(lf - node->lf < node->info.lfs) {
		if(buf->lfs[lf++] >= off) {
			lf--;
			size_t count = 0;
			while(count < node->info.lfs && buf->lfs[lf++] < off + bytes) {
				//pt_dbg("found lf at index %zd\n", buf->lfs[lf]);
				count++;
				//lf++; debugging, otherwise increment above
			}
			pt_dbg("found %zd lfs in total\n", count);
			return count;
		}
	}
	pt_dbg("gave up\n");
	return 0;
}

struct location pt_get_piece(PieceTable *pt, size_t off)
{
	struct pnode *node = pt->tree;
	for(size_t i = 0; i < pt->tree_size; i++, node++) {
		if(off <= node->info.bytes)
			return (struct location) {node, off};
		off -= node->info.bytes;
	}
	return (struct location) {0};
}

PieceTable *pt_new_from_data(const char *data, size_t len)
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
	init->lf_size = init->lf_capacity = 0;
	init->lfs = NULL;
	size_t lfs = buffer_append_lfs(init, len);

	pt->size = len;
	pt->lfs = lfs;
	pt->tree[0] = (struct pnode) {0};
	pt->tree[1] = (struct pnode) {
		.info = {
			.lfs = lfs,
			.bytes = len
		},
		.data = init->data,
		.buf = init,
		.lf = 0
	};
	return pt;
}

PieceTable *pt_new_from_file(const char *path, size_t len, size_t off)
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
	init->lf_size = init->lf_capacity = 0;
	init->lfs = NULL;
	size_t lfs = buffer_append_lfs(init, len);

	pt->size = len;
	pt->lfs = lfs;
	pt->tree[0] = (struct pnode) {0};
	pt->tree[1] = (struct pnode) {
		.info = {
			.lfs = lfs,
			.bytes = len
		},
		.data = init->data,
		.buf = init,
		.lf = 0
	};
	return pt;

fail:
	free(pt->tree);
	free(pt->blocks);
	free(pt);
	return NULL;
}

static void free_buffer(struct block *b)
{
	if(b->type == HEAP)
		free(b->data);
	else // if (pt->init.type == MMAP)
		munmap(b->data, b->size);
	free(b->lfs);
	free(b);
}

void pt_free(PieceTable *pt)
{
	for(struct block *b = pt->blocks, *next; b; b = next) {
		pt_dbg("freeing buffer of size %zd\n", b->size);
		next = b->next;
		free_buffer(b);
	}

	free(pt->tree);
	free(pt);
}

/* the fun stuff */

void pt_insert_piece(PieceTable *pt, struct pnode new, struct pnode *at)
{
	if(pt->tree_size == pt->tree_capacity) {
		size_t index = at - pt->tree;
		pt->tree_capacity = pt->tree_size * 2;
		pt->tree = realloc(pt->tree, pt->tree_capacity * sizeof(struct pnode));
		at = pt->tree + index;
	}
	size_t count = pt->tree + pt->tree_size - at;
	pt_nodes_moved += count;
	memmove(at + 1, at, count * sizeof(struct pnode));
	pt->tree_size++;
	*at = new;
}

void pt_delete_piece(PieceTable *pt, struct pnode *at)
{
	size_t count = pt->tree + pt->tree_size - at - 1;
	pt_nodes_moved += count;
	memmove(at, at + 1, count * sizeof(struct pnode));
	pt->tree_size--;
}

size_t piece_offset(struct block *blocks, struct pnode *node)
{
	for(struct block *b = blocks; b; b = b->next) {
		if(b->data <= node->data && node->data < b->data + b->size)
			return node->data - b->data;
	}
	pt_dbg("doh! piece buffer not found\n");
	_Exit(1);
}

bool pt_insert(PieceTable *pt, size_t pos, char *data, size_t len, struct undo *undo)
{
	size_t lfs;
	struct block *b = buffer_append(pt->blocks, data, len);
	if(b) {
		b->next = pt->blocks;
		pt->blocks = b;
		lfs = buffer_append_lfs(b, len);
	} else
		lfs = buffer_append_lfs(pt->blocks, len);
	char *inserted_data = pt->blocks->data + pt->blocks->size - len;
	size_t lf = pt->blocks->lf_size - lfs;
	struct pnode new = {
		.info = { .bytes = len, .lfs = lfs },
		.data = inserted_data,
		.buf = pt->blocks,
		.lf = lf
	};

	struct location loc = pt_get_piece(pt, pos);
	struct pnode *old = loc.piece;
	if(loc.off == old->info.bytes) {
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
				}, // bytes = 0 means no change
				.is_clone = false,
				.is_undone = false
			};
		}
		pt_insert_piece(pt, new, old + 1);
	} else {
		size_t lfs_on_left = count_lfs(old, piece_offset(pt->blocks, old), loc.off);
		struct pnode new_left = {
			.info = { .bytes = loc.off, .lfs = lfs_on_left },
			.data = old->data,
			.buf = old->buf,
			.lf = old->lf
		};
		struct pnode new_right = {
			.info = {
				.bytes = old->info.bytes - loc.off,
				.lfs = old->info.lfs - lfs_on_left
			},
			.data = old->data + loc.off,
			.buf = old->buf,
			.lf = old->lf + lfs_on_left
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
				}, // bytes = 0 means no change
				.is_clone = false,
				.is_undone = false
			};
		}
		size_t index = old - pt->tree;
		if(pt->tree_size + 2 > pt->tree_capacity) {
			pt->tree_capacity = pt->tree_size * 2;
			pt->tree = realloc(pt->tree, pt->tree_capacity * sizeof(struct pnode));
		}
		size_t count = pt->tree_size - index;
		pt_nodes_moved += count;
		memmove(&pt->tree[index+2], &pt->tree[index], count * sizeof(struct pnode));
		pt->tree[index] = new_left;
		pt->tree[index+1] = new;
		pt->tree[index+2] = new_right;
		pt->tree_size += 2;
	}
	pt->size += len;
	pt->lfs += lfs;
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
	printf("┃piece with %7zd bytes ┃ %7zd lfs ┃ data: %5.*s...┃\n",
			node->info.bytes, node->info.lfs, (int)min(5, node->info.bytes), node->data);
}

void pt_print_tree(PieceTable *pt)
{
	printf("PieceTable with %zd/%zd nodes\n", pt->tree_size, pt->tree_capacity);
	puts("┏━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━┓");
	for(size_t i = 0; i < pt->tree_size; i++) {
		pt_print_node(&pt->tree[i]);
	}
	puts("┗━━━━━━━━━━━━━━━━━━━━━━━━━┻━━━━━━━━━━━━━┻━━━━━━━━━━━━━━━┛");
}

void pt_print_struct_sizes()
{
	printf(
		"sizeof(struct pnode): %zd, "
		"sizeof(struct block): %zd, "
		"sizeof(struct undo): %zd, "
		"sizeof(PieceTable): %zd\n",
		sizeof(struct pnode),
		sizeof(struct block),
		sizeof(struct undo),
		sizeof(PieceTable)
	);
}
