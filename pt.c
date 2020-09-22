#include "pt.h"

#ifndef NDEBUG
size_t pt_nodes_moved = 0;
#endif

/* constructors */

void pt_print_lfs(struct text_buffer *b)
{
	for(size_t i = 0; i < b->lf_size; i++)
		printf("%zd\n", b->lfs[i]);
}

static size_t buffer_append_lfs(struct text_buffer *b, size_t len)
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

static struct text_buffer *buffer_append(struct text_buffer *b, char *data, size_t len)
{
	if (b->size + len <= b->capacity) { // should never hold for MMAP buffers
		assert(b->type == HEAP);
		memcpy(b->data + b->size, data, len);
		b->size += len;
		return NULL;
	} else {
		struct text_buffer *new = malloc(sizeof *new);
		new->type = HEAP;
		new->size = len;
		new->capacity = max(PT_BUFSIZE, len);
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
	struct text_buffer *buf = node->buf;
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
	pt->buffers = malloc(sizeof(struct text_buffer));
	pt->tree_size = pt->tree_capacity = 2;
	pt->tree = malloc(2 * sizeof(struct pnode));

	struct text_buffer *init = pt->buffers;
	init->next = NULL;
	init->type = HEAP;
	init->size = init->capacity = len;
	init->data = malloc(len);
	memcpy(init->data, data, len);
	init->lf_size = init->lf_capacity = 0;
	init->lfs = NULL;
	size_t lfs = buffer_append_lfs(init, len);

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
	pt->buffers = malloc(sizeof(struct text_buffer));
	pt->tree_size = pt->tree_capacity = 2;
	pt->tree = malloc(2 * sizeof(struct pnode));

	int fd = open(path, O_RDONLY);
	if(!fd)
		goto fail;
	len = len ? len : lseek(fd, off, SEEK_END);
	void *data = mmap(NULL, len, PROT_READ, MAP_SHARED, fd, off);
	close(fd);
	if(data == MAP_FAILED)
		goto fail;
	struct text_buffer *init = pt->buffers;
	init->next = NULL;
	init->type = MMAP;
	init->size = init->capacity = len;
	init->data = data;
	init->lf_size = init->lf_capacity = 0;
	init->lfs = NULL;
	size_t lfs = buffer_append_lfs(init, len);

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
	free(pt->buffers);
	free(pt);
	return NULL;
}

static void free_buffer(struct text_buffer *b)
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
	for(struct text_buffer *b = pt->buffers, *next; b; b = next) {
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
	memmove(at + 1, at, count * sizeof(struct pnode));
	pt->tree_size++;
	*at = new;
}

void pt_delete_piece(PieceTable *pt, struct pnode *at)
{
	size_t count = pt->tree + pt->tree_size - at - 1;
	memmove(at, at + 1, count * sizeof(struct pnode));
	pt->tree_size--;
}

size_t piece_offset(struct text_buffer *buffers, struct pnode *node)
{
	for(struct text_buffer *b = buffers; b; b = b->next) {
		if(b->data <= node->data && node->data < b->data + b->size)
			return node->data - b->data;
	}
	assert(0); // this is bad, real bad
}

bool pt_insert(PieceTable *pt, size_t pos, char *data, size_t len, struct undo *undo)
{
	size_t lfs;
	struct text_buffer *b = buffer_append(pt->buffers, data, len);
	if(b) {
		b->next = pt->buffers;
		pt->buffers = b;
		lfs = buffer_append_lfs(b, len);
	} else
		lfs = buffer_append_lfs(pt->buffers, len);
	char *inserted_data = pt->buffers->data + pt->buffers->size - len;
	size_t lf = pt->buffers->lf_size - lfs;
	struct pnode new = {
		.info = { .bytes = len, .lfs = lfs },
		.data = inserted_data,
		.buf = pt->buffers,
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
		size_t lfs_on_left = count_lfs(old, piece_offset(pt->buffers, old), loc.off);
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
		memmove(&pt->tree[index+2], &pt->tree[index],
				(pt->tree_size - index) * sizeof(struct pnode));
		pt->tree[index] = new_left;
		pt->tree[index+1] = new;
		pt->tree[index+2] = new_right;
		pt->tree_size += 2;
		/*
		pt_delete_piece(pt, old);
		pt_insert_piece(pt, new_right, pt->tree + index);
		pt_insert_piece(pt, new, pt->tree + index);
		pt_insert_piece(pt, new_left, pt->tree + index);
		*/
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

/* debug printing */

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
