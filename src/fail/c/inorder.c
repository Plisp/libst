//
// inorder layout (broken, after attempted iterative packing during rebalancing)
// - we cannot allocate a new level without rebuilding the entire tree
// - packing is unnecessarily complex, as we must special-case the inserted node
//

#include <assert.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>

#include "pt.h"

#ifdef NDEBUG
	#define NODENULL(p) ((p)->bytes == 0)
#else // n.b. multiple evaluation
	#define NODENULL(p) ((p)->bytes == 0 && (p)->left.bytes == 0)
#endif

#define ALPHA 0.58

#ifdef __GNUC__
	#define CTZ(i) __builtin_ctzl(i)
	#define FFS(i) __builtin_ffs(i)
#else
	#define CTZ(i) assert(0) // TODO
	#define FFS(i) assert(0) // TODO
#endif

struct text_info {
	long bytes, lfs;
};

struct pnode {
	long bytes;             /* length */
	struct text_info left; /* stats on left subtree */
	char *data;           /* pointer into corresponding block */
};

#define PT_BLKSIZE (1<<20) // must be big as we linear search through block list
enum blktype { HEAP, MMAP };

struct block {
	enum blktype type;
	long size, capacity;
	char *data;
	long lf_size, lf_capacity;
	//TODO track lfs
	long *lfs;
	struct block *next;
};

struct piecetable {
	struct pnode *tree;           /* an inorder array of nodes */
	long tree_capacity;          /* capacity of tree array */
	long node_count, max_nodes; /* needed for deletion */
	struct block *blocks;      /* TODO use array? list of text blocks */
	long size, lfs;           /* byte count, linefeed count */
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

long pt_lfs(PieceTable *pt) {
	return pt->lfs;
}

/* constructors */

static long block_count_lfs(struct block *b, long len)
{
	long count = 0;
	for(long i = b->size - len; i < b->size; i++)
		if(b->data[i] == '\n')
			count++;
	return count;
}

static struct block *block_append(struct block *b, char *data, long len, long *lfs)
{
	if(b->size + len <= b->capacity) { // should never hold for MMAP blocks
		assert(b->type == HEAP);
		memcpy(b->data + b->size, data, len);
		b->size += len;
		*lfs = block_count_lfs(b, len);
		return NULL;
	} else {
		struct block *new = malloc(sizeof *new);
		new->type = HEAP;
		new->size = len;
		new->capacity = MAX(PT_BLKSIZE, len);
		new->data = malloc(new->capacity);
		memcpy(new->data, data, len);
		pt_dbg("new block of size %ld allocated\n", new->capacity);
		*lfs = block_count_lfs(b, len);
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
	pt->tree_capacity = 2;
	pt->node_count = pt->max_nodes = 1;
	pt->tree = malloc(2 * sizeof(struct pnode));

	struct block *init = pt->blocks;
	init->next = NULL;
	init->type = HEAP;
	init->size = init->capacity = len;
	init->data = malloc(len);
	memcpy(init->data, data, len);

	pt->size = len;
	pt->lfs = block_count_lfs(init, len);
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
	pt->tree_capacity = 2;
	pt->node_count = pt->max_nodes = 1;
	pt->tree = malloc(2 * sizeof(struct pnode));

	int fd = open(path, O_RDONLY);
	if(!fd)
		goto fail;
	// mmap manpage asserts len > 0
	len = len ? len : lseek(fd, off, SEEK_END);
	void *data = mmap(NULL, len, PROT_READ, MAP_SHARED, fd, off);
	// TODO fall back to reading the file
	close(fd);
	if(data == MAP_FAILED)
		goto fail;
	struct block *init = pt->blocks;
	init->next = NULL;
	init->type = MMAP;
	init->size = init->capacity = len;
	init->data = data;

	pt->size = len;
	pt->lfs = block_count_lfs(init, len);
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

long alpha_height(long n) {
	assert(n > 0);
	return log2(n)/log2(1/ALPHA);
}

struct pnode *pt_search(PieceTable *pt, long pos, long *offset)
{
	assert(pos > 0);
	struct pnode *base = pt->tree;
	long n = pt->tree_capacity;
	while(n > 1) {
		//pt_print_node(base);
		long half = n/2;
		struct pnode *mid = base + half;
		long sum = mid->left.bytes + mid->bytes;
		//pt_dbg("pos: %ld, n = %ld, sum %ld\n", pos, n, sum);
		if(pos > sum) {
			base = mid;
			pos -= sum;
		} else if(pos > mid->left.bytes) { // <= sum
			*offset = pos - mid->left.bytes;
			return mid;
		}
		n -= half;
	}
	pt_dbg("pos: %ld\n", pos);
	//base++;
	*offset = pos;
	return base;
}

long tree_size(struct pnode *tree, long index, long level)
{
	if(level < 0 || NODENULL(&tree[index]))
		return 0;
	else {
		level--;
		return 1
			+ tree_size(tree, index - 1<<level, level)
			+ tree_size(tree, index + 1<<level, level);
	}
}

struct goat {
	long index, size;
};

struct goat scapegoat(PieceTable *pt, long index, long level, long size)
{
	long off = 1 << level;
	long on = 1 << level+1;
	long parent = index & ~off | on;
	long sibling = (index < parent) ? index + on : index - on;
	pt_dbg("index: %ld, sibling at: %ld of size %ld\n", index, sibling,
			tree_size(pt->tree, sibling, level));
	size += 1 + tree_size(pt->tree, sibling, level);
	level++;
	pt_dbg("parent %ld at level: %ld of size: %ld\n\n", parent, level, size);
	if(level+1 > alpha_height(size)) // parent size, level indexed from 0 = new
		return (struct goat) { parent, size };
	else {
		assert(level <= CTZ(pt->tree_capacity)-1); // there must be a scapegoat
		return scapegoat(pt, parent, level, size);
	}
}

void update_metadata(PieceTable *pt, long index, long bytes, long lfs)
{
	long level = CTZ(index);
	while(level < (CTZ(pt->tree_capacity)-1)){
		long off = 1 << level;
		long on = 1 << level+1;
		long parent = index & ~off | on;
		if(index < parent) {
			pt->tree[parent].left.bytes += bytes;
			pt->tree[parent].left.lfs += lfs;
		}
		index = parent;
		level++;
	}
}

long rebuild(struct pnode *root, long index, long level, struct pnode *nodes, long n)
{
	if(n > 0) {
		long mid = n/2;
		level--;
		long lbytes = rebuild(root, index - (1<<level), level, nodes, mid);
		pt_dbg("overwriting %ld with:\n", index);
		pt_print_node(&nodes[mid]);
		root[index] = nodes[mid];
		nodes[mid].bytes = 0;
//#ifndef NDEBUG
		nodes[mid].left.bytes = 0;
//#endif
		root[index].left.bytes = lbytes;
		long rbytes = rebuild(
				root, index + (1<<level), level, nodes + mid + 1, n - mid - 1
		);
		return lbytes + root[index].bytes + rbytes;
	} else
		return 0; // TODO
}

// no simple way to do this, likely very bad for branch prediction
struct pnode *pack_level(
	PieceTable *pt, long index, long count, struct pnode *new, long new_index
) {
	long level = CTZ(index);
	pt_print_tree(pt);
	pt_dbg("%ld packing subtree with %ld nodes rooted at index: %ld, level %ld\n",
			new_index, count, index, level);
	long tail_index = index + (1<<level) - 1;
	struct pnode *tail = pt->tree + tail_index;
	pt_dbg("tail at: %zd\n", tail_index);

	long i;
	for(i = tail_index; i > new_index; i--) {
		struct pnode *p = &pt->tree[i];
		if(!NODENULL(p) && p != tail) {
			*tail-- = *p;
			p->bytes = 0;
#ifndef NDEBUG
			p->left.bytes = 0;
#endif
			
		}
		tail--;
	}
	*tail = *new;
	tail--;
	for(i = tail_index; i > new_index; i--) {
		struct pnode *p = &pt->tree[i];
		if(!NODENULL(p) && p != tail) {
			*tail-- = *p;
			p->bytes = 0;
#ifndef NDEBUG
			p->left.bytes = 0;
#endif
			
		}
		tail--;
	}
	return tail + 1;
}

// takes the index which new is to be inserted after
void rebalance_insert(PieceTable *pt, struct pnode *new, long index)
{
	pt_dbg("searching for scapegoat from index: %ld\n", index);
	struct goat g = scapegoat(pt, index, 0, 2);
	pt_dbg("scapegoat of size %ld found at %ld\n", g.size, g.index);
	// update now since g.index may contain something else later
	update_metadata(pt, g.index, new->bytes, 0); // TODO lfs
	// pack into array
	struct pnode *start = pack_level(pt, g.index, g.size, new, index);
	pt_print_tree(pt);
	rebuild(pt->tree, g.index, CTZ(g.index), start, g.size);
	//pt_nodes_moved += g.size;
}

void insert0(PieceTable *pt, struct pnode *new)
{
	// TODO handle 0 nodes
	struct pnode *node;
	long i = pt->tree_capacity/2;
	long level = CTZ(i);
	for(;;) {
		if(level < 0) {
			pt_dbg("out of bounds index: %ld, bound: %ld\n", i, pt->tree_capacity);
			return rebalance_insert(pt, new, 0);
		}
		node = &pt->tree[i];
		if(NODENULL(node)) {
			*node = *new;
			break;
		}
		level--;
		i /= 2;
	}
	long lf_delta = 0; // TODO
	update_metadata(pt, i, node->bytes, lf_delta);
	pt->node_count++;
	pt->max_nodes++;
}

void insert_node_after(PieceTable *pt, struct pnode *new, long index)
{
	pt_dbg("inserting after tree index %ld\n", index);
	long old_alpha = alpha_height(pt->node_count);
	pt->node_count++;
	pt->max_nodes++;
	long new_alpha = alpha_height(pt->node_count);
	pt_dbg("old alpha: %ld\n", old_alpha);
	pt_dbg("new node count: %ld\n", pt->node_count);
	pt_dbg("new alpha: %ld\n", new_alpha);
	// capacity = 2^(levels)-1
	// loosely-height-balanced -> height-balanced
	// height-balanced -> height-balanced (reallocate)
	if(old_alpha != new_alpha && CTZ(pt->tree_capacity)-1 == old_alpha) {
		long new_capacity = 1 << new_alpha+1;
		pt_dbg("reallocating nodes array to size %ld\n", new_capacity);
		pt->tree = realloc(pt->tree, new_capacity * sizeof(struct pnode));
		long zero_bytes = (new_capacity - pt->tree_capacity) * sizeof(struct pnode);
		memset(pt->tree + pt->tree_capacity, 0, zero_bytes);
		pt->tree_capacity = new_capacity;
		// rebuild the whole tree
		long root = pt->tree_capacity/2;
		// pack to right side
		struct pnode *start = pack_level(pt, root, pt->node_count, new, index);
		pt_dbg("tail index: %ld\n", start - pt->tree);
		rebuild(pt->tree, root, CTZ(root), start, pt->node_count);
		//pt_nodes_moved += pt->node_count;
		return;
	}
	struct pnode *node;
	long level = CTZ(index);
	if(level == 0) {
		pt_dbg("out of bounds index: %ld\n", index);
		return rebalance_insert(pt, new, index);
	}
	level--;
	index += 1 << level;
	for(;;) {
		pt_dbg("index: %ld, level: %ld\n", index, level);
		node = &pt->tree[index];
		if(NODENULL(node)) {
			*node = *new;
			break;
		}
		if(level == 0) {
			pt_dbg("out of bounds index: %ld\n", index);
			return rebalance_insert(pt, new, index);
		}
		level--;
		index -= 1 << level;
	}
	long lf_delta = 0; // TODO
	update_metadata(pt, index, new->bytes, lf_delta);
}

bool pt_insert(PieceTable *pt, long pos, char *data, long len, struct undo *undo)
{
	if(pos < 0 || pos > pt->size || len <= 0)
		return false;
	pt_dbg("inserting \"%.*s\" at %ld\n", (int)len, data, pos);
	long lfs;
	struct block *b = block_append(pt->blocks, data, len, &lfs);
	if(b) {
		b->next = pt->blocks;
		pt->blocks = b;
	}
	char *inserted_data = pt->blocks->data + pt->blocks->size - len;
	struct pnode new = { .bytes = len, .data = inserted_data };

	long off, lf_off = 0; // TODO
	if(pos > 0) {
		// TODO undo
		struct pnode *node = pt_search(pt, pos, &off);
		pt_print_node(node);
		pt_dbg("offset %ld\n", off);
		if(off == node->bytes) {
			insert_node_after(pt, &new, node - pt->tree);
		} else {
			struct pnode old_left = { .bytes = off, .data = node->data, };
			struct pnode old_right = {
				.bytes = node->bytes - off,
				.data = node->data + off,
			};
			// overwrite node with old_right
			old_left.left.bytes = node->left.bytes;
			*node = old_left;
			update_metadata(pt, node - pt->tree, -old_right.bytes, lf_off);
			insert_node_after(pt, &new, node - pt->tree);
			// insert TODO can optimize search - return index from insert_after
			struct pnode *prev = pt_search(pt, pos + new.bytes, &off);
			pt_dbg("offset is %ld, index: %zd\n", off, prev - pt->tree);
			assert(off == prev->bytes);
			insert_node_after(pt, &old_right, prev - pt->tree);
		}
	} else {
		insert0(pt, &new);
	}
	pt->size += len;
	pt->lfs += lfs;
	return true;
}

/*
void delete_node(PieceTable *pt, struct pnode node, long pos)
{
	//
	long tree_height = alpha_height(pt->node_count);
	pt_dbg("height: %ld, size needed: %d\n", tree_height, 1<<(tree_height+1));
	;
}

bool pt_erase(PieceTable *pt, long pos, long len, struct undo *undo)
{
	;
}
*/

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
	printf("┃ %3ld,%3ld bytes ┃ %3ld,%3ld lfs ┃ data: %5.*s...┃\n",
			node->left.bytes, node->bytes,
			node->left.lfs, count_lfs(node, 0, node->bytes),
			(int)MIN(5, node->bytes), node->data);
}

void pt_print_tree(PieceTable *pt)
{
	printf("PieceTable with %ld/%ld nodes\n", pt->node_count, pt->tree_capacity);
	puts("┏━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━┓");
	for(long i = 0; i < pt->tree_capacity; i++) {
		pt_print_node(&pt->tree[i]);
	}
	puts("┗━━━━━━━━━━━━━━━┻━━━━━━━━━━━━━┻━━━━━━━━━━━━━━━┛");
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
	struct block init = {
		.type = HEAP,
		.size = 9,
		.capacity = 9,
		.data = "/@movecat",
	};
#if 0
	struct pnode model[3] = {
		{ .bytes = 3, .left = { 0 }, .data = init.data + 6 },
		{ .bytes = 4, .left = { 3 }, .data = init.data + 2 },
		{ .bytes = 0, .left = { 0 }, .data = init.data + 0 },
	};
	struct pnode *tree = malloc(3 * sizeof(struct pnode));
	memcpy(tree, &model, 3 * sizeof(struct pnode));
	printf("contents: %.*s\n", (int)tree[0].bytes, tree[0].data);
	printf("contents: %.*s\n", (int)tree[1].bytes, tree[1].data);
	printf("contents: %.*s\n", (int)tree[2].bytes, tree[2].data);
	PieceTable *pt = &(struct piecetable) {
		.size = 7,
		.blocks = &init,
		.node_count = 2,
		.max_nodes = 2,
		.tree_capacity = 3,
		.tree = tree
	};
	/*
	long tmp;
	for(long i = 1; i <= pt_size(pt); i++) {
		pt_dbg("searching for %ld\n", i);
		struct pnode *res = pt_search(pt, i, &tmp);
		pt_dbg("offset: %ld in\n", tmp);
		pt_print_node(res);
	}
	*/
	pt_insert(pt, 3, "st", 2, NULL);
	pt_print_tree(pt);
#endif
#if 1
	/*
#define S 7
	struct pnode model[S] = {
		{ .bytes = 4, .left = { 2 }, .data = init.data + 2 },
		{ .bytes = 2, .left = { 0 }, .data = init.data + 0 },
	};
	struct pnode *tree = malloc(S * sizeof(struct pnode));
	memcpy(tree, &model, S * sizeof(struct pnode));
	printf("contents: %.*s\n", (int)tree[0].bytes, tree[0].data);
	printf("contents: %.*s\n", (int)tree[1].bytes, tree[1].data);
	PieceTable *pt = &(struct piecetable) {
		.size = 6,
		.blocks = &init,
		.node_count = 2,
		.max_nodes = 2,
		.tree_capacity = S,
		.tree = tree
	};
	*/
	//PieceTable *pt =pt_new_from_file("/home/plisp/common-lisp/misc-vico/test.xml",0,0);
	PieceTable *pt = pt_new_from_file("/home/plisp/common-lisp/misc-vico/terms",0,0);
#define NINSERTS 10
	for(int i = 1; i <= NINSERTS; i++) {
		pt_insert(pt, i*6, "thang", 5, NULL);
		pt_print_tree(pt);
	}
	printf("nodes moved: %ld, nodes: %ld/%ld\n", 0 /*pt_nodes_moved*/,
			pt->node_count, pt->tree_capacity);
	//sleep(10);
	pt_free(pt);
#endif
#if 0
	struct pnode tree[3] = {
		{ .bytes = 4, .left = { 2 }, .data = init.data + 2 },
		{ .bytes = 2, .left = { 0 }, .data = init.data + 0 },
		{ .bytes = 3, .left = { 0 }, .data = init.data + 6 },
	};
	printf("contents: %.*s\n", (int)tree[0].bytes, tree[0].data);
	printf("contents: %.*s\n", (int)tree[1].bytes, tree[1].data);
	printf("contents: %.*s\n", (int)tree[2].bytes, tree[2].data);
	PieceTable *pt = &(struct piecetable) {
		.size = 9,
		.blocks = &init,
		.node_count = 3,
		.max_nodes = 3,
		.tree_capacity = 3,
		.tree = tree
	//pt_insert(pt, 6, "test", 4, NULL);
	//pt_print_tree(pt);
#endif
}
