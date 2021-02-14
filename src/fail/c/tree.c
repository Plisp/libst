//
// eytzinger (breadth-first) layout - working, but memory overhead high
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

long pt_nodes_moved = 0, rebalances;

#ifdef NDEBUG
	#define NODENULL(p) ((p)->bytes == 0)
#else // n.b. multiple evaluation
	#define NODENULL(p) ((p)->bytes == 0 && (p)->lbytes == 0)
#endif

#define ALPHA 0.56

#ifdef __GNUC__
	#define CTZ(i) __builtin_ctzl(i)
	#define FFS(i) __builtin_ffs(i)
#else
	#define CTZ(i) assert(0) // TODO
	#define FFS(i) assert(0) // TODO
#endif

struct pnode {
	long bytes, lbytes; /* length of text and left subtree text */
	char *data;        /* pointer into corresponding block */
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
	struct block *blocks;      /* list of text blocks TODO use array? */
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
	pt->tree_capacity = 1;
	pt->node_count = pt->max_nodes = 1;
	pt->tree = malloc(1 * sizeof(struct pnode));

	struct block *init = pt->blocks;
	init->next = NULL;
	init->type = HEAP;
	init->size = init->capacity = len;
	init->data = malloc(len);
	memcpy(init->data, data, len);

	pt->size = len;
	pt->lfs = block_count_lfs(init, len);
	pt->tree[0] = (struct pnode) {
		.bytes = len,
		.data = init->data,
	};
	return pt;
}

PieceTable *pt_new_from_file(const char *path, long len, long off)
{
	PieceTable *pt = malloc(sizeof *pt);
	pt->blocks = malloc(sizeof(struct block));
	pt->tree_capacity = 1;
	pt->node_count = pt->max_nodes = 1;
	pt->tree = malloc(1 * sizeof(struct pnode));

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
	pt->tree[0] = (struct pnode) {
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

//long log2n(long n) { return 8*sizeof(long) - CLZ(n) - 1; }

long alpha_height(long n) {
	assert(n > 0);
	return log2(n)/log2(1/ALPHA);
}

struct pnode *pt_search(PieceTable *pt, long pos, long *offset)
{
	assert(pos > 0);
	pt_dbg("searching for %ld\n", pos);
	long n = pt->tree_capacity;
	long i = 0;
	while(i < n) {
		struct pnode *node = &pt->tree[i];
		long sum = node->lbytes + node->bytes;
		// sum == 0 for null nodes, so this works for all pos > 0
		i = (pos <= sum) ? 2*i+1 : 2*i+2;
		pos = (pos <= sum) ? pos : pos - sum;
	}
	long idx = (i + 1) >> FFS(~(i + 1));
	idx = (idx == 0) ? n : idx-1;
	*offset = pos;
	return pt->tree + idx;
}

void update_metadata(struct pnode *tree, long index, long bytes, long lfs)
{
	char odd = index & 1;
	long parent = (index - 1 - !odd)/2;
	if(parent < 0)
		return;
	if(odd) {
		tree[parent].lbytes += bytes;
	}
	update_metadata(tree, parent, bytes, lfs);
}

long tree_size(struct pnode *tree, long index, long height)
{
	if(height == 0 || NODENULL(&tree[index]))
		return 0;
	else {
		return 1
			+ tree_size(tree, 2*index + 1, height - 1)
			+ tree_size(tree, 2*index + 2, height - 1);
	}
}

struct goat {
	long index, size;
};

struct goat scapegoat(struct pnode *tree, long index, long height, long size)
{
	char odd = index & 1;
	index = (index - !odd - 1)/2;
	assert(index >= 0); // there must be a scapegoat
	size += 1 + tree_size(tree, index*2 + 1 + odd, height);
	pt_dbg("height: %ld, size: %ld\n", height, size);
	if(height > alpha_height(size))
		return (struct goat) { index, size };
	else
		return scapegoat(tree, index, height + 1, size);
}

long rebuild_recurse(struct pnode *root, long index, struct pnode *nodes, long n)
{
	if(n > 0) {
		// n.b. order is important for access locality
		long mid = n/2;
		long lbytes = rebuild_recurse(root, 2*index + 1, nodes, mid);
		root[index] = nodes[mid];
		root[index].lbytes = lbytes;
		long rbytes = rebuild_recurse(root, 2*index + 2, nodes + mid + 1, n - mid - 1);
		return lbytes + root[index].bytes + rbytes;
	} else
		return 0; // TODO
}

long pack_array(PieceTable *pt, long index, struct pnode *dest, long dest_index)
{
	if(index > pt->tree_capacity || NODENULL(&pt->tree[index]))
		return dest_index;
	dest_index = pack_array(pt, 2*index + 1, dest, dest_index);
	dest[dest_index++] = pt->tree[index];
	pt->tree[index].bytes = 0;
	pt->tree[index].lbytes = 0;
	return pack_array(pt, 2*index + 2, dest, dest_index);
}

// takes the index at which new was inserted
void rebalance_insert(PieceTable *pt, struct pnode *new, long index)
{
	pt_dbg("index:%ld\n", index);
	struct goat g = scapegoat(pt->tree, index, 1, 1);
	// update now since g.index may contain something else later
	update_metadata(pt->tree, g.index, new->bytes, 0); // TODO lfs
	pt_nodes_moved += g.size;
	rebalances++;
	pt_dbg("scapegoat of size %ld found at %ld\n", g.size, g.index);
	// this was tested to be faster without keeping the temporary buffer around
	struct pnode *tmp = malloc(g.size * sizeof(struct pnode));
	pack_array(pt, g.index, tmp, 0);
	rebuild_recurse(pt->tree, g.index, tmp, g.size);
	free(tmp);
}

void insert0(PieceTable *pt, struct pnode *new)
{
	long old_alpha = alpha_height(pt->node_count);
	pt->node_count++;
	pt->max_nodes++;
	long new_alpha = alpha_height(pt->node_count);
	if(old_alpha != new_alpha) {
		pt_dbg("new node count: %ld\n", pt->node_count);
		pt_dbg("new alpha: %ld\n", new_alpha);
		long new_capacity = (1 << new_alpha+2) - 1; // +1 for levels, +1 extra
		pt->tree = realloc(pt->tree, new_capacity * sizeof(struct pnode));
		long zero_bytes = (new_capacity - pt->tree_capacity) * sizeof(struct pnode);
		memset(pt->tree + pt->tree_capacity, 0, zero_bytes);
		pt->tree_capacity = new_capacity;
	}
	// TODO handle pt containing 0 nodes
	struct pnode *node;
	long i = 0;
	for(;;) {
		assert(i < pt->tree_capacity);
		if((long)log2(i+1) > new_alpha) {
			pt_dbg("out of bounds index: %ld, bound: %ld\n", i, pt->tree_capacity);
			pt->tree[i] = *new;
			return rebalance_insert(pt, new, i);
		}
		node = &pt->tree[i];
		if(NODENULL(node)) {
			*node = *new;
			break;
		}
		node->lbytes += new->bytes;
	    //long lf_delta = 0; // TODO
		i = 2*i + 1;
	}
}

void insert_node_after(PieceTable *pt, struct pnode *new, long index)
{
	pt_dbg("inserting after index %ld\n", index);
	long old_alpha = alpha_height(pt->node_count);
	pt->node_count++;
	pt->max_nodes++;
	long new_alpha = alpha_height(pt->node_count);
	if(old_alpha != new_alpha) {
		pt_dbg("new node count: %ld\n", pt->node_count);
		pt_dbg("new alpha: %ld\n", new_alpha);
		long new_capacity = (1 << new_alpha+2) - 1; // +1 for levels, +1 extra
		pt->tree = realloc(pt->tree, new_capacity * sizeof(struct pnode));
		long zero_bytes = (new_capacity - pt->tree_capacity) * sizeof(struct pnode);
		memset(pt->tree + pt->tree_capacity, 0, zero_bytes);
		pt->tree_capacity = new_capacity;
	}
	struct pnode *node;
	index = index*2 + 2;
	for(;;) {
		pt_dbg("index %ld\n", index);
		assert(index < pt->tree_capacity);
		if((long)log2(index+1) > new_alpha) {
			pt_dbg("out of bounds index: %ld, bound: %ld\n", index, pt->tree_capacity);
			pt->tree[index] = *new;
			return rebalance_insert(pt, new, index);
		}
		node = &pt->tree[index];
		if(NODENULL(node)) {
			*node = *new;
			break;
		}
		node->lbytes += new->bytes;
		//long lf_delta = 0; // TODO
		index = index*2 + 1;
	}
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
		if(off == node->bytes) {
			insert_node_after(pt, &new, node - pt->tree);
		} else {
			struct pnode old_left = { 
				.bytes = off, 
				.data = node->data,
				.lbytes = node->lbytes,
			};
			struct pnode old_right = {
				.bytes = node->bytes - off,
				.data = node->data + off,
			};
			// overwrite node with old_right
			*node = old_left;
			update_metadata(pt->tree, node - pt->tree, -old_right.bytes, lf_off);
			insert_node_after(pt, &new, node - pt->tree);
			// insert
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
	printf("┃ %3ld,%3ld bytes ┃ %3ld lfs ┃ data: %5.*s...┃",
			node->lbytes, node->bytes,
			count_lfs(node, 0, node->bytes),
			(int)MIN(5, node->bytes), node->data);
}

void pt_print_tree(PieceTable *pt)
{
	printf("PieceTable with %ld/%ld nodes\n", pt->node_count, pt->tree_capacity);
	puts("       ┏━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━┓");
	for(long i = 0; i < pt->tree_capacity; i++) {
		printf("%6ld ", i);
		pt_print_node(&pt->tree[i]);
		puts("");
	}
	puts("       ┗━━━━━━━━━━━━━━━┻━━━━━━━━━━━━━┻━━━━━━━━━━━━━━━┛");
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
	struct pnode tree[3] = {
		{ .bytes = 4, .left = { 0 }, .data = init.data + 2 },
		{ .bytes = 0, .left = { 0 }, .data = init.data + 0 },
		{ .bytes = 3, .left = { 0 }, .data = init.data + 6 },
	};
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
	pt_insert(pt, 0, "st", 2, NULL);
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
	struct timespec before, after;
	clock_gettime(CLOCK_REALTIME_COARSE, &before);
	PieceTable *pt = pt_new_from_file("/home/plisp/common-lisp/misc-vico/test.xml",0,0);
	clock_gettime(CLOCK_REALTIME_COARSE, &after);
	printf("file load: %ld ms\n", (after.tv_nsec - before.tv_nsec)/1000000 + 
			(after.tv_sec - before.tv_sec)*1000);
#define NINSERTS 100000
	clock_gettime(CLOCK_REALTIME_COARSE, &before);
	for(int i = 1; i <= NINSERTS; i++) {
		pt_insert(pt, i*6, "thang", 5, NULL);
	}
	clock_gettime(CLOCK_REALTIME_COARSE, &after);
	printf("insertion: %ld ms\n", (after.tv_nsec - before.tv_nsec)/1000000 + 
			(after.tv_sec - before.tv_sec)*1000);
	printf("rebalances: %ld, nodes moved: %ld, nodes: %ld/%ld\n", 
			rebalances, pt_nodes_moved,
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
