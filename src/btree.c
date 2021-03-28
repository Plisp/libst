/*
 * b+tree-based piece sequence adapted from linux kernel implementation
 */

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "pt.h"

#define LOW_WATER 256
#define HIGH_WATER 4096

enum blktype { IMMUT, IMMUT_MMAP, HEAP };
struct block {
	enum blktype type;
	int refs; // only for IMMUT blocks
	char *data;
	long len;
};

struct piece {
	struct block *blk;
	unsigned long len;
};

#define NODESIZE 128
#define PER_B (sizeof(void *) + sizeof(long))
#define B (NODESIZE / PER_B)
struct inner {
	long bytes[B];
	struct inner *children[B];
};

#define PER_BL (sizeof(struct piece) + sizeof(long))
#define BL (NODESIZE / PER_BL)
struct leaf {
	long spans[BL];
	struct piece pieces[BL];
};

struct PieceTable {
	struct inner *root;
	unsigned height;
};

static struct inner *btree_node_alloc(unsigned height)
{
	if(height == 1) {
		struct leaf *leaf = malloc(sizeof *leaf);
		memset(leaf, 0, sizeof(struct leaf));
		return (struct inner *)leaf;
	} else {
		struct inner *node = malloc(sizeof *node);
		memset(node, 0, sizeof(struct inner));
		return node;
	}
}

static unsigned long bkey(unsigned level, struct inner *node, unsigned n)
{
	if(level == 1) {
		struct leaf *leaf = (struct leaf *)node;
		return leaf->spans[n];
	} else
		return node->bytes[n];
}

static void setkey(unsigned level, struct inner *node, unsigned n, long val)
{
	if(level == 1) {
		struct leaf *leaf = (struct leaf *)node;
		assert(n < BL);
		leaf->spans[n] = val;
	} else
		node->bytes[n] = val;
}

static void *bval(unsigned level, struct inner *node, unsigned n)
{
	if(level == 1) {
		struct leaf *leaf = (struct leaf *)node;
		assert(n < BL);
		struct piece *piece = &leaf->pieces[n];
		// N.B. returns NULL on absent value
		return piece->len > 0 ? piece : NULL;
	} else
		return node->children[n];
}

static void setval(unsigned level, struct inner *node, unsigned n, void *val)
{
	if(level == 1) {
		struct leaf *leaf = (struct leaf *)node;
		assert(n < BL);
		leaf->pieces[n] = *(struct piece *)val;
	} else
		node->children[n] = val;
}

static void clearpair(unsigned level, struct inner *node, unsigned n)
{
	if(level == 1) {
		struct leaf *leaf = (struct leaf *)node;
		assert(n < BL);
		leaf->spans[n] = 0;
		leaf->pieces[n] = (struct piece){ 0 };
	} else {
		node->bytes[n] = 0;
		node->children[n] = 0;
	}
}

/* debug */

void pt_print_struct_sizes(void)
{
	fprintf(stderr,
		"Implementation: \e[38;5;1mbtree\e[0m\n"
		"sizeof(struct inner): %zd\n"
		"sizeof(struct leaf): %zd\n"
		"sizeof(PieceTable): %zd\n",
		sizeof(struct inner), sizeof(struct leaf), sizeof(PieceTable));
}

static void print_node(unsigned level, struct inner *node)
{
	char out[256], *it = out;
	if(level == 1) {
		it += sprintf(it, "[k: ");
		for(unsigned i = 0; i < BL; i++)
			it += sprintf(it, "%lu|", bkey(1, node, i));
		it--;
		it += sprintf(it, " p: ");
		for(unsigned i = 0; i < BL; i++) {
			struct piece *val = bval(1, node, i);
			it += sprintf(it, "%lu|", val ? val->len : 0);
		}
		it--;
		it += sprintf(it, "]");
	} else {
		printf("[k: ");
		for(unsigned i = 0; i < B; i++)
			it += sprintf(it, "%lu|", bkey(level, node, i));
		it += sprintf(it, "]");
	}
	fprintf(stderr, "%s\n", out);
}

// TODO this probably won't work well
void pt_pprint(PieceTable *pt)
{
	struct inner *root = pt->root;
	print_node(pt->height, root);
}

#include "dot.h"

#define FSTR(dest, str, ...)                                                   \
	do {                                                                   \
		int len = snprintf(NULL, 0, str, __VA_ARGS__);                 \
		dest = realloc(dest, len + 1);                                 \
		sprintf(dest, str, __VA_ARGS__);                               \
	} while(0)

void piece_to_dot(FILE *file, const struct piece *piece)
{
	char *tmp = NULL, *port = NULL;
	if(!piece) {
		graph_table_entry(file, NULL, NULL);
		return;
	}
	FSTR(tmp, "len: %lu", piece->len);
	FSTR(port, "%lu", (long)piece->blk);
	graph_table_entry(file, tmp, port);
	// TODO link to block
	//graph_link(file, piece, tmp, piece->blk, "body");
	free(tmp);
	free(port);
}

void leaf_to_dot(FILE *file, const struct leaf *leaf)
{
	char *tmp = NULL;
	graph_table_begin(file, leaf, "aquamarine3");
	for(unsigned i = 0; i < BL; i++) {
		FSTR(tmp, "%lu", bkey(1, (void *)leaf, i));
		graph_table_entry(file, tmp, NULL);
	}
	for(unsigned i = 0; i < BL; i++)
		piece_to_dot(file, bval(1, (void *)leaf, i));
	graph_table_end(file);
	free(tmp);
}

void inner_to_dot(FILE *file, const struct inner *root, unsigned height)
{
	if(!root)
		return;
	if(height == 1)
		return leaf_to_dot(file, (void *)root);
	char *tmp = NULL, *port = NULL;
	graph_table_begin(file, root, NULL);
	for(unsigned i = 0; i < B; i++) {
		FSTR(tmp, "%lu", bkey(height, (void *)root, i));
		FSTR(port, "%u", i);
		graph_table_entry(file, tmp, port);
	}
	graph_table_end(file);
	// output child links
	for(unsigned i = 0; i < B; i++) {
		struct inner *child = bval(height, (void *)root, i);
		if(!child)
			break;
		FSTR(tmp, "%d", i);
		graph_link(file, root, tmp, child, "body");
		inner_to_dot(file, child, height - 1);
	}
	free(tmp);
	free(port);
}

bool pt_to_dot(PieceTable *pt, const char *path)
{
	FILE *file = fopen(path, "w");
	char *tmp = NULL;
	if(!file)
		goto fail;

	graph_begin(file);
	graph_table_begin(file, pt, NULL);
	FSTR(tmp, "height: %u", pt->height);
	graph_table_entry(file, tmp, NULL);
	graph_table_entry(file, "root", "root");
	graph_table_end(file);
	graph_link(file, pt, "root", pt->root, "body");
	if(pt->root)
		inner_to_dot(file, pt->root, pt->height);
	graph_end(file);

	free(tmp);
	if(!fclose(file))
		goto fail;
	return true;

fail:
	perror("pt_to_dot");
	return false;
}

/* basic */

struct PieceTable *btree_new(void)
{
	struct PieceTable *pt = malloc(sizeof *pt);
	pt->root = NULL;
	pt->height = 0;
	return pt;
}

void btree_free(struct PieceTable *pt)
{
	// TODO
	free(pt);
}

void *btree_lookup(struct PieceTable *pt, unsigned long key)
{
	unsigned height = pt->height;
	struct inner *node = pt->root;

	if(height == 0)
		return NULL;

	for(unsigned i; height > 1; height--) {
		for(i = 0; i < B; i++)
			if(key >= bkey(height, node, i))
				break;
		if(i == B)
			return NULL;
		node = bval(height, node, i);
		if(!node)
			return NULL;
	}

	if(!node)
		return NULL;

	for(unsigned i = 0; i < BL; i++)
		if(key == bkey(1, node, i))
			return bval(1, node, i);
	return NULL;
}

#if 0
/*
 * Usually this function is quite similar to normal lookup.  But the key of
 * a parent node may be smaller than the smallest key of all its siblings.
 * In such a case we cannot just return NULL, as we have only proven that no
 * key smaller than key, but larger than this parent key exists.
 * So we set key to the parent key and retry.  We have to use the smallest
 * such parent key, which is the last parent key we encountered.
 */
void *btree_get_prev(struct PieceTable *pt, unsigned long *key)
{
	unsigned i, height;
	struct inner *node, *oldnode;
	unsigned long retry_key = 0, fkey;

	if(key == 0)
		return NULL;
	if(pt->height == 0)
		return NULL;
	fkey = *key;
retry:
	(*key)--;
	node = pt->root;
	for(height = pt->height; height > 1; height--) {
		for(i = 0; i < B; i++)
			if(fkey >= bkey(height, node, i))
				break;
		if(i == B)
			goto miss;
		oldnode = node;
		node = bval(height, node, i);
		if(!node)
			goto miss;
		retry_key = bkey(height, oldnode, i);
	}

	if(!node)
		goto miss;

	for(i = 0; i < BL; i++) {
		if(*key > bkey(1, node, i)) {
			if(bval(1, node, i)) {
				// the previous key is returned in key
				*key = bkey(1, node, i);
				return bval(1, node, i);
			} else
				goto miss;
		}
	}
miss:
	if(retry_key) {
		fkey = retry_key;
		retry_key = 0;
		goto retry;
	}
	return NULL;
}
#endif

// returns index of the first key spanning the search key
// the key is the smallest value of the corresponding span (kprev,k]
static unsigned getpos(unsigned level, struct inner *node, unsigned long key)
{
	unsigned i;
	for(i = 0; i < (level == 1 ? BL : B); i++)
		if(key >= bkey(level, node, i))
			break;
	return i;
}

// count the number of live entries starting from START as an optimization
static unsigned getfill(unsigned level, struct inner *node, int start)
{
	unsigned i;
	for(i = start; i < (level == 1 ? BL : B); i++)
		if(!bval(level, node, i))
			break;
	return i;
}

static struct inner *find_level(struct PieceTable *pt, unsigned long key,
				unsigned level)
{
	struct inner *node = pt->root;
	unsigned i;

	pt_dbg("finding level: %u for key %lu, height %u\n", level, key,
	       pt->height);
	// level >= 1, when level = 1, returned node is a leaf
	for(unsigned height = pt->height; height > level; height--) {
		assert(height > 1);
		for(i = 0; i < B; i++)
			if(key >= bkey(height, node, i))
				break;

		if((i == B) || !bval(height, node, i)) {
			/* right-most key is too large, update it */
			/* FIXME: If the right-most key on higher levels is
			 * always zero, this wouldn't be necessary. */
			i--;
			setkey(height, node, i, key);
		}
		node = bval(height, node, i);
	}
	assert(node);
	return node;
}

/* insertion */

static int btree_grow(struct PieceTable *pt)
{
	struct inner *node = btree_node_alloc(pt->height + 1);
	if(pt->root) {
		int fill = getfill(pt->height, pt->root, 0);
		setkey(pt->height + 1, node, 0,
		       bkey(pt->height, pt->root, fill - 1));
		setval(pt->height + 1, node, 0, pt->root);
	}
	pt->root = node;
	pt->height++;
	return 0;
}

static void btree_insert_level(struct PieceTable *pt, unsigned long key,
			       void *val, unsigned level)
{
	struct inner *node;
	unsigned i, pos, fill;

	if(pt->height < level) {
		btree_grow(pt);
		pt_dbg("expanding for insertng %lu\n", key);
		//pt_to_dot(pt, "p.dot");
	}

retry:
	node = find_level(pt, key, level);
	pos = getpos(level, node, key);
	fill = getfill(level, node, pos);
	pt_dbg("fill: %u at level: %u\n", fill, level);
	/* check for node split */
	if(fill == (level == 1 ? BL : B)) {
		struct inner *new = btree_node_alloc(level);
		btree_insert_level(pt, bkey(level, node, fill / 2 - 1), new,
				   level + 1);
		// copy first half to new, shift latter half down
		for(i = 0; i < fill / 2; i++) {
			setkey(level, new, i, bkey(level, node, i));
			setval(level, new, i, bval(level, node, i));
			setkey(level, node, i, bkey(level, node, i + fill / 2));
			setval(level, node, i, bval(level, node, i + fill / 2));
			clearpair(level, node, i + fill / 2);
		}
		// copy final odd entry
		if(fill & 1) {
			setkey(level, node, i, bkey(level, node, fill - 1));
			setval(level, node, i, bval(level, node, fill - 1));
			clearpair(level, node, fill - 1);
		}
		goto retry;
	}
	assert(fill < (level == 1 ? BL : B));
	/* shift and insert */
	for(i = fill; i > pos; i--) {
		setkey(level, node, i, bkey(level, node, i - 1));
		setval(level, node, i, bval(level, node, i - 1));
	}
	setkey(level, node, pos, key);
	setval(level, node, pos, val);
}

void btree_insert(struct PieceTable *pt, unsigned long key, void *val)
{
	assert(val); // required since we null empty entries instead of storing sizes
	btree_insert_level(pt, key, val, 1);
}

/* deletion */

static void btree_shrink(struct PieceTable *pt)
{
	if(pt->height <= 1)
		return;
	struct inner *node = pt->root;
	int fill = getfill(pt->height, node, 0);
	assert(fill <= 1);
	pt->root = bval(pt->height, node, 0);
	pt->height--;
	free(node);
}

static void *btree_remove_level(struct PieceTable *pt, unsigned long key,
				unsigned level);

static void merge(struct PieceTable *pt, unsigned level, struct inner *left,
		  unsigned lfill, struct inner *right, unsigned rfill,
		  struct inner *parent, int lpos)
{
	for(unsigned i = 0; i < rfill; i++) {
		/* Move all keys to the left */
		setkey(level, left, lfill + i, bkey(level, right, i));
		setval(level, left, lfill + i, bval(level, right, i));
	}
	/* Exchange left and right child in parent */
	setval(level + 1, parent, lpos, right);
	setval(level + 1, parent, lpos + 1, left);
	/* Remove left (formerly right) child from parent */
	btree_remove_level(pt, bkey(level + 1, parent, lpos), level + 1);
	free(right);
}

static void rebalance(struct PieceTable *pt, unsigned long key, unsigned level,
		      struct inner *child, int fill)
{
	if(fill == 0) {
		/* 
		 * Because we don't steal entries from a neighbour, this case
		 * can happen.  Parent node contains a single child, this
		 * node, so merging with a sibling never happens.
		 */
		btree_remove_level(pt, key, level + 1);
		free(child);
		return;
	}
	struct inner *parent = find_level(pt, key, level + 1);
	unsigned i = getpos(level + 1, parent, key);
	assert(bval(level + 1, parent, i) == child);

	if(i > 0) {
		struct inner *left = bval(level + 1, parent, i - 1);
		unsigned no_left = getfill(level, left, 0);
		if(fill + no_left <= B) {
			merge(pt, level, left, no_left, child, fill, parent,
			      i - 1);
			return;
		}
	}
	if(i + 1 < getfill(level + 1, parent, i)) {
		struct inner *right = bval(level + 1, parent, i + 1);
		unsigned no_right = getfill(level, right, 0);
		if(fill + no_right <= B) {
			merge(pt, level, child, fill, right, no_right, parent,
			      i);
			return;
		}
	}
	/*
	 * We could also try to steal one entry from the left or right
	 * neighbor.  By not doing so we changed the invariant from
	 * "all nodes are at least half full" to "no two neighboring
	 * nodes can be merged".  Which means that the average fill of
	 * all nodes is still half or better.
	 */
}

static void *btree_remove_level(struct PieceTable *pt, unsigned long key,
				unsigned level)
{
	if(level > pt->height) {
		/* we recursed all the way up */
		pt->height = 0;
		pt->root = NULL;
		return NULL;
	}
	struct inner *node = find_level(pt, key, level);
	unsigned pos = getpos(level, node, key);
	unsigned fill = getfill(level, node, pos);
	if((level == 1) && (bkey(level, node, pos) != key))
		return NULL;
	void *ret = bval(level, node, pos);
	/* remove and shift */
	for(unsigned i = pos; i < fill - 1; i++) {
		setkey(level, node, i, bkey(level, node, i + 1));
		setval(level, node, i, bval(level, node, i + 1));
	}
	clearpair(level, node, fill - 1);

	if(fill - 1 < B / 2) {
		if(level < pt->height)
			rebalance(pt, key, level, node, fill - 1);
		else if(fill - 1 == 1)
			btree_shrink(pt);
	}

	return ret;
}

void *btree_remove(struct PieceTable *pt, unsigned long key)
{
	if(pt->height == 0)
		return NULL;

	return btree_remove_level(pt, key, 1);
}

int main(void)
{
	pt_print_struct_sizes();
	struct PieceTable *pt = btree_new();
	struct piece p = (struct piece){ .len = 1 };
	/*
	btree_insert(pt, 42, &p);
	p.len++;
	btree_insert(pt, 12, &p);
	p.len++;
	btree_insert(pt, 21, &p);
	p.len++;
	btree_insert(pt, 24, &p);
	p.len++;
	btree_insert(pt, 1, &p);
	p.len++;
	btree_insert(pt, 36, &p);
	*/
	for(int i = 0; i < 100; i += 2) {
		btree_insert(pt, i, &p);
		btree_insert(pt, i+1, &p);
	}
	pt_to_dot(pt, "t.dot");
	//printf("bytes[0]: %ld\n", pt->root->children[0]->children[0]->bytes[0]);
}
