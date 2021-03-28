/*
 * b+tree-based piece sequence
 */

#include <assert.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "pt.h"
#include <sys/types.h>

// TODO what are good values?
#define LOW_WATER 256
#define HIGH_WATER 4096

enum blktype { IMMUT, IMMUT_MMAP, MUT, PTABLE };
struct block {
	enum blktype type;
	int refs; // only for IMMUT blocks
	void *data;
	long len;
};

struct piece {
	struct block *blk;
	unsigned long offset;
};

// TODO try larger powers
#define NODESIZE 128
#define PER_B (sizeof(struct inner *) + sizeof(long))
#define B (NODESIZE / PER_B)
struct inner {
	unsigned long spans[B];
	struct inner *children[B];
};

#define PER_BL (sizeof(struct piece) + sizeof(long))
#define BL (NODESIZE / PER_BL)
struct leaf {
	unsigned long spans[BL];
	struct piece pieces[BL];
};

#define LVAL(leaf, i) (leaf->spans[i] != ULONG_MAX ? &leaf->pieces[i] : NULL)

#define IKEYS(node) ((unsigned long *)&node->spans)
#define IVALS(node) ((struct inner *)&node->children)
#define LKEYS(leaf) ((unsigned long *)&leaf->spans)
#define LVALS(leaf) ((struct piece *)&leaf->pieces)

struct PieceTable {
	struct inner *root;
	unsigned height;
};

static void leaf_clrslots(struct leaf *leaf, unsigned from, unsigned to)
{
	assert(to <= BL);
	for(unsigned i = from; i < to; i++)
		leaf->spans[i] = ULONG_MAX;
}

static void inner_clrslots(struct inner *node, unsigned from, unsigned to)
{
	assert(to <= B);
	for(unsigned i = from; i < to; i++) {
		node->spans[i] = ULONG_MAX;
	}
	memset(IVALS(node) + from, 0, (to - from) * sizeof(struct inner *));
}

static struct leaf *leaf_alloc(void)
{
	struct leaf *leaf = malloc(sizeof *leaf);
	leaf_clrslots(leaf, 0, BL);
	return leaf;
}

static struct inner *inner_alloc(void)
{
	struct inner *node = malloc(sizeof *node);
	inner_clrslots(node, 0, B);
	return node;
}

static unsigned long spansum(unsigned level, struct inner *node, unsigned fill)
{
	unsigned long sum = 0;
	if(level == 1)
		for(unsigned i = 0; i < fill; i++)
			sum += ((struct leaf *)node)->spans[i];
	else
		for(unsigned i = 0; i < fill; i++)
			sum += node->spans[i];
	return sum;
}

/* debug */

void pt_print_struct_sizes(void)
{
	fprintf(stderr,
		"Implementation: \e[38;5;1mpt\e[0m\n"
		"sizeof(struct inner): %zd\n"
		"sizeof(struct leaf): %zd\n"
		"sizeof(PieceTable): %zd\n",
		sizeof(struct inner), sizeof(struct leaf), sizeof(PieceTable));
}

static void print_node(unsigned level, struct inner *node)
{
	char out[256], *it = out;
	if(level == 1) {
		struct leaf *leaf = (struct leaf *)node;
		it += sprintf(it, "[k: ");
		for(unsigned i = 0; i < BL; i++) {
			unsigned long key = leaf->spans[i];
			if(key != ULONG_MAX)
				it += sprintf(it, "%lu|", key);
			else
				it += sprintf(it, "NUL|");
		}
		it--;
		it += sprintf(it, " p: ");
		for(unsigned i = 0; i < BL; i++) {
			struct piece *val = LVAL(leaf, i);
			it += sprintf(it, "%lu|", val ? val->offset : 0);
		}
		it--;
		it += sprintf(it, "]");
	} else {
		for(unsigned i = 0; i < B; i++) {
			unsigned long key = node->spans[i];
			if(key != ULONG_MAX)
				it += sprintf(it, "%lu|", key);
			else
				it += sprintf(it, "0|");
		}
		it--;
		it += sprintf(it, "]");
	}
	fprintf(stderr, "%s\n", out);
}

// TODO breadth-first
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
	FSTR(tmp, "offset: %lu", piece->offset);
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
		unsigned long key = leaf->spans[i];
		if(key != ULONG_MAX)
			FSTR(tmp, "%lu", key);
		else
			tmp = NULL;
		graph_table_entry(file, tmp, NULL);
	}
	for(unsigned i = 0; i < BL; i++)
		piece_to_dot(file, LVAL(leaf, i));
	graph_table_end(file);
	free(tmp);
}

void inner_to_dot(FILE *file, const struct inner *root, unsigned height)
{
	if(!root)
		return;
	if(height == 1)
		return leaf_to_dot(file, (struct leaf *)root);
	char *tmp = NULL, *port = NULL;
	graph_table_begin(file, root, NULL);
	for(unsigned i = 0; i < B; i++) {
		unsigned long key = root->spans[i];
		if(key != ULONG_MAX) {
			FSTR(tmp, "%lu", key);
			FSTR(port, "%u", i);
		} else
			tmp = port = NULL;
		graph_table_entry(file, tmp, port);
	}
	graph_table_end(file);
	// output child links
	for(unsigned i = 0; i < B; i++) {
		struct inner *child = root->children[i];
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
	if(fclose(file))
		goto fail;
	return true;

fail:
	perror("pt_to_dot");
	return false;
}

/* basic */

struct PieceTable *pt_new(void)
{
	struct PieceTable *pt = malloc(sizeof *pt);
	pt->root = NULL;
	pt->height = 0;
	return pt;
}

void pt_free(struct PieceTable *pt)
{
	// TODO just do iteratively
	free(pt);
}

// returns index of the first key spanning the search key
// key contains the offset at the end
static unsigned getpos(struct inner *node, unsigned long *key)
{
	unsigned i = 0;
	while(*key && *key >= node->spans[i]) {
		*key -= node->spans[i];
		i++;
#ifndef NDEBUG
		if(i == B)
			assert(!*key); // off end is permitted, terminates loop
#endif
	}
	return i;
}

static unsigned getleafpos(struct leaf *leaf, unsigned long *key)
{
	unsigned i = 0;
	while(*key && *key >= leaf->spans[i]) {
		*key -= leaf->spans[i];
		i++;
#ifndef NDEBUG
		if(i == BL)
			assert(!*key); // off end is permitted, terminates loop
#endif
	}
	return i;
}

// count the number of live entries starting from START as an optimization
static unsigned getfill(struct inner *node, unsigned start)
{
	unsigned i;
	for(i = start; i < B; i++)
		if(!node->children[i])
			break;
	return i;
}

static unsigned getleaffill(struct leaf *leaf, unsigned start)
{
	unsigned i;
	for(i = start; i < BL; i++)
		if(!LVAL(leaf, i))
			break;
	return i;
}

/* insertion */

// returns the new size of the subtree
static unsigned long tree_insert_recurse(unsigned level, struct inner *root,
					 struct piece *p, unsigned long span,
					 unsigned long pos,
					 struct inner **split,
					 unsigned long *splitsize)
{
	// base case: leaf insertion
	if(level == 1) {
		struct leaf *leaf = (struct leaf *)root;
		unsigned i = getleafpos(leaf, &pos);
		assert(pos == 0);
		unsigned fill = getleaffill(leaf, i);
		pt_dbg("found pos %u, leaf fill %u\n", i, fill);
		if(fill < BL) {
			pt_dbg("moving %u pieces originally at %u\n", fill - i,
			       i);
			print_node(level, root);
			memmove(LKEYS(leaf) + i + 1, LKEYS(leaf) + i,
				(fill - i) * sizeof(long));
			leaf->spans[i] = span;
			memmove(LVALS(leaf) + i + 1, LVALS(leaf) + i,
				(fill - i) * sizeof(struct piece));
			leaf->pieces[i] = *p;
			return span;
		} else { // fill == BL
			struct leaf *right = leaf_alloc();
			unsigned splitpos = BL / 2;
			unsigned long newsize;
			// 12|34 shift 34 if i <= 2 (splitpos)
			// 12|345 shift 345 if i <= 2 (splitpos)
			if(i <= splitpos) {
				memcpy(LKEYS(right), LKEYS(leaf) + splitpos,
				       (BL - splitpos) * sizeof(long));
				memcpy(LVALS(right), LVALS(leaf) + splitpos,
				       (BL - splitpos) * sizeof(struct piece));
				leaf_clrslots(leaf, splitpos, BL);
				print_node(1, root);
				// now insert span in the old node
				memmove(LKEYS(leaf) + i + 1, LKEYS(leaf) + i,
					(splitpos - i) * sizeof(long));
				leaf->spans[i] = span;
				memmove(LVALS(leaf) + i + 1, LVALS(leaf) + i,
					(splitpos - i) * sizeof(struct piece));
				leaf->pieces[i] = *p;
			} else { // i > splitpos, we don't touch first B/2+1 LEAF slots
				// move to right split
				memcpy(LKEYS(right), LKEYS(leaf) + splitpos + 1,
				       (BL - (splitpos + 1)) * sizeof(long));
				memcpy(LVALS(right), LVALS(leaf) + splitpos + 1,
				       (BL - (splitpos + 1)) *
					       sizeof(struct piece));
				leaf_clrslots(leaf, splitpos + 1, BL);
				// do insertion in right split
				i -= splitpos + 1;
				memmove(LKEYS(right) + i + 1, LKEYS(right) + i,
					i * sizeof(long));
				right->spans[i] = span;
				memmove(LVALS(right) + i + 1, LVALS(right) + i,
					i * sizeof(struct piece));
				right->pieces[i] = *p;
			}
			newsize = spansum(1, (struct inner *)leaf,
					  splitpos + 1);
			*split = (struct inner *)right;
			*splitsize = spansum(1, (struct inner *)right,
					     BL - splitpos);
			pt_dbg("new size of leaf: %lu\n", newsize);
			pt_dbg("size of right split: %lu\n", *splitsize);
			pt_dbg("split leaf: ");
			print_node(level, (struct inner *)leaf);
			pt_dbg("right: ");
			print_node(level, (struct inner *)right);
			return newsize;
		}
	} else { // level > 1: inner node recursion
		unsigned i = getpos(root, &pos);
		// newsize is the size delta when no split or
		// the actual size of the original when there was a split
		unsigned long newsize = tree_insert_recurse(
			level - 1, root->children[i], p, span, pos, split, splitsize);
		if(*split) {
			pt_dbg("inner split node: ");
			print_node(level, *split);
			assert(false);
		} else  {
			root->spans[i] += newsize;
			return newsize;
		}
	}
}

static void insert_piece(PieceTable *pt, struct piece *p, unsigned long span,
			 unsigned long pos)
{
	if(!pt->root) {
		pt_dbg("allocating empty root\n");
		pt->root = (struct inner *)leaf_alloc();
		pt->height = 1;
		assert(0 == getleaffill((struct leaf *)pt->root, 0));
	}
	struct inner *split = NULL;
	unsigned long splitsize;
	unsigned long newsize = tree_insert_recurse(
		pt->height, pt->root, p, span, pos, &split, &splitsize);
	// root split
	if(split) {
		pt_dbg("allocating new root\n");
		struct inner *newroot = inner_alloc();
		newroot->spans[0] = newsize;
		newroot->children[0] = pt->root;
		newroot->spans[1] = splitsize;
		newroot->children[1] = split;
		pt->root = newroot;
		pt->height++;
	}
}

long pt_size(PieceTable *pt)
{
	struct inner *root = pt->root;
	if(pt->height == 1) {
		return spansum(1, root, getleaffill((struct leaf *)root, 0));
	} else
		return spansum(pt->height, root, getfill(root, 0));
}

/* deletion */

int main(void)
{
	pt_print_struct_sizes();
	struct PieceTable *pt = pt_new();
	struct piece p = (struct piece){ .offset = 1 };
	insert_piece(pt, &p, 5, 0);
	p.offset = 2;
	insert_piece(pt, &p, 69, 5);
	p.offset = 3;
	insert_piece(pt, &p, 42, 0);
	p.offset = 4;
	insert_piece(pt, &p, 21, 47);
	p.offset = 5;
	insert_piece(pt, &p, 3, 0);
	p.offset = 6;
	insert_piece(pt, &p, 7, 140);
	//p.offset = 7;
	//insert_piece(pt, &p, 1, 7);

	pt_to_dot(pt, "t.dot");
	pt_dbg("piece table size: %ld\n", pt_size(pt));
	/*
	pt_insert(pt, 1, &p);
	p.len++;
	pt_insert(pt, 2, &p);
	p.len++;
	pt_insert(pt, 3, &p);
	p.len++;
	pt_insert(pt, 5, &p);
	p.len++;
	pt_insert(pt, 6, &p);
	p.len++;
	pt_insert(pt, 5, &p);
	pt_to_dot(pt, "t.dot");
	*/
	/*
	for(int i = 0; i < 100000; i += 2) {
		pt_insert(pt, i, &p);
		pt_insert(pt, i + 1, &p);
	}
	*/
	//printf("bytes[0]: %ld\n", pt->root->children[0]->children[0]->bytes[0]);
}
