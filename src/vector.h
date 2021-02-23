//
// my own persistent vector implementation, after realising I wouldn't have to
// figure out rrb concatenation after all
//

#include <assert.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define SHIFT_BITS 2
#define M (1 << SHIFT_BITS)

struct VecInner {
	atomic_int refc;
	struct VecInner *children[M]; // TODO: maybe use a flexible array here?
};

struct VecLeaf {
	atomic_int refc;
	char text[M * sizeof(struct VecInner *)];
};

typedef struct {
	long size;
	long shift;
	struct VecInner *root;
	struct VecLeaf *tail;
} Vector;

static inline void incref(atomic_int *refc)
{
	// can be relaxed as we hold a reference, which is all that matters
	atomic_fetch_add_explicit(refc, 1, memory_order_relaxed);
}

static inline struct VecInner *vecinner_clone(struct VecInner *node)
{
	struct VecInner *copy = malloc(sizeof *copy);
	memcpy(copy, node, sizeof *copy);
	for(int i = 0; i < M; i++) {
		struct VecInner *child = node->children[i];
		if(child)
			incref(&child->refc);
	}
	copy->refc = 1;
	return copy;
}

static inline struct VecLeaf *vecleaf_clone(struct VecLeaf *node)
{
#ifdef NDEBUG
	struct VecLeaf *copy = malloc(sizeof *copy);
#else
	struct VecLeaf *copy = calloc(1, sizeof *copy);
#endif
	memcpy(copy, node, sizeof *copy);
	copy->refc = 1;
	return copy;
}

static void vecleaf_free(struct VecLeaf *leaf)
{
	if(atomic_fetch_sub_explicit(&leaf->refc, 1, memory_order_release) == 1) {
		atomic_thread_fence(memory_order_acquire);
		free(leaf);
	}
}

static void vecinner_free(struct VecInner *node, int shift)
{
	if(shift) {
		// acquire-release is necessary to ensure accesses from other threads
		// finish before attempting to free children. See:
		// https://boost.org/doc/libs/1_55_0/doc/html/atomic/usage_examples.html
		if(atomic_fetch_sub_explicit(&node->refc, 1, memory_order_release) == 1) {
			atomic_thread_fence(memory_order_acquire);
			for(int i = 0; i < M; i++) {
				struct VecInner *child = node->children[i];
				if(!child) // nodes are left-dense and never shrink (no pops)
					break;
				// this recursion stops as soon as a refcount > 1 is detected,
				// so transitively referenced children with refcount 1 will not
				// be freed
				vecinner_free(child, shift - SHIFT_BITS);
			}
			free(node);
		}
	} else {
		vecleaf_free((struct VecLeaf *)node);
	}
}

// These provide sufficient guarantees for safe copy-on-write as we always
// ensure_*_editable on every node down to the leaf during an access operation.
//
// If at any point two nodes A, B reference child X, then X will not be mutated
// inplace as it's reference count is at least 2. However creating a clone of X:
// X' through a call to ensure_*_editable increases the reference count of X's
// children (to at least 2, from X and X'), and thus the same reasoning
// inductively applies to any child of X.
//
// TODO determine if acquire above is even necessary if we're just cloning
// children like this
static struct VecInner *ensure_inner_editable(struct VecInner *node)
{
	int expect_one = 1;
	if (atomic_compare_exchange_strong_explicit(&node->refc, &expect_one, 0,
		memory_order_acquire, memory_order_relaxed)) {
		// X: we just acquired. It's not necessary to synchronize here
		incref(&node->refc);
		return node;
	} else // refc >= 2 or = 0 at point X above, no mutation is possible
		return vecinner_clone(node);
}

static struct VecLeaf *ensure_leaf_editable(struct VecLeaf *node)
{
	int expect_one = 1;
	if (atomic_compare_exchange_strong_explicit(&node->refc, &expect_one, 0,
		memory_order_acquire, memory_order_relaxed)) {
		incref(&node->refc);
		return node;
	} else
		return vecleaf_clone(node);
}

static inline long tail_index(Vector *v) {
	return v->size-1 & ~(M-1);
}

static inline long tail_size(Vector *v) {
	return v->size ? (v->size-1 & M-1) + 1 : 0;
}

/* api */

bool vector_to_dot(Vector *v, const char *path);

Vector *vector_new(void)
{
	Vector *v = malloc(sizeof *v);
	v->size = v->shift = 0;
	v->root = NULL;
	v->tail = malloc(sizeof(struct VecLeaf));
	v->tail->refc = 1;
#ifndef NDEBUG
	memset(v->tail->text, 0, M);
#endif
	return v;
}

Vector *vector_clone(Vector *v)
{
	Vector *clone = malloc(sizeof *v);
	clone->shift = v->shift;
	clone->size = v->size;
	clone->tail = v->tail;
	incref(&v->tail->refc);
	clone->root = v->root;
	incref(&v->root->refc);
	return clone;
}

void vector_free(Vector *v)
{
	if(v->root)
		vecinner_free(v->root, v->shift);
	vecleaf_free(v->tail);
	free(v);
}

static struct VecInner *new_path(int levels, struct VecLeaf *leaf)
{
	struct VecInner *top = (struct VecInner *)leaf;
	for(int level = levels; level > 0; level -= SHIFT_BITS) {
		struct VecInner *new = calloc(1, sizeof *new);
		new->refc = 1;
		new->children[0] = top;
		top = new;
	}
	return top;
}

static void insert_leaf(Vector *v, struct VecLeaf *leaf)
{
	if(v->size > (1 << (v->shift + SHIFT_BITS))) {
		struct VecInner *newroot = calloc(1, sizeof *newroot);
		newroot->refc = 1;
		newroot->children[0] = v->root;
		newroot->children[1] = new_path(v->shift, leaf);
		v->root = newroot;
		v->shift += SHIFT_BITS;
	} else {
		v->root = ensure_inner_editable(v->root);
		struct VecInner *node = v->root;
		long offset = v->size - 1;
		for(int level = v->shift; level > SHIFT_BITS; level -= SHIFT_BITS) {
			int i = (offset >> level) & M-1;
			struct VecInner *child = node->children[i];
			if(!child) {
				node->children[i] = new_path(level - SHIFT_BITS, leaf);
				return;
			}
			child = ensure_inner_editable(child);
			node->children[i] = child;
			node = child;
		}
		node->children[(offset >> SHIFT_BITS) & M-1] = (struct VecInner *)leaf;
	}
}

void vector_append(Vector *v, const char *s, long len)
{
	// fast path for small inserts fitting in the tail
	if(tail_size(v) + len <= M) {
		v->tail = ensure_leaf_editable(v->tail);
		memcpy(v->tail->text + tail_size(v), s, len);
		v->size += len;
	} else {
		v->tail = ensure_leaf_editable(v->tail);
		int prefix = M - tail_size(v);
		memcpy(v->tail->text + tail_size(v), s, prefix);
		v->size += prefix;
		s += prefix;
		len -= prefix;
		if(v->size == M)
			v->root = (struct VecInner *)v->tail;
		else
			insert_leaf(v, v->tail);
		assert(len > 0);
		int chunks = (len - 1) / M;
		for(int i = 0; i < chunks; i++) {
			struct VecLeaf *new = malloc(sizeof *new);
			new->refc = 1;
			memcpy(new->text, s, M);
			v->size += M; // This goes first - inserting a leaf.
			s += M;
			len -= M;
			insert_leaf(v, new);
		}
		assert(len <= M);
		struct VecLeaf *tail = malloc(sizeof *tail);
#ifndef NDEBUG
		memset(tail->text + len, 0, M-len);
#endif
		tail->refc = 1;
		memcpy(tail->text, s, len);
		v->tail = tail;
		v->size += len;
	}
}

void vector_push(Vector *v, char c)
{
	vector_append(v, &c, 1);
}

#include "dot.h"

#define FSTR(dest, str, ...) \
do { \
	int len = snprintf(NULL, 0, str, __VA_ARGS__); \
	dest = realloc(dest, len+1); \
	sprintf(dest, str, __VA_ARGS__); \
} while (0)              \

void vecleaf_to_dot(FILE *file, const struct VecLeaf *leaf, int count)
{
	char *tmp = NULL;
	graph_table_begin(file, leaf, "aquamarine3");
	FSTR(tmp, "refs: %d", leaf->refc);
	graph_table_entry(file, tmp, NULL);
	FSTR(tmp, "%.*s", count, leaf->text);
	graph_table_entry(file, tmp, NULL);
	graph_table_end(file);
	free(tmp);
}

void vecinner_to_dot(FILE *file, const struct VecInner *root, int shift)
{
	if(shift <= 0)
		return vecleaf_to_dot(file, (const struct VecLeaf *) root, M);

	char *tmp = NULL;
	graph_table_begin(file, root, NULL);
	FSTR(tmp, "refs: %d", root->refc);
	graph_table_entry(file, tmp, NULL);
	for(long i = 0; i < M; i++) {
		FSTR(tmp, "%ld", i);
		graph_table_entry(file, tmp, tmp);
	}
	graph_table_end(file);
	// output links to children
	for(int i = 0; i < M; i++) {
		struct VecInner *child = root->children[i];
		if(!child)
			break;
		FSTR(tmp, "%d", i);
		graph_link(file, root, tmp, child, "body");
		vecinner_to_dot(file, child, shift - SHIFT_BITS);
	}
	free(tmp);
}

bool vector_to_dot(Vector *v, const char *path)
{
	FILE *file = fopen(path, "w");
	char *tmp = NULL;
	if(!file)
		goto fail;

	graph_begin(file);
	graph_table_begin(file, v, NULL);
	FSTR(tmp, "size: %ld", v->size);
	graph_table_entry(file, tmp, NULL);
	FSTR(tmp, "shift: %ld", v->shift);
	graph_table_entry(file, tmp, NULL);
	graph_table_entry(file, "root", "root");
	graph_table_entry(file, "tail", "tail");
	graph_table_end(file);
	graph_link(file, v, "tail", v->tail, "body");
	vecleaf_to_dot(file, v->tail, tail_size(v));
	graph_link(file, v, "root", v->root, "body");
	if(v->root)
		vecinner_to_dot(file, v->root, v->shift);
	graph_end(file);

	if(!fclose(file))
		goto fail;
	return true;

fail:
	perror("pt_to_dot");
	free(tmp);
	return false;
}
