//
// my own persistent vector, after realising I wouldn't have to figure out rrb
// concatenation after all TODO: finish after prototype done
//

#include <stdatomic.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define FANOUT 32

typedef enum { INNER, LEAF } NodeType;

typedef struct node {
	NodeType type;
	atomic_int refc;
	struct node *children[FANOUT];
} Node;

typedef struct {
	NodeType type;
	atomic_int refc;
	char *text[FANOUT * sizeof(Node *)];
} Leaf;

struct Vector {
	long size;
	long shift;
	Node *root;
	Leaf *tail;
};

static inline bool refcas_acq(atomic_int *refc, int expected, int desired)
{
	return atomic_compare_exchange_weak_explicit(refc, &expected, desired,
			memory_order_acquire, memory_order_relaxed);
}

static inline bool refcas_rel(atomic_int *refc, int expected, int desired)
{
	return atomic_compare_exchange_weak_explicit(refc, &expected, desired,
			memory_order_release, memory_order_relaxed);
}

static Node *node_clone(Node *node)
{
	Node *copy = malloc(sizeof *copy);
	memcpy(copy, node, sizeof *copy);
	copy->refc = 1;
	return copy;
}

void node_free(Node *node)
{
	switch(node->type) {
		case INNER:
			if(atomic_fetch_sub_explicit(&node->refc, 1, memory_order_acq_rel) == 1) {
				for(int i = 0; i < FANOUT; i++) {
					node_free(node->children[i]);
				}
				free(node);
			}
		case LEAF:
			if(atomic_fetch_sub_explicit(&node->refc, 1, memory_order_acq_rel) == 1)
				free(node);
	}
}

static Node *ensure_editable(Node *node)
{
	if (refcas_acq(&node->refc, 1, 0)) {
		node->refc++; // we are sole owner and just loaded
		return node;
	} else
		return node_clone(node);
}

struct Vector *vector_new(void)
{
	struct Vector *new = malloc(sizeof *new);
	new->size = new->shift = 0;
	new->root = NULL;
	new->tail = malloc(sizeof(Leaf));
	*new->tail = (Leaf) { LEAF, .refc = 1 };
	return new;
}

struct Vector *vector_push(struct Vector *v, char c)
{
	if(v->size < tail) // TODO
		return v->size + c;
}
struct Vector *vector_append(struct Vector *v, const char *s, long len)
{
	return v + s[0] + len;
}
