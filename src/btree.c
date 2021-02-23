//
// b+ tree
//

#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

#include "vector.h"
#include "pt.h"

typedef atomic_uint RefCount;

#define NODESIZE 512
#define PER_B (sizeof(struct node *) + sizeof(long))
#define B (NODESIZE / PER_B)
struct node {
	RefCount refs;
	int count;
	long bytes[B-1];
	struct node *children[B];
};

#define LEN_MASK (1l << sizeof(long)*8-1)
struct piece {
	uintptr_t off;
	// high bits: 1xxx...
	// if 1 is set, off is an index into the append buffer
	long len;
};

#define C ((B-1)*PER_B / sizeof(struct piece))
struct leaf {
	RefCount refs;
	int count;
	struct piece pieces[C];
};

struct PieceTable {
	struct node *root;
	int depth;
	const char *original;
	Vector *append;
};

void pt_print_struct_sizes(void) {
	printf(
		"Implementation: \e[38;5;1mbtree\e[0m\n"
		"sizeof(struct piece): %zd\n"
		"sizeof(struct node): %zd B: %zd\n"
		"sizeof(struct leaf): %zd C: %zd\n"
		"sizeof(PieceTable): %zd\n",
		sizeof(struct piece),
		sizeof(struct node), B,
		sizeof(struct leaf), C,
		sizeof(PieceTable)
	);
}

/* the fun stuff */

PieceTable *pt_new_from_data(const char *data, long len)
{
	PieceTable *pt = (PieceTable*)malloc(sizeof *pt);
	pt->root = NULL;
	pt->original = NULL;
	// TODO:
	pt->append = vector_new();
	vector_append(pt->append, data, len);
	printf("size: %d\n", pt->append->size);
	return pt;
}

bool pt_to_dot(PieceTable *pt)
{
	// TODO:
	return true;
}

void pt_pprint(PieceTable *pt)
{
	return;
}
