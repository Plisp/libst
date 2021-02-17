//
// b+ tree
//

//#include <stdatomic.h>
#include <cstdint>
#include <cstdio>
#include <cstring>

#include <atomic>
//#include "vector.h"
#include <immer/vector.hpp>
#include <immer/algorithm.hpp>

extern "C" {

#include "pt.h"

enum NodeType { INNER, LEAF };

// typedef atomic_uint RefCount;
using RefCount = std::atomic_uint;

#define NODESIZE 512
#define PER_B (sizeof(struct node *) + sizeof(long))
#define B (NODESIZE / PER_B)
struct node {
	RefCount refs;
	NodeType type;
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
	NodeType type;
	struct piece pieces[C];
};

struct PieceTable {
	struct node *root;
	const char *original;
	immer::vector<char> append;
};

void pt_print_struct_sizes(void) {
	printf("sizeof(struct piece): %zd\n", sizeof(struct piece));
	printf("sizeof(struct node): %zd B: %zd\n", sizeof(struct node), B);
	printf("sizeof(struct leaf): %zd C: %zd\n", sizeof(struct leaf), C);
	printf("sizeof(PieceTable): %zd\n", sizeof(PieceTable));
}

void vector_to_dot(immer::vector<char> &vec)
{
	;
}

/* the fun stuff */

PieceTable *pt_new_from_data(const char *data, long len)
{
	//PieceTable *pt = (PieceTable*)malloc(sizeof *pt);
	PieceTable *pt = new PieceTable;
	pt->root = NULL;
	pt->original = NULL;
	pt->append = immer::vector<char>();
	for(int i = 0; i < len; i++) {
		pt->append = pt->append.push_back(data[i]);
	}
	printf("size: %d\n", pt->append.size());
	return pt;
}

bool pt_to_dot(PieceTable *pt, const char *path)
{
	immer::for_each(pt->append, putchar);
	return true;
}

void pt_print_tree(PieceTable *pt)
{
	return;
}




} // extern "C"
