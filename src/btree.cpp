//
// b+ tree
//

#include <atomic>
#include <cstdint>
#include <iostream>
#include <immer/vector.hpp>

extern "C" {

#include "pt.h"

const uintptr_t OFF_MASK = (uintptr_t)1 << sizeof(UINTPTR_MAX)*8-1;

enum struct NodeType { INNER, LEAF };

const int PER_B = sizeof(struct node *) + sizeof(size_t);
const int B = (
	512 - sizeof(NodeType) - sizeof(std::atomic_uint) - sizeof(struct node *)
) / PER_B + 1;

struct node {
	std::atomic_uint refs;
	NodeType type;
	size_t bytes[B-1];
	struct node *children[B];
};

struct piece {
	uintptr_t off; // if highest bit is set, an index into the append buffer
	size_t len;
};

constexpr int C = B*PER_B / sizeof(struct piece);
struct leaf {
	std::atomic_uint refs;
	NodeType type;
	struct piece pieces[C];
};

struct PieceTable {
	struct node *root;
	char *original;
	immer::vector<char> append;
};

void pt_print_struct_sizes(void) {
	std::cout << "sizeof(struct piece): " << sizeof(struct piece) << '\n';
	std::cout << "sizeof(struct node): " << sizeof(struct node) << " B: " << B << '\n';
	std::cout << "sizeof(struct leaf): " << sizeof(struct leaf) << " C: " << C << '\n';
	std::cout << "sizeof(PieceTable): " << sizeof(PieceTable) << '\n';
}








} // extern "C"
