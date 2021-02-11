#include <iostream>
#include <immer/vector.hpp>

#include "pt.h"

using buf = immer::vector<char>;
constexpr int B = 1 << 5;

struct ptable {

	buf<buf> data;
}

enum NODETYPE { INNER, LEAF };

struct pnode {
	NODETYPE type;
}

struct inner {
	nodetype type = inner;
	struct pnode children[B];
}

// note, flex vector takes 400ms just for 100000 insertions
int main()
{
	line content(5900000, 'x');
	for (int i = 0; i < 100000; i++) {
		content = content.insert(i*50+1, 'l');
	}
	std::cout << content.size() << '\n';
	/*
	for (int i = 0; i < 100000; i++) {
		content = content.erase(content.size()/2);
	} std::cout << content.size() << '\n';
	*/
	return 0;
}
