#include <iostream>
#include <immer/flex_vector.hpp>

#include "pt.h"


using line = immer::flex_vector<char>;

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
