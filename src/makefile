CFLAGS = -Wall
CXXFLAGS = -Wall #-std=c++17
DFLAGS = -Wextra -g

debug:
	clang array.c main.c -o array $(CFLAGS) $(DFLAGS)
	#clang++ btree.cpp main.c -o btree $(CXXFLAGS) $(DFLAGS)

opt:
	clang array.c main.c -o array -O3 $(CFLAGS)
	clang++ btree.cpp main.c -o btree -O3 $(CXXFLAGS)

dot:
	./array
	dot -o pt.png -Tpng pt.dot

clean:
	rm -fv array btree
