CFLAGS = -Wall
DFLAGS = -Wextra -g

debug:
	clang array.c main.c -o array $(DFLAGS)
	clang++ btree.cpp main.c -o btree $(DFLAGS)

opt:
	clang array.c main.c -o array -O3
	clang++ btree.cpp main.c -o btree -O3

clean:
	rm -fv array btree
