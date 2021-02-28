CC = gcc
CFLAGS = -Wall
DFLAGS = -Wextra -g

debug:
	$(CC) array.c main.c -o array $(CFLAGS) $(DFLAGS)
	$(CC) btree.c main.c -o btree $(CXXFLAGS) $(DFLAGS)

opt:
	$(CC) array.c main.c -o array -O3 $(CFLAGS)
	$(CC) btree.c main.c -o btree -O3 $(CXXFLAGS)

dot:
	./array
	dot -Tpng array.dot -o array.png
	#./btree
	#dot -Tpng btree.dot -o btree.png

clean:
	rm -fv array btree vector *.dot *.png

vec:
	$(CC) -O3 vector.c -Wno-shift-op-parentheses -o vector
	#valgrind --leak-check=full ./vector
	./vector

d:
	dot -o debug.png -Tpng debug.dot
	sxiv debug.png

f:
	dot -o final.png -Tpng final.dot
	sxiv final.png
