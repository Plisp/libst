CC = gcc
CFLAGS = -Wall -Wno-parentheses
DFLAGS = -Wextra -g

debug:
	$(CC) array.c main.c -o array $(CFLAGS) $(DFLAGS)
	$(CC) btree.c main.c -o btree $(CFLAGS) $(DFLAGS)

opt:
	$(CC) array.c main.c -o array -O3 $(CFLAGS)
	$(CC) btree.c main.c -o btree -O3 $(CFLAGS)

dot:
	./array
	dot -Tpng array.dot -o array.png
	#./btree
	#dot -Tpng btree.dot -o btree.png

clean:
	rm -f array btree vector *.dot *.png

vec:
	$(CC) -O3 vector.c -Wno-shift-op-parentheses -o vector
	#valgrind --leak-check=full ./vector
	./vector

d:
	dot -o debug.png -Tpng debug.dot

f:
	dot -o final.png -Tpng final.dot
