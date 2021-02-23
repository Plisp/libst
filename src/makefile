CC = gcc
CFLAGS = -Wall -Wno-shift-op-parentheses
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
	rm -fv array btree *.dot *.png

v:
	$(CC) -O3 vector.c -Wno-shift-op-parentheses
	./a.out


d:
	dot -o vector.png -Tpng vector.dot
	sxiv vector.png

f:
	dot -o final.png -Tpng final.dot
	sxiv final.png
