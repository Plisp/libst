CC = gcc
CFLAGS = -Wall -Wno-parentheses -std=c11 -D_POSIX_C_SOURCE=199309
DFLAGS = -Wextra -g

debug:
	$(CC) array.c main.c -o array $(CFLAGS) $(DFLAGS)
	#$(CC) btree.c main.c -o btree $(CFLAGS) $(DFLAGS)

opt:
	$(CC) array.c main.c -o array -O3 $(CFLAGS) -DNDEBUG
	#$(CC) btree.c main.c -o btree -O3 $(CFLAGS) -DNDEBUG

dot:
	./a.out
	dot -Tpng t.dot -o t.png
	#./array
	#dot -Tpng array.dot -o array.png
	#./btree
	#dot -Tpng btree.dot -o btree.png

clean:
	rm -f array btree *.dot *.png
