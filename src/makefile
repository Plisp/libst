CC = clang
CFLAGS = -Wall -Wno-parentheses -std=c11 -D_POSIX_C_SOURCE=200809L -D_GNU_SOURCE
DFLAGS = -Wextra -g -fsanitize=undefined -fsanitize=address

debug:
	$(CC) chain/*.c main.c -o pchain $(CFLAGS) $(DFLAGS)
	$(CC) rblinux/*.c rb.c main.c -o rbtree $(CFLAGS) $(DFLAGS)
	$(CC) btree.c main.c -o btree $(CFLAGS) $(DFLAGS)

opt:
	$(CC) chain/*.c main.c -o pchain -O3 $(CFLAGS) -DNDEBUG
	$(CC) rblinux/*.c rb.c main.c -o rbtree -O3 $(CFLAGS) -DNDEBUG
	$(CC) btree.c main.c -o btree -O3 $(CFLAGS) -DNDEBUG

lib:
	$(CC) -c -fPIC btree.c $(CFLAGS)
	$(CC) btree.o -shared -o libst.so

afl:
	afl-gcc btree.c fuzz.c -o fuzz -O3 $(CFLAGS)
	afl-fuzz -i tests -o results ./fuzz

afl-debug:
	$(CC) btree.c fuzz.c -o fuzz $(CFLAGS) $(DFLAGS) -DAFL_DEBUG

clean:
	rm -f btree rbtree pchain *.o *.so fuzz *.dot *.png

loc:
	scc --exclude-dir=.ccls-cache --exclude-dir=test.xml
