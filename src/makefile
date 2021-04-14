CC = clang
CFLAGS = -Wall -Wno-parentheses -std=c11 -D_POSIX_C_SOURCE=199309L # for time.h
DFLAGS = -Wextra -g -fsanitize=undefined #-fsanitize=address

debug:
	#$(CC) array.c main.c -o array $(CFLAGS) $(DFLAGS)
	$(CC) btree.c main.c -o btree $(CFLAGS) $(DFLAGS)

opt:
	$(CC) array.c main.c -o array -O3 $(CFLAGS) -DNDEBUG
	$(CC) btree.c main.c -o btree -O3 $(CFLAGS) -DNDEBUG

afl:
	afl-gcc btree.c fuzz.c -o fuzz -O3 $(CFLAGS)
	afl-fuzz -i tests -o results ./fuzz

afl-debug:
	$(CC) btree.c fuzz.c -o fuzz $(CFLAGS) $(DFLAGS) -DAFL_DEBUG

clean:
	rm -f array btree fuzz *.dot *.png

loc:
	scc --exclude-dir=.ccls-cache --exclude-dir=test.xml
