debug: clean
	$(CC) pt.c demo.c -o demo -Wall -g


release: clean
	$(CC) pt.c demo.c -O3 -o demo -DNDEBUG
	$(CC) pt.c demo.c -O3 -S -fverbose-asm -g -DNDEBUG
	$(CC) pt.c -O3 -fPIC -shared -o libpt.so

clean:
	rm -f demo demo.s pt.s libpt.so
