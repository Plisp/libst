all: clean demo

demo:
	$(CC) pt.c demo.c -O3 -march=native -o demo

clean:
	rm -f demo pt.o libpt.so libpt.a
