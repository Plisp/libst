debug: clean
	$(CC) pt_array.c demo.c -Wall -g -o array_demo
	$(CC) pt_tree.c demo.c -Wall -g -o tree_demo


release: clean
	$(CC) pt_array.c demo.c -O3 -DNDEBUG -o array_demo
	#$(CC) pt_array.c demo.c -O3 -S -fverbose-asm -g -DNDEBUG
	$(CC) pt_array.c demo.c -O3 -DNDEBUG -o tree_demo

clean:
	rm -f array_demo tree_demo demo.s pt_array.s pt_tree.s
