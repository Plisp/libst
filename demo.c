#include <stddef.h>
#include <stdio.h>
#include <time.h>

#include "pt.h"

#ifndef NDEBUG
	#define ITERS 2
#else
	#define ITERS 20000
#endif

long sub_timespec(struct timespec before, struct timespec after)
{
	return (after.tv_nsec - before.tv_nsec)/1000000 + 
		(after.tv_sec - before.tv_sec)*1000;
}

int main(int argc, char *argv[])
{
	if (argc < 2) {
		printf("\e[38;5;1mplease provide a single filename as argument\n");
		return 1;
	}

	PieceTable *pt;
#ifndef NDEBUG
	pt_print_struct_sizes();
	char data[] = "lorem\nipsum";

	pt = pt_new_from_data(data, 11);
	printf("loaded lorem ipsum from str\n");
	pt_print_tree(pt);
	puts("inserting test...");
	pt_insert(pt, 5, "test", 4, NULL);
	pt_print_tree(pt);
	pt_free(pt);
#endif

	struct timespec before, after;
	clock_gettime(CLOCK_REALTIME_COARSE, &before);
	pt = pt_new_from_file(argv[1], 0, 0);
	clock_gettime(CLOCK_REALTIME_COARSE, &after);
	printf("loaded file %s in %ld ms\n", argv[1], sub_timespec(before, after));
	pt_print_tree(pt);

	clock_gettime(CLOCK_REALTIME_COARSE, &before);
	for(int i = 0; i < ITERS; i++)
		pt_insert(pt, pt_size(pt)/2, "t\ns\n", 4, NULL);
	clock_gettime(CLOCK_REALTIME_COARSE, &after);
	printf("inserted %d times in %ld ms\n", ITERS, sub_timespec(before, after));
#ifndef NDEBUG
	pt_print_tree(pt);
#endif
	printf("moved %zd nodes\n", pt_nodes_moved);
	pt_free(pt);
	return 0;
}
