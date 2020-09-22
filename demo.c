#include <time.h>
#include <stddef.h>

#include "pt.h"

#define ITERS 2

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
	printf(
		"sizeof(struct pnode): %zd, "
		"sizeof(struct text_buffer): %zd, "
		"sizeof(struct undo): %zd, "
		"sizeof(PieceTable): %zd\n",
		sizeof(struct pnode),
		sizeof(struct text_buffer),
		sizeof(struct undo),
		sizeof(PieceTable)
	);

	PieceTable *pt;
#ifndef NDEBUG
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
	clock_gettime(CLOCK_MONOTONIC, &before);
	pt = pt_new_from_file(argv[1], 0, 0);
	clock_gettime(CLOCK_MONOTONIC, &after);
	printf("loaded file %s in %ld ms\n", argv[1], sub_timespec(before, after));
	pt_print_tree(pt);

	clock_gettime(CLOCK_MONOTONIC, &before);
	size_t size = pt->tree[1].info.bytes;
	for(int i = 0; i < ITERS; i++) {
		pt_insert(pt, size/2, "t\na\n", 4, NULL);
		size += 4;
	}
	clock_gettime(CLOCK_MONOTONIC, &after);
	printf("inserted %d times in %ld ms\n", ITERS, sub_timespec(before, after));
#ifndef NDEBUG
	pt_print_tree(pt);
	printf("moved %zd nodes\n", pt_nodes_moved);
#endif
	pt_free(pt);
	return 0;
}
