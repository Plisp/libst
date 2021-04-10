#include <assert.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "st.h"

int main(void)
{
#ifndef NDEBUG
	SliceTable *st;
	st_print_struct_sizes();
	st = st_new_from_file("vector.c");
	st_pprint(st);
	assert(st_size(st) == 9616);
	st_dbg("deletion: whole piece case\n");
	st_delete(st, 0, 9616);
	st_pprint(st);
	st_free(st);
	st_dbg("deletion: end boundary case\n");
	st = st_new_from_file("vector.c");
	st_delete(st, 1, 1000000);
	st_pprint(st);
	st_free(st);
	st_dbg("deletion: start boundary case\n");
	st = st_new_from_file("vector.c");
	st_delete(st, 0, 9615);
	st_pprint(st);
	st_free(st);
	st_dbg("deletion: piece split case\n");
	st = st_new_from_file("vector.c");
	st_delete(st, 1, 9614);
	st_pprint(st);
	st_free(st);
	st_dbg("deletion: multiple piece case general\n");
	st = st_new_from_file("vector.c");
	st_insert(st, 2, "XXX", 3);
	st_delete(st, 1, 9616);
	st_pprint(st);
	st_free(st);
	st_dbg("deletion: multiple piece case start boundary\n");
	st = st_new_from_file("vector.c");
	st_insert(st, 1, "XXX", 3);
	st_delete(st, 1, 9616);
	st_pprint(st);
	st_free(st);
	st_dbg("deletion: multiple piece case end boundary\n");
	st = st_new_from_file("vector.c");
	st_insert(st, 2, "XXX", 3);
	st_delete(st, 1, 1000000);
	st_pprint(st);
	st_free(st);
	st_dbg("deletion: multiple piece case start+end boundaries\n");
	st = st_new_from_file("vector.c");
	st_insert(st, 2, "XXX", 3);
	st_delete(st, 0, 1000000);
	st_pprint(st);
	st_free(st);
#else
	st_print_struct_sizes();
	struct timespec before, after;

	const char *xml = "test.xml";
	clock_gettime(CLOCK_REALTIME, &before);
	SliceTable *st = st_new_from_file(xml);
	clock_gettime(CLOCK_REALTIME, &after);
	printf("load time: %f ms\n",
			(after.tv_nsec - before.tv_nsec) / 1000000.0f +
			(after.tv_sec - before.tv_sec) * 1000);

	SliceTable *clone = st_clone(st);

	clock_gettime(CLOCK_REALTIME, &before);
	for(int i = 0; i < 100000; i++) {
		size_t n = 34+i*59;
		st_delete(st, n, 5);
		//st_pprint(st);
		assert(st_check_invariants(st));
		st_insert(st, n, "thang", 5);
		//st_pprint(st);
		assert(st_check_invariants(st));
	}
	clock_gettime(CLOCK_REALTIME, &after);
	//FILE *test = fopen("test", "w");
	//st_dump(st, test);
	printf("replace time: %f ms, len: %ld, depth %u\n",
			(after.tv_nsec - before.tv_nsec) / 1000000.0f +
			(after.tv_sec - before.tv_sec) * 1000,
			st_size(st), st_depth(st));
	st_free(st);
	st_free(clone);
#endif
}
