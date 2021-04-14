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
	st_insert(st, 0, "test\n", 5);
	struct sliceiter *it = st_iter_new(st, 0);
	size_t len;
	char *test = st_iter_chunk(it, &len);
	st_dbg("len: %zd, %.*s", len, (int)len, test);
	/*
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
	*/
#else
	st_print_struct_sizes();
	struct timespec before, after;
	// file may be found at
	// https://raw.githubusercontent.com/jhallen/joes-sandbox/master/editor-perf/test.xml
	const char *xml = "test.xml";
	clock_gettime(CLOCK_REALTIME, &before);
	SliceTable *st = st_new_from_file(xml);
	clock_gettime(CLOCK_REALTIME, &after);
	printf("load time: %f ms\n",
			(after.tv_nsec - before.tv_nsec) / 1000000.0f +
			(after.tv_sec - before.tv_sec) * 1000);

	clock_gettime(CLOCK_REALTIME, &before);
	SliceTable *clone = st_clone(st);
	for(int i = 0; i < 100000; i++) {
		size_t n = 34+i*59;
		st_delete(st, n, 5);
		//st_pprint(st);
		assert(st_check_invariants(st));
		st_insert(st, n, "thang", 5);
		//st_pprint(st);
		assert(st_check_invariants(st));
		//st_free(clone);
		//clone = st_clone(st);
	}
	clock_gettime(CLOCK_REALTIME, &after);
	// dump and check that the clone/replacement worked
	FILE *f = fopen("test", "w");
	st_dump(st, f);
	fclose(f);
	f = fopen("clone", "w");
	st_dump(clone, f);
	fclose(f);

	printf("replace time: %f ms, len: %ld, depth %u\n",
			(after.tv_nsec - before.tv_nsec) / 1000000.0f +
			(after.tv_sec - before.tv_sec) * 1000,
			st_size(st), st_depth(st));
	st_free(st);
	st_free(clone);
#endif
}
