#include <assert.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "st.h"

// TODO use ropey's batch replacement approach
int main(int argc, char **argv)
{
#if 1
	SliceTable *st = st_new();
	st_insert(st, 0, "test", 4);
	SliceIter *it = st_iter_new(st, 0);
	st_iter_prev_chunk(it);
#else
#if 1
	st_print_struct_sizes();
	if(argc < 4) {
		fprintf(stderr, "usage <filename> <search pattern> <replacement pattern>\n");
		return 1;
	}
	const char *pattern = argv[2];
	const char *replace = argv[3];
	size_t len = strlen(pattern);
	size_t replacelen = strlen(replace);

	struct timespec before, after;
	clock_gettime(CLOCK_REALTIME, &before);
	SliceTable *st = st_new_from_file(argv[1]);
	clock_gettime(CLOCK_REALTIME, &after);
	printf("load time: %f ms\n",
			(after.tv_nsec - before.tv_nsec) / 1000000.0f +
			(after.tv_sec - before.tv_sec) * 1000);

	SliceTable *clone = st_clone(st);
	SliceIter *it = st_iter_new(st, 0);
	size_t i = 0;
	char c;
	bool *matchpos = calloc(len, sizeof(bool));
	int matches = 0;

	clock_gettime(CLOCK_REALTIME, &before);
	// a basic search, comparable to ropey's search_and_replace.rs
	while((c = st_iter_next_byte(it, 1)) != -1) {
		for(int m = len - 2; m >= 0; m--) {
			if(matchpos[m]) { // already matched at i
				if(c == pattern[m+1]) {
					matchpos[m+1] = matchpos[m];
				}
				matchpos[m] = 0;
			}
		}
		// try matching latest character
		if(c == pattern[0]) {
			matchpos[0] = i;
		}
		if(matchpos[len-1]) { // fully matched!
			st_delete(st, i - len+2, len);
			st_insert(st, i - len+2, replace, replacelen);
			st_iter_to(it, i+1);
			matchpos[len-1] = 0;
			matches++;
		}
		i++;
	}
	clock_gettime(CLOCK_REALTIME, &after);
	st_pprint(st);
	fprintf(stderr, "found/replaced %d matches in %f ms, "
			"leaves: %zd, size %zd, depth %d\n",
			matches,
			(after.tv_nsec - before.tv_nsec) / 1000000.0f +
			(after.tv_sec - before.tv_sec) * 1000,
			st_node_count(st), st_size(st), st_depth(st));
	free(matchpos);
	st_iter_free(it);
	st_free(clone);
	st_free(st);
#else
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
#endif
#endif
}
