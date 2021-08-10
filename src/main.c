#include <assert.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "st.h"

// TODO use ropey's batch replacement approach
int main(int argc, char **argv)
{
#if 0
	SliceTable *st = st_new();
	st_insert(st, 0, "test", 4);
	SliceIter *it = st_iter_new(st, 0);
	st_iter_prev_chunk(it);
#else
#if 1
	st_print_struct_sizes();
	if(argc < 4) {
		fprintf(stderr, "usage <filename> <search pattern> "
				"<replacement pattern> <max matches>\n");
		return 1;
	}
	const char *pattern = argv[2];
	const char *replace = argv[3];
	int max = atoi(argv[4]);
	if(!max) return 1;
	size_t len = strlen(pattern);
	size_t replacelen = strlen(replace);
	size_t *matches = malloc(sizeof(size_t) * max);

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
	char c = st_iter_byte(it);
	bool *matchpos = calloc(len, sizeof(bool));
	int matchcount = 0;

	clock_gettime(CLOCK_REALTIME_COARSE, &before);
	// a basic search, comparable to ropey's search_and_replace.rs
	do {
		for(int m = len - 2; m >= 0; m--) {
			if(matchpos[m]) { // already matched at i
				if(c == pattern[m+1]) {
					matchpos[m+1] = matchpos[m];
				}
				matchpos[m] = 0;
			}
		}
		if(c == pattern[0]) {
			matchpos[0] = true;
		}
		if(matchpos[len-1]) { // goes after above for len = 1 match
			matchpos[len-1] = 0;
			matches[matchcount++] = i - (len-1);
			if(matchcount == max) break;
		}
		i++;
	} while((c = st_iter_next_byte(it, 1)) != -1);
	// do replacements at once
	int delta = replacelen - len;
	int deltacum = 0;
	for(int i = 0; i < matchcount; i++) {
		st_delete(st, matches[i] + deltacum, len);
		st_insert(st, matches[i] + deltacum, replace, replacelen);
		deltacum += delta;
	}
	clock_gettime(CLOCK_REALTIME_COARSE, &after);
	//st_dump(st, stdout);
	fprintf(stderr, "found/replaced %d matches in %ld ms, "
			"leaves: %zd, size %zd, depth %d\n",
			matchcount,
			(after.tv_nsec - before.tv_nsec) / 1000000 +
			(after.tv_sec - before.tv_sec) * 1000,
			st_node_count(st), st_size(st), st_depth(st));
	free(matchpos);
	free(matches);
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
	clock_gettime(CLOCK_REALTIME_COARSE, &before);
	SliceTable *st = st_new_from_file(xml);
	clock_gettime(CLOCK_REALTIME_COARSE, &after);
	printf("load time: %f ms\n",
			(after.tv_nsec - before.tv_nsec) / 1000000.0f +
			(after.tv_sec - before.tv_sec) * 1000);

	clock_gettime(CLOCK_REALTIME_COARSE, &before);
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
	clock_gettime(CLOCK_REALTIME_COARSE, &after);
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
