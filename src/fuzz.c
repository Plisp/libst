#include <assert.h>
#include <stdlib.h>
#include <string.h>

#include "st.h"

int main(void)
{
	SliceTable *st = st_new();
	st_insert(st, 0, "x", 1);
#ifdef AFL_DEBUG
	FILE *sm = fopen("mini", "r");
#else
	FILE *sm = stdin;
#endif
	char line[10000], *s;
	while(true) {
		s = fgets(line, 10000, sm);
		if(!s)
			break;

		size_t linelen = strlen(s);
		if(linelen < 4)
			continue;

		linelen -= 2;
		bool op = *s++ % 2;
		unsigned i = *s++;
		unsigned j = *s++;

		i = 1000*i + j;
		size_t pos = st_size(st) - (i % st_size(st) + i%2);

		if(op)
			st_insert(st, pos, s, linelen);
		else
			st_delete(st, pos, linelen % st_size(st));
#ifdef AFL_DEBUG
		st_pprint(st);
#endif
		assert(st_check_invariants(st));
	}
}
