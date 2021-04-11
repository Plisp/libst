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
		if(linelen < 3)
			continue;

		linelen -= 2;
		unsigned i = *s++;
		unsigned j = *s++;

		i = 1000*i + j;

		st_insert(st, st_size(st) - (i%st_size(st)+i%2), s, linelen);
#ifdef AFL_DEBUG
		st_pprint(st);
#endif
		assert(st_check_invariants(st));
	}
}
