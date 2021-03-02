#include <assert.h>
#include <stdlib.h>
#include <string.h>
#include "pt.h"

int main(void)
{
#ifndef NDEBUG
	PieceTable *pt;
	pt_dbg("deletion: whole piece case\n");
	pt = pt_new_from_file("vector.c", 0, 0);
	assert(pt_size(pt) == 9410);
	pt_pprint(pt);
	pt_erase(pt, 0, 9410);
	pt_pprint(pt);
	pt_free(pt);
	pt_dbg("deletion: end boundary case\n");
	pt = pt_new_from_file("vector.c", 0, 0);
	pt_erase(pt, 1, 1000000);
	pt_pprint(pt);
	pt_free(pt);
	pt_dbg("deletion: start boundary case\n");
	pt = pt_new_from_file("vector.c", 0, 0);
	pt_erase(pt, 0, 9409);
	pt_pprint(pt);
	pt_free(pt);
	pt_dbg("deletion: piece split case\n");
	pt = pt_new_from_file("vector.c", 0, 0);
	pt_erase(pt, 1, 9408);
	pt_pprint(pt);
	pt_free(pt);
	pt_dbg("deletion: multiple piece case general\n");
	pt = pt_new_from_file("vector.c", 0, 0);
	pt_insert(pt, 2, "XXX", 3);
	pt_erase(pt, 1, 9410);
	pt_pprint(pt);
	pt_free(pt);
	pt_dbg("deletion: multiple piece case start boundary\n");
	pt = pt_new_from_file("vector.c", 0, 0);
	pt_insert(pt, 1, "XXX", 3);
	pt_erase(pt, 1, 9410);
	pt_pprint(pt);
	pt_free(pt);
	pt_dbg("deletion: multiple piece case end boundary\n");
	pt = pt_new_from_file("vector.c", 0, 0);
	pt_insert(pt, 2, "XXX", 3);
	pt_erase(pt, 1, 1000000);
	pt_pprint(pt);
	pt_free(pt);
	pt_dbg("deletion: multiple piece case start+end boundaries\n");
	pt = pt_new_from_file("vector.c", 0, 0);
	pt_insert(pt, 2, "XXX", 3);
	pt_erase(pt, 0, 1000000);
	pt_pprint(pt);
	pt_free(pt);
#else
	;
#endif
}
