#include <string.h>
#include <stdlib.h>
#include "pt.h"

int main(void)
{
	pt_print_struct_sizes();
	PieceTable *pt = pt_new_from_file("main.c", 0, 0);
	pt_insert(pt, 0, "XXX", 3);
	pt_pprint(pt);
	// 1XXX2XXX34567890
	pt_erase(pt, 1, 5);
	pt_pprint(pt);
	pt_to_dot(pt);
	pt_free(pt);
	return 0;
}
