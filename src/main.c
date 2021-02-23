#ifdef __cplusplus
extern "C" {
#endif

#include <string.h>
#include <stdlib.h>
#include "pt.h"

int main(void)
{
	pt_print_struct_sizes();
	PieceTable *pt = pt_new_from_data("1234567890", 10);
	//pt_insert(pt, 2, "XXX", 3);
	pt_pprint(pt);
	pt_to_dot(pt);
	return 0;
}

#ifdef __cplusplus
}
#endif
