#ifdef __cplusplus
extern "C" {
#endif

#include "pt.h"

int main(void)
{
	pt_print_struct_sizes();
	PieceTable *pt = pt_new_from_data("testtestaaaaaaa", 15);
	pt_insert(pt, 2, "xxx", 3);
	pt_print_tree(pt);
	pt_to_dot(pt, "./pt.dot");
	return 0;
}

#ifdef __cplusplus
}
#endif
