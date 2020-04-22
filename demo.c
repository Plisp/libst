#include "pt.h"

int main(void)
{
	PieceTable *pt = pt_new_from_str("lorem\nipsum\n");

	printf("sizeof(struct pnode): %zd sizeof(PieceTable): %zd, lines: %zd\n",
			sizeof(struct pnode),     sizeof(PieceTable));

	printf("root contains: %s\n", pt->tree[0].data, pt->tree[0].info.lfs);

	pt_free(pt);
	return 0;
}
