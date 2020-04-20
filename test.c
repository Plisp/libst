#include "pt.h"

int main(void)
{
	PieceTable *pt = pt_new_from_str("lorem ipsum");

	printf("sizeof(struct pnode): %zd sizeof(PieceTable): %zd\n",
			sizeof(struct pnode),     sizeof(PieceTable));

	printf("root contains: %s\n", pt->tree[0].data);

	pt_free(pt);
	return 0;
}
