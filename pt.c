#include "pt.h"

PieceTable *pt_new_from_str(const char *str)
{
	PieceTable *pt = malloc(sizeof *pt + sizeof(struct pnode));
	pt->cache = NULL;

	pt->edit.data = NULL;
	pt->edit.size = pt->edit.capacity = 0;

	size_t length = strlen(str);
	pt->init.data = malloc(length+1);
	strcpy(pt->init.data, str);
	pt->init.size = length;
	pt->init.type = HEAP;

	struct pnode *tree = pt->tree;
	tree[0] = (struct pnode) { 
		.info = { .bytes = length, .chars = length, .linebrks = 0 },
		.data = pt->init.data
	};
	return pt;
}

void pt_free(PieceTable *pt)
{
	if(pt->init.type == HEAP)
		free(pt->init.data);
	else /*if (pt->init.type == MMAP)*/
		munmap(pt->init.data, pt->init.size);
	free(pt->edit.data);
	free(pt);
}

void pt_insert(PieceTable *pt, size_t pos)
{
	return;
}
