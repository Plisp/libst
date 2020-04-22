#include "pt.h"

static void count_chars_lfs(char *s, size_t bytes, size_t *chars, size_t *lfs)
{
	size_t char_count, lf_count = 0;
	for(int i = 0; i < bytes; i++)
		if(!(s[i] & 0x80)) {
			if(s[i] == '\n')
				lf_count++;
			char_count++;
		}

	*chars = char_count;
	*lfs = lf_count;
}

PieceTable *pt_new_from_str(const char *str)
{
	PieceTable *pt = malloc(sizeof *pt + sizeof(struct pnode));
	pt->cache = NULL;

	pt->edits = malloc(sizeof(struct text_buffer *) * 2);
	pt->edits[0] = malloc(sizeof(struct text_buffer));
	pt->edits[0]->data = NULL;
	pt->edits[0]->size = pt->edits[0]->capacity = 0;
	pt->edits[1] = NULL;

	size_t length = strlen(str);
	pt->init.data = malloc(length+1);
	strcpy(pt->init.data, str);
	pt->init.size = length;
	pt->init.type = HEAP;

	struct pnode *tree = pt->tree;
	size_t chars, lfs;
	count_chars_lfs(pt->init.data, length, &chars, &lfs);
	tree[0] = (struct pnode) {
		.info = {
			.bytes = length,
			.chars = chars,
			.lfs = lfs
		},
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

	for(int i = 0; pt->edits[i] != NULL; i++) {
		free(pt->edits[i]->data);
		free(pt->edits[i]);
	}
	free(pt->edits);

	free(pt);
}
