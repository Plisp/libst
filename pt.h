#ifndef PT_HEADER
#define PT_HEADER

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>

struct text_info {
	size_t bytes, chars, linebrks;
};

struct pnode {
	struct text_info left_info; /* store cumulative info of left child */
	struct text_info info;      /* info of this piece */
	char *text;                 /* pointer into corresponding text_buffer */
	void *data;
};

enum buftype { HEAP, MMAP };

struct text_buffer {
	enum buftype type;
	size_t size, capacity;
	char *data;
};

typedef
struct piece_table {
	size_t bytes, chars, linebrks; /* total bytes, chars and linebreaks */
	struct text_buffer init;       /* text buffer containing original text */
	struct text_buffer edit;       /* text buffer containing inserted text */
	struct pnode *cache;           /* most recently edited node */
	size_t tsize, tcapacity;       /* tree nodes, analogous to c++ */
	struct pnode tree[];           /* FAM so we can copy whole struct at once */
} PieceTable;

PieceTable *pt_new_from_str(const char *str);
PieceTable *pt_new_from_file(const char *path);
void pt_free(PieceTable *pt);

void pt_insert(PieceTable *pt, size_t pos);
void pt_erase(PieceTable *pt, size_t pos, size_t len);

#endif
