#ifndef PT_HEADER
#define PT_HEADER

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <sys/mman.h>


struct text_info {
	size_t bytes, chars, lfs;
};

struct pnode {
	struct text_info left_info; /* store cumulative info of left child */
	struct text_info info;      /* info of this piece */
	char *text;                 /* pointer into corresponding text_buffer */
	void *data;                 /* bloat the struct to 64 bytes */
};

#define BUFSIZE (1<<20)
enum buftype { HEAP, MMAP };

struct text_buffer {
	enum buftype type;
	size_t size, capacity;
	char *data;
};

typedef struct {
	size_t bytes, chars, lfs;      /* total bytes, chars and linefeeds */
	struct text_buffer init;       /* text buffer containing original text */
	struct text_buffer **edits;    /* array of text buffers containing inserted text */
	size_t tsize, tcapacity;       /* tree nodes, analogous to c++ */
	struct pnode tree[];           /* FAM so we can copy whole struct at once */
} PieceTable;

PieceTable *pt_new_from_str(const char *str);
PieceTable *pt_new_from_file(const char *path);
void pt_free(PieceTable *pt);

bool pt_undo(PieceTable *pt);
bool pt_redo(PieceTable *pt);

typedef struct {
	struct pnode *node;
	char *text;
	size_t node_offset, node_lf_offset;
	size_t offset, lf_offset;
} Iterator;

Iterator *pt_iterator_get(PieceTable *pt, size_t pos);

void pt_iterator_next_byte(Iterator *it, size_t count);
void pt_iterator_prev_byte(Iterator *it, size_t count);

void pt_iterator_next_line(Iterator *it, size_t count);
void pt_iterator_next_line(Iterator *it, size_t count);

size_t pt_iterator_cpslen(Iterator *it, size_t count);
void pt_iterator_cps_at(Iterator *it, char *c);
void pt_iterator_next_cp(Iterator *it, size_t count);
void pt_iterator_prev_cp(Iterator *it, size_t count);

void pt_iterator_insert(Iterator *it, char *str);
void pt_iterator_erase(Iterator *it, size_t count);

void pt_insert(PieceTable *pt, size_t pos);
void pt_insert(PieceTable *pt, size_t pos);
void pt_erase(PieceTable *pt, size_t pos, size_t len);

#endif
