#pragma once

#include <stdbool.h>
#include <stdio.h>

#define MAX(a,b) ((a)>(b)?(a):(b))
#define MIN(a,b) ((a)<(b)?(a):(b))

#ifndef NDEBUG
	#define pt_dbg(...) \
		fprintf(stderr, "\e[38;5;1mdebug:\e[0m " __VA_ARGS__)
#else
	#define pt_dbg(...)
#endif

typedef struct PieceTable PieceTable;
typedef struct PieceIterator PieceIterator;

/* API */

PieceTable *pt_new(void);
void pt_free(PieceTable *pt);
PieceTable *pt_new_from_file(const char *path, long len, long off);
PieceTable *pt_write_file(PieceTable *pt, const char *path);

long pt_size(PieceTable *pt);
long pt_lfs(PieceTable *pt);

void pt_insert(PieceTable *pt, long pos, char *data, long len);
void pt_erase(PieceTable *pt, long pos, long len);

void pt_pprint(PieceTable *pt);
void pt_print_struct_sizes(void);
bool pt_to_dot(PieceTable *pt);

/* iterator */

PieceIterator *pt_iter_get(PieceTable *pt, long pos);
PieceIterator *pt_iter_clone(PieceIterator *it);

long pt_iter_pos(PieceIterator *it);
long pt_iter_visual_col(PieceIterator *it);

bool pt_iter_next_byte(PieceIterator *it, long count);
bool pt_iter_prev_byte(PieceIterator *it, long count);

bool pt_iter_next_line(PieceIterator *it, long count);
bool pt_iter_next_line(PieceIterator *it, long count);

bool pt_iter_next_cp(PieceIterator *it, long count);
bool pt_iter_prev_cp(PieceIterator *it, long count);

bool pt_iter_get_bytes(PieceIterator *it, char *buf, long count);

bool pt_iter_insert(PieceIterator *it, char *data);
bool pt_iter_erase(PieceIterator *it, long count);
