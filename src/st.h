#pragma once

#include <stdbool.h>
#include <stdio.h>

#define MAX(a,b) ((a)>(b)?(a):(b))
#define MIN(a,b) ((a)<(b)?(a):(b))

#ifndef NDEBUG
	#define st_dbg(...) \
		fprintf(stderr, "\e[38;5;1mdebug:\e[0m " __VA_ARGS__)
#else
	#define st_dbg(...)
#endif

typedef struct slicetable SliceTable;
typedef struct sliceiter SliceIter;

/* API */

SliceTable *st_new(void);
void st_free(SliceTable *st);
SliceTable *st_new_from_file(const char *path);
void st_dump(SliceTable *st, FILE *file);

size_t st_size(SliceTable *st);
size_t st_lfs(SliceTable *st);

// the caller must check that pos <= st_size(st)
// TODO return deleted lfs as size_t
void st_insert(SliceTable *st, size_t pos, char *data, size_t len);
void st_delete(SliceTable *st, size_t pos, size_t len);

bool st_check_invariants(SliceTable *st);
void st_pprint(SliceTable *st);
void st_print_struct_sizes(void);
bool st_to_dot(SliceTable *st, const char *path);

/* iterator */

SliceIter *st_iter_get(SliceTable *st, size_t pos);
SliceIter *st_iter_clone(SliceIter *it);

size_t st_iter_pos(SliceIter *it);
size_t st_iter_visual_col(SliceIter *it);

bool st_iter_next_byte(SliceIter *it, size_t count);
bool st_iter_prev_byte(SliceIter *it, size_t count);

bool st_iter_next_line(SliceIter *it, size_t count);
bool st_iter_next_line(SliceIter *it, size_t count);

bool st_iter_next_cp(SliceIter *it, size_t count);
bool st_iter_prev_cp(SliceIter *it, size_t count);

bool st_iter_get_bytes(SliceIter *it, char *buf, size_t count);

bool st_iter_insert(SliceIter *it, char *data);
bool st_iter_delete(SliceIter *it, size_t count);
