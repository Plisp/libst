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

/* API
 * in general the caller must check that pos <= st_size(st)
 */

SliceTable *st_new(void);
SliceTable *st_new_from_file(const char *path);
void st_free(SliceTable *st);
SliceTable *st_clone(const SliceTable *st);

size_t st_size(const SliceTable *st);

bool st_insert(SliceTable *st, size_t pos, const char *data, size_t len);
bool st_delete(SliceTable *st, size_t pos, size_t len);

bool st_check_invariants(const SliceTable *st);
void st_pprint(const SliceTable *st);
void st_dump(const SliceTable *st, FILE *file);
void st_print_struct_sizes(void);
bool st_to_dot(const SliceTable *st, const char *path);
int st_depth(const SliceTable *st);
size_t st_node_count(const SliceTable *st);

/* read-only iterator */

// it is an error to call any of st_iter_* except st_iter_free after the
// SliceTable instance has been freed or modified

SliceIter *st_iter_new(SliceTable *st, size_t pos);
void st_iter_free(SliceIter *it);
// reinitializes the iterator
SliceIter *st_iter_to(SliceIter *it, size_t pos);

SliceTable *st_iter_st(const SliceIter *it);
size_t st_iter_pos(const SliceIter *it);

char *st_iter_chunk(const SliceIter *it, size_t *len);
bool st_iter_next_chunk(SliceIter *it);
bool st_iter_prev_chunk(SliceIter *it);

int st_iter_byte(const SliceIter *it);
int st_iter_next_byte(SliceIter *it, size_t count);
int st_iter_prev_byte(SliceIter *it, size_t count);

long st_iter_cp(const SliceIter *it);
long st_iter_next_cp(SliceIter *it, size_t count);
long st_iter_prev_cp(SliceIter *it, size_t count);

bool st_iter_next_line(SliceIter *it, size_t count);
bool st_iter_prev_line(SliceIter *it, size_t count);

//size_t st_iter_visual_col(const SliceIter *it);
