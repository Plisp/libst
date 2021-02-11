#ifndef PT_H
#define PT_H

#include <ctime>

#define MAX(a,b) ((a)>(b)?(a):(b))
#define MIN(a,b) ((a)<(b)?(a):(b))

#ifndef NDEBUG
	#define pt_dbg(...) fprintf(stderr, "\e[38;5;1mdebug:\e[0m " __VA_ARGS__)
#else
	#define pt_dbg(...)
#endif

extern long pt_nodes_moved;

typedef struct PieceTable PieceTable;
typedef struct PieceIterator PieceIterator;

/* API */

PieceTable *pt_new_from_data(const char *data, long len);
PieceTable *pt_new_from_file(const char *path, long len, long off);
void pt_free(PieceTable *pt);

long pt_size(PieceTable *pt);
long pt_lfs(PieceTable *pt);

bool pt_insert(PieceTable *pt, long pos, char *data, long len, struct undo *undo);
bool pt_insert_file(PieceTable *pt, long pos, const char *path, long len, long off, struct undo *undo);
bool pt_erase(PieceTable *pt, long pos, long len, struct undo *undo);

bool pt_undo(PieceTable *pt, struct undo *undo);
bool pt_redo(PieceTable *pt, struct undo *redo);

void pt_print_node(struct pnode *node);
void pt_print_tree(PieceTable *pt);
void pt_print_struct_sizes();

/* PieceIterator API */

PieceIterator *pt_iterator_get(PieceTable *pt, long pos);

void pt_iterator_next_byte(PieceIterator *it, long count);
void pt_iterator_prev_byte(PieceIterator *it, long count);

void pt_iterator_next_line(PieceIterator *it, long count);
void pt_iterator_next_line(PieceIterator *it, long count);

void pt_iterator_next_cp(PieceIterator *it, long count);
void pt_iterator_prev_cp(PieceIterator *it, long count);

void pt_iterator_get_bytes(PieceIterator *it, char *buf, long count);

struct undo *pt_iterator_insert(PieceIterator *it, char *data);
struct undo *pt_iterator_erase(PieceIterator *it, long count);

#endif
