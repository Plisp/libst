#ifndef PT_H
#define PT_H

#include <stdbool.h>

#define max(a,b) ({ \
	__typeof__ (a) _a = (a); \
	__typeof__ (b) _b = (b); \
	_a > _b ? _a : _b; \
})

#define min(a,b) ({ \
	__typeof__ (a) _a = (a); \
	__typeof__ (b) _b = (b); \
	_a < _b ? _a : _b; \
})

#ifndef NDEBUG
	#define pt_dbg(...) fprintf(stderr, "\e[38;5;1mdebug: \e[0m" __VA_ARGS__)
#else
	#define pt_dbg(...)
#endif

size_t pt_nodes_moved;

struct pnode;
struct location;
struct block;
struct undo;
typedef struct piecetable PieceTable;

/* internal */

struct location pt_get_piece(PieceTable *pt, size_t pos);
void pt_insert_piece(PieceTable *pt, struct pnode new, struct pnode *at);
void pt_delete_piece(PieceTable *pt, struct pnode *at);

/* API */

PieceTable *pt_new_from_data(const char *data, size_t len);
PieceTable *pt_new_from_file(const char *path, size_t len, size_t off);
void pt_free(PieceTable *pt);

size_t pt_size(PieceTable *pt);
size_t pt_lfs(PieceTable *pt);

bool pt_insert(PieceTable *pt, size_t pos, char *data, size_t len, struct undo *undo);
bool pt_insert_file(PieceTable *pt, size_t pos, const char *path, size_t len, size_t off, struct undo *undo);
bool pt_erase(PieceTable *pt, size_t pos, size_t len, struct undo *undo);

bool pt_undo(PieceTable *pt, struct undo *undo);
bool pt_redo(PieceTable *pt, struct undo *redo);

void pt_print_node(struct pnode *node);
void pt_print_tree(PieceTable *pt);
void pt_print_struct_sizes();

/* Iterator API */

typedef struct {
	struct pnode *node;
	char *text;
	size_t offset, lf_offset;
} Iterator;

Iterator *pt_iterator_get(PieceTable *pt, size_t pos);

void pt_iterator_next_byte(Iterator *it, size_t count);
void pt_iterator_prev_byte(Iterator *it, size_t count);

void pt_iterator_next_line(Iterator *it, size_t count);
void pt_iterator_next_line(Iterator *it, size_t count);

void pt_iterator_next_cp(Iterator *it, size_t count);
void pt_iterator_prev_cp(Iterator *it, size_t count);

void pt_iterator_get_bytes(Iterator *it, size_t count);

struct undo *pt_iterator_insert(Iterator *it, char *data);
struct undo *pt_iterator_erase(Iterator *it, size_t count);

#endif
