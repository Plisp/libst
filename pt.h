#ifndef PT_H
#define PT_H

#include <assert.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>

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
extern size_t pt_nodes_moved;
#else
	#define pt_dbg(...)
#endif
// TODO make structs static
struct text_info {
	size_t bytes, lfs;
};

// 56 bytes... what a pity
struct pnode {
	struct text_info left_info; /* store cumulative info of left child */
	struct text_info info;     /* info of this piece */
	char *data;               /* pointer into corresponding text_buffer */
	struct text_buffer *buf; /* only sane way to maintain/resize lfs */
	size_t lf;              /* index into corresponding linefeed buffer */
};

struct location {
	struct pnode *piece;
	size_t off;
};

#define PT_BUFSIZE (1<<20)
enum buftype { HEAP, MMAP };

struct text_buffer {
	enum buftype type;
	size_t size, capacity;
	char *data;
	size_t lf_size, lf_capacity;
	size_t *lfs;
	struct text_buffer *next;
};

typedef struct {
	struct text_buffer *buffers;      /* list of text buffers */
	size_t tree_size, tree_capacity; /* no. of nodes in tree */
	struct pnode *tree;             /* an inorder array of nodes */
} PieceTable;

struct undo {
	union {
		PieceTable clone;
		struct {
			struct pnode *old;
			size_t old_len;
			struct pnode *new;
			size_t new_len;
			size_t off;
		} record;
	};
	bool is_clone;
	bool is_undone;
};

/* internal */

struct location pt_get_piece(PieceTable *pt, size_t pos);
void pt_insert_piece(PieceTable *pt, struct pnode new, struct pnode *at);
void pt_delete_piece(PieceTable *pt, struct pnode *at);

/* API */

PieceTable *pt_new_from_data(const char *data, size_t len);
PieceTable *pt_new_from_file(const char *path, size_t len, size_t off);
void pt_free(PieceTable *pt);

bool pt_insert(PieceTable *pt, size_t pos, char *data, size_t len, struct undo *undo);
bool pt_insert_file(PieceTable *pt, size_t pos, const char *path, size_t len, size_t off,
		struct undo *undo);
bool pt_erase(PieceTable *pt, size_t pos, size_t len, struct undo *undo);

bool pt_undo(PieceTable *pt, struct undo *undo);
bool pt_redo(PieceTable *pt, struct undo *redo);

void pt_print_node(struct pnode *node);
void pt_print_tree(PieceTable *pt);

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
