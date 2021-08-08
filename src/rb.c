/*
 * simple array layout
 */

#include <assert.h>
#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>

#include "st.h"
#include "rblinux/rbtree_augmented.h"

#define HIGH_WATER (1<<12)

enum blktype { HEAP, MMAP };

struct block {
	enum blktype type;
	char *data;
	size_t size; // capacity = HIGH_WATER unless larger
	struct block *next;
};

#define RED 0
#define BLK (1ULL<<63) // this is *not* general purpose
struct slice {
	struct rb_node rb;
	size_t len; // length
	size_t llen; // left subtree length
	char *data;
};

struct slicetable {
	struct rb_root root;
	struct block *blocks;
};

/* simple */

size_t tree_size(struct rb_node *node) {
	if(!node)
		return 0;
	else {
		struct slice *s = rb_entry(node, struct slice, rb);
		return s->llen + s->len + tree_size(s->rb.rb_right);
	}
}

size_t st_size(const SliceTable *st) {
	return tree_size(st->root.rb_node);
}

size_t tree_depth(struct rb_node *node) {
	if(!node)
		return 0;
	else {
		return 1+ MAX(tree_depth(node->rb_left), tree_depth(node->rb_right));
	}
}

int st_depth(const SliceTable *st) {
	return tree_depth(st->root.rb_node);
}

size_t st_node_count(const SliceTable *st) {
	size_t count = 0;
	struct rb_node *rb = rb_first(&st->root);
	while(rb) {
		count++;
		rb = rb_next(rb);
	};
	return count;
}

static void print_slice(struct slice *slice)
{
	fprintf(stderr, "%s|llen: %zd, %zd|%s",
		rb_is_red(&slice->rb) ? "\e[38;5;1m" : "",
		slice->llen, slice->len,
		rb_is_red(&slice->rb) ? "\e[0m" : "");
}

struct q {
	int level;
	struct rb_node *rb;
};

#define QSIZE 100000
struct q queue[QSIZE]; // a ring buffer
int tail = 0, head = 0;

static void enqueue(struct q q) {
	assert(head == tail || head % QSIZE != tail % QSIZE);
	queue[head++ % QSIZE] = q;
}

static struct q *dequeue(void) {
	return (tail == head) ? NULL : &queue[tail++ % QSIZE];
}

void st_pprint(const SliceTable *st)
{
	if(st->root.rb_node)
		enqueue((struct q){ 0, st->root.rb_node});
	struct q *next;
	int lastlevel = 0;
	while((next = dequeue())) {
		if(lastlevel != next->level)
			puts("");
		if(next->rb) {
			print_slice(rb_entry(next->rb, struct slice, rb));
			enqueue((struct q){ next->level+1, next->rb->rb_left });
			enqueue((struct q){ next->level+1, next->rb->rb_right });
		} else
			fprintf(stderr, "|nul|");
		lastlevel = next->level;
	}
	puts("");
}

void st_dump(const SliceTable *st, FILE *file)
{
	struct rb_node *rb = rb_first(&st->root);
	while(rb) {
		struct slice *s = rb_entry(rb, struct slice, rb);
		fprintf(file, "%.*s", (int)s->len, s->data);
		rb = rb_next(rb);
	}
}

bool st_check_invariants(const SliceTable *st)
{
	return st;
}

void st_print_struct_sizes(void)
{
	fprintf(stderr,
		"Implementation: \e[38;5;1mred-black tree\e[0m\n"
		"sizeof(struct slice): %zd\n"
		"sizeof(PieceTable): %zd\n",
		sizeof(struct slice), sizeof(SliceTable));
}

/* the fun stuff */

static struct block *new_block(const char *data, size_t len)
{
	struct block *new = malloc(sizeof *new);
	new->size = len;
	new->data = malloc(HIGH_WATER);
	memcpy(new->data, data, len);
	// XXX assuming we don't get large >= HIGH_WATER inserts for now
	assert(len <= HIGH_WATER);
	new->type = HEAP;
	return new;
}

static void free_block(struct block *block)
{
	switch(block->type) {
	case HEAP:
		free(block->data);
		free(block);
		break;
	case MMAP:
		munmap(block->data, block->size);
		free(block);
	}
}

SliceTable *st_new(void)
{
	SliceTable *st = malloc(sizeof *st);
	st->root.rb_node = NULL;
	st->blocks = NULL;
	return st;
}

SliceTable *st_new_from_file(const char *path)
{
	int fd = open(path, O_RDONLY);
	if(!fd)
		return NULL;
	size_t len = lseek(fd, 0, SEEK_END);
	if(!len)
		return st_new(); // mmap cannot handle 0-length mappings

	enum blktype type;
	void *data;
	if(len <= HIGH_WATER) {
		data = malloc(HIGH_WATER);
		if(read(fd, data, len) != (long)len) {
			free(data);
			return NULL;
		}
		type = HEAP;
	} else {
		data = mmap(NULL, len, PROT_READ, MAP_SHARED, fd, 0);
		close(fd);
		if(data == MAP_FAILED)
			return NULL;
		type = MMAP;
	}

	SliceTable *st = malloc(sizeof *st);
	struct slice *s = malloc(sizeof *s);
	s->data = data;
	s->len = len;
	s->llen = 0;
	s->rb = (struct rb_node){0};
	st->root.rb_node = &s->rb;
	rb_insert_color(&s->rb, &st->root);

	struct block *init = malloc(sizeof(struct block));
	*init = (struct block){ type, data, len, NULL };
	st->blocks = init;
	return st;
}

SliceTable *st_clone(const SliceTable *st)
{
	return NULL;
}

static void tree_free(struct rb_node *node)
{
	if(node->rb_left)
		tree_free(node->rb_left);
	if(node->rb_right)
		tree_free(node->rb_right);
	free(rb_entry(node, struct slice, rb));
}

void st_free(SliceTable *st)
{
	if(!st)
		return;
	struct block *this, *next = st->blocks;
	while(next) {
		this = next;
		next = next->next; // save pointer before freeing
		free_block(this);
	}
	tree_free(st->root.rb_node);
	free(st);
}

// n.b. does not handle empty table, will not detect pos bounds
struct slice *st_search(SliceTable *st, size_t pos, size_t *off, long delta)
{
	struct slice *s = NULL;
	struct rb_node *node = st->root.rb_node;
	if(pos) {
		while(true) {
			s = rb_entry(node, struct slice, rb);
			//st_dbg("pos: %zd search at: ", pos);
			//print_slice(s);
			//puts("");
			if(pos <= s->llen) {
				s->llen += delta;
				node = node->rb_left;
			} else if (pos <= s->llen + s->len) {
				pos -= s->llen;
				break;
			} else {
				pos -= s->llen + s->len;
				node = node->rb_right;
			}
		}
	} else {
		node = rb_first(&st->root);
		s = rb_entry(node, struct slice, rb);
	}
	*off = pos; // zero is only reached when pos == 0 as llen >= 0
	return s;
}

long delta; // XXX hack for correct propagation
static void augment_propagate(struct rb_node *rb, struct rb_node *stop)
{
	// n.b. these are the two deletion cases where this is ever called
	if(!stop) { // propagation up to root
		if(!rb)
			return;
		struct rb_node *parent = rb_parent(rb);
		while(parent) {
			if(rb == parent->rb_left)
				rb_entry(parent, struct slice, rb)->llen += delta;
			parent = rb_parent(rb);
			rb = parent;
		}
	} else { // propagation due to swapping during deletion
		struct slice *s = rb_entry(stop, struct slice, rb);
		size_t successor_size = s->len;
		do { // occurs at least once - exactly 1 when p = right(n)
			s = rb_entry(rb, struct slice, rb);
			s->llen -= successor_size;
			rb = rb_parent(rb);
		} while (rb != stop);
	}
}

static void augment_copy(struct rb_node *old, struct rb_node *new)
{
	struct slice *old_slice = rb_entry(old, struct slice, rb);
	struct slice *new_slice = rb_entry(new, struct slice, rb);
	new_slice->llen = old_slice->llen;
}

static void augment_rotate(struct rb_node *old, struct rb_node *new)
{
	struct slice *old_slice = rb_entry(old, struct slice, rb);
	struct slice *new_slice = rb_entry(new, struct slice, rb);
	if(new == old->rb_left) // right rotation
		old_slice->llen -= new_slice->len + new_slice->llen;
	else
		new_slice->llen += old_slice->len + old_slice->llen;
}

static const struct rb_augment_callbacks augment_callbacks = {
	augment_propagate, augment_copy, augment_rotate
};

// n.b. does not do bounds checking, only treats boundary insertion
bool st_insert(SliceTable *st, size_t pos, const char *data, size_t len)
{
	if(len == 0)
		return 0;
	char *inserted = NULL;
	if(st->blocks->size + len <= HIGH_WATER) {
		char *point = &st->blocks->data[st->blocks->size];
		memcpy(point, data, len);
		inserted = point;
		st->blocks->size += len;
	} else {
		struct block *old = st->blocks;
		st->blocks = new_block(data, len);
		inserted = st->blocks->data;
		st->blocks->next = old;
	}
	// insert into rbtree
	struct slice *old = st_search(st, pos, &pos, +len);
	if(pos == old->len) {
		// TODO note no direct append optimization implemented
		struct slice *new = malloc(sizeof *new);
		*new = (struct slice){ .llen = 0, .len = len, .data = inserted };
		if(old->rb.rb_right) {
			struct rb_node *n = old->rb.rb_right;
			do {
				old = rb_entry(n, struct slice, rb);
				old->llen += len;
				n = n->rb_left;
			} while (n);
			// old will be left on the correct node
			rb_link_node(&new->rb, &old->rb, &old->rb.rb_left);
		} else {
			rb_link_node(&new->rb, &old->rb, &old->rb.rb_right);
		}
		rb_insert_augmented(&new->rb, &st->root, &augment_callbacks);
		return true;
	} else if(pos == 0) {
		assert(false); // same case but I'm lazy
	} else // XXX split case not treated
		assert(false);
}

bool st_delete(SliceTable *st, size_t pos, size_t len)
{
	if(len == 0)
		return true;
	pos++; // note we never search for the size of st, len == 0 there
	struct slice *old = st_search(st, pos, &pos, -len);
	pos--; // offset is one less
	if(pos + len < old->len) {
		struct slice *new = malloc(sizeof *new);
		*new = (struct slice){
			.llen = 0, .len = old->len - pos - len,
			.data = old->data + pos + len
		};
		old->len = pos; // truncate left part
		if(old->rb.rb_right) {
			struct rb_node *n = old->rb.rb_right;
			do {
				old = rb_entry(n, struct slice, rb);
				old->llen += new->len;
				n = n->rb_left;
			} while (n);
			// old will be left on the correct node
			rb_link_node(&new->rb, &old->rb, &old->rb.rb_left);
		} else {
			rb_link_node(&new->rb, &old->rb, &old->rb.rb_right);
		}
		rb_insert_augmented(&new->rb, &st->root, &augment_callbacks);
		return true;
	} else if(pos == 0 && len == old->len) { // whole piece deletion
		st_dbg("full node case\n");
		delta = -len;
		rb_erase_augmented(&old->rb, &st->root, &augment_callbacks);
	} else // TODO multi-deletion we don't need
		assert(false);
	return true;
}

struct sliceiter {
	struct slice *s;
	char *data;
	size_t offset;
	size_t span;
};

SliceIter *st_iter_new(SliceTable *st, size_t pos)
{
	st_dbg("and that's what st looks like");
	//st_pprint(st);
	//puts("");
	struct sliceiter *it = malloc(sizeof *it);
	struct slice *s = st_search(st, pos, &pos, 0);
	it->offset = pos;
	it->s = s;
	it->span = it->s->len;
	it->data = it->s->data;
	// allow off-end iterators only at end of document for iter_byte
	if(pos == s->len) {
		struct rb_node *next = rb_next(&s->rb);
		if(next) {
			it->s = rb_entry(next, struct slice, rb);
			it->offset = 0;
			it->span = s->len;
			it->data = s->data;
		}
	}
	return it;
}

void st_iter_free(SliceIter *it) { free(it); }

static bool iter_off_end(const SliceIter *it) {
	return it->offset == it->span;
}

char st_iter_byte(const SliceIter *it)
{
	if(iter_off_end(it))
		return -1;
	else
		return *it->data;
}

char st_iter_next_byte(SliceIter *it, size_t count)
{
	while(count--) {
		if(iter_off_end(it))
			return -1;
		if(++(it->offset) == it->span) {
			struct rb_node *next;
			if(next = rb_next(&it->s->rb)) {
				struct slice *s = rb_entry(next, struct slice, rb);
				it->s = s;
				it->offset = 0;
				it->span = s->len;
				it->data = s->data;
			}
		} else
			it->data++;
	}
	return st_iter_byte(it);
}
