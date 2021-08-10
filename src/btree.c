/*
 * persistent b+tree slice sequence
 */

#include <assert.h>
#include <limits.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef __unix__
	#include <fcntl.h>
	#include <unistd.h>
	#include <sys/mman.h>
#else
	#error TODO file mapping for non-unix systems
#endif

#include "st.h"

#define HIGH_WATER (1<<12)
#define LOW_WATER (HIGH_WATER/2)

#if __x86_64__
	#define USETAGS
#endif

// data is owned by the block - lives as long as the slicetable, same with
// the leaves that immutably point into it. So this is safe, but how in rust?
enum blktype { HEAP, MMAP };
struct block {
	atomic_int refc; // packed with int below
	enum blktype type;
	char *data;
	size_t len; // needed for mmap
	struct block *next; // for freeing later
};

#define NODESIZE (256 - sizeof(atomic_int)) // close enough
#define PER_B (sizeof(size_t) + sizeof(void *))
#define B ((int)(NODESIZE / PER_B))
struct node {
	atomic_int refc;
	size_t spans[B];
	void *child[B]; // in leaves (level 1), these are data pointers
};

struct slicetable {
	struct node *root;
	struct block *blocks;
	int levels; // we could use tagging but blocks need to be tracked anyways
};

/* blocks */

static void free_block(struct block *block)
{
	switch(block->type) {
		case MMAP: munmap(block->data, block->len); break;
		case HEAP: free(block->data);
	}
	free(block);
}

static void drop_block(struct block *block)
{
	if(atomic_fetch_sub_explicit(&block->refc,1,memory_order_release) == 1) {
		atomic_thread_fence(memory_order_acquire);
		if(block->next)
			drop_block(block->next); // TODO this should be iterative
		free_block(block);
	}
}

static void block_insert(char *block, size_t blocklen, size_t off,
						const char *data, size_t len)
{
	memmove(block + off + len, block + off, blocklen - off);
	memcpy(block + off, data, len);
}

static void block_delete(char *block, size_t blocklen, size_t off, size_t len)
{
	memmove(block + off, block + off + len, blocklen - off - len);
}

/* tree utilities */

static void print_node(const struct node *node, int level);
bool st_check_invariants(const SliceTable *st);

static void node_clrslots(struct node *node, int from, int to)
{
	assert(to <= B);
	for(int i = from; i < to; i++)
		node->spans[i] = ULONG_MAX;

	memset(&node->child[from], 0, (to - from) * sizeof(void *));
}

static struct node *new_node(void)
{
	struct node *node = malloc(sizeof *node);
	node_clrslots(node, 0, B);
	atomic_store_explicit(&node->refc, 1, memory_order_relaxed);
	return node;
}

// sums the spans of entries in node, up to fill
static size_t node_sum(const struct node *node, int fill)
{
	size_t sum = 0;
	for(int i = 0; i < fill; i++)
		sum += node->spans[i];
	return sum;
}

// returns index of the first key spanning the search key in node
// key contains the offset at the end
static int node_offset(const struct node *node, size_t *key)
{
	int i = 0;
	while(*key > node->spans[i])
		*key -= node->spans[i++];
	return i;
}

// count the number of live entries in node counting up from node(start)
static int node_fill(const struct node *node, int start)
{
	int i;
	for(i = start; i < B; i++)
		if(!node->child[i])
			break;
	return i;
}

void drop_node(struct node *root, int level)
{
	if(level == 1) {
		if(atomic_fetch_sub_explicit(&root->refc,1,memory_order_release)==1) {
			atomic_thread_fence(memory_order_acquire);
			for(int i = 0; i < node_fill(root, 0); i++)
				if(root->spans[i] <= HIGH_WATER)
					free(root->child[i]); // free small allocations
			free(root);
		}
	} else // inner node
		if(atomic_fetch_sub_explicit(&root->refc,1,memory_order_release)==1) {
			atomic_thread_fence(memory_order_acquire);
			for(int i = 0; i < node_fill(root, 0); i++)
				drop_node(root->child[i], level - 1);
			free(root);
		}
}


static void incref(atomic_int *refc)
{
	// I'm trusting the boost atomics documentation here
	atomic_fetch_add_explicit(refc, 1, memory_order_relaxed);
}

static void ensure_node_editable(struct node **nodeptr, int level)
{
	struct node *node = *nodeptr;
	if(atomic_load_explicit(&node->refc, memory_order_acquire) != 1) {
		struct node *copy = malloc(sizeof *copy);
		memcpy(copy, node, sizeof *copy);
		atomic_store_explicit(&copy->refc, 1, memory_order_relaxed);
		// in a leaf, copy small data blocks as we modify them inplace
		int fill = node_fill(node, 0);
		if(level == 1) {
			for(int i = 0; i < fill; i++)
				if(node->spans[i] <= HIGH_WATER) {
					char *copy = malloc(HIGH_WATER);
					memcpy(copy, node->child[i], node->spans[i]);
					node->child[i] = copy;
				}
		} else
			for(int i = 0; i < fill; i++)
				incref(&((struct node *)node->child[i])->refc);

		drop_node(node, level);
		*nodeptr = copy;
	}
}

/* simple */

int st_depth(const SliceTable *st) { return st->levels - 1; }

size_t node_count(const struct node *node, int level)
{
	if(level == 1)
		return 1;
	else {
		size_t count = 0;
		for(int i = 0; i < node_fill(node, 0); i++)
			count += node_count(node->child[i], level-1);
		return count;
	}
}

size_t st_node_count(const SliceTable *st)
{
	return node_count(st->root, st->levels);
}

size_t st_size(const SliceTable *st)
{
	return node_sum(st->root, node_fill(st->root, 0));
}

SliceTable *st_new(void)
{
	SliceTable *st = malloc(sizeof *st);
	st->root = new_node();
	st->blocks = NULL;
	st->levels = 1;
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

	SliceTable *st = malloc(sizeof *st);
	void *data;
	if(len <= HIGH_WATER) {
		data = malloc(HIGH_WATER);
		lseek(fd, 0, SEEK_SET);
		if(read(fd, data, len) != (long)len) {
			free(data);
			free(st);
			return NULL;
		}
		st->blocks = NULL;
	} else {
		data = mmap(NULL, len, PROT_READ, MAP_SHARED, fd, 0);
		close(fd);
		if(data == MAP_FAILED) {
			free(st);
			return NULL;
		}
		struct block *init = malloc(sizeof(struct block));
		*init = (struct block){
			.type = MMAP, .refc = 1, .data = data, .len = len, .next = NULL
		};
		st->blocks = init;
	}
	struct node *leaf = new_node();
	leaf->spans[0] = len;
	leaf->child[0] = data;
	st->root = (struct node *)leaf;
	st->levels = 1;
	return st;
}

void st_free(SliceTable *st)
{
	drop_node(st->root, st->levels);
	if(st->blocks)
		drop_block(st->blocks);
	free(st);
}

SliceTable *st_clone(const SliceTable *st)
{
	SliceTable *clone = malloc(sizeof *clone);
	clone->levels = st->levels;
	clone->root = st->root;
	clone->blocks = st->blocks;
	incref(&st->root->refc);
	incref(&st->blocks->refc);
	return clone;
}

/* utilities */

struct block *slice_insert(void **target_ptr, size_t offset,
						const char *data, size_t len, size_t *tspan)
{
	size_t oldspan = *tspan;
	char *target = *target_ptr;
#ifdef USETAGS
	// if TARGET is tagged as LARGE, untag and copy it
	if((uintptr_t)target >> 63) {
		char *new = malloc(HIGH_WATER);
		memcpy(new, (void *)((uintptr_t)target <<1 >>1), oldspan);
		*target_ptr = target = new;
	}
	// untag DATA for copying
	if((uintptr_t)data >> 63)
		data = (char *)((uintptr_t)data <<1 >>1);
#endif
	size_t newspan = oldspan + len;
	*tspan = newspan;

	if(newspan <= HIGH_WATER) {
		block_insert(target, oldspan, offset, data, len);
		return NULL;
	} else {
		struct block *new = malloc(sizeof *new);
		new->len = newspan;
		target = realloc(target, new->len);
		new->data = target;
		memmove(target + offset + len, &target[offset], oldspan - offset);
		memcpy(target + offset, data, len);
		new->type = HEAP;
		atomic_store_explicit(&new->refc, 1, memory_order_relaxed);
		return new;
	}
}

int merge_slices(size_t spans[static 5], char *data[static 5],
				int fill)
{
	int i = 1;
	while(i < fill) {
		if(spans[i] + spans[i-1] <= HIGH_WATER) {
			// We only worry about underfull nodes, so no need to handle split
			slice_insert((void **)&data[i-1], spans[i-1],
						data[i], spans[i], &spans[i-1]);
#ifdef USETAGS // free if not tagged as large
			if(!((uintptr_t)data[i] >> 63))
				free(data[i]);
#else
			free(data[i]);
#endif
			memmove(&spans[i], &spans[i+1], (fill - (i+1)) * sizeof(size_t));
			memmove(&data[i], &data[i+1], (fill - (i+1)) * sizeof(char *));
			fill--;
		} else // couldn't merge, proceed to next pair
			i++;
	}
	return fill;
}

static void slotmove(struct node *n, int to, int from, int count)
{
	memmove(&n->spans[to], &n->spans[from], count * sizeof(size_t));
	memmove(&n->child[to], &n->child[from], count * sizeof(void *));
}

static struct node *split_node(struct node *node, int offset)
{
	struct node *split = new_node();
	int count = B - offset;
	memcpy(&split->spans[0], &node->spans[offset], count * sizeof(size_t));
	memcpy(&split->child[0], &node->child[offset], count * sizeof(void *));
	node_clrslots(node, offset, B);
	return split;
}

// steals slots from j into i, returning the total size of slots moved
size_t rebalance_node(struct node * restrict i, struct node * restrict j,
					int ifill, int jfill, bool i_on_left)
{
	size_t delta = 0;
	int count = (ifill + jfill <= B) ? jfill : (B/2 + (B&1) - ifill);
	if(i_on_left) {
		for(int c = 0; c < count; c++) {
			i->spans[ifill+c] = j->spans[c];
			i->child[ifill+c] = j->child[c];
			delta += i->spans[ifill+c];
		}
		slotmove(j, 0, count, jfill - count);
		node_clrslots(j, jfill - count, jfill);
	} else {
		slotmove(i, count, 0, ifill);
		for(int c = 0; c < count; c++) {
			i->spans[c] = j->spans[jfill-count+c];
			i->child[c] = j->child[jfill-count+c];
			delta += i->spans[c];
		}
		node_clrslots(j, jfill - count, jfill);
	}
	return delta;
}

size_t merge_boundary(struct node **lptr, int lfill)
{
	struct node *l = lptr[0], *r = lptr[1];

	if(l->spans[lfill - 1] + r->spans[0] <= HIGH_WATER) {
		size_t delta = l->spans[lfill-1];
		slice_insert(&r->child[0], 0, l->child[lfill-1], delta, &r->spans[0]);
		free(l->child[lfill-1]);
		node_clrslots(l, lfill - 1, lfill);
		return delta;
	}
	return 0;
}

// removes the jth slot of root
// root(j) **MUST** be editable and its slices must have been moved already
void node_remove(struct node *root, int fill, int j)
{
	free(root->child[j]); // slices shifted over, no need for full drop
	size_t count = fill - (j+1);
	slotmove(root, j, j+1, count);
	node_clrslots(root, fill - 1, fill);
}

/* the complex stuff */

typedef long (*leaf_case)(struct node *leaf, size_t pos, long *span,
						struct node **split, size_t *splitsize, void *ctx);

static long edit_recurse(SliceTable *st, int level, struct node *root,
						size_t pos, long *span,
						leaf_case base_case, void *ctx,
						struct node **split, size_t *splitsize)
{
	if(level == 1)
		return base_case((void*)root, pos, span, (void*)split, splitsize, ctx);
	else { // level > 1: inner node recursion
		struct node *childsplit = NULL;
		size_t childsize = 0;
		int i = node_offset(root, &pos);
		ensure_node_editable((struct node **)&root->child[i], level - 1);

		long delta = edit_recurse(st, level - 1, root->child[i], pos, span,
								base_case, ctx, &childsplit, &childsize);
		st_dbg("applying upwards delta at level %d: %ld\n", level, delta);
		root->spans[i] += delta;
		delta = *span; // is used to update split. reset it now for parents
		if(childsize) {
			if(childsplit) { // overflow: attempt to insert childsplit at i+1
				i++;
				int fill = node_fill(root, i);
				if(fill == B) { // TODO this is repeated x3 clean it up
					fill = B/2 + (i > B/2);
					*split = split_node(root, fill);
					*splitsize = node_sum(*split, B - fill);
					delta -= *splitsize;
					if(i > B/2) {
						delta -= childsize;
						*splitsize += childsize;
						root = *split;
						i -= fill;
					}
				}
				slotmove(root, i+1, i, fill - i);
				root->spans[i] = childsize;
				root->child[i] = childsplit;
			} else { // children[i] underflowed
				st_dbg("handling underflow at %d, level %d\n", i, level);
				int j = i > 0 ? i-1 : i+1;
				int fill = node_fill(root, i);
				long shifted = 0;
				//
				if(childsize == ULONG_MAX)
					root->spans[j = i] = 0; // mark j = i as deleted
				else {
					int jfill = node_fill((void *)root->child[j], 0);
					ensure_node_editable((void *)&root->child[j], level - 1);
					if(level-1 == 1) {
						size_t res;
						if(i < j) {
							if(res = merge_boundary((void *)&root->child[i],
													childsize))
								childsize--, shifted -= res;
						} else // j < i
							if(res = merge_boundary((void *)&root->child[j],
													jfill))
								jfill--, shifted += res;
					}
					shifted += rebalance_node((void *)root->child[i],
											(void *)root->child[j],
											childsize, jfill, i < j);
				}
				root->spans[i] += shifted;
				root->spans[j] -= shifted;
				// j was merged into oblivion
				if(root->spans[j] == 0) {
					node_remove(root, fill, j); // propagate underflow up
					if(fill - 1 < B/2 + (B&1))
						*splitsize = fill - 1;
				}
			}
		}
		return delta;
	}
}

/* insertion */

static long insert_within_slice(struct node *leaf, int fill,
								int i, size_t off, char *new, size_t newlen,
								struct node **split, size_t *splitsize)
{
	size_t right_span = leaf->spans[i] - off;
	char *right;
	// maintain block uniqueness
	if(right_span <= HIGH_WATER) {
		right = malloc(HIGH_WATER);
		memcpy(right, leaf->child[i] + off, right_span);
	} else
		right = leaf->child[i] + off;
	// demote left slice if necessary
	if(leaf->spans[i] > HIGH_WATER && off <= HIGH_WATER) {
		char *new = malloc(HIGH_WATER);
		memcpy(new, leaf->child[i], off);
		leaf->child[i] = new;
	} // then truncate
	leaf->spans[i] = off;
	// fill tmp
	size_t tmpspans[5]; char *tmp[5];
	int tmpfill = 0;
	if(i > 0) {
		tmpspans[tmpfill] = leaf->spans[i-1];
		tmp[tmpfill++] = leaf->child[i-1];
	}
	tmpspans[tmpfill] = leaf->spans[i]; tmp[tmpfill++] = leaf->child[i];
	tmpspans[tmpfill] = newlen;         tmp[tmpfill++] = new;
	tmpspans[tmpfill] = right_span;     tmp[tmpfill++] = right;
	if(i+1 < fill) {
		tmpspans[tmpfill] = leaf->spans[i+1];
		tmp[tmpfill++] = leaf->child[i+1];
	}
	int newfill = merge_slices(tmpspans, tmp, tmpfill);
	int delta = tmpfill - newfill;
	assert(delta <= 3); // [S][S1|Si|S2][S] -> [L][S], S1+S2 > HIGH_WATER
	st_dbg("merged %d nodes\n", delta);
	i -= i>0; // see above
	int realfill = fill - (delta-2);
	if(realfill <= B) {
		size_t count = fill - (i + (tmpfill-2));
		slotmove(leaf, i + newfill, i + tmpfill-2, count);
		// when delta == 0, newfill exceeds tmpfill-2 and may overwrite
		// old slots, so we copy afterwards
		memcpy(&leaf->spans[i], tmpspans, newfill * sizeof(size_t));
		memcpy(&leaf->child[i], tmp, newfill * sizeof(char *));
		if(delta > 2)
			node_clrslots(leaf, realfill, fill);
		if(realfill < B/2 + (B&1))
			*splitsize = realfill; // indicate underflow
		return newlen;
	} else { // realfill > B: leaf split, we have at most 2 new slices
		size_t spans[B + 2]; char *blocks[B + 2];
		// copy all data to temporary buffers and distribute. merge impossible
		memcpy(spans, leaf->spans, i * sizeof(size_t));
		memcpy(blocks, leaf->child, i * sizeof(char *));
		memcpy(&spans[i], tmpspans, newfill * sizeof(size_t));
		memcpy(&blocks[i], tmp, newfill * sizeof(char *));
		int count = fill - (i + tmpfill-2);
		memcpy(&spans[i+newfill], &leaf->spans[i+tmpfill-2], count*sizeof(size_t));
		memcpy(&blocks[i+newfill], &leaf->child[i+tmpfill-2], count*sizeof(char *));
		struct node *right_split = new_node();
		// n.b. we must compute delta directly since merging moves the insert
		size_t oldsum = node_sum(leaf, fill) + right_span;
		size_t new_node_fill = B/2 + 1; // B=5 6,7 -> 3,4 in right
		size_t right_fill = realfill - (B/2 + 1); // B=4 5,6 -> 2,3 in right
		memcpy(leaf->spans, spans, new_node_fill * sizeof(size_t));
		memcpy(leaf->child, blocks, new_node_fill * sizeof(char *));
		memcpy(right_split->spans, &spans[new_node_fill], right_fill*sizeof(size_t));
		memcpy(right_split->child, &blocks[new_node_fill], right_fill*sizeof(char *));
		node_clrslots(leaf, new_node_fill, fill);
		node_clrslots(right_split, right_fill, B);
		size_t newsum = node_sum(leaf, new_node_fill);
		*splitsize = node_sum(right_split, right_fill);
		*split = right_split;
		return newsum - oldsum;
	}
}

struct insert_ctx {
	const char *data;
	SliceTable *st; // for attaching new blocks
};

static long insert_leaf(struct node *leaf, size_t pos, long *span,
						struct node **split, size_t *splitsize, void *ctx)
{
	int i = node_offset(leaf, &pos);
	int fill = node_fill(leaf, i);
	st_dbg("insertion: found slot %d, offset %zu target fill %d\n",
			i, pos, fill);
	size_t len = *span;
	long delta = len;
	bool at_bound = (pos == leaf->spans[i]);
	const char *data = ((struct insert_ctx *)ctx)->data;
	SliceTable *st = ((struct insert_ctx *)ctx)->st;
	// if we are inserting at 0, pos will be 0
	if(pos == 0 && leaf->spans[0]+len <= HIGH_WATER) {
		assert(i == 0);
		if(leaf->spans[0] == ULONG_MAX) { // empty document insertion
			leaf->spans[0] = len;
			leaf->child[0] = malloc(HIGH_WATER);
			memcpy(leaf->child[0], data, len);
		} else
			slice_insert(&leaf->child[0], 0, data, len, &leaf->spans[0]);
	}
	else if(leaf->spans[i]+len <= HIGH_WATER) {
		slice_insert(&leaf->child[i], pos, data, len, &leaf->spans[i]);
	} // try start of i+1
	else if(at_bound && (i < fill-1) && leaf->spans[i+1]+len <= HIGH_WATER) {
		slice_insert(&leaf->child[i+1], 0, data, len, &leaf->spans[i+1]);
	} else { // all has failed, we must make a copy and deal with splitting
		char *copy;
		if(len > HIGH_WATER) {
			copy = malloc(len);
			struct block *new = malloc(sizeof *new);
			new->data = copy;
			new->type = HEAP;
			new->len = len;
			atomic_store_explicit(&new->refc, 1, memory_order_relaxed);
			new->next = st->blocks;
			st->blocks = new; // still pointing, no refc update
		} else {
			copy = malloc(HIGH_WATER);
		}
		memcpy(copy, data, len);
		// insertion on boundary [L]|[L], no merging possible
		if(at_bound || pos == 0) {
			i += at_bound; // if at_bound, we are inserting at index i+1
			if(fill == B) {
				fill = B/2 + (i > B/2);
				*split = split_node(leaf, fill);
				*splitsize = node_sum(*split, B - fill);
				delta -= *splitsize;
				if(i > B/2) {
					delta -= len;
					*splitsize += len;
					leaf = *split;
					i -= fill;
				}
			}
			slotmove(leaf, i+1, i, fill - i);
			leaf->spans[i] = len;
			leaf->child[i] = copy;
		} else
			return insert_within_slice(leaf, fill, i, pos, copy, len,
										split, splitsize);
	}
	return delta;
}

bool st_insert(SliceTable *st, size_t pos, const char *data, size_t len)
{
	if(pos > st_size(st))
		return false;
	if(len == 0)
		return true;

	st_dbg("st_insert at pos %zd of len %zd\n", pos, len);
	struct node *split = NULL;
	size_t splitsize;
	long span = (long)len;
	struct insert_ctx ctx = { .data = data, .st = st };

	ensure_node_editable(&st->root, st->levels);
	edit_recurse(st, st->levels, st->root, pos, &span, &insert_leaf, &ctx,
				&split, &splitsize);
	// handle root underflow
	if(st->levels > 1 && node_fill(st->root, 0) == 1) {
		st_dbg("handling root underflow\n");
		struct node *oldroot = st->root;
		st->root = st->root->child[0];
		free(oldroot);
		st->levels--;
	}
	// handle root split
	if(split) {
		st_dbg("allocating new root\n");
		struct node *newroot = new_node();
		newroot->spans[0] = st_size(st);
		newroot->child[0] = st->root; // we only switched the pointer
		newroot->spans[1] = splitsize;
		newroot->child[1] = split;
		st->root = newroot;
		st->levels++;
	}
	return true;
}

/* deletion */

static int delete_within_slice(struct node *leaf, int fill,
								int i, size_t new_right_span, char *new_right)
{
	size_t tmpspans[5]; char *tmp[5];
	int tmpfill = 0;
	if(i > 0) {
		tmpspans[tmpfill] = leaf->spans[i-1];
		tmp[tmpfill++] = leaf->child[i-1];
	}
	tmpspans[tmpfill] = leaf->spans[i]; tmp[tmpfill++] = leaf->child[i];
	tmpspans[tmpfill] = new_right_span; tmp[tmpfill++] = new_right;
	if(i+1 < fill) {
		tmpspans[tmpfill] = leaf->spans[i+1];
		tmp[tmpfill++] = leaf->child[i+1];
	}
	// clearly we can create at most one extra slice
	// unmergeable [L]*[L] -> [L]*[X]|[L] <=> full leaf +1 overflow
	// delta == 0 means +1 for new_right being inserted
	int newfill = merge_slices(tmpspans, tmp, tmpfill);
	int delta = tmpfill - newfill;
	assert(delta <= 3); // [S][S|S][S] -> [S]
	int realfill = fill - (delta-1);
	if(realfill > B)
		return B + 1;
	st_dbg("merged %d nodes\n", delta);
	i -= i>0;
	int count = fill - (i + (tmpfill-1)); // exclude new_right
	slotmove(leaf, i + newfill, i + tmpfill-1, count);
	memcpy(&leaf->spans[i], tmpspans, newfill * sizeof(size_t));
	memcpy(&leaf->child[i], tmp, newfill * sizeof(char *));
	if(delta > 0)
		node_clrslots(leaf, realfill, fill);
	return realfill;
}

// span is negative to indicate deltas for partial deletions
static long delete_leaf(struct node *leaf, size_t pos, long *span,
						struct node **split, size_t *splitsize, void *ctx)
{
	int i = node_offset(leaf, &pos);
	int fill = node_fill(leaf, i);
	// we search for pos + 1 as we assume our next chunk is in this leaf
	pos--;
	st_dbg("deletion: found slot %d, offset %zd, target fill %d\n",
			i, pos, fill);
	size_t len = -*span; // positive deletion length
	// delete within slice?
	if(pos > 0 && pos + len < leaf->spans[i]) {
		size_t oldspan = leaf->spans[i];
		char *olddata = leaf->child[i];
		size_t delta = -len;
		size_t right_span = oldspan - pos - len;
		char *right;
		// copy right slice's data
		if(right_span <= HIGH_WATER) {
			right = malloc(HIGH_WATER);
			memcpy(right, olddata + pos + len, right_span);
		} else
			right = olddata + pos + len;
		// truncate slice
		leaf->spans[i] = pos;
		// truncation might have resulted in a small block
		bool truncated_large = oldspan > HIGH_WATER && pos <= HIGH_WATER;
		if(truncated_large) {
#ifdef USETAGS
			// assume userspace 0 bits, use high bit tag
			leaf->child[i] = (void *)((uintptr_t)leaf->child[i] | 1ULL<<63);
#else
			char *new = malloc(HIGH_WATER);
			memcpy(new, olddata, pos);
			leaf->child[i] = new;
#endif
		}
		int newfill = delete_within_slice(leaf, fill, i, right_span, right);
#ifdef USETAGS
		// untag and copy if not done already
		// leaf(i) could not have shifted backwards unless it was merged
		if(truncated_large && ((uintptr_t)leaf->child[i] >> 63)) {
			char *new = malloc(HIGH_WATER);
			memcpy(new, (void *)((uintptr_t)leaf->child[i] <<1 >>1),
					leaf->spans[i]);
			leaf->child[i] = new;
		}
#endif
		if(newfill > B) {
			assert(newfill == B+1);
			st_dbg("deletion within piece: overflow\n");
			i++;
			// fill == B, we must split
			fill = B/2 + (i > B/2);
			*split = split_node(leaf, fill);
			*splitsize = node_sum(*split, B - fill);
			delta -= *splitsize; // = -(len + *splitsize)
			if(i > B/2) {
				delta -= right_span;
				*splitsize += right_span;
				leaf = *split;
				i -= fill;
			}
			slotmove(leaf, i+1, i, fill - i);
			leaf->spans[i] = right_span;
			leaf->child[i] = right;
		}
		else if(newfill < B/2 + (B&1)) // underflow
			*splitsize = newfill;
		return delta;
	} else { // pos + len >= leaf->spans[i]
		int start = i;
		if(pos > 0) {
			len -= leaf->spans[i] - pos; // no. deleted characters remaining
			// may need to reallocate after truncation
			if(leaf->spans[i] > HIGH_WATER && pos <= HIGH_WATER) {
				char *new = malloc(HIGH_WATER);
				memcpy(new, leaf->child[i], pos);
				leaf->child[i] = new;
			}
			leaf->spans[i] = pos;
			start++;
		}
		int end = start;
		while(end < fill && len >= leaf->spans[end]) {
			if(leaf->spans[end] <= HIGH_WATER)
				free(leaf->child[end]);
			len -= leaf->spans[end];
			end++;
		}
		if(end < fill) { // if len == 0, st=end nothing happens. that's fine
			if(leaf->spans[end] <= HIGH_WATER) {
				block_delete(leaf->child[end], leaf->spans[end], 0, len);
				leaf->spans[end] -= len;
			} else { // cannot become 0 as the loop would've continued
				leaf->spans[end] -= len;
				// was large, now small needs to be copied
				if(leaf->spans[end] <= HIGH_WATER) {
					char *new = malloc(HIGH_WATER);
					memcpy(new, leaf->child[end], leaf->spans[end]);
					leaf->child[end] = new;
				} else
					leaf->child[end] += len;
			}
			len = 0;
		}
		slotmove(leaf, start, end, fill - end);
		int oldfill = fill;
		fill = start + fill-end;
		size_t tmpspans[5]; char *tmp[5];
		// it's this simple! n.b. start may be truncated. Thus use start - 2
		start = MAX(0, start - 2);
		int tmpfill = MIN(fill - start, 4); // [][s|][|e][]
		memcpy(tmpspans, &leaf->spans[start], tmpfill * sizeof(size_t));
		memcpy(tmp, &leaf->child[start], tmpfill * sizeof(char *));
		// merge and copy in
		int newfill = merge_slices(tmpspans, tmp, tmpfill);
		st_dbg("merged %d nodes\n", tmpfill - newfill);
		fill -= tmpfill - newfill;
		memcpy(&leaf->spans[start], tmpspans, newfill * sizeof(size_t));
		memcpy(&leaf->child[start], tmp, newfill * sizeof(char *));
		// move old entries down
		slotmove(leaf, start+newfill, start+tmpfill, oldfill-(start+tmpfill));
		node_clrslots(leaf, fill, oldfill);
		if(fill < B/2 + (B&1))
			*splitsize = fill ? fill : ULONG_MAX; // ULONG_MAX indicates 0 size
		return *span += len; // span (-) + (len - deleted (+)) = change
	}
}

bool st_delete(SliceTable *st, size_t pos, size_t len)
{
	if(pos + len > st_size(st))
		return false;
	if(len == 0)
		return true;

	st_dbg("st_delete at pos %zd of len %zd\n", pos, len);
	struct node *split = NULL;
	size_t splitsize;
	// we only need to ensure root uniqueness once
	ensure_node_editable(&st->root, st->levels);
	do {
		long remaining = -len;
		// n.b. remaining = bytes *left* to delete.
		st_dbg("deleting... %ld bytes remaining\n", remaining);
		// search for pos + 1 (see above)
		// n.b. we never search for st_size+1 since that entails len = 0
		edit_recurse(st, st->levels, st->root, pos+1, &remaining,
					&delete_leaf, NULL, &split, &splitsize);
		len += remaining; // adjusted to byte delta (e.g. -3)
		// handle underflow
		if(st->levels > 1 && node_fill(st->root, 0) == 1) {
			st_dbg("handling root underflow\n");
			struct node *oldroot = st->root;
			st->root = st->root->child[0];
			free(oldroot);
			st->levels--;
		}
		// handle root split
		if(split) {
			st_dbg("allocating new root\n");
			struct node *newroot = new_node();
			newroot->spans[0] = st_size(st);
			newroot->child[0] = st->root;
			newroot->spans[1] = splitsize;
			newroot->child[1] = split;
			st->root = newroot;
			st->levels++;
		}
		assert(st_check_invariants(st));
	} while(len > 0);
	return true;
}

/* iterator */

struct stackentry {
	struct node *node;
	int idx;
};

#define STACKSIZE 3
struct sliceiter {
	size_t span; // span of current slice
	size_t off; // offset into slice
	char *data;
	size_t pos; // absolute position
	struct node *leaf;
	int node_offset;
	struct stackentry stack[STACKSIZE];
	SliceTable *st;
};

SliceIter *st_iter_to(SliceIter *it, size_t pos)
{
	it->pos = pos;
	// TODO having 2 exceptions is quite ugly
	size_t size = st_size(it->st);
	bool off_end = (pos == size);
	if(pos > 0)
		pos -= off_end;

	struct node *node = it->st->root;
	int level = it->st->levels;
	while(level > 1) {
		int i = 0;
		while(pos && pos >= node->spans[i])
			pos -= node->spans[i++];
		st_dbg("iter_to: found i: %d at level %d\n", i, level);
		int stackidx = level - 2; // level 2 goes at stack[0], etc.
		if(stackidx < STACKSIZE)
			it->stack[stackidx] = (struct stackentry){ node, i };

		node = node->child[i];
		level--;
	}
	struct node *leaf = (struct node *)node;
	it->leaf = leaf;
	// find position within leaf
	int i = 0;
	while(pos && pos >= leaf->spans[i])
		pos -= leaf->spans[i++];

	it->node_offset = i;
	it->span = leaf->spans[i];
	it->off = pos;
	st_dbg("iter_to at leaf: i: %d, pos %zd\n", i, pos);

	if(size > 0) {
		it->data = (char *)leaf->child[i] + pos;
		// we searched for pos - 1
		if(off_end) {
			it->data++;
			it->off++;
		}
	}
	return it;
}

SliceIter *st_iter_new(SliceTable *st, size_t pos)
{
	SliceIter *it = malloc(sizeof *it);
	it->st = st;
	return st_iter_to(it, pos);
}

int iter_stacksize(SliceIter *it)
{
	return MIN(it->st->levels - 1, STACKSIZE);
}

void st_iter_free(SliceIter *it)
{
	// We shouldn't have to manage reference counting of nodes given the
	// invalidation upon freeing/modification of the corresponding slicetable.
	free(it);
}

SliceTable *st_iter_st(const SliceIter *it) { return it->st; }
size_t st_iter_pos(const SliceIter *it) { return it->pos; }

static bool iter_off_end(const SliceIter *it)
{
	return it->off == it->span;
}

bool st_iter_next_chunk(SliceIter *it)
{
	int i = it->node_offset;
	struct node *leaf = it->leaf;
	it->pos += leaf->spans[i] - it->off;
	// fast path: same leaf
	if(i < B-1 && leaf->spans[i+1] != ULONG_MAX) {
		it->node_offset++;
		it->span = leaf->spans[i+1];
		it->off = 0;
		it->data = leaf->child[i+1];
		return true;
	}
	int si = 0;
	struct stackentry *s = &it->stack[si];
	while(si < iter_stacksize(it) && s->idx < B-1 &&
			s->node->spans[s->idx+1] == ULONG_MAX)
		s++, si++;
	// note: condition below is false if off-end
	if(si < iter_stacksize(it)) {
		it->stack[si].idx++;
		while(--si >= 0) {
			it->stack[si].node = it->stack[si+1].node->child[0];
			it->stack[si].idx = 0;
		}
		int leaf_idx = it->stack[0].idx;
		it->leaf = (struct node *)it->stack[0].node->child[leaf_idx];
		it->node_offset = 0;
		it->span = it->leaf->spans[0];
		it->off = 0;
		it->data = it->leaf->child[0];
		return true;
	} else {
		st_dbg("gave up. scanning from root for %zd\n", it->pos);
		st_iter_to(it, it->pos);
		return !iter_off_end(it);
	}
}

bool st_iter_prev_chunk(SliceIter *it)
{
	int i = it->node_offset;
	struct node *leaf = it->leaf;
	if(it->pos == it->off) { // only true in first chunk
		it->off = it->pos = 0;
		it->data = leaf->child[0];
		return false;
	}
	it->pos -= it->off + 1; // = 0 when [X][X|...
	// fast path: same leaf
	if(i > 0) {
		it->node_offset--;
		it->span = it->leaf->spans[i-1];
		it->off = it->span - 1;
		it->data = (char *)leaf->child[i-1] + it->off;
		return true;
	}
	int si = 0;
	struct stackentry *s = &it->stack[si];
	while(si < iter_stacksize(it) && s->idx == 0)
		s++, si++;
	if(si < iter_stacksize(it)) {
		it->stack[si].idx--;
		while(--si >= 0) {
			struct node *parent = it->stack[si+1].node;
			it->stack[si].node = parent->child[node_fill(parent, 0) - 1];
			it->stack[si].idx = node_fill(it->stack[si].node, 0) - 1;
		}
		int leaf_i = it->stack[0].idx;
		struct node *leaf = (struct node *)it->stack[0].node->child[leaf_i];
		int fill = node_fill(leaf, 0);
		it->leaf = leaf;
		it->node_offset = fill - 1;
		it->span = leaf->spans[fill-1];
		it->off = it->span - 1;
		it->data = (char *)leaf->child[fill-1] + it->off;
	} else
		st_iter_to(it, it->pos);
	return true;
}

char *st_iter_chunk(const SliceIter *it, size_t *len)
{
	*len = it->span;
	return it->data - it->off;
}

char st_iter_byte(const SliceIter *it)
{
	return iter_off_end(it) ? -1 : it->data[0];
}

char st_iter_next_byte(SliceIter *it, size_t count)
{
	if(iter_off_end(it))
		return -1;

	size_t left = it->span - it->off;
	if(count < left) {
		it->off += count;
		it->data += count;
		it->pos += count;
		return *it->data;
	} // cursor ends up off end if no next chunk
	st_dbg("iter_next_byte: wanted %zd, had %zd\n", count, left);
	st_iter_next_chunk(it);
	return st_iter_next_byte(it, count - left);
}

char st_iter_prev_byte(SliceIter *it, size_t count)
{
	if(it->pos == 0)
		return -1;

	size_t left = it->off;
	if(count <= left) {
		it->off -= count;
		it->data -= count;
		it->pos -= count;
		return *it->data;
	}
	st_dbg("iter_prev_byte: wanted %zd, had %zd\n", count, left);
	st_iter_prev_chunk(it);
	return st_iter_prev_byte(it, count - left);
}

// Assume utf-8
// Assume that codepoints are not split across leaf boundaries
// if this occurs, it means the user has already corrupted the file,
// which should not be allowed. We never redistribute leaves, only merge
long st_iter_cp(const SliceIter *it)
{
	static const char utf8_len[] = {
		1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
		0, 0, 0, 0, 0, 0, 0, 0, 2, 2, 2, 2, 3, 3, 4, 0
	};
	static const char utf8_lead_masks[] = { 0, 0xFF, 0x1F, 0x0F, 0x07 };
	if(iter_off_end(it)) // otherwise we always have at least one byte
		return -1;
	char lead = *it->data;
	unsigned char len = utf8_len[lead >> 3];
	if(len < it->span - it->off) // incomplete seq
		return -1;
	long cp = lead & utf8_lead_masks[len];
	for(unsigned char i = 1; i < len; i++)
		cp = cp<<6 | (it->data[i] & 0x3F);
	return cp <= 0x10FFFF ? cp : -1;
}

long st_iter_next_cp(SliceIter *it, size_t count)
{
	/*
	size_t chunk_remaining = it->span - it->off;
	char *data;
	// scan complete chunks so no it->off update is needed
	// n.b. count = remaining means we need to at least reach the next chunk
	while(count >= chunk_remaining) {
		data = it->data + 1;
		while(chunk_remaining--) { // could be vectorised?
			if((*data++ & 0xC0) != 0x80) count--;
		}
		if(!st_iter_next_chunk(it))
			return -1;
		// at least chunk_remaining bytes must be checked
		if((chunk_remaining = MIN(it->span, count)) > 0)
			chunk_remaining = it->span;
	}
	*/
	while(count > 0) {
		char byte = st_iter_next_byte(it, 1);
		if(byte == -1) return -1;
		if((byte & 0xC0) != 0x80) count--;
	}
	return st_iter_cp(it);
}

long st_iter_prev_cp(SliceIter *it, size_t count)
{
	while(count > 0) {
		char byte = st_iter_prev_byte(it, 1);
		if(byte == -1) return -1;
		if((byte & 0xC0) != 0x80) count--;
	}
	return st_iter_cp(it);
}

// go forwards count newlines, step forward once
bool st_iter_next_line(SliceIter *it, size_t count)
{
	while(count > 0) {
		char *match = memchr(it->data, '\n', it->span - it->off);
		if(match) {
			size_t delta = match - it->data;
			it->pos += delta;
			it->off += delta;
			it->data = match;
			count--;
			continue;
		}
		if(!st_iter_next_chunk(it))
			return false;
	}
	st_iter_next_byte(it, 1); // it's ok if it's off end
	return true;
}

// go backwards count+1 newlines, step forward once
bool st_iter_prev_line(SliceIter *it, size_t count)
{
	count++;
	while(count > 0) {
		char *match = memrchr(it->data - it->off, '\n', it->off);
		if(match) {
			size_t delta = it->data - match;
			it->pos -= delta;
			it->off -= delta;
			it->data = match;
			count--;
			continue;
		}
		if(!st_iter_prev_chunk(it))
			return false;
	}
	st_iter_next_byte(it, 1);
	return true;
}

/* debugging */

void st_print_struct_sizes(void)
{
	printf(
		"Implementation: \e[38;5;1mpersistent btree\e[0m with B=%u\n"
		"sizeof(struct node): %zd\n"
		"sizeof(PieceTable): %zd\n",
		B, sizeof(struct node), sizeof(SliceTable)
	);
}

static void print_node(const struct node *node, int level)
{
	char out[256], *it = out;

	it += sprintf(it, "[");
	if(level == 1) {
		for(int i = 0; i < B; i++) {
			size_t key = node->spans[i];
			if(key != ULONG_MAX)
				it += sprintf(it, "\e[38;5;%dm%lu|",
							node->spans[i] <= HIGH_WATER ? 2 : 1, key);
			else
				it += sprintf(it, "\e[0mNUL|");
		}
	} else {
		for(int i = 0; i < B; i++) {
			size_t key = node->spans[i];
			it += sprintf(it, (key == ULONG_MAX) ? "NUL|" : "%lu|", key);
		}
	}
	it--;
	sprintf(it, "]\e[0m");
	fprintf(stderr, "%s ", out);
}

static bool check_recurse(struct node *root, int height, int level)
{
	int fill = node_fill(root, 0);
	if(level == 1) {
		bool fillcheck = (height == 1) || fill >= B/2 + (B&1);
		if(!fillcheck) {
			st_dbg("leaf fill violation in ");
			print_node(root, 1);
			return false;
		}

		int lastsize = HIGH_WATER, size;
		for(int i = 0; i < fill; i++) {
			size_t span = root->spans[i];
			if(span == 0) {
				st_dbg("zero span in ");
				print_node(root, 1);
				return false;
			}
			size = span;
			if(lastsize + size <= HIGH_WATER) {
				st_dbg("adjacent slice size violation in slot %d of ", i);
				print_node(root, 1);
				return false;
			}
			lastsize = size;
		}
		return true;
	} else {
		bool fillcheck = fill >= (level == height ? 2 : B/2 + (B&1));
		if(!fillcheck) {
			st_dbg("node fill violation in ");
			print_node(root, 2);
			return false;
		}

		for(int i = 0; i < fill; i++) {
			struct node *child = root->child[i];
			int childlevel = level - 1;
			if(!check_recurse(child, height, childlevel))
				return false;

			size_t spansum;
			if(childlevel == 1) {
				size_t fill = node_fill(child, 0);
				spansum = node_sum(child, fill);
			} else {
				size_t fill = node_fill(child, 0);
				spansum = node_sum(child, fill);
			}
			if(spansum != root->spans[i]) {
				st_dbg("child span violation in slot %d of ", i);
				print_node(root, 2);
				st_dbg("with child sum: %zd span %zd\n",spansum,root->spans[i]);
				return false;
			}
		}
		return true;
	}
}

bool st_check_invariants(const SliceTable *st)
{
	return check_recurse(st->root, st->levels, st->levels);
}

/* global queue */

struct q {
	int level;
	struct node *node;
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
	enqueue((struct q){ st->levels, st->root });
	struct q *next;
	int lastlevel = 1;
	while((next = dequeue())) {
		if(lastlevel != next->level)
			puts("");
		print_node(next->node, next->level);
		if(next->level > 1)
			for(int i = 0; i < node_fill(next->node, 0); i++)
				enqueue((struct q){ next->level-1, next->node->child[i] });
		lastlevel = next->level;
	}
	puts("");
}

void st_dump(const SliceTable *st, FILE *file)
{
	enqueue((struct q){ st->levels, st->root });
	struct q *next;
	while((next = dequeue()))
		if(next->level > 1)
			for(int i = 0; i < node_fill(next->node, 0); i++)
				enqueue((struct q){ next->level-1, next->node->child[i] });
		else // start dumping
			for(int i = 0; i < node_fill(next->node, 0); i++)
				fprintf(file, "%.*s", (int)next->node->spans[i],
						(char *)next->node->child[i]);
}

/* dot output */

#include "dot.h"

static void leaf_to_dot(FILE *file, const struct node *leaf)
{
	char *tmp = NULL, *port = NULL;
	graph_table_begin(file, leaf, "aquamarine3");

	for(int i = 0; i < B; i++) {
		size_t key = leaf->spans[i];
		if(key != ULONG_MAX) {
			FSTR(tmp, "%lu", key);
			graph_table_entry(file, tmp, NULL);
		} else
			graph_table_entry(file, NULL, NULL);
	}
	for(int i = 0; i < B; i++) {
		if(leaf->child[i]) {
			FSTR(tmp, "%.*s", (int)leaf->spans[i], (char *)leaf->child[i]);
			graph_table_entry(file, tmp, NULL);
		} else
			graph_table_entry(file, NULL, NULL);
	}
	graph_table_end(file);
	free(tmp);
	free(port);
}

static void node_to_dot(FILE *file, const struct node *root, int height)
{
	if(!root)
		return;
	if(height == 1)
		return leaf_to_dot(file, (struct node *)root);

	char *tmp = NULL, *port = NULL;
	graph_table_begin(file, root, NULL);

	for(int i = 0; i < B; i++) {
		size_t key = root->spans[i];
		if(key != ULONG_MAX) {
			FSTR(tmp, "%lu", key);
			FSTR(port, "%u", i);
		} else
			tmp = port = NULL;
		graph_table_entry(file, tmp, port);
	}
	graph_table_end(file);

	for(int i = 0; i < B; i++) {
		struct node *child = root->child[i];
		if(!child)
			break;
		FSTR(tmp, "%d", i);
		graph_link(file, root, tmp, child, "body");
		node_to_dot(file, child, height - 1);
	}
	free(tmp);
	free(port);
}

bool st_to_dot(const SliceTable *st, const char *path)
{
	char *tmp = NULL;
	FILE *file = fopen(path, "w");
	if(!file)
		goto fail;
	graph_begin(file);

	graph_table_begin(file, st, NULL);
	FSTR(tmp, "height: %u", st->levels);
	graph_table_entry(file, tmp, NULL);
	graph_table_entry(file, "root", "root");
	graph_table_end(file);

	graph_link(file, st, "root", st->root, "body");
	if(st->root)
		node_to_dot(file, st->root, st->levels);

	graph_end(file);
	free(tmp);
	if(fclose(file))
		goto fail;
	return true;

fail:
	perror("st_to_dot");
	return false;
}
