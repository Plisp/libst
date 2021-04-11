/*
 * persistent b+tree slice sequence
 */

#include <assert.h>
#include <limits.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef __unix__
	#include <fcntl.h>
	#include <unistd.h>
	#include <sys/mman.h>
#else
	#error TODO mmap for non-unix systems
#endif

#include "st.h"

#define HIGH_WATER 4

enum blktype { LARGE, LARGE_MMAP, SMALL };
struct block {
	enum blktype type;
	atomic_int refc; // only for LARGE blocks, SMALL ones are cloned
	char *data; // owned by the block
	size_t len;
};

#define SLICESIZE sizeof(struct slice)
#define SZSIZE sizeof(size_t)
struct slice {
	struct block *blk;
	size_t offset;
};

#define NODESIZE (256 - sizeof(atomic_int)) // close enough
#define PER_B (SZSIZE + sizeof(void *))
#define B ((int)(NODESIZE / PER_B))
struct inner {
	atomic_int refc;
	size_t spans[B];
	struct inner *children[B];
};

#define LEAFSIZE (512 - sizeof(atomic_int))
#define PER_BL (SZSIZE + SLICESIZE)
#define BL ((int)(LEAFSIZE / PER_BL))
struct leaf {
	atomic_int refc;
	size_t spans[BL];
	struct slice slices[BL];
};

struct slicetable {
	struct inner *root;
	int levels;
};

/* blocks */

static struct block *new_block(const char *data, size_t len)
{
	struct block *new = malloc(sizeof *new);
	new->len = len;
	new->data = malloc(MAX(HIGH_WATER, len));
	memcpy(new->data, data, len);
	new->type = len > HIGH_WATER ? LARGE : SMALL;
	// we have exclusive access here
	atomic_store_explicit(&new->refc, 1, memory_order_relaxed);
	return new;
}

static void free_block(struct block *block)
{
	switch(block->type) {
		case LARGE_MMAP: munmap(block->data, block->len); break;
		case SMALL:
		case LARGE:
			free(block->data);
	}
	free(block);
}

static void drop_block(struct block *block)
{
	if(atomic_fetch_sub_explicit(&block->refc,1,memory_order_release) == 1) {
		atomic_thread_fence(memory_order_acquire);
		free_block(block);
	}
}

static void block_insert(struct block *block, size_t offset, const char *data,
			 size_t len)
{
	assert(block->type == SMALL);
	assert(offset <= block->len);
	// we maintain the invariant that SMALL blocks->len <= HIGH_WATER
	if(block->len + len > HIGH_WATER)
		block->data = realloc(block->data, block->len + len);
	char *start = block->data + offset;
	memmove(start + len, start, block->len - offset);
	memcpy(start, data, len);
	block->len += len;
	if(block->len > HIGH_WATER)
		block->type = LARGE;
}

static void block_delete(struct block *block, size_t offset, size_t len)
{
	assert(block->type == SMALL);
	assert(offset + len <= block->len);
	char *start = block->data + offset;
	memmove(start, start + len, block->len - offset - len);
	block->len -= len;
}

static void ensure_block_editable(struct block **blkptr)
{
	struct block *blk = *blkptr;
	assert(blk->type == SMALL); // we should never be modifying LARGE blocks
	if(atomic_load_explicit(&blk->refc, memory_order_acquire) != 1) {
		struct block *copy = new_block(blk->data, blk->len);
		drop_block(blk);
		*blkptr = copy;
	}
}

static struct slice new_slice(char *data, size_t len)
{
	struct block *block = new_block(data, len);
	struct slice s = (struct slice){ .blk = block, .offset = 0 };
	return s;
}

/* tree utilities */

static void print_node(int level, const struct inner *node);

static void inner_clrslots(struct inner *node, int from, int to)
{
	assert(to <= B);
	for(int i = from; i < to; i++)
		node->spans[i] = ULONG_MAX;

	memset(&node->children[from], 0, (to - from) * sizeof(void *));
}

static void leaf_clrslots(struct leaf *leaf, int from, int to)
{
	assert(to <= BL);
	for(int i = from; i < to; i++)
		leaf->spans[i] = ULONG_MAX;
}

static struct inner *new_inner(void)
{
	struct inner *node = malloc(sizeof *node);
	inner_clrslots(node, 0, B);
	atomic_store_explicit(&node->refc, 1, memory_order_relaxed);
	return node;
}

static struct leaf *new_leaf(void)
{
	struct leaf *leaf = malloc(sizeof *leaf);
	leaf_clrslots(leaf, 0, BL);
	atomic_store_explicit(&leaf->refc, 1, memory_order_relaxed);
	return leaf;
}

// sums the spans of entries in node, up to fill
static size_t inner_sum(const struct inner *node, int fill)
{
	size_t sum = 0;
	for(int i = 0; i < fill; i++)
		sum += node->spans[i];
	return sum;
}

// sums the spans of entries in leaf, up to fill
static size_t leaf_sum(const struct leaf *leaf, int fill)
{
	size_t sum = 0;
	for(int i = 0; i < fill; i++)
		sum += leaf->spans[i];
	return sum;
}

// returns index of the first key spanning the search key in node
// key contains the offset at the end
static int inner_offset(const struct inner *node, size_t *key)
{
	int i = 0;
	while(*key > node->spans[i])
		*key -= node->spans[i++];
	return i;
}

// returns index of the first key spanning the search key in leaf
// key contains the offset at the end
static int leaf_offset(const struct leaf *leaf, size_t *key)
{
	int i = 0;
	while(*key > leaf->spans[i])
		*key -= leaf->spans[i++];
	return i;
}

// count the number of live entries in node counting up from START
static int inner_fill(const struct inner *node, int start)
{
	int i;
	for(i = start; i < B; i++)
		if(!node->children[i])
			break;
	return i;
}

// count the number of live entries in leaf counting up from start
static int leaf_fill(const struct leaf *leaf, int start)
{
	int i;
	for(i = start; i < BL; i++)
		if(leaf->spans[i] == ULONG_MAX)
			break;
	return i;
}

void drop_node(struct inner *root, int level)
{
	if(level == 1) {
		struct leaf *leaf = (struct leaf *)root;
		if(atomic_fetch_sub_explicit(&leaf->refc,1,memory_order_release) == 1) {
			atomic_thread_fence(memory_order_acquire);
			for(int i = 0; i < leaf_fill(leaf, 0); i++)
				drop_block(leaf->slices[i].blk);
			free(leaf);
		}
	} else // inner node
		if(atomic_fetch_sub_explicit(&root->refc, 1, memory_order_release == 1)) {
			atomic_thread_fence(memory_order_acquire);
			for(int i = 0; i < inner_fill(root, 0); i++)
				drop_node(root->children[i], level - 1);
			free(root);
		}
}


static void incref(atomic_int *refc)
{
	// Should be safe as long as this increment is made visible to another
	// thread T in passing the object to T, avoiding a data race when T reads
	// refc == 1 and proceeds to modify the object inplace whilst our original
	// thread is accessing it.
	// I'm trusting the boost atomics documentation here
	atomic_fetch_add_explicit(refc, 1, memory_order_relaxed);
}

static void ensure_leaf_editable(struct leaf **leafptr)
{
	struct leaf *leaf = *leafptr;
	if(atomic_load_explicit(&leaf->refc, memory_order_acquire) != 1) {
		struct leaf *copy = malloc(sizeof *copy);
		memcpy(copy, leaf, sizeof *copy);
		atomic_store_explicit(&copy->refc, 1, memory_order_relaxed);
		// we now have more references to each block
		for(int i = 0; i < leaf_fill(leaf, 0); i++)
			incref(&leaf->slices[i].blk->refc);
		drop_node((struct inner *)leaf, 1);
		*leafptr = copy;
	}
}

static void ensure_inner_editable(struct inner **innerptr)
{
	struct inner *inner = *innerptr;
	if(atomic_load_explicit(&inner->refc, memory_order_acquire) != 1) {
		struct inner *copy = malloc(sizeof *copy);
		memcpy(copy, inner, sizeof *copy);
		atomic_store_explicit(&copy->refc, 1, memory_order_relaxed);
		// we now have more references
		for(int i = 0; i < inner_fill(inner, 0); i++)
			incref(&inner->children[i]->refc);
		drop_node(inner, 2); // level > 1
		*innerptr = copy;
	}
}

/* simple */

SliceTable *st_new(void)
{
	SliceTable *st = malloc(sizeof *st);
	st->root = (struct inner *)new_leaf();
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

	enum blktype type;
	void *data;
	if(len <= HIGH_WATER) {
		data = malloc(HIGH_WATER);
		lseek(fd, 0, SEEK_SET);
		// FIXME
		if(read(fd, data, len) != len) {
			free(data);
			return NULL;
		}
		type = SMALL;
	} else {
		data = mmap(NULL, len, PROT_READ, MAP_SHARED, fd, 0);
		close(fd);
		if(data == MAP_FAILED)
			return NULL;
		type = LARGE_MMAP;
	}

	SliceTable *st = malloc(sizeof *st);
	struct block *init = malloc(sizeof(struct block));
	*init = (struct block){ type, .refc = 1, data, len };

	struct leaf *leaf = new_leaf();
	leaf->slices[0] = (struct slice){ .blk = init, .offset = 0 };
	leaf->spans[0] = len;
	st->levels = 1;
	st->root = (struct inner *)leaf;
	return st;
}

int st_depth(SliceTable *st) { return st->levels - 1; }

void st_free(SliceTable *st)
{
	drop_node(st->root, st->levels);
	free(st);
}

SliceTable *st_clone(SliceTable *st)
{
	SliceTable *clone = malloc(sizeof *clone);
	clone->levels = st->levels;
	clone->root = st->root;
	if(st->levels == 1)
		incref(&((struct leaf *)st->root)->refc);
	else
		incref(&st->root->refc);
	return clone;
}

size_t st_size(SliceTable *st)
{
	struct inner *root = st->root;
	if(st->levels == 1) {
		return leaf_sum((struct leaf *)root, leaf_fill((struct leaf *)root, 0));
	} else
		return inner_sum(root, inner_fill(root, 0));
}

/* editing utilities */

void demote_slice(struct slice *slice, size_t span) {
	struct block *new = new_block(slice->blk->data + slice->offset, span);
	drop_block(slice->blk);
	slice->blk = new;
	slice->offset = 0;
}

void slice_insert(struct slice *slice, size_t offset, char *data, size_t len,
				size_t *span)
{
	assert(*span <= HIGH_WATER);
	if(slice->blk->type != SMALL)
		demote_slice(slice, *span);
	else
		ensure_block_editable(&slice->blk);

	block_insert(slice->blk, offset, data, len);
	*span += len;
}

void slice_delete(struct slice *slice, size_t offset, size_t len, size_t *span)
{
	assert(*span <= HIGH_WATER);
	if(slice->blk->type != SMALL)
		demote_slice(slice, *span);
	else
		ensure_block_editable(&slice->blk);

	block_delete(slice->blk, offset, len);
	*span -= len;
}

// merge slices in slices, returning the new number of slices.
int merge_slices(size_t spans[static 5], struct slice slices[static 5],
				int fill)
{
	int i = 1;
	while(i < fill) {
		if(spans[i] > HIGH_WATER)
			i += 2; // X|L__ to XL_|_
		else if(spans[i-1] <= HIGH_WATER) {
			// both are smaller S|S, i doesn't move
			slice_insert(&slices[i-1], spans[i-1],
						slices[i].blk->data + slices[i].offset, spans[i],
						&spans[i-1]);
			drop_block(slices[i].blk);
			memmove(&spans[i], &spans[i+1], (fill - (i+1)) * SZSIZE);
			memmove(&slices[i], &slices[i+1], (fill - (i+1)) * SLICESIZE);
			fill--;
		} else
			i++; // L|S_ -> LS|_
	}
	return fill;
}

static struct inner *split_inner(struct inner *node, int offset)
{
	struct inner *split = new_inner();
	int count = B - offset;
	memcpy(&split->spans[0], &node->spans[offset], count * SZSIZE);
	memcpy(&split->children[0], &node->children[offset], count * sizeof(void*));
	inner_clrslots(node, offset, B);
	return split;
}

static struct leaf *split_leaf(struct leaf *leaf, int offset)
{
	struct leaf *split = new_leaf();
	int count = BL - offset;
	memcpy(&split->spans[0], &leaf->spans[offset], count * SZSIZE);
	memcpy(&split->slices[0], &leaf->slices[offset], count * SLICESIZE);
	leaf_clrslots(leaf, offset, BL);
	return split;
}

// steals slots from j into i, returning the total size of slots moved
size_t rebalance_leaves(struct leaf * restrict i, struct leaf * restrict j,
					int ifill, int jfill, bool i_on_left)
{
	size_t delta = 0;
	int count = (ifill + jfill <= BL) ? jfill : (BL/2 + (BL&1) - ifill);
	if(i_on_left) {
		for(int c = 0; c < count; c++) {
			i->spans[ifill+c] = j->spans[c];
			i->slices[ifill+c] = j->slices[c];
			delta += j->spans[c];
		}
		memmove(&j->spans[0], &j->spans[count], (jfill - count) * SZSIZE);
		memmove(&j->slices[0], &j->slices[count], (jfill - count) * SLICESIZE);
		leaf_clrslots(j, jfill - count, jfill);
	} else {
		memmove(&i->spans[count], &i->spans[0], ifill * SZSIZE);
		memmove(&i->slices[count], &i->slices[0], ifill * SLICESIZE);
		for(int c = 0; c < count; c++) {
			i->spans[c] = j->spans[jfill-count+c];
			i->slices[c] = j->slices[jfill-count+c];
			delta += i->spans[c];
		}
		leaf_clrslots(j, jfill - count, jfill);
	}
	return delta;
}

size_t rebalance_inner(struct inner * restrict i, struct inner * restrict j,
					int ifill, int jfill, bool i_on_left)
{
	size_t delta = 0;
	int count = B/2 + (B&1) - ifill;
	if(i_on_left) {
		for(int c = 0; c < count; c++) {
			i->spans[ifill+c] = j->spans[c];
			i->children[ifill+c] = j->children[c];
			delta += i->spans[ifill+c];
		}
		memmove(&j->spans[0], &j->spans[count], (jfill - count) * SZSIZE);
		memmove(&j->children[0], &j->children[count],
				(jfill - count) * sizeof(void *));
		inner_clrslots(j, jfill - count, jfill);
	} else {
		memmove(&i->spans[count], &i->spans[0], ifill * SZSIZE);
		memmove(&i->children[count], &i->children[0], ifill * sizeof(void *));
		for(int c = 0; c < count; c++) {
			i->spans[c] = j->spans[jfill-count+c];
			i->children[c] = j->children[jfill-count+c];
			delta += i->spans[c];
		}
		inner_clrslots(j, jfill - count, jfill);
	}
	return delta;
}

// returns bytes transferred from left to right
size_t merge_boundary(struct inner **lptr, int lfill)
{
	struct leaf *l = (struct leaf *)lptr[0];
	struct leaf *r = (struct leaf *)lptr[1];
	// merge the boundary slices if they're both small
	if(l->spans[lfill-1] <= HIGH_WATER && r->spans[0] <= HIGH_WATER) {
		slice_insert(&r->slices[0], 0,
					l->slices[lfill-1].blk->data + l->slices[lfill-1].offset,
					l->spans[lfill-1], &r->spans[0]);
		drop_block(l->slices[lfill-1].blk);
		size_t delta = l->spans[lfill-1];
		leaf_clrslots(l, lfill - 1, lfill);
		return delta;
	}
	return 0;
}

// removes the jth slot of root
// root(j) **MUST** be editable and its slices must have been moved already
void inner_remove(struct inner *root, int fill, int j)
{
	free(root->children[j]); // slices shifted over
	size_t count = fill - (j+1);
	memmove(&root->spans[j], &root->spans[j+1], count * SZSIZE);
	memmove(&root->children[j], &root->children[j+1], count * sizeof(void *));
	inner_clrslots(root, fill - 1, fill);
}

/* the complex stuff */

typedef long (*leaf_case)(struct leaf *leaf, size_t pos, long *span,
						struct leaf **split, size_t *splitsize, void *ctx);

static long edit_recurse(int level, struct inner *root,
						size_t pos, long *span,
						leaf_case base_case, void *ctx,
						struct inner **split, size_t *splitsize)
{
	if(level == 1) {
		return base_case((void*)root, pos, span, (void*)split, splitsize, ctx);
	} else { // level > 1: inner node recursion
		struct inner *childsplit = NULL;
		size_t childsize = 0;
		int i = inner_offset(root, &pos);

		if(level == 2)
			ensure_leaf_editable((void*)&root->children[i]);
		else
			ensure_inner_editable(&root->children[i]);

		long delta = edit_recurse(level - 1, root->children[i], pos, span,
								base_case, ctx, &childsplit, &childsize);
		st_dbg("applying upwards delta at level %d: %ld\n", level, delta);
		root->spans[i] += delta;
		// reset delta
		delta = *span;

		if(childsize) {
			if(childsplit) { // overflow: attempt to insert childsplit at i+1
				i++;
				int fill = inner_fill(root, i);
				if(fill == B) {
					fill = B / 2 + (i > B / 2);
					*split = split_inner(root, fill);
					*splitsize = inner_sum(*split, B - fill);
					delta -= *splitsize;
					if(i > B / 2) {
						delta -= childsize;
						*splitsize += childsize;
						root = *split;
						i -= fill;
					}
				}
				size_t *start = &root->spans[i];
				struct inner **cstart = &root->children[i];
				memmove(start + 1, start, (fill - i) * SZSIZE);
				memmove(cstart + 1, cstart, (fill - i) * sizeof(void*));
				root->spans[i] = childsize;
				root->children[i] = childsplit;
			} else { // children[i] underflowed
				st_dbg("handling underflow at %d, level %d\n", i, level);
				int j = i > 0 ? i-1 : i+1;
				int jfill;
				int fill = inner_fill(root, i);
				if(level == 2) {
					jfill = leaf_fill((void *)root->children[j], 0);
					ensure_leaf_editable((void *)&root->children[j]);
					long shifted = 0;
					// handle two adjacent SMALL slices immediately
					size_t res;
					if(i < j) {
						if(res = merge_boundary(&root->children[i], childsize))
							childsize--, shifted -= res;
					} else // j < i
						if(res = merge_boundary(&root->children[j], jfill))
							jfill--, shifted += res;
					// transfer some slots from j to i
					shifted += rebalance_leaves((void *)root->children[i],
											(void *)root->children[j],
											childsize, jfill, i < j);
					root->spans[i] += shifted;
					root->spans[j] -= shifted;
					// j was merged into oblivion
					if(root->spans[j] == 0) {
						inner_remove(root, fill, j); // propagate underflow up
						if(fill - 1 < B/2 + (B&1))
							*splitsize = fill - 1;
					}
				} else {
					jfill = inner_fill(root->children[j], 0);
					ensure_inner_editable(&root->children[j]);
					size_t shifted= rebalance_inner(root->children[i],
												root->children[j],
												childsize, jfill, i < j);
					root->spans[i] += shifted;
					root->spans[j] -= shifted;
					// j was merged
					if(root->spans[j] == 0) {
						inner_remove(root, fill, j);
						if(fill - 1 < B/2 + (B&1))
							*splitsize = fill - 1;
					}
				}
			}
		}
		return delta;
	}
}

/* insertion */

// handle insertion within LARGE slices
static long insert_within_slice(struct leaf *leaf, int fill, 
							int i, size_t off, struct slice *new,
							struct leaf **split, size_t *splitsize)
{
	size_t *left_span = &leaf->spans[i];
	struct slice *left = &leaf->slices[i];
	size_t right_span = *left_span - off;
	size_t newlen = new->blk->len; // n.b. is is necessary to save this delta
	struct slice right = (struct slice) {
		.blk = left->blk,
		.offset = left->offset + off
	};
	left->blk->refc++;
	*left_span = off; // truncate
	// fill tmp
	size_t tmpspans[5];
	struct slice tmp[5];
	int tmpfill = 0;
	if(i > 0) {
		tmpspans[tmpfill] = leaf->spans[i-1];
		tmp[tmpfill++] = leaf->slices[i-1];
	}
	tmpspans[tmpfill] = *left_span;
	tmp[tmpfill++] = *left;
	tmpspans[tmpfill] = newlen;
	tmp[tmpfill++] = *new;
	tmpspans[tmpfill] = right_span;
	tmp[tmpfill++] = right;

	if(i+1 < fill) {
		tmpspans[tmpfill] = leaf->spans[i+1];
		tmp[tmpfill++] = leaf->slices[i+1];
	}
	int newfill = merge_slices(tmpspans, tmp, tmpfill);
	int delta = tmpfill - newfill;
	assert(delta <= 3); // [S][S1|Si|S2][S] -> [L][S], S1+S2 > HIGH_WATER
	st_dbg("merged %d nodes\n", delta);
	if(i > 0) {
		i--, left_span--, left--; // see above
	}
	int realfill = fill - (delta-2);
	if(realfill <= BL) {
		size_t count = fill - (i + (tmpfill-2));
		memmove(left_span + newfill, left_span + (tmpfill-2), count * SZSIZE);
		memmove(left + newfill, left + (tmpfill-2), count * SLICESIZE);
		// when delta == 0, newfill exceeds tmpfill-2 and may overwrite
		// old slots, so we copy afterwards
		memcpy(left_span, tmpspans, newfill * SZSIZE);
		memcpy(left, tmp, newfill * SLICESIZE);
		if(delta > 0)
			leaf_clrslots(leaf, realfill, fill);
		if(realfill < BL/2 + (BL&1))
			*splitsize = realfill; // indicate underflow
		return newlen;
	} else { // realfill > BL: leaf split, we have at most 2 new slices
		size_t spans[BL + 2];
		struct slice slices[BL + 2];
		// copy all data to temporary buffers and distribute
		memcpy(spans, leaf->spans, i * SZSIZE);
		memcpy(slices, leaf->slices, i * SLICESIZE);
		memcpy(&spans[i], tmpspans, newfill * SZSIZE);
		memcpy(&slices[i], tmp, newfill * SLICESIZE);
		int count = fill - (i + (tmpfill-2));
		memcpy(&spans[i+newfill], &leaf->spans[i+tmpfill-2], count * SZSIZE);
		memcpy(&slices[i+newfill], &leaf->slices[i+tmpfill-2],count*SLICESIZE);
		struct leaf *right_split = new_leaf();
		// n.b. we must compute delta directly since merging moves the insert
		size_t oldsum = leaf_sum(leaf, fill) + right_span;
		size_t new_leaf_fill = BL/2 + 1; // B=5 6,7 -> 3,4 in right
		size_t right_fill = realfill - (BL/2 + 1); // B=4 5,6 -> 2,3 in right
		memcpy(leaf->spans, spans, new_leaf_fill * SZSIZE);
		memcpy(leaf->slices, slices, new_leaf_fill * SLICESIZE);
		memcpy(right_split->spans, &spans[new_leaf_fill], right_fill * SZSIZE);
		memcpy(right_split->slices,&slices[new_leaf_fill],right_fill*SLICESIZE);
		leaf_clrslots(leaf, new_leaf_fill, fill);
		leaf_clrslots(right_split, right_fill, BL);
		size_t newsum = leaf_sum(leaf, new_leaf_fill);
		*splitsize = leaf_sum(right_split, right_fill);
		*split = right_split;
		return newsum - oldsum;
	}
}

static long insert_leaf(struct leaf *leaf, size_t pos, long *span,
						struct leaf **split, size_t *splitsize, void *data)
{
	int i = leaf_offset(leaf, &pos);
	int fill = leaf_fill(leaf, i);
	st_dbg("insertion: found slot %d, offset %zu target fill %d\n",
			i, pos, fill);
	size_t len = *span;
	long delta = len;
	bool at_bound = (pos == leaf->spans[i]);
	// if we are inserting at 0, pos will be 0
	if(pos == 0 && leaf->spans[0] <= HIGH_WATER) {
		assert(i == 0);
		slice_insert(&leaf->slices[0], 0, data, len, &leaf->spans[0]);
	}
	else if(at_bound && leaf->spans[i] <= HIGH_WATER) {
		slice_insert(&leaf->slices[i], pos, data, len, &leaf->spans[i]);
	} // try start of i+1
	else if(at_bound && (i < fill-1) && leaf->spans[i+1] <= HIGH_WATER) {
		slice_insert(&leaf->slices[i+1], 0, data, len, &leaf->spans[i+1]);
	} // insertion on boundary [L]|[L], no merging possible
	else if(at_bound || pos == 0) {
		i += at_bound; // if at_bound, we are inserting the slice at index i+1
		if(fill == BL) {
			fill = BL / 2 + (i > BL / 2);
			*split = split_leaf(leaf, fill);
			*splitsize = leaf_sum((struct leaf *)*split, BL - fill);
			delta -= *splitsize;
			if(i > BL / 2) {
				delta -= len;
				*splitsize += len;
				leaf = (struct leaf *)*split;
				i -= fill;
			}
		}
		memmove(&leaf->spans[i+1], &leaf->spans[i], (fill - i) * SZSIZE);
		memmove(&leaf->slices[i+1], &leaf->slices[i], (fill - i) * SLICESIZE);
		leaf->spans[i] = len;
		leaf->slices[i] = new_slice(data, len);
	} else {
		struct slice new = new_slice(data, len);
		return insert_within_slice(leaf, fill, i, pos, &new, split, splitsize);
	}
	return delta;
}

void st_insert(SliceTable *st, size_t pos, char *data, size_t len)
{
	if(len == 0)
		return;

	st_dbg("st_insert at pos %zd of len %zd\n", pos, len);
	struct inner *split = NULL;
	size_t splitsize;
	long span = (long)len;
	if(st->levels == 1)
		ensure_leaf_editable((struct leaf **)&st->root);
	else
		ensure_inner_editable(&st->root);

	edit_recurse(st->levels, st->root, pos, &span, &insert_leaf, data,
				&split, &splitsize);
	// handle root underflow
	if(st->levels > 1 && inner_fill(st->root, 0) == 1) {
		st_dbg("handling root underflow\n");
		st->root = st->root->children[0];
		st->levels--;
	}
	// handle root split
	if(split) {
		st_dbg("allocating new root\n");
		struct inner *newroot = new_inner();
		newroot->spans[0] = st_size(st);
		newroot->children[0] = st->root; // we only switched the pointer
		newroot->spans[1] = splitsize;
		newroot->children[1] = split;
		st->root = newroot;
		st->levels++;
	}
}

/* deletion */

static int delete_within_slice(struct leaf *leaf, int fill, int i,
								size_t off, size_t len)
{
	size_t *slice_span = &leaf->spans[i];
	struct slice *slice = &leaf->slices[i];
	if(*slice_span <= HIGH_WATER) {
		slice_delete(slice, off, len, slice_span);
		return fill;
	} else {
		size_t new_right_span = *slice_span - off - len;
		struct slice new_right = (struct slice){
			.blk = slice->blk,
			.offset = slice->offset + off + len
		};
		slice->blk->refc++;
		*slice_span = off; // truncate slice

		size_t tmpspans[5];
		struct slice tmp[5];
		int tmpfill = 0;
		if(i > 0) {
			tmpspans[tmpfill] = leaf->spans[i-1];
			tmp[tmpfill++] = leaf->slices[i-1];
		}
		tmpspans[tmpfill] = *slice_span;
		tmp[tmpfill++] = *slice;
		tmpspans[tmpfill] = new_right_span;
		tmp[tmpfill++] = new_right;

		if(i+1 < fill) {
			tmpspans[tmpfill] = leaf->spans[i+1];
			tmp[tmpfill++] = leaf->slices[i+1];
		}
		// clearly we can create at most one extra slice
		// unmergeable [L]*[L] -> [L]*[X]|[L] <=> full leaf +1 overflow
		// delta == 0 means +1 for new_right being inserted
		int newfill = merge_slices(tmpspans, tmp, tmpfill);
		int delta = tmpfill - newfill;
		assert(delta <= 3); // [S][S|S][S] -> [S]
		int realfill = fill - (delta-1);

		if(realfill > BL)
			return BL + 1;
		st_dbg("merged %d nodes\n", delta);
		if(i > 0) {
			i--, slice_span--, slice--; // see above
		}
		int count = fill - (i + (tmpfill-1)); // exclude new_right
		memmove(slice_span + newfill, slice_span + (tmpfill-1), count * SZSIZE);
		memmove(slice + newfill, slice + (tmpfill-1), count * SLICESIZE);
		memcpy(slice_span, tmpspans, newfill * SZSIZE);
		memcpy(slice, tmp, newfill * SLICESIZE);
		if(delta > 0)
			leaf_clrslots(leaf, realfill, fill);
		return realfill;
	}
}

// span is negative to indicate deltas for partial deletions
// TODO use ctx to indicate deleted lfs
static long delete_leaf(struct leaf *leaf, size_t pos, long *span,
						struct leaf **split, size_t *splitsize, void *ctx)
{
	int i = leaf_offset(leaf, &pos);
	int fill = leaf_fill(leaf, i);
	st_dbg("deletion: found slot %d, offset %zd, target fill %d\n",
			i, pos, fill);
	size_t len = -*span;

	if(pos + len < leaf->spans[i]) {
		size_t oldspan = leaf->spans[i];
		size_t delta = -len;
		int newfill = delete_within_slice(leaf, fill, i, pos, len);
		if(newfill > BL) {
			assert(newfill == BL+1);
			st_dbg("deletion within piece: overflow\n");
			leaf->spans[i] = oldspan; // restore for right_span calculation
			size_t right_span = leaf->spans[i] - pos - len;
			struct slice new_right = {
				.blk = leaf->slices[i].blk,
				.offset = leaf->slices[i].offset + pos + len
			};
			leaf->slices[i].blk->refc++;
			leaf->spans[i] = pos;
			i++;
			// fill == BL, we must split
			fill = BL / 2 + (i > BL / 2);
			*split = split_leaf(leaf, fill);
			*splitsize = leaf_sum((struct leaf *)*split, BL - fill);
			delta -= *splitsize; // - len - splitsize
			if(i > BL / 2) {
				delta -= right_span;
				*splitsize += right_span;
				leaf = (struct leaf *)*split;
				i -= fill;
			}
			size_t n = fill - i;
			memmove(&leaf->spans[i+1], &leaf->spans[i], n * SZSIZE);
			memmove(&leaf->slices[i+1], &leaf->slices[i], n * SLICESIZE);
			leaf->spans[i] = right_span;
			leaf->slices[i] = new_right;
		}
		else if(newfill < BL/2 + (BL&1)) // underflow
			*splitsize = newfill;
		return delta;
	} else { // pos + len > leaf->spans[i]
		int start = i;
		if(pos > 0) {
			len -= leaf->spans[i] - pos; // no. deleted characters remaining
			leaf->spans[i] = pos; // n.b. may become zero, will be merged
			start++;
		}
		int end = start;
		while(end < fill && len >= leaf->spans[end]) {
			drop_block(leaf->slices[end].blk);
			len -= leaf->spans[end];
			end++;
		}
		if(end < fill) {
			if(leaf->spans[end] <= HIGH_WATER)
				slice_delete(&leaf->slices[end], 0, len, &leaf->spans[end]);
			else {
				leaf->slices[end].offset += len;
				leaf->spans[end] -= len;
			}
			len = 0;
		}
		memmove(&leaf->spans[start], &leaf->spans[end], (fill-end)*SZSIZE);
		memmove(&leaf->slices[start], &leaf->slices[end],(fill-end)*SLICESIZE);
		int oldfill = fill;
		fill = start + fill-end;
		size_t tmpspans[5];
		struct slice tmp[5];
		// it's this simple! n.b. start may be truncated. Thus use start - 2
		start = MAX(0, start - 2);
		int tmpfill = MIN(fill - start, 4); // [][s|][|e][]
		memcpy(tmpspans, &leaf->spans[start], tmpfill * SZSIZE);
		memcpy(tmp, &leaf->slices[start], tmpfill * SLICESIZE);
		int newfill = merge_slices(tmpspans, tmp, tmpfill);
		st_dbg("merged %d nodes\n", tmpfill - newfill);
		fill -= tmpfill - newfill;
		memcpy(&leaf->spans[start], tmpspans, newfill * SZSIZE);
		memcpy(&leaf->slices[start], tmp, newfill * SLICESIZE);
		leaf_clrslots(leaf, fill, oldfill);

		if(fill < BL/2 + (BL&1))
			*splitsize = fill;
		return *span += len;
	}
}

void st_delete(SliceTable *st, size_t pos, size_t len)
{
	len = MIN(len, st_size(st) - pos);
	if(len == 0)
		return;

	st_dbg("st_delete at pos %zd of len %zd\n", pos, len);
	struct inner *split = NULL;
	size_t splitsize;
	// we only need to ensure root uniqueness once
	if(st->levels == 1)
		ensure_leaf_editable((struct leaf **)&st->root);
	else
		ensure_inner_editable(&st->root);
	do {
		long remaining = -len;
		// n.b. remaining is bytes left to delete.
		st_dbg("deleting... %ld bytes remaining\n", remaining);
		edit_recurse(st->levels, st->root, pos, &remaining,
					&delete_leaf, NULL, &split, &splitsize);
		len += remaining; // adjusted to byte delta (e.g. -3)
		// handle underflow
		if(st->levels > 1 && inner_fill(st->root, 0) == 1) {
			st_dbg("handling root underflow\n");
			st->root = st->root->children[0];
			st->levels--;
		}
		// handle root split
		if(split) {
			st_dbg("allocating new root\n");
			struct inner *newroot = new_inner();
			newroot->spans[0] = st_size(st);
			newroot->children[0] = st->root;
			newroot->spans[1] = splitsize;
			newroot->children[1] = split;
			st->root = newroot;
			st->levels++;
		}
	} while(len > 0);
}

/* debugging */

void st_print_struct_sizes(void)
{
	printf(
		"Implementation: \e[38;5;1mpersistent b+-tree\e[0m\n"
		"B=%u, BL=%u\n"
		"sizeof(struct inner): %zd\n"
		"sizeof(struct leaf): %zd\n"
		"sizeof(PieceTable): %zd\n",
		B, BL, sizeof(struct inner), sizeof(struct leaf), sizeof(SliceTable)
	);
}

#define LVAL(leaf, i) (leaf->spans[i] != ULONG_MAX ? &leaf->slices[i] : NULL)

static void print_node(int level, const struct inner *node)
{
	char out[256], *it = out;

	if(level == 1) {
		struct leaf *leaf = (struct leaf *)node;
		it += sprintf(it, "[k: ");
		for(int i = 0; i < BL; i++) {
			size_t key = leaf->spans[i];
			if(key != ULONG_MAX)
				it += sprintf(it, "%lu|", key);
			else
				it += sprintf(it, "NUL|");
		}
		it--;
		it += sprintf(it, " p: ");
		for(int i = 0; i < BL; i++) {
			struct slice *val = LVAL(leaf, i);
			it += sprintf(it, "%lu|", val ? val->offset : 0);
		}
		it--;
		sprintf(it, "]");
	} else {
		it += sprintf(it, "[");
		for(int i = 0; i < B; i++) {
			size_t key = node->spans[i];
			if(key != ULONG_MAX)
				it += sprintf(it, "%lu|", key);
			else
				it += sprintf(it, "0|");
		}
		it--;
		sprintf(it, "]");
	}
	fprintf(stderr, "%s ", out);
}

static bool check_recurse(struct inner *root, int height, int level)
{
	if(level == 1) {
		struct leaf *leaf = (struct leaf *)root;
		int fill = leaf_fill(leaf, 0);
		bool fillcheck = (height == 1) || fill >= BL/2 + (BL&1);
		if(!fillcheck) {
			st_dbg("leaf fill violation in ");
			print_node(1, root);
			return false;
		}

		bool last_issmall = false, issmall;
		for(int i = 0; i < fill; i++) {
			issmall = leaf->spans[i] <= HIGH_WATER;
			if(last_issmall && issmall) {
				st_dbg("adjacent slice size violation in slot %d of ", i);
				print_node(1, root);
				return false;
			}
			last_issmall = issmall;
		}
		return true;
	} else {
		int fill = inner_fill(root, 0);
		bool fillcheck = fill >= (level == height ? 2 : B/2 + (B&1));
		if(!fillcheck) {
			st_dbg("inner fill violation in ");
			print_node(level, root);
			return false;
		}

		for(int i = 0; i < fill; i++) {
			struct inner *child = root->children[i];
			int childlevel = level - 1;
			if(!check_recurse(child, height, childlevel))
				return false;

			size_t spansum;
			if(childlevel == 1) {
				struct leaf *leaf = (struct leaf *)child;
				size_t fill = leaf_fill(leaf, 0);
				spansum = leaf_sum(leaf, fill);
			} else {
				size_t fill = inner_fill(child, 0);
				spansum = inner_sum(child, fill);
			}
			if(spansum != root->spans[i]) {
				st_dbg("child span violation in slot %d of ", i);
				print_node(level, root);
				st_dbg("with child sum: %zd span %zd\n",spansum,root->spans[i]);
				return false;
			}
		}
		return true;
	}
}

bool st_check_invariants(SliceTable *st)
{
	return check_recurse(st->root, st->levels, st->levels);
}

/* global queue */

struct q {
	int level;
	struct inner *node;
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

void st_pprint(SliceTable *st)
{
	enqueue((struct q){ st->levels, st->root });
	struct q *next;
	int lastlevel = 1;
	while((next = dequeue())) {
		if(lastlevel != next->level)
			puts("");
		print_node(next->level, next->node);
		if(next->level > 1)
			for(int i = 0; i < inner_fill(next->node, 0); i++)
				enqueue((struct q){ next->level-1, next->node->children[i] });
		lastlevel = next->level;
	}
	puts("");
}

void st_dump(SliceTable *st, FILE *file)
{
	enqueue((struct q){ st->levels, st->root });
	struct q *next;
	while((next = dequeue())) {
		if(next->level > 1)
			for(int i = 0; i < inner_fill(next->node, 0); i++)
				enqueue((struct q){ next->level-1, next->node->children[i] });
		else { // start dumping
			struct leaf *leaf = (struct leaf *)next->node;
			for(int i = 0; i < leaf_fill(leaf, 0); i++) {
				fprintf(file, "%.*s", (int)leaf->spans[i], // FIXME use write()
						leaf->slices[i].blk->data + leaf->slices[i].offset);
			}
		}

	}
}

/* dot output */

#include "dot.h"

static void slice_to_dot(FILE *file, const struct slice *slice)
{
	char *tmp = NULL, *port = NULL;

	if(!slice) {
		graph_table_entry(file, NULL, NULL);
		return;
	}
	FSTR(tmp, "offset: %lu", slice->offset);
	FSTR(port, "%ld", (long)slice);
	graph_table_entry(file, tmp, port);

	free(tmp);
	free(port);
}

static void leaf_to_dot(FILE *file, const struct leaf *leaf)
{
	char *tmp = NULL, *port = NULL;
	graph_table_begin(file, leaf, "aquamarine3");

	for(int i = 0; i < BL; i++) {
		size_t key = leaf->spans[i];
		if(key != ULONG_MAX) {
			FSTR(tmp, "%lu", key);
			graph_table_entry(file, tmp, NULL);
		} else
			graph_table_entry(file, NULL, NULL);
	}
	for(int i = 0; i < BL; i++)
		slice_to_dot(file, LVAL(leaf, i));
	graph_table_end(file);
	// output blocks
	for(int i = 0; i < leaf_fill(leaf, 0); i++) {
		const struct slice *slice = LVAL(leaf, i);
		graph_table_begin(file, slice->blk, "darkgreen");
		FSTR(tmp, "%.*s", (int)slice->blk->len, (char *)slice->blk->data);
		graph_table_entry(file, tmp, NULL);
		graph_table_end(file);
		FSTR(port, "%ld", (long)slice);
		graph_link(file, leaf, port, slice->blk, "body");
	}
	free(tmp);
	free(port);
}

static void inner_to_dot(FILE *file, const struct inner *root, int height)
{
	if(!root)
		return;
	if(height == 1)
		return leaf_to_dot(file, (struct leaf *)root);

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
		struct inner *child = root->children[i];
		if(!child)
			break;
		FSTR(tmp, "%d", i);
		graph_link(file, root, tmp, child, "body");
		inner_to_dot(file, child, height - 1);
	}
	free(tmp);
	free(port);
}

bool st_to_dot(SliceTable *st, const char *path)
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
		inner_to_dot(file, st->root, st->levels);

	graph_end(file);
	free(tmp);
	if(fclose(file))
		goto fail;
	return true;

fail:
	perror("st_to_dot");
	return false;
}
