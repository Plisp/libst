/*
 * TODO impl persistent b+tree slice sequence
 */

#include <assert.h>
#include <limits.h>
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

// TODO update this value
#define HIGH_WATER 3

enum blktype { LARGE, LARGE_MMAP, SMALL };
struct block {
	enum blktype type;
	int refs; // only for LARGE blocks
	char *data; // owned by the block
	size_t len;
};

#define SLICESIZE sizeof(struct slice)
#define SZSIZE sizeof(size_t)
struct slice {
	struct block *blk;
	size_t offset;
};

#define NODESIZE 128
#define PER_B (SZSIZE + sizeof(void *))
#define B ((int)(NODESIZE / PER_B))
struct inner {
	size_t spans[B];
	struct inner *children[B];
};

#define PER_BL (SZSIZE + SLICESIZE)
#define BL ((int)(NODESIZE / PER_BL))
struct leaf {
	size_t spans[BL];
	struct slice slices[BL];
};

#define LVAL(leaf, i) (leaf->spans[i] != ULONG_MAX ? &leaf->slices[i] : NULL)

struct slicetable {
	struct inner *root;
	int height;
};

static void print_node(int level, const struct inner *node);

/* blocks */

static struct block *new_block(const char *data, size_t len)
{
	struct block *new = malloc(sizeof *new);
	new->len = len;
	new->data = malloc(MAX(HIGH_WATER, len));
	memcpy(new->data, data, len);

	if(len > HIGH_WATER) {
		new->type = LARGE;
		new->refs = 1;
	} else {
		new->type = SMALL;
		new->refs = 0;
	}
	return new;
}

static void drop_block(struct block *block)
{
	switch(block->type) {
	case SMALL:
		free(block->data);
		free(block);
		break;
	case LARGE_MMAP:
		if(--block->refs == 0) {
			munmap(block->data, block->len);
			free(block);
		}
		break;
	case LARGE:
		if(--block->refs == 0) {
			free(block->data);
			free(block);
		}
		break;
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
	if(block->len > HIGH_WATER) {
		block->type = LARGE;
		block->refs = 1;
	}
}

static void block_delete(struct block *block, size_t offset, size_t len)
{
	assert(block->type == SMALL);
	assert(offset + len <= block->len);
	char *start = block->data + offset;
	memmove(start, start + len, block->len - offset - len);
	block->len -= len;
}

static struct slice new_slice(char *data, size_t len)
{
	struct block *block = new_block(data, len);
	struct slice s = (struct slice){ .blk = block, .offset = 0 };
	return s;
}

/* tree utilities */

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
	return node;
}

static struct leaf *new_leaf(void)
{
	struct leaf *leaf = malloc(sizeof *leaf);
	leaf_clrslots(leaf, 0, BL);
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
	while(*key && *key >= node->spans[i]) {
		*key -= node->spans[i];
		i++;
#ifndef NDEBUG
		if(i == B) assert(!*key); // off end is permitted, terminates loop
#endif
	}
	return i;
}

// returns index of the first key spanning the search key in leaf
// key contains the offset at the end
static int leaf_offset(const struct leaf *leaf, size_t *key)
{
	int i = 0;
	while(*key && *key >= leaf->spans[i]) {
		*key -= leaf->spans[i];
		i++;
#ifndef NDEBUG
		if(i == BL) assert(!*key); // off end is permitted, terminates loop
#endif
	}
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

/* simple */

SliceTable *st_new(void)
{
	SliceTable *st = malloc(sizeof *st);
	st->root = (struct inner *)new_leaf();
	st->height = 1;
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
	*init = (struct block){ type, .refs = 1, data, len };

	struct leaf *leaf = new_leaf();
	leaf->slices[0] = (struct slice){ .blk = init, .offset = 0 };
	leaf->spans[0] = len;
	st->height = 1;
	st->root = (struct inner *)leaf;
	return st;
}

void free_node(struct inner *root, int level)
{
	if(level == 1) {
		struct leaf *leaf = (struct leaf *)root;
		for(int i = 0; i < leaf_fill(leaf, 0); i++)
			drop_block(leaf->slices[i].blk);
		free(leaf);
	} else { // inner node
		for(int i = 0; i < inner_fill(root, 0); i++)
			free_node(root->children[i], level - 1);
		free(root);
	}
}

void st_free(SliceTable *st)
{
	free_node(st->root, st->height);
	free(st);
}

size_t st_size(SliceTable *st)
{
	struct inner *root = st->root;
	if(st->height == 1) {
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
	if(*span <= HIGH_WATER && slice->blk->type != SMALL)
		demote_slice(slice, *span);
	block_insert(slice->blk, offset, data, len);
	*span += len;
}

void slice_delete(struct slice *slice, size_t offset, size_t len, size_t *span)
{
	if(*span <= HIGH_WATER && slice->blk->type != SMALL)
		demote_slice(slice, *span);
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
	memcpy(&split->children[0], &node->children[offset], count * sizeof(void *));
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

// merge j's slots into i, freeing j, returning the total size of slots moved
size_t merge_leaves(struct leaf * restrict i, struct leaf * restrict j,
				int ifill, int jfill)
{
	size_t sum = leaf_sum(j, jfill);
	if(i < j) {
		memcpy(&i->spans[ifill], &j->spans[0], jfill * SZSIZE);
		memcpy(&i->slices[ifill], &j->slices[0], jfill * SLICESIZE);
	} else { // j < i
		memmove(&i->spans[jfill], &i->spans[0], ifill);
		memmove(&i->slices[jfill], &i->slices[0], ifill);
		memcpy(&i->spans[0], &j->spans[0], jfill * SZSIZE);
		memcpy(&i->slices[0], &j->slices[0], jfill * SLICESIZE);
	}
	// free_node is too drastic here, as the slices stick around
	free(j);
	return sum;
}

size_t merge_inner(struct inner * restrict i, struct inner * restrict j,
				int ifill, int jfill)
{
	size_t sum = inner_sum(j, jfill);
	if(i < j) {
		memcpy(&i->spans[ifill], &j->spans[0], jfill * SZSIZE);
		memcpy(&i->children[ifill], &j->children[0], jfill * SLICESIZE);
	} else { // j < i
		memmove(&i->spans[jfill], &i->spans[0], ifill);
		memmove(&i->children[jfill], &i->children[0], ifill);
		memcpy(&i->spans[0], &j->spans[0], jfill * SZSIZE);
		memcpy(&i->children[0], &j->children[0], jfill * SLICESIZE);
	}
	// free_node is too drastic here, as the children stick around
	free(j);
	return sum;
}

// steals slots from j into i, returning the total size of slots moved
size_t balance_leaves(struct leaf * restrict i, struct leaf * restrict j,
					int ifill, int jfill)
{
	size_t delta = 0;
	int count = BL/2 + (BL&1) - ifill;
	if(i < j) {
		for(int c = 0; c < count; c++) {
			i->spans[ifill+c] = j->spans[c];
			i->slices[ifill+c] = j->slices[c];
			delta += i->spans[ifill+c];
		}
		memmove(&j->spans[0], &j->spans[count], (jfill - count) * SZSIZE);
		memmove(&j->slices[0], &j->slices[count], (jfill - count) * SLICESIZE);
		leaf_clrslots(j, jfill - count, jfill);
	} else {
		memmove(&i->spans[count], &i->spans[0], count * SZSIZE);
		memmove(&i->slices[count], &i->slices[0], count * SLICESIZE);
		for(int c = 0; c < count; c++) {
			i->spans[c] = j->spans[jfill-c-1];
			i->slices[c] = j->slices[jfill-c-1];
			delta += i->spans[c];
		}
		leaf_clrslots(j, jfill - count, jfill);
	}
	return delta;
}

size_t balance_inner(struct inner * restrict i, struct inner * restrict j,
					int ifill, int jfill)
{
	size_t delta = 0;
	int count = B/2 + (B&1) - ifill;
	if(i < j) {
		for(int c = 0; c < count; c++) {
			i->spans[ifill+c] = j->spans[c];
			i->children[ifill+c] = j->children[c];
			delta += i->spans[ifill+c];
		}
		memmove(&j->spans[0], &j->spans[count], count * SZSIZE);
		memmove(&j->children[0], &j->children[count], count * SLICESIZE);
		inner_clrslots(j, jfill - count, jfill);
	} else {
		memmove(&i->spans[count], &i->spans[0], count * SZSIZE);
		memmove(&i->children[count], &i->children[0], count * SLICESIZE);
		for(int c = 0; c < count; c++) {
			i->spans[c] = j->spans[jfill-c-1];
			i->children[c] = j->children[jfill-c-1];
			delta += i->spans[c];
		}
		inner_clrslots(j, jfill - count, jfill);
	}
	return delta;
}

typedef long (*leaf_case)(struct leaf *leaf, size_t pos, long *span,
						struct leaf **split, size_t *splitsize, void *ctx);

static long edit_recurse(int level, struct inner *root,
						size_t pos, long *span,
						leaf_case base_case, void *ctx,
						struct inner **split, size_t *splitsize)
{
	if(level == 1) {
		return base_case((void *)root, pos, span, (void*)split, splitsize, ctx);
	} else { // level > 1: inner node recursion
		struct inner *childsplit = NULL;
		size_t childsize = 0;
		int i = inner_offset(root, &pos);
		long delta = edit_recurse(level - 1, root->children[i], pos, span,
								base_case, ctx, &childsplit, &childsize);
		st_dbg("applying upwards delta at level %u: %ld\n", level, delta);
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
				memmove(&root->spans[i+1], &root->spans[i],
						(fill - i) * SZSIZE);
				memmove(&root->children[i+1], &root->children[i],
						(fill - i) * sizeof(void *));
				st_dbg("child split size from level %u: %lu\n", level-1, childsize);
				root->spans[i] = childsize;
				root->children[i] = childsplit;
			} else { // children[i] underflowed
				st_dbg("handling underflow at %u, level %u\n", i, level);
				int j = i > 0 ? i-1 : i+1;
				int jfill;
				int fill = inner_fill(root, i);
				if(level == 2) {
					jfill = leaf_fill((void *)root->children[j], 0);
					if(childsize + jfill <= BL) {
						root->spans[i] += 
								merge_leaves((struct leaf *)root->children[i],
											(struct leaf *)root->children[j],
											childsize, jfill);
						memmove(&root->spans[j], &root->spans[j+1],fill-(j+1));
						memmove(&root->children[j], &root->children[j+1],
								fill - (j+1));
						if(fill - 1 < BL/2 + (BL&1)) // root underflowed
							*splitsize = fill - 1;
					} else {
						balance_leaves((struct leaf *)root->children[i],
										(struct leaf *)root->children[j],
										childsize, jfill);
						root->spans[j] -= delta;
						root->spans[i] += delta;
					}
				} else {
					jfill = inner_fill(root->children[j], 0);
					if(childsize + jfill <= B) {
						root->spans[i] += merge_inner(root->children[i],
													root->children[j],
													childsize, jfill);
						memmove(&root->spans[j], &root->spans[j+1],fill-(j+1));
						memmove(&root->children[j], &root->children[j+1],
								fill - (j+1));
						if(fill - 1 < B/2 + (B&1)) // root underflowed
							*splitsize = fill - 1;
					} else {
						size_t delta = balance_inner(root->children[i],
													root->children[j],
													childsize, jfill);
						root->spans[j] -= delta;
						root->spans[i] += delta;
					}
				}
			}
		}
		return delta;
	}
}

/* insertion */

// handle insertion within LARGE* slices
static long insert_within_slice(struct leaf *leaf, int fill, 
							int i, size_t off, struct slice *new,
							struct leaf **split, size_t *splitsize)
{
	size_t *left_span = &leaf->spans[i];
	struct slice *left = &leaf->slices[i];
	size_t right_span = *left_span - off;
	size_t newlen = new->blk->len; // NOTE: is is necessary to save this delta
	struct slice right = (struct slice) {
		.blk = left->blk,
		.offset = left->offset + off
	};
	left->blk->refs++;
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
	st_dbg("merged %u nodes\n", delta);
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
		// NOTE: we must compute delta directly since merging moves the insert
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
	st_dbg("insertion: found slot %u, target fill %u\n", i, fill);
	size_t len = *span;
	long delta = len;
	// first try appending to slices[i-1], or mutating slices[i]
	if(pos == 0 && i > 0 && leaf->spans[i-1] <= HIGH_WATER) {
		size_t *ispan = &leaf->spans[i-1];
		slice_insert(&leaf->slices[i-1], *ispan, data, len, ispan);
		assert(i == fill || leaf->spans[i] > HIGH_WATER);
		return delta;
	}
	else if(i < fill && leaf->spans[i] <= HIGH_WATER) {
		assert(pos > 0 || i == 0 || leaf->spans[i-1] > HIGH_WATER);
		size_t *ispan = &leaf->spans[i];
		slice_insert(&leaf->slices[i], pos, data, len, ispan);
		return delta;
	}
	else if(pos == 0) { // insertion on boundary [L]|[L], no merging possible
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
		return delta;
	} else {
		struct slice new = new_slice(data, len);
		return insert_within_slice(leaf, fill, i, pos, &new, split, splitsize);
	}
}

void st_insert(SliceTable *st, size_t pos, char *data, size_t len)
{
	if(len == 0)
		return;

	st_dbg("st_insert at pos %zd of len %zd\n", pos, len);
	struct inner *split = NULL;
	size_t splitsize;
	long span = (long)len;
	edit_recurse(st->height, st->root, pos, &span, insert_leaf, data,
				&split, &splitsize);
	// handle root underflow
	if(st->height > 1 && inner_fill(st->root, 0) == 1) {
		st->root = &st->root[0];
		st->height--;
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
		st->height++;
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
		slice->blk->refs++;
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
		st_dbg("merged %u nodes\n", delta);
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
	st_dbg("delete_leaf: pos: %zd span %zd\n", pos, *span);
	int i = leaf_offset(leaf, &pos);
	int fill = leaf_fill(leaf, i);
	st_dbg("deletion: found slot %u, offset %zd, target fill %u\n",i,pos,fill);
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
			leaf->slices[i].blk->refs++;
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
			leaf->spans[i] = pos; // NOTE: may become zero, will be merged
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
		// it's this simple! NOTE: start may be truncated. Thus use start - 2
		start = MAX(0, start - 2);
		int tmpfill = MIN(fill - start, 4); // [][s|][|e][]
		memcpy(tmpspans, &leaf->spans[start], tmpfill * SZSIZE);
		memcpy(tmp, &leaf->slices[start], tmpfill * SLICESIZE);
		int newfill = merge_slices(tmpspans, tmp, tmpfill);
		st_dbg("merged %u nodes\n", tmpfill - newfill);
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
	do {
		long remaining = -len;
		// NOTE: remaining is bytes left to delete.
		st_dbg("deleting... %ld bytes remaining\n", remaining);
		edit_recurse(st->height, st->root, pos, &remaining,
					delete_leaf, NULL, &split, &splitsize);
		len += remaining; // adjusted to byte delta (e.g. -3)
		// handle underflow
		if(st->height > 1 && inner_fill(st->root, 0) == 1) {
			st->root = st->root->children[0];
			st->height--;
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
			st->height++;
		}
	} while(len > 0);
}

/* debugging */

void st_print_struct_sizes(void)
{
	printf(
		"Implementation: \e[38;5;1mst\e[0m\n"
		"sizeof(struct inner): %zd\n"
		"sizeof(struct leaf): %zd\n"
		"sizeof(PieceTable): %zd\n",
		sizeof(struct inner), sizeof(struct leaf), sizeof(SliceTable)
	);
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
				st_dbg("leaf size violation in slot %u of ", i);
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
				st_dbg("child span violation in slot %u of ", i);
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
	return check_recurse(st->root, st->height, st->height);
}

void st_dump(SliceTable *st, FILE *file);
#if 0
int main(void)
{
	st_print_struct_sizes();
	SliceTable *st = st_new();
	st_dbg("original size: %ld\n", st_size(st));
	st_insert(st, 0, "test", 4);
	st_insert(st, 0, "test", 4);
	st_insert(st, 0, "test", 4);
	st_insert(st, 0, "test", 4);
	st_insert(st, 0, "test", 4);
	assert(st_check_invariants(st));
	st_pprint(st);
	st_dbg("size: %ld\n", st_size(st));

	st_delete(st, 2, 4);
	st_pprint(st);
	st_dbg("size: %ld\n", st_size(st));
	assert(st_check_invariants(st));
	//st_to_dot(st, "t.dot");
	st_dump(st, "test");
}
#endif

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
	enqueue((struct q){ st->height, st->root });
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
	enqueue((struct q){ st->height, st->root });
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
		if(key != ULONG_MAX)
			FSTR(tmp, "%lu", key);
		else
			tmp = NULL;
		graph_table_entry(file, tmp, NULL);
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
	FSTR(tmp, "height: %u", st->height);
	graph_table_entry(file, tmp, NULL);
	graph_table_entry(file, "root", "root");
	graph_table_end(file);

	graph_link(file, st, "root", st->root, "body");
	if(st->root)
		inner_to_dot(file, st->root, st->height);

	graph_end(file);
	free(tmp);
	if(fclose(file))
		goto fail;
	return true;

fail:
	perror("st_to_dot");
	return false;
}
