#include "text-util.h"
#include "util.h"
#include <wchar.h>
#include <errno.h>
#include <stdlib.h>

bool text_range_valid(const Filerange *r) {
	return r->start != EPOS && r->end != EPOS && r->start <= r->end;
}

size_t text_range_size(const Filerange *r) {
	return text_range_valid(r) ? r->end - r->start : 0;
}

Filerange text_range_empty(void) {
	return (Filerange){ .start = EPOS, .end = EPOS };
}

Filerange text_range_union(const Filerange *r1, const Filerange *r2) {
	if (!text_range_valid(r1))
		return *r2;
	if (!text_range_valid(r2))
		return *r1;
	return (Filerange) {
		.start = MIN(r1->start, r2->start),
		.end = MAX(r1->end, r2->end),
	};
}

Filerange text_range_intersect(const Filerange *r1, const Filerange *r2) {
	if (!text_range_overlap(r1, r2))
		return text_range_empty();
	return text_range_new(MAX(r1->start, r2->start), MIN(r1->end, r2->end));
}

Filerange text_range_new(size_t a, size_t b) {
	return (Filerange) {
		.start = MIN(a, b),
		.end = MAX(a, b),
	};
}

bool text_range_equal(const Filerange *r1, const Filerange *r2) {
	if (!text_range_valid(r1) && !text_range_valid(r2))
		return true;
	return r1->start == r2->start && r1->end == r2->end;
}

bool text_range_overlap(const Filerange *r1, const Filerange *r2) {
	if (!text_range_valid(r1) || !text_range_valid(r2))
		return false;
	return r1->start < r2->end && r2->start < r1->end;
}

bool text_range_contains(const Filerange *r, size_t pos) {
	return text_range_valid(r) && r->start <= pos && pos <= r->end;
}
