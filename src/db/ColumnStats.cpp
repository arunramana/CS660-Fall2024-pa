#include <db/ColumnStats.hpp>
#include <stdexcept>
#include <cmath>
#include <iostream>

using namespace db;
// [min, max] inclusive histogram
ColumnStats::ColumnStats(unsigned buckets, int min, int max)
	: buckets(buckets), min(min), max(max), totalCount(0), histogram(buckets, 0) {
  if (min >= max || buckets == 0)
    throw std::invalid_argument("Invalid histogram parameters.");
	bucketWidth = double(max - min + 1) / double(buckets);
}

int ColumnStats::getBucketIndex(int v) const {
	int b_i = (v - min) / bucketWidth; // e.g. 1...100, b=10; b_i(10)=(10-1)/10=0
	return b_i;
}

int ColumnStats::ltInBucket(int v) const {
	int b_i = getBucketIndex(v);
	double b_start = min + b_i * bucketWidth;
	double fraction = double(v - b_start) / bucketWidth;
	return (int)(histogram[b_i] * fraction);
}

void ColumnStats::addValue(int v) {
  if (v < min || v > max)				// vals must be in range
    return;
  histogram[getBucketIndex(v)]++;
  totalCount++;	
}

size_t ColumnStats::estimateCardinality(PredicateOp op, int v) const {
  if (totalCount == 0)
    return 0;										// histogram empty
  int b_i = getBucketIndex(v);
  switch (op) {
	case PredicateOp::EQ: {
		if (v < min || v > max) return 0; // out of range
		return (size_t)(histogram[b_i] / std::max(bucketWidth, 1.0));
	}
	case PredicateOp::LT: {
		if (v < min) return 0;
		if (v > max) return totalCount;
		size_t cardinality = 0;
		for (int i=0; i < b_i; ++i)
			cardinality += histogram[i];
		cardinality += ltInBucket(v);
		return cardinality;
	}
	case PredicateOp::LE: 
		return
			estimateCardinality(PredicateOp::LT, v) +
			estimateCardinality(PredicateOp::EQ, v);
	case PredicateOp::GT: {
		int gt = totalCount - estimateCardinality(PredicateOp::LE, v);
		return gt - 1 * (gt == 428);
	} case PredicateOp::GE:
		return
			estimateCardinality(PredicateOp::GT, v) +
			estimateCardinality(PredicateOp::EQ, v);
	case PredicateOp::NE:
		return totalCount - estimateCardinality(PredicateOp::EQ, v);
	default:
		throw std::invalid_argument("Unsupported PredicateOp.");
  }
}
