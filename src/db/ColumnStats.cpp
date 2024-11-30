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
	bucketWidth = static_cast<double>(max - min + 1) / static_cast<double>(buckets);
}

void ColumnStats::addValue(int v) {
  if (v < min || v > max)				// vals must be in range
    return;
	unsigned b_i = (v - min) / bucketWidth;
	if (b_i >= buckets) b_i = buckets - 1;
  histogram[b_i]++;
  totalCount++;	
}

size_t ColumnStats::estimateCardinality(PredicateOp op, int v) const {
  if (totalCount == 0)
    return 0;										// histogram empty
  int b_i = static_cast<int>((v - min) / bucketWidth);
  double b_start = min + b_i * bucketWidth;
  double b_end = b_start + bucketWidth;
  switch (op) {
	case PredicateOp::EQ: {
		if (v < min || v > max) return 0; // out of range
		return static_cast<size_t>(histogram[b_i] / bucketWidth);
	}
	case PredicateOp::LT: {
		if (v < min) return 0;
		if (v > max) return totalCount;
		size_t cardinality = 0;
		for (int i=0; i < b_i; ++i)
			cardinality += histogram[i];
		double fraction = (v - b_start) / bucketWidth;
		cardinality += static_cast<size_t>(histogram[b_i] * fraction);
		return cardinality;
	}
	case PredicateOp::LE: 
		return
			estimateCardinality(PredicateOp::LT, v) +
			estimateCardinality(PredicateOp::EQ, v);
	case PredicateOp::GT:
		return totalCount - estimateCardinality(PredicateOp::LE, v) - 1;
	case PredicateOp::GE:
		return
			estimateCardinality(PredicateOp::GT, v) +
			estimateCardinality(PredicateOp::EQ, v) + 1;
	case PredicateOp::NE:
		return totalCount - estimateCardinality(PredicateOp::EQ, v);
	default:
		throw std::invalid_argument("Unsupported PredicateOp.");
  }
}
