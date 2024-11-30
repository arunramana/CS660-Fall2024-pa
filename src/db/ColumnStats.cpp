#include <db/ColumnStats.hpp>
#include <stdexcept>
#include <cmath>

using namespace db;

ColumnStats::ColumnStats(unsigned buckets, int min, int max)
	: buckets(buckets), min(min), max(max), totalCount(0), histogram(buckets, 0) {
  if (min >= max || buckets == 0) {
    throw std::invalid_argument("Invalid histogram parameters.");
  }
  bucketWidth = static_cast<int>(max - min + 1) / buckets;
}

void ColumnStats::addValue(int v) {
  if (v < min || v > max)				// vals must be in range
    return;
  unsigned bucketIndex = (v - min) / bucketWidth;
  bucketIndex = std::min(bucketIndex, buckets - 1);
  histogram[bucketIndex]++;
  totalCount++;
}

size_t ColumnStats::estimateCardinality(PredicateOp op, int v) const {
  if (totalCount == 0)
    return 0;										// histogram empty
  int bucketIndex = static_cast<int>((v - min) / bucketWidth);
  double bucketStart = min + bucketIndex * bucketWidth;
  double bucketEnd = bucketStart + bucketWidth;
  if (bucketIndex < 0)
		bucketIndex = 0; // must be valid index
  if (bucketIndex >= static_cast<int>(buckets))
		bucketIndex = buckets - 1;
  switch (op) {
	case PredicateOp::EQ: {
		if (v < min || v > max) return 0; // out of range
		return static_cast<size_t>(histogram[bucketIndex] / bucketWidth);
	}
	case PredicateOp::LT: {
		if (v < min) return 0;
		if (v >= max) return totalCount;
		size_t cardinality = 0;
		for (int i=0; i < bucketIndex; ++i)
			cardinality += histogram[i];
		double fraction = (v - bucketStart) / bucketWidth;
		cardinality += static_cast<size_t>(histogram[bucketIndex] * fraction);
		return cardinality;
	}
	case PredicateOp::LE: 
		return
			estimateCardinality(PredicateOp::LT, v) +
			estimateCardinality(PredicateOp::EQ, v);
	case PredicateOp::GT:
		return totalCount - estimateCardinality(PredicateOp::LE, v);
	case PredicateOp::GE:
		return
			estimateCardinality(PredicateOp::GT, v) +
			estimateCardinality(PredicateOp::EQ, v);
	case PredicateOp::NE:
		return totalCount - estimateCardinality(PredicateOp::EQ, v);
	default:
		throw std::invalid_argument("Unsupported PredicateOp.");
  }
}
