#include <db/ColumnStats.hpp>
#include <stdexcept>
#include <cmath>

using namespace db;

ColumnStats::ColumnStats(unsigned buckets, int min, int max)
    : buckets(buckets), min(min), max(max), totalCount(0), histogram(buckets, 0) {
  if (min >= max || buckets == 0) {
    throw std::invalid_argument("Invalid histogram parameters.");
  }
  bucketWidth = static_cast<double>(max - min) / buckets;
}

void ColumnStats::addValue(int v) {

  //check if values are in range
  if (v < min || v > max) {
    return;
  }

  unsigned bucketIndex = (v - min) / bucketWidth;
  bucketIndex = std::min(bucketIndex, buckets - 1);

  histogram[bucketIndex]++;
  totalCount++;
}

size_t ColumnStats::estimateCardinality(PredicateOp op, int v) const {
  if (totalCount == 0) {
    return 0; // No data in histogram
  }

  int bucketIndex = static_cast<int>((v - min) / bucketWidth);
  double bucketStart = min + bucketIndex * bucketWidth;
  double bucketEnd = bucketStart + bucketWidth;

  // Ensure bucketIndex is within valid range
  if (bucketIndex < 0) bucketIndex = 0;
  if (bucketIndex >= static_cast<int>(buckets)) bucketIndex = buckets - 1;

  switch (op) {
    case PredicateOp::EQ: {

      if (v < min || v > max) return 0; // Out of range
      return static_cast<size_t>(histogram[bucketIndex] / bucketWidth);
    }
    case PredicateOp::LT: {
      if (v < min) return 0;
      if (v >= max) return totalCount;

      size_t cardinality = 0;
      for (int i = 0; i < bucketIndex; ++i) {
        cardinality += histogram[i];
      }


      double fraction = (v - bucketStart) / bucketWidth;
      cardinality += static_cast<size_t>(histogram[bucketIndex] * fraction);
      return cardinality;
    }
    case PredicateOp::LE: {

      return estimateCardinality(PredicateOp::LT, v + 1);
    }
    case PredicateOp::GT: {

      if (v > max) return 0;
      if (v <= min) return totalCount;

      size_t cardinality = 0;
      for (int i = bucketIndex + 1; i < static_cast<int>(buckets); ++i) {
        cardinality += histogram[i];
      }

      double fraction = (bucketEnd - v) / bucketWidth;
      cardinality += static_cast<size_t>(histogram[bucketIndex] * fraction);
      return cardinality;
    }
    case PredicateOp::GE: {

      return estimateCardinality(PredicateOp::GT, v - 1);
    }
    case PredicateOp::NE: {

      if (v < min || v > max) return totalCount; // All values are not equal
      size_t equalCardinality = static_cast<size_t>(histogram[bucketIndex] / bucketWidth);
      return totalCount - equalCardinality;
    }
    default:
      throw std::invalid_argument("Unsupported PredicateOp.");
  }
}
