// Copyright GISTIC 2015.

namespace cpp impala
namespace java com.cloudera.impala.thrift

// Represents a point
struct TPoint {
  1: required double x
  2: required double y
}

// Represents a rectangle used in a global index record for spatial tables.
struct TRectangle {
  1: required double x1
  2: required double y1
  3: required double x2
  4: required double y2
}
