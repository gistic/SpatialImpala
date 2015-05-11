// Copyright GISTIC 2015.

namespace cpp impala
namespace java com.cloudera.impala.thrift

enum TShapeType {
  POINT,
  LINE,
  RECTANGLE
}

// Represents a 2D point.
struct TPoint {
  1: required double x
  2: required double y
}

// Represents a 2D line.
struct TLine {
  1: required double x1
  2: required double y1
  3: required double x2
  4: required double y2
}

// Represents a 2D rectangle.
// Used in a global index record for spatial tables.
struct TRectangle {
  1: required double x1
  2: required double y1
  3: required double x2
  4: required double y2
}

// Represents a 2D shape
struct TShape {
  1: required TShapeType type
  2: optional TPoint point
  3: optional TLine line
  4: optional TRectangle rectangle
}
