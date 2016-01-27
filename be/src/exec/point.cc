// Copyright 2015 GISTIC.

#include "exec/point.h"
#include "exec/rectangle.h"

using namespace spatialimpala;

Point::Point(TShape& shape) : Shape(shape) {
  TPoint point = shape.point;
  this->x_ = point.x;
  this->y_ = point.y;
}

Point::Point(double x, double y) {
  type_ = TShapeType::POINT;
  this->x_ = x;
  this->y_ = y;
}

Point::~Point() {
}

bool Point::Intersects(Shape* other) {
  TShapeType::type other_type = other->GetType();
  switch (other_type) {
    case TShapeType::RECTANGLE:
      return false;
    case TShapeType::LINE:
      return other->Contains(this);
    case TShapeType::POINT:
      return false;
    default:
      return false;
  }
  return false;
}

bool Point::Contains(Shape* other) {
  return false;
}

void Point::GetMBR(Shape* mbr) {
  Rectangle* mbr_rect = dynamic_cast<Rectangle*>(mbr);
  if (mbr_rect == NULL) return;
  mbr_rect->x1_ = this->x_;
  mbr_rect->y1_ = this->y_;
  mbr_rect->x2_ = this->x_;
  mbr_rect->y2_ = this->y_;
}

Point Point::FromPointVal(PointVal& pv) {
  Point p(pv.x, pv.y);
  return p;
}

std::ostream& spatialimpala::operator<< (std::ostream& out, Point const &value) {
  out << "Point(";
  out << value.x_ << ", " << value.y_ << ")";
  return out;
}
