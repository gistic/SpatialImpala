// Copyright 2015 GISTIC.

#include "exec/rectangle.h"
#include "exec/point.h"
#include "exec/line.h"

using namespace spatialimpala;

Rectangle::Rectangle(TRectangle& rect) {
  type_ = TShapeType::RECTANGLE;
  this->x1_ = rect.x1;
  this->y1_ = rect.y1;
  this->x2_ = rect.x2;
  this->y2_ = rect.y2;
}

Rectangle::Rectangle(TShape& shape) : Shape(shape) {
  TRectangle rect = shape.rectangle;
  this->x1_ = rect.x1;
  this->y1_ = rect.y1;
  this->x2_ = rect.x2;
  this->y2_ = rect.y2;
}

Rectangle::Rectangle(double x1, double y1, double x2, double y2) {
  type_ = TShapeType::RECTANGLE;
  this->x1_ = x1;
  this->y1_ = y1;
  this->x2_ = x2;
  this->y2_ = y2;
}

Rectangle::~Rectangle() {
}

bool Rectangle::Intersects(Shape* other) {
  TShapeType::type other_type = other->GetType();
  switch (other_type) {
    case TShapeType::RECTANGLE: {
      Rectangle* other_rect = dynamic_cast<Rectangle*>(other);
      if (other_rect != NULL)
        return this->x2_ > other_rect->x1_ && other_rect->x2_ > this->x1_
          && this->y2_ > other_rect->y1_ && other_rect->y2_ > this->y1_;
      return false;
    }
    case TShapeType::LINE: { 
      Line* other_line = dynamic_cast<Line*>(other);
      if (other_line != NULL)
        return this->Contains(other_line->x1_, other_line->y1_)
          ^ this->Contains(other_line->x2_, other_line->y2_);
      return false;
    }
    case TShapeType::POINT:
      return false;
    default:
      return false;
  }
  return false;
}

bool Rectangle::Contains(Shape* other) {
  TShapeType::type other_type = other->GetType();
  switch (other_type) {
    case TShapeType::RECTANGLE: {
      Rectangle* other_rect = dynamic_cast<Rectangle*>(other);
      if (other_rect != NULL)
        return this->x1_ <= other_rect->x1_ && this->x2_ > other_rect->x2_
          && this->y1_ <= other_rect->y1_ && this->y2_ > other_rect->y2_;
      return false;
    }
    case TShapeType::LINE: { 
      Line* other_line = dynamic_cast<Line*>(other);
      if (other_line != NULL)
        return this->Contains(other_line->x1_, other_line->y1_)
          && this->Contains(other_line->x2_, other_line->y2_);
      return false;
    }
    case TShapeType::POINT: {
      Point* other_point = dynamic_cast<Point*>(other);
      if (other_point != NULL)
        return this->x1_ <= other_point->x_ && this->x2_ > other_point->x_
          && this->y1_ <= other_point->y_ && this->y2_ > other_point->y_;
      return false;
    }
    default:
      return false;
  }
}

bool Rectangle::Contains(double x, double y) {
  return this->x1_ <= x && this->x2_ > x
    && this->y1_ <= y && this->y2_ > y;
}

void Rectangle::GetMBR(Shape* mbr) {
  Rectangle* mbr_rect = dynamic_cast<Rectangle*>(mbr);
  if (mbr_rect == NULL) return;
  mbr_rect->x1_ = this->x1_;
  mbr_rect->y1_ = this->y1_;
  mbr_rect->x2_ = this->x2_;
  mbr_rect->y2_ = this->y2_;
}

Rectangle Rectangle::FromRectangleVal(RectangleVal& rv) {
  Rectangle r(rv.x1, rv.y1, rv.x2, rv.y2);
  return r;
}

std::ostream& spatialimpala::operator<< (std::ostream& out, Rectangle const &value) {
  out << "Rectangle(";
  out << value.x1_ << ", " << value.y1_ << ", " << value.x2_ << ", " << value.y2_ << ")";
  return out;
}
