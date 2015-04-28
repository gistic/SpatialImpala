// Copyright 2015 GISTIC.

#include "exec/rectangle.h"

using namespace spatialimpala;

Rectangle::Rectangle(double x1, double y1, double x2, double y2) {
  this->x1_ = x1;
  this->y1_ = y1;
  this->x2_ = x2;
  this->y2_ = y2;
}

Rectangle::Rectangle(TRectangle& rect) {
  this->x1_ = rect.x1;
  this->y1_ = rect.y1;
  this->x2_ = rect.x2;
  this->y2_ = rect.y2;
}

Rectangle::~Rectangle() {
}

bool Rectangle::Intersects(Rectangle* other) {
  return this->x2_ > other->x1_ && other->x2_ > this->x1_
   && this->y2_ > other->y1_ && other->y2_ > this->y1_;
}

bool Rectangle::Contains(Rectangle* other) {
  return this->x1_ <= other->x1_ && this->x2_ > other->x2_
    && this->y1_ <= other->y1_ && this->y2_ > other->y2_;
}

bool Rectangle::Contains(double x, double y) {
  return this->x1_ <= x && this->x2_ > x
    && this->y1_ <= y && this->y2_ > y;
}

void Rectangle::ToThrift(TRectangle* rect) {
  rect->x1 = this->x1_;
  rect->y1 = this->y1_;
  rect->x2 = this->x2_;
  rect->y2 = this->y2_;
}
