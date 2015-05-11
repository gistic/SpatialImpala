// Copyright 2015 GISTIC.

#include "exec/shape.h"

using namespace spatialimpala;

Shape::Shape() {
}

Shape::Shape(TShape& shape) {
  this->shape_ = shape;
}

Shape::~Shape() {
}


TShapeType::type Shape::GetType() {
  return this->shape_.type;
}
