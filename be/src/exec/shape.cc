// Copyright 2015 GISTIC.

#include "exec/shape.h"

using namespace spatialimpala;

Shape::Shape() {
}

Shape::Shape(TShape& shape) {
  this->type_ = shape.type;
}

Shape::~Shape() {
}


TShapeType::type Shape::GetType() {
  return this->type_;
}

double Shape::ConvertToDouble(const char* data) {
  unsigned long long double_bytes = 0;
  for (int i = 0; i < 8; i++)
    double_bytes = (double_bytes << 8) | (*(data + i) & 0xff);
  return *reinterpret_cast<double*>(&double_bytes);
}
