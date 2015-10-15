// Copyright 2015 GISTIC.

#include "exec/polygon.h"
#include "exec/point.h"
#include "exec/rectangle.h"

using namespace spatialimpala;

Polygon::Polygon(TPolygon& poly) {
  type_ = TShapeType::POLYGON;
  len_ = poly.serializedData.size();
  this->serializedData_ = new char[len_];
  memcpy(serializedData_, poly.serializedData.c_str(), len_);
}

Polygon::Polygon(TShape& shape) : Shape(shape) {
  TPolygon poly = shape.polygon;
  len_ = poly.serializedData.size();
  this->serializedData_ = new char[len_];
  memcpy(serializedData_, poly.serializedData.c_str(), len_);
}

Polygon Polygon::FromPolygonVal(PolygonVal& pv) {
  Polygon p;
  p.len_ = pv.len;
  p.serializedData_ = new char[p.len_];
  memcpy(p.serializedData_, pv.serializedData, p.len_);
  return p;
}

Polygon::Polygon(char* str, int length) {
  len_ = length;
  serializedData_ = str;
}


Polygon::Polygon(std::vector<LineString> lList) {
  type_ = TShapeType::POLYGON;
  int32_t lineListSize, pointsListSize;
  lineListSize = lList.size();
  int32_t index = 0;
  len_ = sizeof(int32_t);
  for (int i = 0; i < lineListSize; i++) {
    len_ += sizeof(int32_t);
    len_ += lList[i].pList_.size() * 2 * sizeof(double);
  }
  serializedData_ = new char[len_];
  memcpy(serializedData_ + index, &lineListSize, sizeof(int32_t));
  index += sizeof(int32_t);
  for (int i = 0; i < lineListSize; i++) {
    pointsListSize = lList[i].pList_.size();
    memcpy(serializedData_ + index, &pointsListSize, sizeof(int32_t));
    index += sizeof(int32_t);
    for (int j = 0; j < pointsListSize; j++) {
      memcpy(serializedData_ + index, &lList[i].pList_[j].x_, sizeof(double));
      index += sizeof(double);
      memcpy(serializedData_ + index, &lList[i].pList_[j].y_, sizeof(double));
      index += sizeof(double);
    }
  }
}

Polygon::Polygon() {
}

Polygon::~Polygon() {
}

bool Polygon::Intersects(Shape* other) {
  return false;
}

bool Polygon::Contains(Shape* other) {
  return false;
}

void Polygon::GetMBR(Shape* mbr) {
  Rectangle* mbr_rect = dynamic_cast<Rectangle*>(mbr);
  if (mbr_rect == NULL) return;
  mbr_rect->x1_ = 0;
  mbr_rect->y1_ = 0;
  mbr_rect->x2_ = 0;
  mbr_rect->y2_ = 0;
}

std::ostream& spatialimpala::operator<< (std::ostream& out, Polygon const &value) {
  out << "Polygon(";
  int32_t lineStringLen, pointListLen;
  int index = 0;
  double x,y;
  memcpy(&lineStringLen, value.serializedData_ + index, sizeof(int32_t));
  index += sizeof(int32_t);
  for (int i = 0; i < lineStringLen; i++) {
    out << "(";
    memcpy(&pointListLen, value.serializedData_ + index, sizeof(int32_t));
    index += sizeof(int32_t);
    for (int j = 0; j < pointListLen ; j++) {
      memcpy(&x, value.serializedData_ + index, sizeof(double));
      index += sizeof(double);
      memcpy(&y, value.serializedData_ + index, sizeof(double));
      index += sizeof(double);
      out << x << " " << y;
      out << ", ";
    }
    out << ")";
  }
  out << ")";
  return out;
}
