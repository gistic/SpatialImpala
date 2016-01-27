// Copyright 2015 GISTIC.

#include "exec/polygon.h"
#include "exec/point.h"
#include "exec/rectangle.h"
#include "exec/line.h"

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
  type_ = TShapeType::POLYGON;
  len_ = length;
  serializedData_ = str;
}

Polygon::Polygon() {
}

Polygon::~Polygon() {
}

bool Polygon::Intersects(Shape* other) {
  int32_t lineStringLen, pointListLen;
  int index = 4 * sizeof(double);
  bool intersects = false;
  memcpy(&lineStringLen, serializedData_ + index, sizeof(int32_t));
  index += sizeof(int32_t);
  LOG(INFO) << "Total number of linestrings = " << lineStringLen;
  for (int i = 0; i < lineStringLen; i++) {
    memcpy(&pointListLen, serializedData_ + index, sizeof(int32_t));
    index += sizeof(int32_t);
    LOG(INFO)<<"Total number of points = "<<pointListLen;
    if (pointListLen > 1) {
      double x1, y1, x2, y2, firstX, firstY;
      memcpy(&x1, serializedData_ + index, sizeof(double));
      index += sizeof(double);
      memcpy(&y1, serializedData_ + index, sizeof(double));
      index += sizeof(double);
      firstX = x1;
      firstY = y1;
      for (int j = 1; j < pointListLen ; j++) {
        memcpy(&x2, serializedData_ + index, sizeof(double));
        index += sizeof(double);
        memcpy(&y2, serializedData_ + index, sizeof(double));
        index += sizeof(double);
        LOG(INFO) << "x1= " << x1 << ", y1= " << y1 << ", x2= " << x2 << ", y2= " << y2;
        Shape *temp = new Line(x1, y1, x2, y2);
        if (other->Intersects(temp)) {
          delete temp;
          return true;
        }
        delete temp;
        x1 = x2;
        y1 = y2;
      }
      Shape *temp = new Line(x1, y1, firstX, firstY);
      if (other->Intersects(temp)) {
        delete temp;
        return true;
      }
      delete temp;
    }
  }
  return false;
}

bool Polygon::Contains(Shape* other) {
  return false;
}

void Polygon::GetMBR(Shape* mbr) {
  Rectangle* mbr_rect = dynamic_cast<Rectangle*>(mbr);
  if (mbr_rect == NULL) return;
  memcpy(&mbr_rect->x1_, serializedData_, sizeof(double));
  memcpy(&mbr_rect->y1_, serializedData_ + sizeof(double), sizeof(double));
  memcpy(&mbr_rect->x2_, serializedData_ + 2 * sizeof(double), sizeof(double));
  memcpy(&mbr_rect->y2_, serializedData_ + 3 * sizeof(double), sizeof(double));
}

std::ostream& spatialimpala::operator<< (std::ostream& out, Polygon const &value) {
  out << "Polygon(";
  int32_t lineStringLen, pointListLen;
  int index = 4 * sizeof(double);
  double x, y;
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
    out << "), ";
  }
  out << ")";
  return out;
}
