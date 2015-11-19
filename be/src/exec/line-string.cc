// Copyright 2015 GISTIC.

#include "exec/polygon.h"
#include "exec/point.h"
#include "exec/line.h"
#include "exec/rectangle.h"

using namespace spatialimpala;

LineString::LineString(TLineString& line) {
  type_ = TShapeType::LINESTRING;
  len_ = line.serializedData.size();
  this->serializedData_ = new char[len_];
  memcpy(serializedData_, line.serializedData.c_str(), len_);
}

LineString::LineString(TShape& shape) : Shape(shape) {
  TLineString line = shape.linestring;
  len_ = line.serializedData.size();
  this->serializedData_ = new char[len_];
  memcpy(serializedData_, line.serializedData.c_str(), len_);
}

LineString LineString::FromLineStringVal(LineStringVal& pv) {
  LineString p;
  p.len_ = pv.len;
  p.serializedData_ = new char[p.len_];
  memcpy(p.serializedData_, pv.serializedData, p.len_);
  return p;
}

LineString::LineString(char* str, int length) {
  type_ = TShapeType::POLYGON;
  len_ = length;
  serializedData_ = str;
}


LineString::LineString() {
}

LineString::~LineString() {
}

bool LineString::Intersects(Shape* other) {
  int32_t pointsCount;
  int index = 4 * sizeof(double);
  bool intersects = false;
  memcpy(&pointsCount, serializedData_ + index, sizeof(int32_t));
  index += sizeof(int32_t);
  if (pointsCount > 1) {
    double x1, y1, x2, y2;
    memcpy(&x1, serializedData_ + index, sizeof(double));
    index += sizeof(double);
    memcpy(&y1, serializedData_ + index, sizeof(double));
    index += sizeof(double);
   
    for (int i = 1; i < pointsCount ; i++) {
      memcpy(&x2, serializedData_ + index, sizeof(double));
      index += sizeof(double);
      memcpy(&y2, serializedData_ + index, sizeof(double));
      index += sizeof(double);
      Shape *temp = new Line(x1, y1, x2, y2);
      if (other->Intersects(temp)) {
         delete temp;
         return true;
      }
      delete temp;
      x1 = x2;
      y1 = y2;
    }
  }
  return false;
}

bool LineString::Contains(Shape* other) {
  return false;
}

void LineString::GetMBR(Shape* mbr) {
  Rectangle* mbr_rect = dynamic_cast<Rectangle*>(mbr);
  if (mbr_rect == NULL) return;
  memcpy(&mbr_rect->x1_, serializedData_, sizeof(double));
  memcpy(&mbr_rect->y1_, serializedData_ + sizeof(double), sizeof(double));
  memcpy(&mbr_rect->x2_, serializedData_ + 2 * sizeof(double), sizeof(double));
  memcpy(&mbr_rect->y2_, serializedData_ + 3 * sizeof(double), sizeof(double));
}

std::ostream& spatialimpala::operator<< (std::ostream& out, LineString const &value) {
  out << "LineString (";
  int32_t pointsCount;
  int index = 4 * sizeof(double);
  double x,y;
  memcpy(&pointsCount, value.serializedData_ + index, sizeof(int32_t));
  index += sizeof(int32_t);
  
  for (int j = 0; j < pointsCount ; j++) {
    memcpy(&x, value.serializedData_ + index, sizeof(double));
    index += sizeof(double);
    memcpy(&y, value.serializedData_ + index, sizeof(double));
    index += sizeof(double);
    out << x << " " << y;
    out << ", ";
  }
  out << ")";
  return out;
}
