// Copyright 2015 GISTIC.

#include "exec/polygon.h"
#include "exec/point.h"
#include "exec/rectangle.h"

using namespace spatialimpala;

LineString::LineString(TLineString& lString) {
  type_ = TShapeType::LINE_STRING;
  int len = lString.pList.size();
  this->pList_ = std::vector<Point>(len);
  for (int i = 0; i < len; i++) {
    this->pList_[i] = Point(lString.pList[i].x, lString.pList[i].y);
  }
}

LineString& LineString::operator=( const LineString& other ) {
  int pointsListSize;
  pointsListSize = other.pList_.size();
  pList_ = std::vector<Point>(pointsListSize);
  for (int i = 0; i < pointsListSize; i++) {
    pList_[i] = other.pList_[i];
  }
  return *this;
}

LineString::LineString(TShape& shape) : Shape(shape) {
  TLineString lString = shape.lineString;
  int len = lString.pList.size();
  this->pList_ = std::vector<Point>(len);
  for (int i = 0; i < len; i++) {
    this->pList_[i] = Point(lString.pList[i].x, lString.pList[i].y);
  }
}

LineString LineString::FromLineStringVal(LineStringVal& pv) {
  int listSize = pv.pList.size();
  std::vector<Point> tempList(listSize);
  for (int i = 0; i < listSize; i++) {
    tempList[i] = Point::FromPointVal(pv.pList[i]);
  }
  LineString p(tempList);
  return p;
}

LineString::LineString(std::vector<Point> pList) {
  type_ = TShapeType::LINE_STRING;
  this->pList_ = pList;
}

LineString::LineString() {
}

LineString::~LineString() {
}

bool LineString::Intersects(Shape* other) {
  return false;
}

bool LineString::Contains(Shape* other) {
  return false;
}

void LineString::GetMBR(Shape* mbr) {
  Rectangle* mbr_rect = dynamic_cast<Rectangle*>(mbr);
  if (mbr_rect == NULL) return;
  mbr_rect->x1_ = 0;
  mbr_rect->y1_ = 0;
  mbr_rect->x2_ = 0;
  mbr_rect->y2_ = 0;
}

std::ostream& spatialimpala::operator<< (std::ostream& out, LineString const &value) {
  out << "LineString(";
  int len = value.pList_.size();
  for (int i = 0; i < len; i++) {
    out << value.pList_[i];
    out << ", ";
  }
  out << ")";
  return out;
}
