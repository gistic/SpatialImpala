// Copyright 2015 GISTIC.

#ifndef IMPALA_EXEC_RECTANGLE_H
#define IMPALA_EXEC_RECTANGLE_H

#include "gen-cpp/CatalogObjects_types.h"

using namespace impala;

namespace spatialimpala {

// Rtree implementation that represents rtree records.
class Rectangle {
  public:
    Rectangle(double x1=0, double y1=0, double x2=0, double y2=0);
    Rectangle(TRectangle& rect);
    virtual ~Rectangle();

    bool Intersects(Rectangle* other);
    bool Contains(Rectangle* other);
    bool Contains(double x, double y);
    void ToThrift(TRectangle* rect);

 // private:
    double x1_;
    double y1_;
    double x2_;
    double y2_;
};

}

#endif
