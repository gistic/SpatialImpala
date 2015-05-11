// Copyright 2015 GISTIC.

#ifndef IMPALA_EXEC_RECTANGLE_H
#define IMPALA_EXEC_RECTANGLE_H

#include "gen-cpp/Shapes_types.h"
#include "exec/shape.h"

using namespace impala;

namespace spatialimpala {

// Rtree implementation that represents rtree records.
class Rectangle : public Shape {
  public:
    Rectangle(TShape& shape);
    Rectangle(TRectangle& rect);
    Rectangle(double x1 = 0, double y1 = 0, double x2 = 0, double y2 = 0);
    virtual ~Rectangle();

    virtual bool Intersects(Shape* other);
    virtual bool Contains(Shape* other);
    virtual void GetMBR(Shape* mbr);

    bool Contains(double x, double y);

 // private:
    double x1_;
    double y1_;
    double x2_;
    double y2_;
};

}

#endif
