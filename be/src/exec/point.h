// Copyright 2015 GISTIC.

#ifndef IMPALA_EXEC_POINT_H
#define IMPALA_EXEC_POINT_H

#include "gen-cpp/Shapes_types.h"
#include "exec/shape.h"

using namespace impala;

namespace spatialimpala {

class Point : public Shape {
  public:
    Point(TShape& shape);
    Point(double x_, double y_);
    virtual ~Point();
    
    virtual bool Intersects(Shape* other);
    virtual bool Contains(Shape* other);
    virtual void GetMBR(Shape* mbr);

//  private:
    double x_;
    double y_;
};

}

#endif
