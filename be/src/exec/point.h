// Copyright 2015 GISTIC.

#ifndef IMPALA_EXEC_POINT_H
#define IMPALA_EXEC_POINT_H

#include "gen-cpp/Shapes_types.h"
#include "exec/shape.h"
#include "udf/udf.h"

using namespace impala;
using namespace impala_udf;

namespace spatialimpala {

class Point : public Shape {
  public:
    Point(TShape& shape);
    Point(double x_ = 0, double y_ = 0);
    virtual ~Point();
    
    virtual bool Intersects(Shape* other);
    virtual bool Contains(Shape* other);
    virtual void GetMBR(Shape* mbr);

    static Point FromPointVal(PointVal& pv);

//  private:
    double x_;
    double y_;
};

}

#endif
