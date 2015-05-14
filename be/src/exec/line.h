// Copyright 2015 GISTIC.

#ifndef IMPALA_EXEC_LINE_H
#define IMPALA_EXEC_LINE_H

#include "gen-cpp/Shapes_types.h"
#include "exec/shape.h"
#include "udf/udf.h"

using namespace impala;
using namespace impala_udf;

namespace spatialimpala {

class Line : public Shape {
  public:
    Line(TShape& shape);
    Line(double x1 = 0, double y1 = 0, double x2 = 0, double y2 = 0);
    virtual ~Line();
    
    virtual bool Intersects(Shape* other);
    virtual bool Contains(Shape* other);
    virtual void GetMBR(Shape* mbr);

    static Line FromLineVal(LineVal& lv);

//  private:
    double x1_;
    double y1_;
    double x2_;
    double y2_;
};

std::ostream& operator<< (std::ostream& out, Line const &value);

}

#endif
