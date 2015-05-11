// Copyright 2015 GISTIC.

#ifndef IMPALA_EXEC_LINE_H
#define IMPALA_EXEC_LINE_H

#include "gen-cpp/Shapes_types.h"
#include "exec/shape.h"

using namespace impala;

namespace spatialimpala {

class Line : public Shape {
  public:
    Line(TShape& shape);
    virtual ~Line();
    
    virtual bool Intersects(Shape* other);
    virtual bool Contains(Shape* other);
    virtual void GetMBR(Shape* mbr);

//  private:
    double x1_;
    double y1_;
    double x2_;
    double y2_;
};

}

#endif
