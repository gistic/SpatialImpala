// Copyright 2015 GISTIC.

#ifndef IMPALA_EXEC_SHAPE_H
#define IMPALA_EXEC_SHAPE_H

#include "gen-cpp/Shapes_types.h"

using namespace impala;

namespace spatialimpala {

class Shape {
  public:
    Shape();
    Shape(TShape& shape);
    virtual ~Shape();
    
    virtual bool Intersects(Shape* other) = 0;
    virtual bool Contains(Shape* other) = 0;
    virtual void GetMBR(Shape* mbr) = 0;

    TShapeType::type GetType();

    static double ConvertToDouble(const char* data);

  protected:
    TShapeType::type type_;
};

}

#endif
