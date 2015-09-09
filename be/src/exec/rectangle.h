// Copyright 2015 GISTIC.

#ifndef IMPALA_EXEC_RECTANGLE_H
#define IMPALA_EXEC_RECTANGLE_H

#include "gen-cpp/Shapes_types.h"
#include "exec/shape.h"
#include "udf/udf.h"

#include "util/hash-util.h"

using namespace impala;
using namespace impala_udf;

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

    bool operator==(const Rectangle& other) const { return (x1_ == other.x1_ && y1_ == other.y1_ && x2_ == other.x2_ && y2_ == other.y2_); }
    bool operator!=(const Rectangle& other) const { return (x1_ != other.x1_ || y1_ != other.y1_ || x2_ != other.x2_ || y2_ != other.y2_); }
    bool operator<=(const Rectangle& other) const { return true; }
    bool operator>=(const Rectangle& other) const { return true; }
    bool operator<(const Rectangle& other) const { return true; }
    bool operator>(const Rectangle& other) const { return true; }
  
    
     
    bool Contains(double x, double y);

    static Rectangle FromRectangleVal(RectangleVal& rv);

 // private:
    double x1_;
    double y1_;
    double x2_;
    double y2_;
};
// This function must be called 'hash_value' to be picked up by boost.
inline std::size_t hash_value(const Rectangle& v) {
    const char* temp = "any";
    const int len = 3;
    return HashUtil::Hash(temp, len, 0);
}

std::ostream& operator<< (std::ostream& out, Rectangle const &value);

}

#endif
