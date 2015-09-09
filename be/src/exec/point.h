// Copyright 2015 GISTIC.

#ifndef IMPALA_EXEC_POINT_H
#define IMPALA_EXEC_POINT_H

#include "gen-cpp/Shapes_types.h"
#include "exec/shape.h"
#include "udf/udf.h"

#include "util/hash-util.h"

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


    bool operator==(const Point& other) const { return (x_ == other.x_ && y_ == other.y_); }
    bool operator!=(const Point& other) const { return (x_ != other.x_ || y_ != other.y_); }
    bool operator<=(const Point& other) const { return true; }
    bool operator>=(const Point& other) const { return true; }
    bool operator<(const Point& other) const { return true; }
    bool operator>(const Point& other) const { return true; }

    static Point FromPointVal(PointVal& pv);

//  private:
    double x_;
    double y_;
};

// This function must be called 'hash_value' to be picked up by boost.
inline std::size_t hash_value(const Point& v) {
    const char* temp = "any";
    const int len = 3;
    return HashUtil::Hash(temp, len, 0);
}

std::ostream& operator<< (std::ostream& out, Point const &value);

}

#endif
