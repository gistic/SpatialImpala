// Copyright 2015 GISTIC.

#ifndef IMPALA_EXEC_POLYGON_H
#define IMPALA_EXEC_POLYGON_H

#include "gen-cpp/Shapes_types.h"
#include "exec/shape.h"
#include "udf/udf.h"
#include "exec/point.h"
#include <vector>
#include "util/hash-util.h"
#include "exec/line-string.h"
#include "exec/rectangle.h"

using namespace impala;
using namespace impala_udf;

namespace spatialimpala {

// Rtree implementation that represents rtree records.
class Polygon : public Shape {
  public:
    Polygon(TShape& shape);
    Polygon(TPolygon& poly);
    Polygon(char* str, int length);
    Polygon();
    virtual ~Polygon();

    virtual bool Intersects(Shape* other);
    virtual bool Contains(Shape* other);
    virtual void GetMBR(Shape* mbr);
    
    bool operator==(const Polygon& other) const { return true; }
    bool operator!=(const Polygon& other) const { return true; }
    bool operator<=(const Polygon& other) const { return true; }
    bool operator>=(const Polygon& other) const { return true; }
    bool operator<(const Polygon& other) const { return true; }
    bool operator>(const Polygon& other) const { return true; }

    static Polygon FromPolygonVal(PolygonVal& pv);
  
    
 // private:
    char *serializedData_;
    int len_;
};
// This function must be called 'hash_value' to be picked up by boost.
inline std::size_t hash_value(const Polygon& v) {
    const char* temp = "any";
    const int len = 3;
    return HashUtil::Hash(temp, len, 0);
}

std::ostream& operator<< (std::ostream& out, Polygon const &value);

};

#endif
