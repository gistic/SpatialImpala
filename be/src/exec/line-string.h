// Copyright 2015 GISTIC.

#ifndef IMPALA_EXEC_LINESTRING_H
#define IMPALA_EXEC_LINESTRING_H

#include "gen-cpp/Shapes_types.h"
#include "exec/shape.h"
#include "udf/udf.h"
#include "exec/point.h"
#include <vector>
#include "util/hash-util.h"

using namespace impala;
using namespace impala_udf;

namespace spatialimpala {

// Rtree implementation that represents rtree records.
class LineString : public Shape {
  public:
    LineString(TShape& shape);
    LineString(TLineString& lString);
    LineString(std::vector<Point> pList);
    LineString();
    LineString& operator=( const LineString& other );
    virtual ~LineString();

    virtual bool Intersects(Shape* other);
    virtual bool Contains(Shape* other);
    virtual void GetMBR(Shape* mbr);
    
    bool operator==(const LineString& other) const { return true; }
    bool operator!=(const LineString& other) const { return true; }
    bool operator<=(const LineString& other) const { return true; }
    bool operator>=(const LineString& other) const { return true; }
    bool operator<(const LineString& other) const { return true; }
    bool operator>(const LineString& other) const { return true; }

    static LineString FromLineStringVal(LineStringVal& pv);
  
    
 // private:
    std::vector<Point> pList_;
};
// This function must be called 'hash_value' to be picked up by boost.
inline std::size_t hash_value(const LineString& v) {
    const char* temp = "any";
    const int len = 3;
    return HashUtil::Hash(temp, len, 0);
}

std::ostream& operator<< (std::ostream& out, LineString const &value);

};

#endif
