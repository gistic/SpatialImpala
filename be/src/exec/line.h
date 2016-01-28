// Copyright 2015 GISTIC.

#ifndef IMPALA_EXEC_LINE_H
#define IMPALA_EXEC_LINE_H

#include "gen-cpp/Shapes_types.h"
#include "exec/shape.h"
#include "udf/udf.h"

#include "util/hash-util.h"

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

   bool operator==(const Line& other) const {
     return (x1_ == other.x1_ && y1_ == other.y1_ && x2_ == other.x2_ && y2_ == other.y2_);
   }

   bool operator!=(const Line& other) const {
     return (x1_ != other.x1_ || y1_ != other.y1_ || x2_ != other.x2_ || y2_ != other.y2_);
   }

   bool operator<=(const Line& other) const { return true; }
   bool operator>=(const Line& other) const { return true; }
   bool operator<(const Line& other) const { return true; }
   bool operator>(const Line& other) const { return true; }

   static Line FromLineVal(LineVal& lv);

//  private:
   double x1_;
   double y1_;
   double x2_;
   double y2_;
};

// This function must be called 'hash_value' to be picked up by boost.
inline std::size_t hash_value(const Line& v) {
  const char* temp = "any";
  const int len = 3;
  return HashUtil::Hash(temp, len, 0);
}

std::ostream& operator<< (std::ostream& out, Line const &value);

}

#endif
