// Copyright 2015 GISTIC.

#include "exec/line.h"
#include "exec/point.h"
#include "exec/rectangle.h"
#include <limits>
#include <math.h>

using namespace std;
using namespace spatialimpala;

Line::Line(TShape& shape) : Shape(shape) {
  TLine line = shape.line;
  this->x1_ = line.x1;
  this->y1_ = line.y1;
  this->x2_ = line.x2;
  this->y2_ = line.y2;
}

Line::Line(double x1, double y1, double x2, double y2) {
  type_ = TShapeType::LINE;
  this->x1_ = x1;
  this->y1_ = y1;
  this->x2_ = x2;
  this->y2_ = y2;
}

Line::~Line() {
}

bool Line::Intersects(Shape* other) {
  TShapeType::type other_type = other->GetType();
  switch (other_type) {
    case TShapeType::RECTANGLE:
      return other->Intersects(this);
    case TShapeType::LINE: {   
      Line* other_line = dynamic_cast<Line*>(other);
      if (other_line != NULL) {
        double denominator = (this->x1_ - this->x2_) * (other_line->y1_ - other_line->y2_)
          - (this->y1_ - this->y2_) * (other_line->x1_ - other_line->x2_);

        // Parallel lines.
        if (fabs(denominator) < numeric_limits<double>::epsilon())
          return false;

        double nominator_x = ((this->x1_ * this->y2_ - this->y1_ * this->x2_)
          * (other_line->x1_ - other_line->x2_)) - ((this->x1_ - this->x2_)
          * (other_line->x1_ * other_line->y2_ - other_line->y1_ * other_line->x2_));

        double nominator_y = ((this->x1_ * this->y2_ - this->y1_ * this->x2_)
          * (other_line->y1_ - other_line->y2_)) - ((this->y1_ - this->y2_)
          * (other_line->x1_ * other_line->y2_ - other_line->y1_ * other_line->x2_));

        Rectangle* mbr = new Rectangle();
        this->GetMBR(mbr);
        return mbr->Contains(nominator_x, nominator_y);
      }
      return false;
    }
    case TShapeType::POINT:
      return false;
    default:
      return false;
  }
}

bool Line::Contains(Shape* other) {
  TShapeType::type other_type = other->GetType();
  switch (other_type) {
    case TShapeType::RECTANGLE:
      return false;
    case TShapeType::LINE: {
      Line* other_line = dynamic_cast<Line*>(other);
      if (other_line != NULL) {
        Point first_end(other_line->x1_, other_line->y1_);
        Point second_end(other_line->x2_, other_line->y2_);
        return this->Contains(&first_end) && this->Contains(&second_end);  
      }
      return false;
    }
    case TShapeType::POINT: {
      Point* other_point = dynamic_cast<Point*>(other);
      if (other_point != NULL) {
        Rectangle* mbr = new Rectangle();
        this->GetMBR(mbr);
        
        if (! mbr->Contains(other_point)) return false;
        
        double first_slope = (this->y2_ - this->y1_) / (this->x2_ - this->x1_);
        double second_slope = (this->y2_ - other_point->y_) / (this->x2_ - other_point->x_);
        return fabs(first_slope - second_slope) < numeric_limits<double>::epsilon();
      }
      return false;
    }
    default:
      return false;
  }
}

void Line::GetMBR(Shape* mbr) {
  Rectangle* mbr_rect = dynamic_cast<Rectangle*>(mbr);
  if (mbr_rect == NULL) return;
  mbr_rect->x1_ = min(this->x1_, this->x2_);
  mbr_rect->y1_ = min(this->y1_, this->y2_);
  mbr_rect->x2_ = max(this->x1_, this->x2_);
  mbr_rect->y2_ = max(this->y1_, this->y2_);
}

Line Line::FromLineVal(LineVal& lv) {
  Line l(lv.x1, lv.y1, lv.x2, lv.y2);
  return l;
}

std::ostream& spatialimpala::operator<< (std::ostream& out, Line const &value) {
  out << "Line(";
  out << value.x1_ << ", " << value.y1_ << ", " << value.x2_ << ", " << value.y2_ << ")";
  return out;
}
