// Copyright GISTIC 2015.

#include <sstream>
#include "exprs/overlap-query.h"
#include "runtime/runtime-state.h"
#include "codegen/codegen-anyval.h"
#include "codegen/llvm-codegen.h"

#include "exec/shape.h"

using namespace std;
using namespace impala;
using namespace spatialimpala;
using namespace llvm;

OverlapQuery::OverlapQuery(const TExprNode& node) : Expr(node, true) {
}

OverlapQuery::~OverlapQuery() {
}

Status OverlapQuery::GetCodegendComputeFn(RuntimeState* state, llvm::Function** fn) {
  if (ir_compute_fn_ != NULL) {
    *fn = ir_compute_fn_;
    return Status::OK;
  }

  DCHECK_EQ(GetNumChildren(), 2);

  if (! (children()[0]->is_slotref() && children()[1]->is_slotref())) {
    stringstream ss;
    ss << "OverlapQuery expression wasn't created correctly.";
    return Status(ss.str());
  }

  *fn = NULL;
  return Status("Codegen for Overlap is not supported.");
}


BooleanVal OverlapQuery::GetBooleanVal(ExprContext* context, TupleRow* row) {
  DCHECK_EQ(GetNumChildren(), 2);

  if (! (children()[0]->is_slotref() && children()[1]->is_slotref()))
    return BooleanVal::null();

  Shape* first_shape;
  Shape* second_shape;
  
  switch (children()[0]->type().type) {
    case TYPE_POINT: {
      PointVal p_val = children()[0]->GetPointVal(NULL, row);
      first_shape = new Point(p_val.x, p_val.y);
      break;
    }
    case TYPE_LINE: {
      LineVal l_val = children()[0]->GetLineVal(NULL, row);
      first_shape = new Line(l_val.x1, l_val.y1, l_val.x2, l_val.y2);
      break;
    }
    case TYPE_RECTANGLE: {
      RectangleVal r_val = children()[0]->GetRectangleVal(NULL, row);
      first_shape = new Rectangle(r_val.x1, r_val.y1, r_val.x2, r_val.y2);
      break;
    }
    case TYPE_POLYGON: {
      PolygonVal p_val = children()[0]->GetPolygonVal(NULL, row);
      first_shape = new Polygon(p_val.serializedData, p_val.len);
      break;
    }
    default:
      return BooleanVal::null();
  }

  switch (children()[1]->type().type) {
    case TYPE_POINT: {
      PointVal p_val = children()[1]->GetPointVal(NULL, row);
      second_shape = new Point(p_val.x, p_val.y);
      break;
    }
    case TYPE_LINE: {
      LineVal l_val = children()[1]->GetLineVal(NULL, row);
      second_shape = new Line(l_val.x1, l_val.y1, l_val.x2, l_val.y2);
      break;
    }
    case TYPE_RECTANGLE: {
      RectangleVal r_val = children()[1]->GetRectangleVal(NULL, row);
      second_shape = new Rectangle(r_val.x1, r_val.y1, r_val.x2, r_val.y2);
      break;
    }
    case TYPE_POLYGON: {
      PolygonVal p_val = children()[1]->GetPolygonVal(NULL, row);
      second_shape = new Polygon(p_val.serializedData, p_val.len);
      break;
    }
    default: {
      delete first_shape;
      return BooleanVal::null();
    }
  }
  
  bool result = first_shape->Intersects(second_shape);
  delete first_shape;
  delete second_shape;

  return BooleanVal(result);
}

