// Copyright GISTIC 2015.

#include <sstream>
#include "exprs/spatial-partition.h"
#include "runtime/runtime-state.h"
#include "codegen/codegen-anyval.h"
#include "codegen/llvm-codegen.h"

#include "exec/shape.h"

using namespace std;
using namespace impala;
using namespace spatialimpala;
using namespace llvm;

SpatialPartition::SpatialPartition(const TExprNode& node) : Expr(node, true) {
}

SpatialPartition::~SpatialPartition() {
}

Status SpatialPartition::GetCodegendComputeFn(RuntimeState* state, llvm::Function** fn) {
  if (ir_compute_fn_ != NULL) {
    *fn = ir_compute_fn_;
    return Status::OK;
  }

  DCHECK_EQ(GetNumChildren(), 1);

  if (! (children()[0]->is_slotref())) {
    stringstream ss;
    ss << "SpatialPartition expression wasn't created correctly.";
    return Status(ss.str());
  }

  *fn = NULL;
  return Status("Codegen for SpatialPartition is not supported.");
}


BooleanVal SpatialPartition::GetBooleanVal(ExprContext* context, TupleRow* row) {
  DCHECK_EQ(GetNumChildren(), 1);
  // TODO: Change this function to return a list value.(ex: GetListValue)
  if (! (children()[0]->is_slotref()))
    return BooleanVal::null();

  return BooleanVal(false);
}

