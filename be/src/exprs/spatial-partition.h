// Copyright GISTIC 2015.

#ifndef IMPALA_EXPRS_SPATIAL_PARTITION_H_
#define IMPALA_EXPRS_SPATIAL_PARTITION_H_

#include "exprs/expr.h"

using namespace impala;

namespace spatialimpala {

class SpatialPartition: public Expr {
  public:
    SpatialPartition(const TExprNode& node);
    virtual ~SpatialPartition();

    virtual Status GetCodegendComputeFn(RuntimeState* state, llvm::Function** fn);

    virtual BooleanVal GetBooleanVal(ExprContext* context, TupleRow* row);
};

}

#endif
