// Copyright GISTIC 2015.

#ifndef IMPALA_EXPRS_OVERLAP_QUERY_H_
#define IMPALA_EXPRS_OVERLAP_QUERY_H_

#include "exprs/expr.h"

using namespace impala;

namespace spatialimpala {

class OverlapQuery: public Expr {
  public:
    OverlapQuery(const TExprNode& node);
    virtual ~OverlapQuery();

    virtual Status GetCodegendComputeFn(RuntimeState* state, llvm::Function** fn);

    virtual BooleanVal GetBooleanVal(ExprContext* context, TupleRow* row);
};

}

#endif
