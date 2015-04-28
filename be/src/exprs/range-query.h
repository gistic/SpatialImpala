// Copyright GISTIC 2015.

#ifndef IMPALA_EXPRS_RANGE_QUERY_H_
#define IMPALA_EXPRS_RANGE_QUERY_H_

#include "exprs/expr.h"
#include "exec/rectangle.h"

using namespace impala;

namespace spatialimpala {

class RangeQuery: public Expr {
  public:
    RangeQuery(const TExprNode& node);
    virtual ~RangeQuery();

    virtual Status GetCodegendComputeFn(RuntimeState* state, llvm::Function** fn);

    virtual BooleanVal GetBooleanVal(ExprContext* context, TupleRow* row);

  protected:
    Rectangle* range_;
};

}

#endif
