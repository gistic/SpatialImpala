// Copyright GISTIC 2015.

#include "exprs/range-query.h"

using namespace spatialimpala;

RangeQuery::RangeQuery(const TExprNode& node) : Expr(node, true) {
  // TODO: Initialize Range with the value provided from TExprNode.
  range_ = NULL;
}

RangeQuery::~RangeQuery() {
}

Status RangeQuery::GetCodegendComputeFn(RuntimeState* state, llvm::Function** fn) {
  // TODO: Handle creating Codegend function for RangeQuery.
  return Status::OK;
}


BooleanVal RangeQuery::GetBooleanVal(ExprContext* context, TupleRow*) {
  // TODO: Handle returning the appropriate value using Range.
  return BooleanVal::null();
}

