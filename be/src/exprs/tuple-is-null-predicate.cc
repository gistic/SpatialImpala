// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "exprs/tuple-is-null-predicate.h"

#include <sstream>

#include "gen-cpp/Exprs_types.h"

#include "common/names.h"

namespace impala {

BooleanVal TupleIsNullPredicate::GetBooleanVal(ExprContext* ctx, TupleRow* row) {
  int count = 0;
  for (int i = 0; i < tuple_idxs_.size(); ++i) {
    count += row->GetTuple(tuple_idxs_[i]) == NULL;
  }
  return BooleanVal(!tuple_idxs_.empty() && count == tuple_idxs_.size());
}

TupleIsNullPredicate::TupleIsNullPredicate(const TExprNode& node)
  : Predicate(node),
    tuple_ids_(node.tuple_is_null_pred.tuple_ids.begin(),
               node.tuple_is_null_pred.tuple_ids.end()) {
}

Status TupleIsNullPredicate::Prepare(RuntimeState* state, const RowDescriptor& row_desc,
                                     ExprContext* ctx) {
  RETURN_IF_ERROR(Expr::Prepare(state, row_desc, ctx));
  DCHECK_EQ(0, children_.size());
  // Resolve tuple ids to tuple indexes.
  for (int i = 0; i < tuple_ids_.size(); ++i) {
    int32_t tuple_idx = row_desc.GetTupleIdx(tuple_ids_[i]);
    if (tuple_idx == RowDescriptor::INVALID_IDX) {
      // This should not happen and indicates a planner issue. This code is tricky
      // so rather than crashing, do this as a stop gap.
      // TODO: remove this code and replace with DCHECK.
      return Status("Invalid plan. TupleIsNullPredicate has invalid tuple idx.");
    }
    if (row_desc.TupleIsNullable(tuple_idx)) tuple_idxs_.push_back(tuple_idx);
  }
  return Status::OK();
}

Status TupleIsNullPredicate::GetCodegendComputeFn(RuntimeState* state,
                                                  llvm::Function** fn) {
  return GetCodegendComputeFnWrapper(state, fn);
}

string TupleIsNullPredicate::DebugString() const {
  stringstream out;
  out << "TupleIsNullPredicate(tupleids=[";
  for (int i = 0; i < tuple_ids_.size(); ++i) {
    out << (i == 0 ? "" : " ") << tuple_ids_[i];
  }
  out << "])";
  return out.str();
}

}
