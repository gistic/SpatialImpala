// Copyright GISTIC 2015.

#include <sstream>
#include "exprs/range-query.h"
#include "runtime/runtime-state.h"
#include "codegen/codegen-anyval.h"
#include "codegen/llvm-codegen.h"

using namespace std;
using namespace impala;
using namespace spatialimpala;
using namespace llvm;

RangeQuery::RangeQuery(const TExprNode& node) : Expr(node, true) {
  // TODO: Initialize Range with the value provided from TExprNode.
  range_ = NULL;
}

RangeQuery::~RangeQuery() {
}

Status RangeQuery::GetCodegendComputeFn(RuntimeState* state, llvm::Function** fn) {
  if (ir_compute_fn_ != NULL) {
    *fn = ir_compute_fn_;
    return Status::OK;
  }

  DCHECK_EQ(GetNumChildren(), 2);

  if (! (children()[0]->is_slotref() && children()[1]->is_slotref())) {
    stringstream ss;
    ss << "RangeQuery expression wasn't created correctly.";
    return Status(ss.str());
  }

  Function* slot_ref_x;
  RETURN_IF_ERROR(children()[0]->GetCodegendComputeFn(state, &slot_ref_x));
  Function* slot_ref_y;
  RETURN_IF_ERROR(children()[1]->GetCodegendComputeFn(state, &slot_ref_y));

  LlvmCodeGen* codegen;
  RETURN_IF_ERROR(state->GetCodegen(&codegen));
  LLVMContext& context = codegen->context();
  LlvmCodeGen::LlvmBuilder builder(context);

  Value* args[2];
  *fn = CreateIrFunctionPrototype(codegen, "ApplyRangeQuery", &args);

  CodegenAnyVal x_value = CodegenAnyVal::CreateCallWrapped(
      codegen, &builder, TYPE_DOUBLE, slot_ref_x, args, "x_val");

  CodegenAnyVal y_value = CodegenAnyVal::CreateCallWrapped(
      codegen, &builder, TYPE_DOUBLE, slot_ref_y, args, "y_val");

  Value* cmp = builder.CreateOr(x_value.GetIsNull(), y_value.GetIsNull(), "is_null");
  
  CodegenAnyVal x1_value(codegen, &builder, TYPE_DOUBLE, NULL, "x1_value");
  CodegenAnyVal y1_value(codegen, &builder, TYPE_DOUBLE, NULL, "y1_value");
  CodegenAnyVal x2_value(codegen, &builder, TYPE_DOUBLE, NULL, "x2_value");
  CodegenAnyVal y2_value(codegen, &builder, TYPE_DOUBLE, NULL, "y2_value");
  
  x1_value.SetVal(range_->x1_);
  y1_value.SetVal(range_->y1_);
  x2_value.SetVal(range_->x2_);
  y2_value.SetVal(range_->y2_);

  Value* boundary_x1 = builder.CreateFCmpULE(x1_value.GetVal(), x_value.GetVal(), "boundary_x1");
  Value* boundary_x2 = builder.CreateFCmpUGT(x2_value.GetVal(), x_value.GetVal(), "boundary_x2");
  Value* boundary_y1 = builder.CreateFCmpULE(y1_value.GetVal(), y_value.GetVal(), "boundary_y1");
  Value* boundary_y2 = builder.CreateFCmpUGT(y2_value.GetVal(), y_value.GetVal(), "boundary_y2");

  Value* boundary_x = builder.CreateAnd(boundary_x1, boundary_x2, "boundary_x");
  Value* boundary_y = builder.CreateAnd(boundary_y1, boundary_y2, "boundary_y");

  Value* boundary = builder.CreateAnd(boundary_x, boundary_y, "boundary");

  BasicBlock* entry_block = BasicBlock::Create(context, "entry", *fn);
  BasicBlock* null_block = BasicBlock::Create(context, "null_block", *fn);
  BasicBlock* not_null_block = BasicBlock::Create(context, "not_null_block", *fn);

  builder.SetInsertPoint(entry_block);

  builder.CreateCondBr(cmp, null_block, not_null_block);
  builder.SetInsertPoint(null_block);
  builder.CreateRet(codegen->false_value());
  builder.SetInsertPoint(not_null_block);
  builder.CreateRet(boundary);

  ir_compute_fn_ = *fn;

  return Status::OK;
}


BooleanVal RangeQuery::GetBooleanVal(ExprContext* context, TupleRow* row) {
  DCHECK_EQ(GetNumChildren(), 2);

  if (! (children()[0]->is_slotref() && children()[1]->is_slotref()))
    return BooleanVal::null();

  DoubleVal x = children()[0]->GetDoubleVal(NULL, row);
  DoubleVal y = children()[1]->GetDoubleVal(NULL, row);
  
  DoubleVal null_val = DoubleVal::null();
  if (x == null_val || y == null_val)
    return BooleanVal::null();

  return BooleanVal(range_->Contains(x.val, y.val));
}

