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
  TRangeQuery range_query = node.range_query;
  this->range_ = new Rectangle(range_query.rectangle);
}

RangeQuery::~RangeQuery() {
}

Status RangeQuery::GetCodegendComputeFn(RuntimeState* state, llvm::Function** fn) {
  if (GetNumChildren() == 1 && (children()[0]->type().type == TYPE_POINT
    || children()[0]->type().type == TYPE_LINE || children()[0]->type().type == TYPE_RECTANGLE 
    || children()[0]->type().type == TYPE_POLYGON || children()[0]->type().type == TYPE_LINESTRING)) {
    *fn = NULL;
    return Status("Codegen for Shapes not supported.");
  }

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

  if (children()[0]->type().type != TYPE_DOUBLE || children()[1]->type().type != TYPE_DOUBLE) {
    stringstream ss;
    ss << "Slots are not of TYPE_DOUBLE" << endl;
    ss << "First Slot is: " << children()[0]->type() << endl;
    ss << "Second Slot is: " << children()[0]->type() << endl;
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

  BasicBlock* entry_block = BasicBlock::Create(context, "entry", *fn);
  BasicBlock* null_block = BasicBlock::Create(context, "null_block", *fn);
  BasicBlock* not_null_block = BasicBlock::Create(context, "not_null_block", *fn);

  builder.SetInsertPoint(entry_block);

  CodegenAnyVal x_value = CodegenAnyVal::CreateCallWrapped(
      codegen, &builder, children()[0]->type(), slot_ref_x, args, "x_val");

  CodegenAnyVal y_value = CodegenAnyVal::CreateCallWrapped(
      codegen, &builder, children()[1]->type(), slot_ref_y, args, "y_val");

  Value* cmp = builder.CreateOr(x_value.GetIsNull(), y_value.GetIsNull(), "is_null");

  builder.CreateCondBr(cmp, null_block, not_null_block);
  builder.SetInsertPoint(null_block);
  
  CodegenAnyVal ret_null(codegen, &builder, TYPE_BOOLEAN, NULL, "ret_null");
  ret_null.SetVal(codegen->false_value());
  builder.CreateRet(ret_null.value());

  builder.SetInsertPoint(not_null_block);

  Type* column_type = codegen->GetType(TYPE_DOUBLE);

  Value* x1_value = ConstantFP::get(column_type, range_->x1_);
  Value* y1_value = ConstantFP::get(column_type, range_->y1_);
  Value* x2_value = ConstantFP::get(column_type, range_->x2_);
  Value* y2_value = ConstantFP::get(column_type, range_->y2_);

  Value* boundary_x1 = builder.CreateFCmpULE(x1_value, x_value.GetVal(), "boundary_x1");
  Value* boundary_x2 = builder.CreateFCmpUGT(x2_value, x_value.GetVal(), "boundary_x2");
  Value* boundary_y1 = builder.CreateFCmpULE(y1_value, y_value.GetVal(), "boundary_y1");
  Value* boundary_y2 = builder.CreateFCmpUGT(y2_value, y_value.GetVal(), "boundary_y2");

  Value* boundary_x = builder.CreateAnd(boundary_x1, boundary_x2, "boundary_x");
  Value* boundary_y = builder.CreateAnd(boundary_y1, boundary_y2, "boundary_y");

  Value* boundary = builder.CreateAnd(boundary_x, boundary_y, "boundary");
  
  CodegenAnyVal ret(codegen, &builder, TYPE_BOOLEAN, NULL, "ret");
  ret.SetVal(boundary);
  builder.CreateRet(ret.value());

  *fn = codegen->FinalizeFunction(*fn);

  ir_compute_fn_ = *fn;

  return Status::OK;
}


BooleanVal RangeQuery::GetBooleanVal(ExprContext* context, TupleRow* row) {
  if (GetNumChildren() == 2) {
    if (! (children()[0]->is_slotref() && children()[1]->is_slotref()))
      return BooleanVal::null();

    DoubleVal x = children()[0]->GetDoubleVal(NULL, row);
    DoubleVal y = children()[1]->GetDoubleVal(NULL, row);
  
    DoubleVal null_val = DoubleVal::null();
    if (x == null_val || y == null_val)
      return BooleanVal::null();

    return BooleanVal(range_->Contains(x.val, y.val));
  }
  else if (GetNumChildren() == 1) {
    if (! children()[0]->is_slotref())
      return BooleanVal::null();

    switch (children()[0]->type().type) {
      case TYPE_POINT: {
        PointVal p_val = children()[0]->GetPointVal(NULL, row);
        return BooleanVal(range_->Contains(new Point(p_val.x, p_val.y)));
      }
      case TYPE_LINE: {
        LineVal l_val = children()[0]->GetLineVal(NULL, row);
        return BooleanVal(range_->Contains(new Line(l_val.x1, l_val.y1, l_val.x2, l_val.y2)));
      }
      case TYPE_RECTANGLE: {
        RectangleVal r_val = children()[0]->GetRectangleVal(NULL, row);
        return BooleanVal(range_->Contains(new Rectangle(r_val.x1, r_val.y1, r_val.x2, r_val.y2)));
      }
      case TYPE_POLYGON: {
        StringVal strval = children()[0]->GetStringVal(NULL, row);
        PolygonVal poly = PolygonVal(reinterpret_cast<char*>(strval.ptr), strval.len);
        RectangleVal r_val = poly.GetMBR();
        return BooleanVal(range_->Contains(new Rectangle(r_val.x1, r_val.y1, r_val.x2, r_val.y2)));
      }
      case TYPE_LINESTRING: {
        StringVal strval = children()[0]->GetStringVal(NULL, row);
        LineStringVal line = LineStringVal(reinterpret_cast<char*>(strval.ptr), strval.len);
        RectangleVal r_val = line.GetMBR();
        return BooleanVal(range_->Contains(new Rectangle(r_val.x1, r_val.y1, r_val.x2, r_val.y2)));
      }
      default:
        return BooleanVal::null();
    }
  }
  else {
    return BooleanVal::null();
  }
}

