// Copyright 2015 GISTIC.

#include "exec/spatial-join-node.h"

#include "exprs/expr.h"
#include "exprs/expr-context.h"
#include "runtime/row-batch.h"

using namespace impala;
using namespace spatialimpala;

bool IsIntersected(TupleRow* row1, TupleRow* row2, ExprContext* build, ExprContext* probe) {
  RectangleVal rect1 = build->GetRectangleVal(row1);
  RectangleVal rect2 = probe->GetRectangleVal(row2);
  return rect1.isOverlappedWith(rect2);
}

double GET_X1(TupleRow* row, ExprContext* expr_ctx) {
  return expr_ctx->GetRectangleVal(row).x1;
}

double GET_X2(TupleRow* row, ExprContext* expr_ctx) {
  return expr_ctx->GetRectangleVal(row).x2;
}

int CompareRowsOnX1 (const void * val1, const void * val2) {
  //TupleRow* row1 = * (TupleRow **) val1;
  //TupleRow* row2 = * (TupleRow **) val2;

  // TODO: Replace the comparing function to be able to use expressions.
  return 0;
  // Comparing on x1
  //return (int)(GET_X1(row1) - GET_X1(row2));
}

void SpatialJoinNode::ProcessBuildBatch(RowBatchList* build_batch) {
  built_rows_ = new TupleRow*[build_batch->total_num_rows()];
  built_rows_count_ = build_batch->total_num_rows();
  RowBatchList::TupleRowIterator iterator = build_batch->Iterator();

  for(int i = 0; i < built_rows_count_; i++) {
    TupleRow* row = iterator.GetRow();
    built_rows_[i] = row;
    iterator.Next();
  }
  // Sort the rows on the X Axis value
  qsort(built_rows_, built_rows_count_, sizeof(TupleRow*), CompareRowsOnX1);
}

int SpatialJoinNode::ProcessProbeBatch(RowBatch* out_batch, RowBatch* probe_batch, int max_added_rows) {
  TupleRow** probe_sorted_list = NULL;
  if(probe_batch_pos_ == 0) {
    probe_sorted_list = new TupleRow*[probe_batch->num_rows()];

    for(int i = 0; i < probe_batch->num_rows(); i++) {
      probe_sorted_list[i] = probe_batch->GetRow(i);
    }
					
    // Sort the rows on the X Axis value
    qsort(probe_sorted_list, probe_batch->num_rows(), sizeof(TupleRow*), CompareRowsOnX1);
    build_batch_pos_ = 0;
  } else {
    probe_sorted_list = lastest_probe_batch_;
  }

  // Probing the build batch
  int rows_added = 0;
  int i = build_batch_pos_;
  int j = probe_batch_pos_;
  TupleRow** R = built_rows_;
  TupleRow** S = probe_sorted_list;
  int R_length = built_rows_count_;
  int S_length = probe_batch->num_rows();

  while(i < R_length && j < S_length && rows_added < max_added_rows ) {
    TupleRow* r;
    TupleRow* s;
    if (CompareRowsOnX1(&R[i], &S[j]) < 0) {
      r = R[i];
      int jj = (last_jj_ > 0) ? last_jj_ : j;
      last_jj_ = -1;

      while ((jj < S_length) && (GET_X1((s = S[jj]), probe_expr_ctx_) <= GET_X2(r, build_expr_ctx_))) {
        if (IsIntersected(r, s, build_expr_ctx_, probe_expr_ctx_)) rows_added++;
        jj++;

        if(rows_added == max_added_rows) {
          last_jj_ = jj;
          break;
        }
      }

      if(rows_added < max_added_rows) i++;        
    } else {
      s = S[j];
      int ii = (last_ii_ > 0) ? last_ii_ : i;
      last_ii_ = -1;

      while ((ii < R_length) && (GET_X1((r = R[ii]), build_expr_ctx_) <= GET_X2(s, probe_expr_ctx_))) {
        if (IsIntersected(r, s, build_expr_ctx_, probe_expr_ctx_)) rows_added++;
        ii++;

        if(rows_added == max_added_rows) {
          last_ii_ = ii;
          break;
        }
      }
      if(rows_added < max_added_rows) j++;
    }

  }

  build_batch_pos_ = i;
  probe_batch_pos_ = j;

  // Free the memory if not needed anymore
  if(probe_batch_pos_ == probe_batch->num_rows()) {
    delete lastest_probe_batch_;
  }

  if(lastest_probe_batch_ != probe_sorted_list) {
    lastest_probe_batch_ = probe_sorted_list;
  }

  return rows_added;
}
