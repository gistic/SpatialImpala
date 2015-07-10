// Copyright 2015 GISTIC.

#include "exec/spatial-join-node.h"

#include "exprs/expr.h"
#include "exprs/expr-context.h"
#include "runtime/row-batch.h"

using namespace impala;
using namespace spatialimpala;

#define GET_X1(tupleRow, expr_ctx) expr_ctx->GetRectangleVal(tupleRow).x1
#define GET_Y1(tupleRow, expr_ctx) expr_ctx->GetRectangleVal(tupleRow).y1
#define GET_X2(tupleRow, expr_ctx) expr_ctx->GetRectangleVal(tupleRow).x2
#define GET_Y2(tupleRow, expr_ctx) expr_ctx->GetRectangleVal(tupleRow).y2

bool IsIntersected(TupleRow* row1, TupleRow* row2, ExprContext* build, ExprContext* probe) {
  RectangleVal rect1 = build->GetRectangleVal(row1);
  RectangleVal rect2 = probe->GetRectangleVal(row2);
  return rect1.isOverlappedWith(rect2);
}

struct RowsX1Comparator {
  RowsX1Comparator(ExprContext* expr_ctx) {
    this->expr_ctx = expr_ctx;
  }

  bool operator() (TupleRow* first_row, TupleRow* second_row) {   
    return GET_X1(first_row, expr_ctx) < GET_X1(second_row, expr_ctx);
  }

  ExprContext* expr_ctx;
};

void SpatialJoinNode::AddOutputRow
  (RowBatch* out_batch, int* rows_added, uint8_t** out_row_mem, TupleRow* build, TupleRow* probe, int max_added_rows) {

  ExprContext* const* other_conjunct_ctxs = &other_join_conjunct_ctxs_[0];
  int num_other_conjunct_ctxs = other_join_conjunct_ctxs_.size();

  ExprContext* const* conjunct_ctxs = &conjunct_ctxs_[0];
  int num_conjunct_ctxs = conjunct_ctxs_.size();

  TupleRow* out_row = reinterpret_cast<TupleRow*>(*out_row_mem);
  CreateOutputRow(out_row, probe, build);

  if (!EvalConjuncts(other_conjunct_ctxs, num_other_conjunct_ctxs, out_row)) {
    return;
  }

  if (EvalConjuncts(conjunct_ctxs, num_conjunct_ctxs, out_row)) {
    (*rows_added)++;
    // Filled up out batch or hit limit
    if (UNLIKELY((*rows_added) == max_added_rows)) return;
    // Advance to next out row
    out_row_mem += out_batch->row_byte_size();
    out_row = reinterpret_cast<TupleRow*>(out_row_mem);
  }
}

void SpatialJoinNode::ProcessBuildBatch(RowBatchList* build_batch) {  
  int built_rows_count = build_batch->total_num_rows();
  RowBatchList::TupleRowIterator iterator = build_batch->Iterator();

  for(int i = 0; i < built_rows_count; i++) {
    TupleRow* row = iterator.GetRow();
    build_rows.push_back(row);
    iterator.Next();
  }

  // Sort the rows on the X Axis value
  RowsX1Comparator rowsX1Comparator(build_expr_ctx_);
  std::sort(build_rows.begin(), build_rows.end(), rowsX1Comparator);    
}

int SpatialJoinNode::ProcessProbeBatch(RowBatch* out_batch, RowBatch* probe_batch, int max_added_rows) {
  std::vector<TupleRow*> *probe_sorted_list = NULL;

  // ensure that we can add enough the required rows
  int row_idx = out_batch->AddRows(max_added_rows);
  DCHECK(row_idx != RowBatch::INVALID_ROW_INDEX);
  uint8_t* out_row_mem = reinterpret_cast<uint8_t*>(out_batch->GetRow(row_idx));

  // First time to enter for this probe patch, prepare the lists, and sort the probe batch
  if(probe_batch_pos_ == 0){
    probe_sorted_list = new std::vector<TupleRow*>();

    for(int i = 0; i < probe_batch->num_rows() ; i++){
      probe_sorted_list->push_back(probe_batch->GetRow(i));
    }         

    // Sort the rows on the X Axis value
    RowsX1Comparator rowsX1Comparator(probe_expr_ctx_);
    std::sort(probe_sorted_list->begin(), probe_sorted_list->end(), rowsX1Comparator);    

    build_batch_pos_ = 0;
  } else {
    probe_sorted_list = lastest_probe_batch;
  }         

  // Probing the build batch    
  int rows_added = 0;   

  int i = build_batch_pos_;
  int j = probe_batch_pos_;

  std::vector<TupleRow*> *R = &build_rows;
  std::vector<TupleRow*> *S = probe_sorted_list;

  int R_length = R->size();
  int S_length = S->size();

  // Plane Sweep Alg.
  while(i < R_length && j < S_length && rows_added < max_added_rows) {     
    TupleRow* r;
    TupleRow* s;

    if (GET_X1(R->at(i), build_expr_ctx_) < GET_X1(S->at(j), probe_expr_ctx_)) {
      r = R->at(i);
      int jj = (last_jj_ > 0) ? last_jj_ : j;
      last_jj_ = -1; 

      while ((jj < S_length) && (GET_X1((s = S->at(jj)), probe_expr_ctx_) <= GET_X2(r, build_expr_ctx_))) {
        if (IsIntersected(r, s, build_expr_ctx_, probe_expr_ctx_))
          AddOutputRow(out_batch, &rows_added, &out_row_mem, r, s, max_added_rows);

        jj++;

        if(rows_added == max_added_rows) {
          last_jj_ = jj;
          break;
        }
      }

      if(rows_added < max_added_rows) i++;
    } else {
      s = S->at(j);
      int ii = (last_ii_ > 0) ? last_ii_ : i;
      last_ii_ = -1;

      while ((ii < R_length) && (GET_X1((r = R->at(ii) ), build_expr_ctx_) <= GET_X2(s, probe_expr_ctx_))) {
        if (IsIntersected(r, s, build_expr_ctx_, probe_expr_ctx_))
          AddOutputRow(out_batch, &rows_added, &out_row_mem, r, s, max_added_rows);

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
  if(probe_batch_pos_ == probe_batch->num_rows()){      
    delete lastest_probe_batch;
  }

  if(lastest_probe_batch != probe_sorted_list ){      
    lastest_probe_batch = probe_sorted_list;
  } 

  out_batch->CommitRows(rows_added);

  return rows_added;  
}
