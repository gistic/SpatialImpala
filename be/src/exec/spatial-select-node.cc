// Copyright 2015 GISTIC.

#include "exec/spatial-select-node.h"

using namespace spatialimpala;

bool SpatialSelectNode::InsideRange(TupleRow* row) {
  DoubleVal x = x_->GetDoubleVal(NULL, row);
  DoubleVal y = y_->GetDoubleVal(NULL, row);
  
  DoubleVal null_val = DoubleVal::null();
  if (x == null_val || y == null_val)
    return false;

  return range_->Contains(x.val, y.val);
}

bool SpatialSelectNode::CopyRows(RowBatch* output_batch) {
  for (; child_row_idx_ < child_row_batch_->num_rows(); ++child_row_idx_) {
    // Add a new row to output_batch
    int dst_row_idx = output_batch->AddRow();
    if (dst_row_idx == RowBatch::INVALID_ROW_INDEX) return true;
    TupleRow* dst_row = output_batch->GetRow(dst_row_idx);
    TupleRow* src_row = child_row_batch_->GetRow(child_row_idx_);

    // Checking if the spatial data is within the range.
    if (InsideRange(src_row)) {
      output_batch->CopyRow(src_row, dst_row);
      output_batch->CommitLastRow();
      ++num_rows_returned_;
      COUNTER_SET(rows_returned_counter_, num_rows_returned_);
      if (ReachedLimit()) return true;
    }
  }
  return output_batch->AtCapacity();
}
