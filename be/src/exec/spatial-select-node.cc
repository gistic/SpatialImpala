// Copyright 2015 GISTIC.

#include "exec/spatial-select-node.h"
#include "exec/spatial-hdfs-scan-node.h"

using namespace spatialimpala;

SpatialSelectNode::SpatialSelectNode(
    ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
    : SelectNode(pool, tnode, descs) {
  TSpatialSelectNode spatial_select_node = tnode.spatial_select_node;
  this->range_ = new Rectangle(spatial_select_node.rectangle);
  this->x_ = new SlotRef(spatial_select_node.x);
  this->y_ = new SlotRef(spatial_select_node.y);
}

Status SpatialSelectNode::Open(RuntimeState* state) {
  RETURN_IF_ERROR(SelectNode::Open(state));

  // If the underlying child is of type SpatialHdfsScanNode, assign the range query to it.
  SpatialHdfsScanNode* scan_node = dynamic_cast<SpatialHdfsScanNode*>(child(0));
  if (scan_node != NULL) { scan_node->SetRangeQuery(range_); }
  return Status::OK;
}

Status SpatialSelectNode::Prepare(RuntimeState* state) {
  RETURN_IF_ERROR(SelectNode::Prepare(state));

  this->x_->Prepare(state, child(0)->row_desc(), NULL);
  this->y_->Prepare(state, child(0)->row_desc(), NULL);
  return Status::OK;
}

bool SpatialSelectNode::InsideRange(TupleRow* row) {
  DoubleVal x = x_->GetDoubleVal(NULL, row);
  DoubleVal y = y_->GetDoubleVal(NULL, row);
  
  DoubleVal null_val = DoubleVal::null();
  if (x == null_val || y == null_val) { return false; }
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
