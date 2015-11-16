// Copyright 2015 Cloudera Inc.
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

#include "exec/subplan-node.h"
#include "exec/singular-row-src-node.h"
#include "exec/subplan-node.h"
#include "exec/unnest-node.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"

namespace impala {

SubplanNode::SubplanNode(ObjectPool* pool, const TPlanNode& tnode,
    const DescriptorTbl& descs)
    : ExecNode(pool, tnode, descs),
      input_batch_(NULL),
      input_eos_(false),
      input_row_idx_(0),
      current_input_row_(NULL),
      subplan_is_open_(false),
      subplan_eos_(false) {
}

Status SubplanNode::Init(const TPlanNode& tnode) {
  RETURN_IF_ERROR(ExecNode::Init(tnode));
  DCHECK_EQ(children_.size(), 2);
  SetContainingSubplan(this, child(1));
  return Status::OK();
}

void SubplanNode::SetContainingSubplan(SubplanNode* ancestor, ExecNode* node) {
  node->set_containing_subplan(ancestor);
  if (node->type() == TPlanNodeType::SUBPLAN_NODE) {
    // Only traverse the first child and not the second one, because the Subplan
    // parent of nodes inside it should be 'node' and not 'ancestor'.
    SetContainingSubplan(ancestor, node->child(0));
  } else {
    int num_children = node->num_children();
    for (int i = 0; i < num_children; ++i) {
      SetContainingSubplan(ancestor, node->child(i));
    }
  }
}

Status SubplanNode::Prepare(RuntimeState* state) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  RETURN_IF_ERROR(ExecNode::Prepare(state));
  input_batch_.reset(
      new RowBatch(child(0)->row_desc(), state->batch_size(), mem_tracker()));
  return Status::OK();
}

Status SubplanNode::Open(RuntimeState* state) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  RETURN_IF_ERROR(ExecNode::Open(state));
  RETURN_IF_ERROR(child(0)->Open(state));
  return Status::OK();
}

Status SubplanNode::GetNext(RuntimeState* state, RowBatch* row_batch, bool* eos) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  RETURN_IF_CANCELLED(state);
  RETURN_IF_ERROR(QueryMaintenance(state));
  *eos = false;

  while (true) {
    if (subplan_is_open_) {
      if (subplan_eos_) {
        // Reset the subplan before opening it again. At this point, all resources from
        // the subplan are assumed to have been transferred to the output row_batch.
        RETURN_IF_ERROR(child(1)->Reset(state));
        subplan_is_open_ = false;
      } else {
        // Continue fetching rows from the open subplan into the output row_batch.
        DCHECK(!row_batch->AtCapacity());
        RETURN_IF_ERROR(child(1)->GetNext(state, row_batch, &subplan_eos_));
        // Apply limit and check whether the output batch is at capacity.
        if (limit_ != -1 && num_rows_returned_ + row_batch->num_rows() >= limit_) {
          row_batch->set_num_rows(limit_ - num_rows_returned_);
          num_rows_returned_ += row_batch->num_rows();
          *eos = true;
          break;
        }
        if (row_batch->AtCapacity()) {
          num_rows_returned_ += row_batch->num_rows();
          return Status::OK();
        }
        // Check subplan_eos_ and repeat fetching until the output batch is at capacity
        // or we have reached our limit.
        continue;
      }
    }

    if (input_row_idx_ >= input_batch_->num_rows()) {
      input_batch_->TransferResourceOwnership(row_batch);
      if (input_eos_) {
        *eos = true;
        break;
      }
      // Could be at capacity after resources have been transferred to it.
      if (row_batch->AtCapacity()) return Status::OK();
      // Continue fetching input rows.
      input_batch_->Reset();
      RETURN_IF_ERROR(child(0)->GetNext(state, input_batch_.get(), &input_eos_));
      input_row_idx_ = 0;
      if (input_batch_->num_rows() == 0) continue;
    }

    // Advance the current input row to be picked up by dependent nodes,
    // and Open() the subplan.
    current_input_row_ = input_batch_->GetRow(input_row_idx_);
    ++input_row_idx_;
    RETURN_IF_ERROR(child(1)->Open(state));
    subplan_is_open_ = true;
    subplan_eos_ = false;
  }

  COUNTER_SET(rows_returned_counter_, num_rows_returned_);
  return Status::OK();
}

Status SubplanNode::Reset(RuntimeState* state) {
  input_eos_ = false;
  input_row_idx_ = 0;
  subplan_eos_ = false;
  num_rows_returned_ = 0;
  RETURN_IF_ERROR(child(0)->Reset(state));
  // If child(1) is not open it means that we have just Reset() it and returned from
  // GetNext() without opening it again. It is not safe to call Reset() on the same
  // exec node twice in a row.
  if (subplan_is_open_) RETURN_IF_ERROR(child(1)->Reset(state));
  return Status::OK();
}

void SubplanNode::Close(RuntimeState* state) {
  if (is_closed()) return;
  input_batch_.reset();
  ExecNode::Close(state);
}

}
