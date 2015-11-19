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

#ifndef IMPALA_EXEC_NESTED_LOOP_JOIN_NODE_H
#define IMPALA_EXEC_NESTED_LOOP_JOIN_NODE_H

#include <boost/scoped_ptr.hpp>
#include <string>

#include "exec/exec-node.h"
#include "exec/blocking-join-node.h"
#include "exec/row-batch-list.h"
#include "runtime/descriptors.h"  // for TupleDescriptor
#include "runtime/mem-pool.h"

#include "gen-cpp/PlanNodes_types.h"

namespace impala {

class Bitmap;
class RowBatch;
class TupleRow;
class RowBatchCache;

/// Operator to perform nested-loop join.
/// This operator does not support spill to disk. Supports all join modes except
/// null-aware left anti-join.
/// This operator will operate in one of two modes depending on the memory ownership of
/// row batches pulled from the child node on the build side. If the row batches own all
/// tuple memory, the non-copying mode is used and row batches are simply accumulated in
/// this node. If the batches reference tuple data they do not own, the copying mode is
/// used and all data is deep copied into memory owned by this node.
///
/// TODO: Add support for null-aware left-anti join.
class NestedLoopJoinNode : public BlockingJoinNode {
 public:
  NestedLoopJoinNode(ObjectPool* pool, const TPlanNode& tnode,
      const DescriptorTbl& descs);
  virtual ~NestedLoopJoinNode();

  virtual Status Init(const TPlanNode& tnode);
  virtual Status Prepare(RuntimeState* state);
  virtual Status Open(RuntimeState* state);
  virtual Status GetNext(RuntimeState* state, RowBatch* row_batch, bool* eos);
  virtual Status Reset(RuntimeState* state);
  virtual void Close(RuntimeState* state);

 protected:
  virtual Status InitGetNext(TupleRow* first_left_row);
  virtual Status ConstructBuildSide(RuntimeState* state);

 private:
  /////////////////////////////////////////
  /// BEGIN: Members that must be Reset()

  /// Creates and caches RowBatches for the build side. The RowBatch objects are owned by
  /// this cache, but the tuple data is always transferred to the output batch in
  /// GetNext() when eos_ is set to true. The cache helps to avoid creating new
  /// RowBatches after a Reset().
  boost::scoped_ptr<RowBatchCache> build_batch_cache_;

  /// List of build batches from child.
  RowBatchList raw_build_batches_;

  /// List of build batches that were deep copied and are backed by each row batch's pool.
  RowBatchList copied_build_batches_;

  /// Pointer to either raw_build_batches_ or copied_build_batches_ that contains the
  /// batches to use during the probe phase.
  RowBatchList* build_batches_;

  RowBatchList::TupleRowIterator build_row_iterator_;

  /// Ordinal position of current_build_row_ [0, num_build_rows_).
  int64_t current_build_row_idx_;

  /// Bitmap used to identify matching build tuples for the case of OUTER/SEMI/ANTI
  /// joins. Owned exclusively by the nested loop join node.
  /// Non-NULL if a bitmap is used to record build rows that match a probe row.
  boost::scoped_ptr<Bitmap> matching_build_rows_;

  /// If true, we've started processing the unmatched build rows. Only used in
  /// RIGHT OUTER JOIN, RIGHT ANTI JOIN and FULL OUTER JOIN modes.
  bool process_unmatched_build_rows_;

  /// END: Members that must be Reset()
  /////////////////////////////////////////

  /// Join conjuncts
  std::vector<ExprContext*> join_conjunct_ctxs_;

  Status GetNextInnerJoin(RuntimeState* state, RowBatch* output_batch);
  Status GetNextLeftOuterJoin(RuntimeState* state, RowBatch* output_batch);
  Status GetNextRightOuterJoin(RuntimeState* state, RowBatch* output_batch);
  Status GetNextLeftSemiJoin(RuntimeState* state, RowBatch* output_batch);
  Status GetNextRightSemiJoin(RuntimeState* state, RowBatch* output_batch);
  Status GetNextFullOuterJoin(RuntimeState* state, RowBatch* output_batch);
  Status GetNextLeftAntiJoin(RuntimeState* state, RowBatch* output_batch);
  Status GetNextRightAntiJoin(RuntimeState* state, RowBatch* output_batch);
  Status GetNextNullAwareLeftAntiJoin(RuntimeState* state, RowBatch* output_batch);

  /// Iterates through the build rows searching for matches with the current probe row.
  /// If a match is found, a result row is produced and is added to the output batch.
  /// Sets *return_output_batch to true if the limit is reached or output_batch is
  /// at capacity, false otherwise.
  Status FindBuildMatches(RuntimeState* state, RowBatch* output_batch,
      bool* return_output_batch);

  /// Retrieves the next probe row from the left child. This function does
  /// not guarantee that a valid probe row is produced as it may exit if
  /// the output_batch is at capacity. If a valid probe row is retrieved, the
  /// build row iterator is reset. Use HasValidProbeRow() to check if
  /// current_probe_row_ points to a valid probe row. Callers of this function
  /// should check if the 'output_batch' is at capacity and return immediately
  /// if that is the case.
  Status NextProbeRow(RuntimeState* state, RowBatch* output_batch);

  /// Processes all the build rows that couldn't be matched to a probe row.
  /// Depending on the join type, a result row might be produced for every unmatched
  /// build row. Valid for only specific join types (right-outer, right-semi
  /// and full-outer joins).
  Status ProcessUnmatchedBuildRows(RuntimeState* state, RowBatch* output_batch);

  /// Processes a probe row that couldn't match with any build rows and generates
  /// an output row according to the join type. Valid for only specific
  /// join types (full-outer, left-outer and left-anti joins).
  Status ProcessUnmatchedProbeRow(RuntimeState* state, RowBatch* output_batch);

  // Returns true if there are more rows to be processed, false otherwise.
  bool HasMoreProbeRows() { return probe_batch_pos_ != 0 || !probe_side_eos_; }

  // Returns true if there is a valid probe row to process, false otherwise.
  bool HasValidProbeRow() {
    DCHECK((current_probe_row_ == NULL) == (probe_batch_pos_ == 0));
    return current_probe_row_ != NULL;
  }

  /// Deep copy all build batches in raw_build_batches_ to copied_build_batches_.
  /// Resets all the source batches and clears raw_build_batches_.
  /// If the memory limit is exceeded while copying batches, returns a MEM_LIMIT_EXCEEDED
  /// status, sets the query status to MEM_LIMIT_EXCEEDED and leave the row batches to
  /// be cleaned up later when the node is closed.
  Status DeepCopyBuildBatches(RuntimeState* state);
};

}

#endif
