// Copyright 2015 GISTIC.

#ifndef IMPALA_EXEC_SPATIAL_JOIN_NODE_H
#define IMPALA_EXEC_SPATIAL_JOIN_NODE_H

#include <boost/scoped_ptr.hpp>
#include <boost/thread.hpp>
#include <string>

#include "exec/row-batch-list.h"
#include "exec/exec-node.h"
#include "exec/blocking-join-node.h"

#include "gen-cpp/PlanNodes_types.h"  // for TJoinOp

using namespace impala;

namespace spatialimpala {

// Node for in-memory spatial joins:
// - builds up a RTree with the rows produced by our right input
//   (child(1)); build exprs are not used.
// - for each row from our left input, probes the RTree to retrieve
//   matching entries; the probe exprs are generated from the spatial-join predicates.
//
// Row batches:
// - In general, we are not able to pass our output row batch on to our left child (when
//   we're fetching the probe rows): if we have a 1xn join, our output will contain
//   multiple rows per left input row
// - TODO: fix this, so in the case of 1x1/nx1 joins (for instance, fact to dimension tbl)
//   we don't do these extra copies
class SpatialJoinNode : public BlockingJoinNode {
 public:
  SpatialJoinNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);

  virtual Status Init(const TPlanNode& tnode);
  virtual Status Prepare(RuntimeState* state);
  // Open() implemented in BlockingJoinNode
  virtual Status GetNext(RuntimeState* state, RowBatch* row_batch, bool* eos);
  virtual void Close(RuntimeState* state);

  static const char* LLVM_CLASS_NAME;

 protected:
  virtual void AddToDebugString(int indentation_level, std::stringstream* out) const;
  virtual Status InitGetNext(TupleRow* first_probe_row);
  virtual Status ConstructBuildSide(RuntimeState* state);

 private:

  // position of the last processed build for the current probe pos.
  int build_batch_pos_;

  TupleRow** built_rows_;
  TupleRow** lastest_probe_batch_;
  int built_rows_count_;
  int last_ii_;
  int last_jj_;

  // our predicate is separated into
  // build_expr_ (over child(1)) and probe_expr_ (over child(0))
  ExprContext* probe_expr_ctx_;
  ExprContext* build_expr_ctx_;

  // spatial-join conjunct from the JOIN clause
  ExprContext* spatial_join_conjunct_ctx_;

  // non-spatial-join conjuncts from the JOIN clause
  std::vector<ExprContext*> other_join_conjunct_ctxs_;

  // Construct the build hash table, adding all the rows in 'build_batch'
  void ProcessBuildBatch(RowBatchList* build_batch);

  // Processes a probe batch for the common (non right-outer join) cases.
  //  out_batch: the batch for resulting tuple rows
  //  probe_batch: the probe batch to process.  This function can be called to
  //    continue processing a batch in the middle
  //  max_added_rows: maximum rows that can be added to out_batch
  // return the number of rows added to out_batch
  int ProcessProbeBatch(RowBatch* out_batch, RowBatch* probe_batch, int max_added_rows);
};

}

#endif
