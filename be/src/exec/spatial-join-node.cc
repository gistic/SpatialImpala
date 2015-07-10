// Copyright 2015 GISTIC.

#include "exec/spatial-join-node.h"

#include <sstream>

#include "exprs/expr.h"
#include "exprs/expr-context.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "util/runtime-profile.h"

#include "gen-cpp/PlanNodes_types.h"

using namespace boost;
using namespace impala;
using namespace llvm;
using namespace std;
using namespace spatialimpala;

DEFINE_bool(enable_spatial_probe_side_filtering, true,
    "Enables pushing build side filters to probe side");

const char* SpatialJoinNode::LLVM_CLASS_NAME = "class.impala::SpatialJoinNode";

SpatialJoinNode::SpatialJoinNode(
    ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
  : BlockingJoinNode("SpatialJoinNode", tnode.hash_join_node.join_op, pool, tnode, descs) {

  // The spatial join node does not support cross or anti joins
  DCHECK_NE(join_op_, TJoinOp::CROSS_JOIN);
  DCHECK_NE(join_op_, TJoinOp::LEFT_ANTI_JOIN);
  DCHECK_NE(join_op_, TJoinOp::RIGHT_SEMI_JOIN);
  DCHECK_NE(join_op_, TJoinOp::RIGHT_ANTI_JOIN);

  build_batch_pos_ = 0;
  can_add_probe_filters_ = tnode.spatial_join_node.add_probe_filters;
  can_add_probe_filters_ &= FLAGS_enable_spatial_probe_side_filtering;
}

Status SpatialJoinNode::Init(const TPlanNode& tnode) {
  RETURN_IF_ERROR(BlockingJoinNode::Init(tnode));
  DCHECK(tnode.__isset.hash_join_node);

  RETURN_IF_ERROR(
      Expr::CreateExprTree(pool_, tnode.spatial_join_node.spatial_join_expr,
                            &spatial_join_conjunct_ctx_));
  //RETURN_IF_ERROR(
  //    Expr::CreateExprTree(pool_, tnode.spatial_join_node.probe_expr, &ctx));
  //probe_expr_ctxs_.push_back(ctx);
  //RETURN_IF_ERROR(
  //    Expr::CreateExprTree(pool_, tnode.spatial_join_node.build_expr, &ctx));
  //build_expr_ctxs_.push_back(ctx);
  RETURN_IF_ERROR(
      Expr::CreateExprTrees(pool_, tnode.spatial_join_node.other_join_conjuncts,
                            &other_join_conjunct_ctxs_));
  return Status::OK;
}


Status SpatialJoinNode::Prepare(RuntimeState* state) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  RETURN_IF_ERROR(BlockingJoinNode::Prepare(state));

  // build and probe exprs are evaluated in the context of the rows produced by our
  // right and left children, respectively
  RETURN_IF_ERROR(build_expr_ctx_->Prepare(state, child(1)->row_desc()));
  RETURN_IF_ERROR(probe_expr_ctx_->Prepare(state, child(0)->row_desc()));

  // TODO: Use Spatial Join Conjuncts to apply the joining instead of the default OverlapJoin.
  // spatial_join_conjunct_ctx_ are evaluated in the context of the rows produced by this
  // node
  RETURN_IF_ERROR(spatial_join_conjunct_ctx_->Prepare(state, row_descriptor_));

  // other_join_conjunct_ctxs_ are evaluated in the context of the rows produced by this
  // node
  RETURN_IF_ERROR(Expr::Prepare(other_join_conjunct_ctxs_, state, row_descriptor_));

  return Status::OK;
}

void SpatialJoinNode::Close(RuntimeState* state) {
  if (is_closed()) return;
  build_expr_ctx_->Close(state);
  probe_expr_ctx_->Close(state);
  spatial_join_conjunct_ctx_->Close(state);
  Expr::Close(other_join_conjunct_ctxs_, state);
  BlockingJoinNode::Close(state);
}


Status SpatialJoinNode::ConstructBuildSide(RuntimeState* state) {
  RETURN_IF_ERROR(build_expr_ctx_->Open(state));
  RETURN_IF_ERROR(probe_expr_ctx_->Open(state));
  RETURN_IF_ERROR(spatial_join_conjunct_ctx_->Open(state));
  RETURN_IF_ERROR(Expr::Open(other_join_conjunct_ctxs_, state));

  // Do a full scan of child(1) and store everything in hash_tbl_
  // The hash join node needs to keep in memory all build tuples, including the tuple
  // row ptrs.  The row ptrs are copied into the hash table's internal structure so they
  // don't need to be stored in the build_pool_.
  RowBatchList build_batches;
  boost::scoped_ptr<ObjectPool> build_batch_pool;
  build_batch_pool.reset(new ObjectPool());
  while (true) {
    RowBatch* batch = build_batch_pool->Add(
        new RowBatch(child(1)->row_desc(), state->batch_size(), mem_tracker()));
    RETURN_IF_CANCELLED(state);
    RETURN_IF_ERROR(state->QueryMaintenance());
    bool eos;
    RETURN_IF_ERROR(child(1)->GetNext(state, batch, &eos));
    SCOPED_TIMER(build_timer_);
    build_batches.AddRowBatch(batch);
    COUNTER_SET(build_row_counter_,
        static_cast<int64_t>(build_batches.total_num_rows()));
    if (eos) break;
  }

  ProcessBuildBatch(&build_batches);
  return Status::OK;
}

Status SpatialJoinNode::InitGetNext(TupleRow* first_probe_row) {
  // TODO: Handle the initialization of RTree state.
  matched_probe_ = false;
  return Status::OK;
}


Status SpatialJoinNode::GetNext(RuntimeState* state, RowBatch* out_batch, bool* eos) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  RETURN_IF_ERROR(ExecDebugAction(TExecNodePhase::GETNEXT, state));
  RETURN_IF_CANCELLED(state);
  RETURN_IF_ERROR(state->QueryMaintenance());

  if (ReachedLimit()) {
    *eos = true;
    return Status::OK;
  }

  ScopedTimer<MonotonicStopWatch> probe_timer(probe_timer_);
  while (!eos_) {
    // Compute max rows that should be added to out_batch
    int64_t max_added_rows = out_batch->capacity() - out_batch->num_rows();
    if (limit() != -1) max_added_rows = min(max_added_rows, limit() - rows_returned());

    // Continue processing this row batch
    num_rows_returned_ +=
        ProcessProbeBatch(out_batch, probe_batch_.get(), max_added_rows);

    COUNTER_SET(rows_returned_counter_, num_rows_returned_);

    if (ReachedLimit() || out_batch->AtCapacity()) {
      *eos = ReachedLimit();
      break;
    }

    // Check to see if we're done processing the current probe batch
    if (probe_batch_pos_ == probe_batch_->num_rows()) {
      probe_batch_->TransferResourceOwnership(out_batch);
      probe_batch_pos_ = 0;
      if (out_batch->AtCapacity()) break;
      if (probe_side_eos_) {
        *eos = eos_ = true;
        break;
      } else {
        probe_timer.Stop();
        RETURN_IF_ERROR(child(0)->GetNext(state, probe_batch_.get(), &probe_side_eos_));
        probe_timer.Start();
        COUNTER_ADD(probe_row_counter_, probe_batch_->num_rows());
      }
    }
  }

  return Status::OK;
}

void SpatialJoinNode::AddToDebugString(int indentation_level, stringstream* out) const {
  *out << " Spatial Join=";
  *out << string(indentation_level * 2, ' ');
  *out << "("
       << " spatial_exprs=" << spatial_join_conjunct_ctx_->root()->DebugString()
       << " other_exprs=" << Expr::DebugString(other_join_conjunct_ctxs_);
  *out << ")";
}
