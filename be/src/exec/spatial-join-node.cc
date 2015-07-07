// Copyright 2015 GISTIC.

#include "exec/spatial-join-node.h"

#include <sstream>

#include "exprs/expr.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "util/debug-util.h"
#include "util/runtime-profile.h"

#include "gen-cpp/PlanNodes_types.h"

using namespace boost;
using namespace impala;
using namespace llvm;
using namespace std;
using namespace spatialimpala;

const char* SpatialJoinNode::LLVM_CLASS_NAME = "class.impala::SpatialJoinNode";

SpatialJoinNode::SpatialJoinNode(
    ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
  : BlockingJoinNode("HashJoinNode", tnode.hash_join_node.join_op, pool, tnode, descs) {

  // The spatial join node does not support cross or anti joins
  DCHECK_NE(join_op_, TJoinOp::CROSS_JOIN);
  DCHECK_NE(join_op_, TJoinOp::LEFT_ANTI_JOIN);
  DCHECK_NE(join_op_, TJoinOp::RIGHT_SEMI_JOIN);
  DCHECK_NE(join_op_, TJoinOp::RIGHT_ANTI_JOIN);

  // TODO: Handle Spatial Join Node thrift
  //can_add_probe_filters_ = tnode.hash_join_node.add_probe_filters;
  //can_add_probe_filters_ &= FLAGS_enable_probe_side_filtering;
}

Status SpatialJoinNode::Init(const TPlanNode& tnode) {
  RETURN_IF_ERROR(BlockingJoinNode::Init(tnode));
  DCHECK(tnode.__isset.hash_join_node);
  // TODO: Handle creating spatial join conjuncts;

  //RETURN_IF_ERROR(
  //    Expr::CreateExprTrees(pool_, tnode.hash_join_node.spatial_join_conjuncts,
  //                          &spatial_join_conjunct_ctxs_));
  //RETURN_IF_ERROR(
  //    Expr::CreateExprTrees(pool_, tnode.hash_join_node.other_join_conjuncts,
  //                          &other_join_conjunct_ctxs_));
  return Status::OK;
}


Status SpatialJoinNode::Prepare(RuntimeState* state) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  RETURN_IF_ERROR(BlockingJoinNode::Prepare(state));

  // spatial_join_conjunct_ctxs_ are evaluated in the context of the rows produced by this
  // node
  RETURN_IF_ERROR(Expr::Prepare(spatial_join_conjunct_ctxs_, state, row_descriptor_));

  // other_join_conjunct_ctxs_ are evaluated in the context of the rows produced by this
  // node
  RETURN_IF_ERROR(Expr::Prepare(other_join_conjunct_ctxs_, state, row_descriptor_));

  return Status::OK;
}

void SpatialJoinNode::Close(RuntimeState* state) {
  if (is_closed()) return;
  Expr::Close(spatial_join_conjunct_ctxs_, state);
  Expr::Close(other_join_conjunct_ctxs_, state);
  BlockingJoinNode::Close(state);
}


Status SpatialJoinNode::ConstructBuildSide(RuntimeState* state) {
  RETURN_IF_ERROR(Expr::Open(spatial_join_conjunct_ctxs_, state));
  RETURN_IF_ERROR(Expr::Open(other_join_conjunct_ctxs_, state));

  // Do a full scan of child(1) and store everything in hash_tbl_
  // The hash join node needs to keep in memory all build tuples, including the tuple
  // row ptrs.  The row ptrs are copied into the hash table's internal structure so they
  // don't need to be stored in the build_pool_.
  RowBatch build_batch(child(1)->row_desc(), state->batch_size(), mem_tracker());
  RETURN_IF_ERROR(child(1)->Open(state));
  while (true) {
    RETURN_IF_CANCELLED(state);
    RETURN_IF_ERROR(state->QueryMaintenance());
    bool eos;
    RETURN_IF_ERROR(child(1)->GetNext(state, &build_batch, &eos));
    SCOPED_TIMER(build_timer_);
    // take ownership of tuple data of build_batch
    build_pool_->AcquireData(build_batch.tuple_data_pool(), false);
    RETURN_IF_ERROR(state->QueryMaintenance());

    ProcessBuildBatch(&build_batch);
    
    // TODO: Handle build row counter.
    //COUNTER_SET(build_row_counter_, hash_tbl_->size());
    build_batch.Reset();
    DCHECK(!build_batch.AtCapacity());
    if (eos) break;
  }

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

  // TODO: Probe the built RTree and create output rows.
  return Status::OK;
}

void SpatialJoinNode::AddToDebugString(int indentation_level, stringstream* out) const {
  *out << " Spatial Join=";
  *out << string(indentation_level * 2, ' ');
  *out << "("
       << " spatial_exprs=" << Expr::DebugString(spatial_join_conjunct_ctxs_)
       << " other_exprs=" << Expr::DebugString(other_join_conjunct_ctxs_);
  *out << ")";
}

void SpatialJoinNode::ProcessBuildBatch(RowBatch* build_batch) {
  // TODO: insert build rows into RTree.
}
