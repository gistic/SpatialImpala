// Copyright 2015 GISTIC.

#ifndef IMPALA_EXEC_SPATIAL_SELECT_NODE_H
#define IMPALA_EXEC_SPATIAL_SELECT_NODE_H

#include "exec/select-node.h"
#include "exec/r-tree.h"
#include "exprs/slot-ref.h"

using namespace impala;

namespace spatialimpala {

class SpatialSelectNode : public SelectNode {
  public:
   SpatialSelectNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);
   virtual Status Open(RuntimeState* state);
   virtual Status Prepare(RuntimeState* state);

  private:
   bool InsideRange(TupleRow* row);
   virtual bool CopyRows(RowBatch* output_batch);

   RTree* rtree_;
   Rectangle* range_;

   // Used to get values of the Spatial columns in a single row.
   // TODO: Should be updated to a Spatial Column.
   SlotRef* x_;
   SlotRef* y_;
};

}

#endif
