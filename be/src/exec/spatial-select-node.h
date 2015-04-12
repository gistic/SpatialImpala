// Copyright 2015 GISTIC.

#ifndef IMPALA_EXEC_SPATIAL_SELECT_NODE_H
#define IMPALA_EXEC_SPATIAL_SELECT_NODE_H

#include "exec/select-node.h"
#include "exec/r-tree.h"
#include "exprs/slot-ref.h"

using namespace impala;

namespace spatialimpala {

class SpatialSelectNode : SelectNode {
  private:
    bool InsideRange(TupleRow* row);
    bool CopyRows(RowBatch* output_batch);

    RTree* rtree_;
    Rectangle* range_;
    
    // Used to get values of the Spatial columns in a single row.
    // TODO: Should be updated to a Spatial Column.
    SlotRef* x_;
    SlotRef* y_;
};

}

#endif

