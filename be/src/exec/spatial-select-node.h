// Copyright 2015 GISTIC.

#ifndef IMPALA_EXEC_SPATIAL_SELECT_NODE_H
#define IMPALA_EXEC_SPATIAL_SELECT_NODE_H

#include "exec/select-node.h"
#include "exec/r-tree.h"

using namespace impala;

namespace spatialimpala {

class SpatialSelectNode : SelectNode {
  public:
    bool insideRange(TupleRow* row);

    RTree* rtree_;
    Rectangle* range_;
};

}

#endif

