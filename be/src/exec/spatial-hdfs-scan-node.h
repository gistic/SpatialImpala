// Copyright 2015 GISTIC.

#ifndef IMPALA_EXEC_SPATIAL_HDFS_SCAN_NODE_H
#define IMPALA_EXEC_SPATIAL_HDFS_SCAN_NODE_H

#include "exec/hdfs-scan-node.h"
#include "exec/r-tree.h"

using namespace impala;

namespace spatialimpala {

class SpatialHdfsScanNode : HdfsScanNode {
  public:
    RTree* getRTree();

    RTree* rtree_;
    bool has_local_index_;
};

}

#endif

