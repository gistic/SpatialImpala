// Copyright 2015 GISTIC.

#include "exec/spatial-hdfs-scan-node.h"

using namespace spatialimpala;

RTree* SpatialHdfsScanNode::getRTree() {
  return this->has_local_index_ ? this->rtree_ : NULL;
}
