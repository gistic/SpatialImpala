// Copyright 2015 GISTIC.

#ifndef IMPALA_EXEC_SPATIAL_HDFS_SCAN_NODE_H
#define IMPALA_EXEC_SPATIAL_HDFS_SCAN_NODE_H

#include "exec/hdfs-scan-node.h"
#include "exec/r-tree.h"

using namespace impala;
using namespace std;

namespace spatialimpala {

class SpatialHdfsScanNode : public HdfsScanNode {
  public:
   SpatialHdfsScanNode(ObjectPool* pool, const TPlanNode& tnode,
       const DescriptorTbl& descs);
   ~SpatialHdfsScanNode();

   // Uses FileMetadata and casts it to RTree object.
   RTree* GetRTree(const string& filename);
   void SetRangeQuery(Rectangle* rect);
   Rectangle* GetRangeQuery();

   // Updates the number of scan ranges with the new one.
   // This method is thread safe.
   void UpdateScanRanges(const THdfsFileFormat::type& file_type,
       const THdfsCompression::type& compression_type, int num_of_splits,
       int new_num_of_splits);

  protected:
   Rectangle* range_;
};

}

#endif
