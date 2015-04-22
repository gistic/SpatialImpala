// Copyright 2015 GISTIC.

#include "exec/spatial-hdfs-scan-node.h"

using namespace spatialimpala;

SpatialHdfsScanNode::SpatialHdfsScanNode(ObjectPool* pool, const TPlanNode& tnode,
                           const DescriptorTbl& descs)
    : HdfsScanNode(pool, tnode, descs) {
  TSpatialHdfsScanNode spatial_hdfs_scan_node = tnode.spatial_hdfs_scan_node;
  this->range_ = new Rectangle(spatial_hdfs_scan_node.rectangle);
}

SpatialHdfsScanNode::~SpatialHdfsScanNode() {
}

RTree* SpatialHdfsScanNode::GetRTree(const string& filename) {
  return reinterpret_cast<RTree*>(GetFileMetadata(filename));
}

void SpatialHdfsScanNode::SetRangeQuery(Rectangle* rect) {
  range_ = rect;
}

Rectangle* SpatialHdfsScanNode::GetRangeQuery() {
  return range_;
}

void SpatialHdfsScanNode::UpdateScanRanges(const THdfsFileFormat::type& file_type,
  const THdfsCompression::type& compression_type, int num_of_splits, int new_num_of_splits) {
  // handles updating progress_updater_ either by creating a new one,
  // or by updating the number of completed (splits/scan ranges).
  if (num_of_splits >= new_num_of_splits) {
    int scan_ranges_to_complete = num_of_splits - new_num_of_splits;
    scan_ranges_complete_counter()->Add(scan_ranges_to_complete);
    progress_.Update(scan_ranges_to_complete);
    {
      ScopedSpinLock l(&file_type_counts_lock_);
      file_type_counts_[make_pair(file_type, compression_type)] += scan_ranges_to_complete;
    }
  }
  else  {
    int scan_ranges_to_add = new_num_of_splits - num_of_splits;
    stringstream ss;
    ss << "Splits complete (node=" << id() << "):";
    {
      boost::unique_lock<boost::mutex> lock(lock_);
      int num_of_completed = progress_.num_complete();
      progress_ = ProgressUpdater(ss.str(), progress_.total() + scan_ranges_to_add);
      progress_.Update(num_of_completed);
    }
  }
}
