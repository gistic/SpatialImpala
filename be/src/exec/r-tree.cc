// Copyright 2015 GISTIC.

#include "exec/r-tree.h"
#include <queue>
#include <math.h>

using namespace spatialimpala;

RTree::RTree(int degree, int height, int tree_size) {
  this->degree_ = degree;
  this->height_ = height;
  this->tree_size_ = tree_size;
  this->node_count_ = (int) ((pow(degree, height) - 1) / (degree - 1));
  this->leaf_node_count_ = (int) (pow(degree, height - 1));
  this->non_leaf_node_count_ = node_count_ - leaf_node_count_;
  VLOG_QUERY << "Degree: " << this->degree_;
  VLOG_QUERY << "Height: " << this->height_;
  VLOG_QUERY << "Tree size: " << this->tree_size_;
  VLOG_QUERY << "Node Count: " << this->node_count_;
  VLOG_QUERY << "Leaf nodes: " << this->leaf_node_count_;
  VLOG_QUERY << "Non Leaf nodes: " << this->non_leaf_node_count_;
}

RTree::~RTree() {
  this->tree_.clear();
}

Rectangle RTree::GetMBR() {
  return tree_.empty() ? Rectangle(0,0,0,0) : (tree_[0]->mbr_);
}

void RTree::AddNode(char* node_data) {
  int offset = 0;
  for (int i = 0; i < 4; i++)
    offset = (offset << 8) | (*(node_data + i) & 0xff);
  
  unsigned long long x1_bytes = 0;
  for (int i = 0; i < 8; i++)
    x1_bytes = (x1_bytes << 8) | (*(node_data + X1_POS + i) & 0xff);

  unsigned long long y1_bytes = 0;
  for (int i = 0; i < 8; i++)
    y1_bytes = (y1_bytes << 8) | (*(node_data + Y1_POS + i) & 0xff);
  
  unsigned long long x2_bytes = 0;
  for (int i = 0; i < 8; i++)
    x2_bytes = (x2_bytes << 8) | (*(node_data + X2_POS + i) & 0xff);

  unsigned long long y2_bytes = 0;
  for (int i = 0; i < 8; i++)
    y2_bytes = (y2_bytes << 8) | (*(node_data + Y2_POS + i) & 0xff);

  double x1 = *reinterpret_cast<double*>(&x1_bytes);
  double y1 = *reinterpret_cast<double*>(&y1_bytes);
  double x2 = *reinterpret_cast<double*>(&x2_bytes);
  double y2 = *reinterpret_cast<double*>(&y2_bytes);
  
  VLOG_QUERY << "RTree Node: " << offset << " [ (" << x1 << ", " << y1 << "), (" << x2 << ", " << y2 <<") ]"; 
  Rectangle mbr(x1, y1, x2, y2);
  RTreeNode* node = new RTreeNode(offset, mbr);
  this->tree_.push_back(node);
}

int RTree::GetFirstChildOfNode(int index){
  return index * degree_ + 1;
}

int RTree::GetDegree() {
  return degree_;
}

void RTree::ApplyRangeQuery(Rectangle* range_query, vector<RTreeSplit>* list_of_splits) {
  // TODO: Apply the range query and create new splits.
  queue<int> to_be_searched;

  // Adding the first node index to be searched.
  to_be_searched.push(0);
  while (!to_be_searched.empty()) {
    int index_to_be_searched = to_be_searched.front();
    VLOG_QUERY << "Index to be searched: " << index_to_be_searched;
    to_be_searched.pop();
    int num_of_mbrs = index_to_be_searched == 0 ? 1 : degree_;
    for (int i = 0; i < num_of_mbrs; i++) {
      if (range_query->Contains(&tree_[index_to_be_searched + i]->mbr_)) {
        CreateRTreeSplit(index_to_be_searched + i, list_of_splits);
        VLOG_QUERY << "Node: " << index_to_be_searched + i << " is fully contained.";
        VLOG_QUERY << "Split created: [" << list_of_splits->back().start_offset << ", "
            << list_of_splits->back().end_offset << "]";
      }
      else if(range_query->Intersects(&tree_[index_to_be_searched + i]->mbr_)) {
        if (index_to_be_searched < non_leaf_node_count_)
          to_be_searched.push(GetFirstChildOfNode(index_to_be_searched + i));
        else {
          CreateRTreeSplit(index_to_be_searched + i, list_of_splits);
          VLOG_QUERY << "Node: " << index_to_be_searched + i << " is intersected.";
          VLOG_QUERY << "Split created: [" << list_of_splits->back().start_offset << ", "
              << list_of_splits->back().end_offset << "]";
        }
      }
    }
  }
}

void RTree::CreateRTreeSplit(int node_index, vector<RTreeSplit>* list_of_splits) {
  RTreeSplit split;
  split.start_offset = tree_[node_index]->offset_of_first_element_ + FILE_OFFSET;
  split.end_offset = (node_index == tree_.size() - 1) ?
  tree_size_ : tree_[node_index + 1]-> offset_of_first_element_;
  
  VLOG_QUERY << "Current Split: [" << split.start_offset << ", " << split.end_offset << "]";
  // The node was the last node in the level
  if (split.end_offset <= split.start_offset)
    split.end_offset = tree_size_;

    if (!list_of_splits->empty()) {
      RTreeSplit old_split = list_of_splits->back();
      VLOG_QUERY << "Old Split: [" << old_split.start_offset << ", " << old_split.end_offset << "]"; 

      // The 2 Splits should be merged into one.
      if (old_split.end_offset >= split.start_offset
        && split.end_offset >= old_split.start_offset) {

        split.start_offset = min(old_split.start_offset, split.start_offset);
        split.end_offset = max(old_split.end_offset, split.end_offset);
        list_of_splits->pop_back();
      }
  }

  list_of_splits->push_back(split);
}

RTreeNode::RTreeNode(int offset_of_first_element, Rectangle mbr) {
  this->offset_of_first_element_ = offset_of_first_element;
  this->mbr_ = mbr;
}

RTreeNode::~RTreeNode(){
}
