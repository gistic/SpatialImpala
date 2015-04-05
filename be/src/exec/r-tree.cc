// Copyright 2015 GISTIC.

#include "exec/r-tree.h"

using namespace spatialimpala;

RTree::RTree(int degree, int height) {
  this->degree_ = degree;
  this->height_ = height;
}

RTree::~RTree() {
}

Rectangle* RTree::GetMBR() {
  return tree_.empty() ? NULL : &(tree_[0]->mbr_);
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
  
  Rectangle mbr(x1, y1, x2, y2);
  RTreeNode node(offset, mbr);
  this->tree_.push_back(&node);
}

int RTree::GetFirstChildOfNode(int index){
  return index * degree_ + 1;
}

int RTree::GetDegree() {
  return degree_;
}

RTreeNode::RTreeNode(int offset_of_first_element, Rectangle mbr) {
  this->offset_of_first_element_ = offset_of_first_element;
  this->mbr_ = mbr;
}

RTreeNode::~RTreeNode(){
}
