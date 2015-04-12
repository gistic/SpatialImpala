// Copyright 2015 GISTIC.

#ifndef IMPALA_EXEC_RTREE_H
#define IMPALA_EXEC_RTREE_H

#include "exec/rectangle.h"
#include <vector>

#define X1_POS 4
#define Y1_POS 12
#define X2_POS 20
#define Y2_POS 28

using namespace std;

namespace spatialimpala {

struct RTreeSplit {
  int start_offset;
  int end_offset;
};

// Rtree-Node implementation that represents a node with its minimum bounding rectangle.
class RTreeNode {
  public:
    RTreeNode(int offset_of_first_element, Rectangle mbr);
    virtual ~RTreeNode();
    
    int offset_of_first_element_;
    Rectangle mbr_;
};

// Rtree implementation that represents rtree records.
class RTree {
  public:
    RTree(int degree, int height, int tree_size);
    virtual ~RTree();

    Rectangle* GetMBR();
    void AddNode(char* node_data);
    int GetFirstChildOfNode(int index);
    int GetDegree();

    void ApplyRangeQuery(Rectangle* range_query, vector<RTreeSplit*>* list_of_splits);

    vector<RTreeNode*> tree_;

    int degree_;
    int height_;
    int tree_size_;
    int node_count_;
    int leaf_node_count_;
    int non_leaf_node_count_;

  private:
    void CreateRTreeSplit(int node_index, vector<RTreeSplit*>* list_of_splits);
};

}

#endif
