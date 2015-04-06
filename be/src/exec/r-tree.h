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
    RTree(int degree, int height);
    virtual ~RTree();
    Rectangle* GetMBR();
    void AddNode(char* node_data);
    int GetFirstChildOfNode(int index);
    int GetDegree();

    vector<RTreeNode*> tree_;
    int degree_;
    int height_;
};

}

#endif
