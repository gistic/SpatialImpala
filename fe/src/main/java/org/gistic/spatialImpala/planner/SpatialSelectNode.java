// Copyright 2015 GISTIC.

package org.gistic.spatialImpala.planner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.impala.planner.*;
import com.cloudera.impala.analysis.Analyzer;
import com.cloudera.impala.thrift.TExplainLevel;
import com.cloudera.impala.thrift.TPlanNode;
import com.cloudera.impala.thrift.TPlanNodeType;
import com.cloudera.impala.thrift.TSpatialSelectNode;
import com.google.common.base.Preconditions;
import org.gistic.spatialImpala.catalog.*;

/**
 * Node that applies conjuncts and a limit clause. Has exactly one child.
 */
public class SpatialSelectNode extends SelectNode {
  private final static Logger LOG = LoggerFactory.getLogger(SelectNode.class);
  
  private Rectangle rect_;

  protected SpatialSelectNode(PlanNodeId id, PlanNode child, Rectangle rect) {
    super(id, child);
    this.rect_ = rect;
    this.displayName_ = "SPATIAL_SELECT";
  }
  
  public Rectangle getRectangle(){
	  return this.rect_;
  }

  @Override
  protected void toThrift(TPlanNode msg) {
    msg.node_type = TPlanNodeType.SPATIAL_SELECT_NODE;
    msg.spatial_select_node = new TSpatialSelectNode(rect_.toThrift());
  }

  /*@Override
  public void computeStats(Analyzer analyzer) {
    super.computeStats(analyzer);
    if (getChild(0).cardinality_ == -1) {
      cardinality_ = -1;
    } else {
      cardinality_ = Math.round(((double) getChild(0).cardinality_) * computeSelectivity());
      Preconditions.checkState(cardinality_ >= 0);
    }
    LOG.debug("stats Select: cardinality=" + Long.toString(cardinality_));
  }

  @Override
  protected String getNodeExplainString(String prefix, String detailPrefix,
      TExplainLevel detailLevel) {
    StringBuilder output = new StringBuilder();
    output.append(String.format("%s%s:%s\n", prefix, id_.toString(), displayName_));
    if (detailLevel.ordinal() >= TExplainLevel.STANDARD.ordinal()) {
      if (!conjuncts_.isEmpty()) {
        output.append(detailPrefix + "predicates: " +
            getExplainString(conjuncts_) + "\n");
      }
    }
    return output.toString();
  }*/
}
