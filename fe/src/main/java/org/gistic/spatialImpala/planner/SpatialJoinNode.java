// Copyright 2015 GISTIC.
//

package org.gistic.spatialImpala.planner;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.impala.analysis.ExprSubstitutionMap;
import com.cloudera.impala.planner.PlanNode;
import com.cloudera.impala.planner.Planner;
import com.cloudera.impala.analysis.Analyzer;
import com.cloudera.impala.analysis.Expr;
import com.cloudera.impala.analysis.SlotDescriptor;
import com.cloudera.impala.analysis.JoinOperator;
import com.cloudera.impala.analysis.SlotRef;
import com.cloudera.impala.analysis.TableRef;
import com.cloudera.impala.catalog.ColumnStats;
import com.cloudera.impala.catalog.Table;
import com.cloudera.impala.catalog.Type;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.common.InternalException;
import com.cloudera.impala.common.Pair;
import com.cloudera.impala.thrift.TEqJoinCondition;
import com.cloudera.impala.thrift.TExplainLevel;
import com.cloudera.impala.thrift.TExpr;
import com.cloudera.impala.thrift.TSpatialJoinNode;
import com.cloudera.impala.thrift.TPlanNode;
import com.cloudera.impala.thrift.TPlanNodeType;
import com.cloudera.impala.thrift.TQueryOptions;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;


import org.gistic.spatialImpala.analysis.OverlapQueryPredicate;

public class SpatialJoinNode extends PlanNode {
  private final static Logger LOG = LoggerFactory.getLogger(SpatialJoinNode.class);

  // Default per-host memory requirement used if no valid stats are available.
  // TODO: Come up with a more useful heuristic (e.g., based on scanned partitions).
  private final static long DEFAULT_PER_HOST_MEM = 2L * 1024L * 1024L * 1024L;

  // tableRef corresponding to the left or right child of this join; only used for
  // getting the plan hints of this join, so it's irrelevant which child exactly
  private final TableRef tblRef_;
  
  private final JoinOperator joinOp_;
  
  public enum DistributionMode {
    NONE("NONE"),
    BROADCAST("BROADCAST"),
    PARTITIONED("PARTITIONED");

    private final String description;

    private DistributionMode(String descr) {
      this.description = descr;
    }

    @Override
    public String toString() { return description; }
  }
  
//join conjuncts_ from the JOIN clause that aren't equi-join predicates
 private List<Expr> otherJoinConjuncts_;

 private DistributionMode distrMode_;

  // overlap conjunct
  private OverlapQueryPredicate overlapPredicate_;

  // If true, this node can add filters for the probe side that can be generated
  // after reading the build side. This can be very helpful if the join is selective and
  // there are few build rows.
  private boolean addProbeFilters_;

  public SpatialJoinNode(
      PlanNode outer, PlanNode inner, TableRef tblRef,
      OverlapQueryPredicate overlapPredicate, List<Expr> otherJoinConjuncts) {
    super("SPATIAL JOIN");
    tupleIds_.addAll(outer.getTupleIds());
    tupleIds_.addAll(inner.getTupleIds());
    tblRefIds_.addAll(outer.getTblRefIds());
    tblRefIds_.addAll(inner.getTblRefIds());
    tblRef_ = tblRef;
    joinOp_ = tblRef.getJoinOp();
    distrMode_ = DistributionMode.NONE;
    overlapPredicate_ = overlapPredicate;
    children_.add(outer);
    children_.add(inner);
    
    otherJoinConjuncts_ = otherJoinConjuncts;

    // Inherits all the nullable tuple from the children
    // Mark tuples that form the "nullable" side of the outer join as nullable.
    nullableTupleIds_.addAll(inner.getNullableTupleIds());
    nullableTupleIds_.addAll(outer.getNullableTupleIds());
    if (joinOp_.equals(JoinOperator.FULL_OUTER_JOIN)) {
      nullableTupleIds_.addAll(outer.getTupleIds());
      nullableTupleIds_.addAll(inner.getTupleIds());
    } else if (joinOp_.equals(JoinOperator.LEFT_OUTER_JOIN)) {
      nullableTupleIds_.addAll(inner.getTupleIds());
    } else if (joinOp_.equals(JoinOperator.RIGHT_OUTER_JOIN)) {
      nullableTupleIds_.addAll(outer.getTupleIds());
    }
  }

  public OverlapQueryPredicate getOverlapPredicate() { return overlapPredicate_; }
  public JoinOperator getJoinOp() { return joinOp_; }
  public TableRef getTableRef() { return tblRef_; }
  public DistributionMode getDistributionMode() { return distrMode_; }
  public void setDistributionMode(DistributionMode distrMode) { distrMode_ = distrMode; }
  public void setAddProbeFilters(boolean b) { addProbeFilters_ = true; }

  @Override
  public void init(Analyzer analyzer) throws InternalException {
    assignConjuncts(analyzer);

    // Set smap to the combined childrens' smaps and apply that to all conjuncts_.
    createDefaultSmap(analyzer);

    computeStats(analyzer);
    assignedConjuncts_ = analyzer.getAssignedConjuncts();
    
    ExprSubstitutionMap combinedChildSmap = getCombinedChildSmap();
    otherJoinConjuncts_ =
            Expr.substituteList(otherJoinConjuncts_, combinedChildSmap, analyzer);
  }

  @Override
  public void computeStats(Analyzer analyzer) {
    super.computeStats(analyzer);

    // For a join between child(0) and child(1), we look for join conditions "L.c = R.d"
    // (with L being from child(0) and R from child(1)) and use as the cardinality
    // estimate the maximum of
    //   child(0).cardinality * R.cardinality / # distinct values for R.d
    //     * child(1).cardinality / R.cardinality
    // across all suitable join conditions, which simplifies to
    //   child(0).cardinality * child(1).cardinality / # distinct values for R.d
    // The reasoning is that
    // - each row in child(0) joins with R.cardinality/#DV_R.d rows in R
    // - each row in R is 'present' in child(1).cardinality / R.cardinality rows in
    //   child(1)
    //
    // This handles the very frequent case of a fact table/dimension table join
    // (aka foreign key/primary key join) if the primary key is a single column, with
    // possible additional predicates against the dimension table. An example:
    // FROM FactTbl F JOIN Customers C D ON (F.cust_id = C.id) ... WHERE C.region = 'US'
    // - if there are 5 regions, the selectivity of "C.region = 'US'" would be 0.2
    //   and the output cardinality of the Customers scan would be 0.2 * # rows in
    //   Customers
    // - # rows in Customers == # of distinct values for Customers.id
    // - the output cardinality of the join would be F.cardinality * 0.2

    long maxNumDistinct = 0;
    if (overlapPredicate_.getChild(0).unwrapSlotRef(false) != null){
	    SlotRef rhsSlotRef = overlapPredicate_.getChild(1).unwrapSlotRef(false);
	    if (rhsSlotRef != null){
		    SlotDescriptor slotDesc = rhsSlotRef.getDesc();
		    if (slotDesc != null){
			    ColumnStats stats = slotDesc.getStats();
			    if (stats.hasNumDistinctValues()){
				    long numDistinct = stats.getNumDistinctValues();
				    Table rhsTbl = slotDesc.getParent().getTable();
				    if (rhsTbl != null && rhsTbl.getNumRows() != -1) {
				      // we can't have more distinct values than rows in the table, even though
				      // the metastore stats may think so
				      LOG.debug("#distinct=" + numDistinct + " #rows="
				          + Long.toString(rhsTbl.getNumRows()));
				      numDistinct = Math.min(numDistinct, rhsTbl.getNumRows());
				    }
				    if (getChild(1).getCardinality() != -1 && numDistinct != -1) {
				      // The number of distinct values of a slot cannot exceed the cardinality_
				      // of the plan node the slot is coming from.
				      numDistinct = Math.min(numDistinct, getChild(1).getCardinality());
				    }
				    maxNumDistinct = Math.max(maxNumDistinct, numDistinct);
				    LOG.debug("min slotref=" + rhsSlotRef.toSql() + " #distinct="
				          + Long.toString(numDistinct));
			    }
		    }
	    }
    }
    
    if (maxNumDistinct == 0) {
      // if we didn't find any suitable join predicates or don't have stats
      // on the relevant columns, we very optimistically assume we're doing an
      // FK/PK join (which doesn't alter the cardinality of the left-hand side)
      cardinality_ = getChild(0).getCardinality();
    } else if (getChild(0).getCardinality() != -1 && getChild(1).getCardinality() != -1) {
      cardinality_ = multiplyCardinalities(getChild(0).getCardinality(),
          getChild(1).getCardinality());
      cardinality_ = Math.round((double)cardinality_ / (double) maxNumDistinct);
    }

    // Impose lower/upper bounds on the cardinality based on the join type.
    long leftCard = getChild(0).getCardinality();
    long rightCard = getChild(1).getCardinality();
    switch (joinOp_) {
      case LEFT_SEMI_JOIN: {
        if (leftCard != -1) {
          cardinality_ = Math.min(leftCard, cardinality_);
        }
        break;
      }
      case RIGHT_SEMI_JOIN: {
        if (rightCard != -1) {
          cardinality_ = Math.min(rightCard, cardinality_);
        }
        break;
      }
      case LEFT_OUTER_JOIN: {
        if (leftCard != -1) {
          cardinality_ = Math.max(leftCard, cardinality_);
        }
        break;
      }
      case RIGHT_OUTER_JOIN: {
        if (rightCard != -1) {
          cardinality_ = Math.max(rightCard, cardinality_);
        }
        break;
      }
      case FULL_OUTER_JOIN: {
        if (leftCard != -1 && rightCard != -1) {
          long cardinalitySum = addCardinalities(leftCard, rightCard);
          cardinality_ = Math.max(cardinalitySum, cardinality_);
        }
        break;
      }
      // Cap cardinality at 1 because a value of 0 triggers pathological edge cases.
      case LEFT_ANTI_JOIN: {
        if (leftCard != -1) {
          cardinality_ = leftCard;
          if (rightCard != -1) {
            cardinality_ = Math.max(1, leftCard - rightCard);
          }
        }
        break;
      }
      case RIGHT_ANTI_JOIN: {
        if (rightCard != -1) {
          cardinality_ = rightCard;
          if (leftCard != -1) {
            cardinality_ = Math.max(1, rightCard - leftCard);
          }
        }
        break;
      }
    }

    Preconditions.checkState(hasValidStats());
    LOG.debug("stats HashJoin: cardinality=" + Long.toString(cardinality_));
  }

  @Override
  protected String debugString() {
    return Objects.toStringHelper(this)
        .add("eqJoinConjuncts_", overlapPredicateDebugString())
        .addValue(super.debugString())
        .toString();
  }

  private String overlapPredicateDebugString() {
    Objects.ToStringHelper helper = Objects.toStringHelper(this);
    
    helper.add("lhs" , overlapPredicate_.getChild(0)).add("rhs", overlapPredicate_.getChild(1));
    
    return helper.toString();
  }

  @Override
  protected void toThrift(TPlanNode msg) {
    msg.node_type = TPlanNodeType.SPATIAL_JOIN_NODE;
    msg.spatial_join_node = new TSpatialJoinNode();
    msg.spatial_join_node.join_op = joinOp_.toThrift();
    
	TExpr spatialJoinCond = overlapPredicate_.treeToThrift();
    msg.spatial_join_node.spatial_join_expr = spatialJoinCond;
    
    TExpr exprBuild = overlapPredicate_.getChild(1).treeToThrift();
	TExpr exprProbe = overlapPredicate_.getChild(0).treeToThrift();
	msg.spatial_join_node.build_expr = exprBuild; 
	msg.spatial_join_node.probe_expr = exprProbe;
	
	for (Expr e: otherJoinConjuncts_) {
      msg.spatial_join_node.addToOther_join_conjuncts(e.treeToThrift());
    }
	
	msg.spatial_join_node.setAdd_probe_filters(addProbeFilters_);
  }

  @Override
  protected String getDisplayLabelDetail() {
    StringBuilder output = new StringBuilder(joinOp_.toString());
    if (distrMode_ != DistributionMode.NONE) output.append(", " + distrMode_.toString());
    return output.toString();
  }

  @Override
  protected String getNodeExplainString(String prefix, String detailPrefix,
      TExplainLevel detailLevel) {
    StringBuilder output = new StringBuilder();
    output.append(String.format("%s%s [%s]\n", prefix, getDisplayLabel(),
        getDisplayLabelDetail()));

    if (detailLevel.ordinal() > TExplainLevel.MINIMAL.ordinal()) {
      output.append(detailPrefix + "overlap predicate: ");
      
      output.append(overlapPredicate_.toSql());
    }
    output.append("\n");
    if (!otherJoinConjuncts_.isEmpty()) {
      output.append(detailPrefix + "other join predicates: ")
      .append(getExplainString(otherJoinConjuncts_) + "\n");
    }
    if (!conjuncts_.isEmpty()) {
      output.append(detailPrefix + "other predicates: ")
      .append(getExplainString(conjuncts_) + "\n");
    }
    return output.toString();
  }

  @Override
  public void computeCosts(TQueryOptions queryOptions) {
    if (getChild(1).getCardinality() == -1 || getChild(1).getAvgRowSize() == -1
        || numNodes_ == 0) {
      perHostMemCost_ = DEFAULT_PER_HOST_MEM;
      return;
    }
    perHostMemCost_ =
        (long) Math.ceil(getChild(1).getCardinality() * getChild(1).getAvgRowSize()
          * Planner.HASH_TBL_SPACE_OVERHEAD);
    if (distrMode_ == DistributionMode.PARTITIONED) perHostMemCost_ /= numNodes_;
  }
}
