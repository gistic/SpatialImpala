// Copyright 2015 GISTIC.

package org.gistic.spatialImpala.planner;

import com.cloudera.impala.planner.*;
import org.gistic.spatialImpala.catalog.*;
import org.gistic.spatialImpala.analysis.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.impala.analysis.Predicate;
import com.cloudera.impala.analysis.Analyzer;
import com.cloudera.impala.analysis.BinaryPredicate;
import com.cloudera.impala.analysis.BinaryPredicate.Operator;
import com.cloudera.impala.analysis.CompoundPredicate;
import com.cloudera.impala.analysis.DescriptorTable;
import com.cloudera.impala.analysis.Expr;
import com.cloudera.impala.analysis.InPredicate;
import com.cloudera.impala.analysis.IsNullPredicate;
import com.cloudera.impala.analysis.LiteralExpr;
import com.cloudera.impala.analysis.StringLiteral;
import com.cloudera.impala.analysis.SlotDescriptor;
import com.cloudera.impala.analysis.SlotId;
import com.cloudera.impala.analysis.SlotRef;
import com.cloudera.impala.analysis.TupleDescriptor;
import com.cloudera.impala.catalog.Column;
import com.cloudera.impala.catalog.HdfsFileFormat;
import com.cloudera.impala.catalog.HdfsPartition;
import com.cloudera.impala.catalog.HdfsPartition.FileBlock;
import com.cloudera.impala.catalog.HdfsTable;
import com.cloudera.impala.catalog.PrimitiveType;
import com.cloudera.impala.common.InternalException;
import com.cloudera.impala.common.PrintUtils;
import com.cloudera.impala.common.RuntimeEnv;
import com.cloudera.impala.thrift.TExplainLevel;
import com.cloudera.impala.thrift.THdfsFileBlock;
import com.cloudera.impala.thrift.THdfsFileSplit;
import com.cloudera.impala.thrift.THdfsScanNode;
import com.cloudera.impala.thrift.TNetworkAddress;
import com.cloudera.impala.thrift.TPlanNode;
import com.cloudera.impala.thrift.TPlanNodeType;
import com.cloudera.impala.thrift.TQueryOptions;
import com.cloudera.impala.thrift.TScanRange;
import com.cloudera.impala.thrift.TScanRangeLocation;
import com.cloudera.impala.thrift.TScanRangeLocations;
import com.cloudera.impala.thrift.THdfsScanNode;
import com.cloudera.impala.thrift.TSpatialHdfsScanNode;
import com.google.common.base.Objects;
import com.google.common.base.Objects.ToStringHelper;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * Scan of a single single table. Currently limited to full-table scans.
 * TODO: pass in range restrictions.
 */
public class SpatialHdfsScanNode extends HdfsScanNode {

  private final static Logger LOG = LoggerFactory.getLogger(SpatialHdfsScanNode.class);
  List<GlobalIndexRecord> GIsForPartitions;
  Rectangle rect_;
  /**
   * Constructs node to scan given data files of table 'tbl_'.
   */
  public SpatialHdfsScanNode(PlanNodeId id, TupleDescriptor desc, HdfsTable tbl,
      List<GlobalIndexRecord> GIs) {
    super(id, desc, tbl);
    GIsForPartitions = GIs;
    displayName_ = "Spatial Scan Hdfs";
  }

  public SpatialHdfsScanNode(PlanNodeId id, TupleDescriptor desc, HdfsTable tbl) {
    super(id, desc, tbl);
    displayName_ = "Spatial Scan Hdfs";
  }

  /**
   * Populate conjuncts_, partitions_, and scanRanges_.
   */
  @Override
  public void init(Analyzer analyzer) throws InternalException {
    ArrayList<Expr> bindingPredicates = analyzer.getBoundPredicates(tupleIds_.get(0));
    conjuncts_.addAll(bindingPredicates);

    // also add remaining unassigned conjuncts
    assignConjuncts(analyzer);
    analyzer.createEquivConjuncts(tupleIds_.get(0), conjuncts_);
    Predicate predicate;
    for (int i = 0 ; i < conjuncts_.size(); i++) {
      predicate = (Predicate)conjuncts_.get(i);
      if (predicate instanceof RangeQueryPredicate) {
        GIsForPartitions = ((RangeQueryPredicate) predicate).getGIs();
        if (((RangeQueryPredicate) predicate).isPrunedDataRatioIsAccepted()
            && ((RangeQueryPredicate) predicate).getRangeQueryColsAreIndexed()) {
          rect_ = ((RangeQueryPredicate) predicate).getRectangle();
          LOG.info(
              "Pruning ratio is accepted for spatial query. An r-tree will be constructed");
        }
        break;
      }
    }

    // do partition pruning before deciding which slots to materialize,
    // we might end up removing some predicates
    computeSpatialPartitions(analyzer);

    // mark all slots referenced by the remaining conjuncts as materialized
    markSlotsMaterialized(analyzer, conjuncts_);
    computeMemLayout(analyzer);

    // compute scan range locations
    computeScanRangeLocations(analyzer);

    // do this at the end so it can take all conjuncts into account
    computeStats(analyzer);

    // TODO: do we need this?
    assignedConjuncts_ = analyzer.getAssignedConjuncts();
  }

  /**
   * Populate partitions_ based on the list of the global indexes
   */
  private void computeSpatialPartitions(Analyzer analyzer) throws InternalException {
    DescriptorTable descTbl = analyzer.getDescTbl();	
    // Populate the list of valid, non-empty partitions to process
    List<HdfsPartition> allPartitions = tbl_.getPartitions();
    for (HdfsPartition partition: allPartitions) {
      Preconditions.checkNotNull(partition);
      if (!partition.hasFileDescriptors())
        continue;

      if (GIsForPartitions == null) {
        partitions_.add(partition);
        descTbl.addReferencedPartition(tbl_, partition.getId());
        continue;
      }

      List<LiteralExpr> values = partition.getPartitionValues();
      for (LiteralExpr value : values) {
        if (! (value instanceof StringLiteral))
          continue;

        boolean found = false;
        StringLiteral string_value = (StringLiteral) value;
        for (GlobalIndexRecord record: GIsForPartitions) {
          if (string_value.getValue().equals(record.getTag())) {
            LOG.info("Partition to process::(Name: " + record.getTag() + ")");
            partitions_.add(partition);
            descTbl.addReferencedPartition(tbl_, partition.getId());
            found = true;
            break;
          }
        }

        if (found)
          break;
      }
    }
  }

  @Override
  protected void toThrift(TPlanNode msg) {
    msg.hdfs_scan_node = new THdfsScanNode(desc_.getId().asInt());
    msg.spatial_hdfs_scan_node = new TSpatialHdfsScanNode((rect_ != null));

    if (rect_ != null)
      msg.spatial_hdfs_scan_node.rectangle = rect_.toThrift();

    msg.node_type = TPlanNodeType.SPATIAL_HDFS_SCAN_NODE;
  }
}
