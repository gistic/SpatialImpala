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

import com.cloudera.impala.analysis.Expr;
import com.cloudera.impala.planner.DataPartition;
import com.cloudera.impala.thrift.TPartitionType;
import com.cloudera.impala.thrift.TDataPartition;
/**
 * Scan of a single single table. Currently limited to full-table scans.
 * TODO: pass in range restrictions.
 */
public class SpatialDataPartition extends DataPartition {
	
  public SpatialDataPartition(TPartitionType type, List<Expr> exprs, HashMap<String, List<String>> intersectedPartitions) {
	  super (type, exprs);
	  intersectedPartitions_ = intersectedPartitions;
  }
  
  private HashMap<String, List<String>> intersectedPartitions_;

  @Override
  public TDataPartition toThrift() {
	LOG.debug("Creating spatial TDataPartition");
    TDataPartition result = new TDataPartition(type_, true);
    if (partitionExprs_ != null) {
      result.setPartition_exprs(Expr.treesToThrift(partitionExprs_));
    }
    
    if (intersectedPartitions_ != null) {
    	result.setIntersected_partitions(intersectedPartitions_);
    }
    
    return result;
  }
}
