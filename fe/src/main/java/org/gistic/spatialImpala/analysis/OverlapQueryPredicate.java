// Copyright 2015 GISTIC.

package org.gistic.spatialImpala.analysis;

import org.gistic.spatialImpala.catalog.*;

import com.cloudera.impala.catalog.Table;
import com.cloudera.impala.catalog.PrimitiveType;
import com.cloudera.impala.catalog.ScalarType;
import com.cloudera.impala.catalog.Type;
import com.cloudera.impala.catalog.HdfsTable;
import com.cloudera.impala.authorization.Privilege;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.analysis.Analyzer;
import com.cloudera.impala.analysis.StatementBase;
import com.cloudera.impala.analysis.SelectStmt;
import com.cloudera.impala.analysis.SelectListItem;
import com.cloudera.impala.analysis.SelectList;
import com.cloudera.impala.analysis.TableName;
import com.cloudera.impala.analysis.TableRef;
import com.cloudera.impala.analysis.SlotRef;
import com.cloudera.impala.analysis.IsNullPredicate;
import com.cloudera.impala.analysis.TupleDescriptor;
import com.cloudera.impala.analysis.Expr;
import com.cloudera.impala.thrift.TExprNode;
import com.cloudera.impala.thrift.TRangeQuery;
import com.cloudera.impala.thrift.TSlotRef;
import com.cloudera.impala.thrift.TExprNodeType;
import com.cloudera.impala.analysis.Predicate;
import com.cloudera.impala.analysis.CompoundPredicate;
import com.cloudera.impala.analysis.BinaryPredicate;
import com.cloudera.impala.analysis.IsNullPredicate;
import com.cloudera.impala.analysis.StringLiteral;
import com.cloudera.impala.analysis.NumericLiteral;
import com.google.common.base.Preconditions;

import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;
import java.math.BigDecimal;
import java.util.Collection;
import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class OverlapQueryPredicate extends Predicate {
  private final static Logger LOG = LoggerFactory.getLogger(OverlapQueryPredicate.class);
  private SlotRef col1;
  private SlotRef col2;
  private List<Expr> globalIndexSlotRef;

  public OverlapQueryPredicate(SlotRef col1, SlotRef col2) {
    this.col1 = col1;
    this.col2 = col2;
    globalIndexSlotRef = new ArrayList<Expr>();
  }
  private HashMap<String, List<String>> intersectedPartitions_;
  
  
  private OverlapQueryPredicate(OverlapQueryPredicate overlapPreicate, SlotRef col1, SlotRef col2) {
    super(overlapPreicate);
    this.col1 = col1;
    this.col2 = col2;
    globalIndexSlotRef = new ArrayList<Expr>();
  }
  
  public Expr getLeftHandSidePartitionCol() {
	  return globalIndexSlotRef.get(0);
  }
  
  public Expr getRightHandSidePartitionCol() {
	  return globalIndexSlotRef.get(1);
  }
  
  public HashMap<String, List<String>> getIntersectedPartitions() {
	  return intersectedPartitions_;
  }
    
  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
	
	  LOG.info("Analysis of the overlapqurypredicate");
	Collection<TupleDescriptor> tupleDescs = analyzer.getTubleDescriptors();
	Iterator itr = tupleDescs.iterator();
	children_.clear();
	children_.add(col1);
	children_.add(col2);
	
	intersectedPartitions_ = new HashMap<String, List<String>>();
	
	SpatialHdfsTable spatialTable1, spatialTable2;
	
    super.analyze(analyzer);
    
  //Make sure that the columns type is shape
    if (!((ScalarType)children_.get(0).getType()).isShapeType() || !((ScalarType)children_.get(1).getType()).isShapeType())
    {
	   	throw new AnalysisException("Error: Overlaps predicate shoud take 2 columns from shapes data type ");
    }
    spatialTable1 = spatialTable2 = null;
    
    
    if(((SlotRef)children_.get(0)).getDesc().getParent().getTable() instanceof SpatialHdfsTable) {
    	spatialTable1 = (SpatialHdfsTable) ((SlotRef)children_.get(0)).getDesc().getParent().getTable(); 
	}
    if(((SlotRef)children_.get(1)).getDesc().getParent().getTable() instanceof SpatialHdfsTable) {
    	spatialTable2 = (SpatialHdfsTable) ((SlotRef)children_.get(1)).getDesc().getParent().getTable(); 
	}
    
    SlotRef leftSlotRef = new SlotRef(((SlotRef)children_.get(0)).getTableName(), "tag");
    SlotRef rightSlotRef = new SlotRef(((SlotRef)children_.get(1)).getTableName(), "tag");
    
    leftSlotRef.analyze(analyzer);
    rightSlotRef.analyze(analyzer);
    globalIndexSlotRef.add(0, leftSlotRef);
    globalIndexSlotRef.add(1, rightSlotRef);
    analyzer.materializeSlots(globalIndexSlotRef);
    if ((spatialTable1 == null) || (spatialTable2 == null))
    {
	   	throw new AnalysisException("Overlap predicate should only be used with spatial tables");
    }
    
    GlobalIndex globalIndexLeft, globalIndexRight;
    
    LOG.info("Before checking if spatialtabl1 and spatialTable2 are same");
    
    if (spatialTable1 != spatialTable2) {
    	globalIndexLeft = spatialTable1.getGlobalIndexIfAny();
    	globalIndexRight = spatialTable2.getGlobalIndexIfAny();

    	HashMap<String, GlobalIndexRecord> globalIndexMapLeft = globalIndexLeft.getGlobalIndexMap();
    	HashMap<String, GlobalIndexRecord> globalIndexMapRight = globalIndexRight.getGlobalIndexMap();
		
    	int index;
		for (GlobalIndexRecord gIRecordRight : globalIndexMapRight.values()) {
			index = 0;
			List<String> tmpIntersected = new ArrayList<String>();
			for (GlobalIndexRecord gIRecordLeft : globalIndexMapLeft.values()) {
				if (gIRecordRight.getMBR().intersects(gIRecordLeft.getMBR())) {
					tmpIntersected.add(gIRecordLeft.getTag());
				}
				index++;
			}
			if (tmpIntersected.size() > 0) {
				LOG.info("Right hand side: "+gIRecordRight.getTag() + " intesect with the following partitions from left hand side:");
				for (String str : tmpIntersected) {
					LOG.info(str);
				}
				intersectedPartitions_.put(gIRecordRight.getTag(), tmpIntersected);
			}
		}
    }
  }
  
  @Override
  protected void toThrift(TExprNode msg) {
    msg.node_type = TExprNodeType.OVERLAP_QUERY;
  }

  @Override
  public String toSqlImpl() {
    StringBuilder strBuilder = new StringBuilder();
    strBuilder.append("Overlaps");
    strBuilder.append("(");
    strBuilder.append(col1.getColumnName());
    strBuilder.append(",");
    strBuilder.append(col2.getColumnName());
    strBuilder.append(")");
    return strBuilder.toString();
  }
  
  @Override
  public Expr clone() { return new OverlapQueryPredicate(this, this.col1, this.col2); }
}
