// Copyright 2015 GISTIC.

package org.gistic.spatialImpala.analysis;

import org.gistic.spatialImpala.catalog.*;

import com.cloudera.impala.catalog.Table;
import com.cloudera.impala.catalog.PrimitiveType;
import com.cloudera.impala.catalog.ScalarType;
import com.cloudera.impala.catalog.Type;
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


public class RangeQueryPredicate extends Predicate {
  private final static Logger LOG = LoggerFactory.getLogger(RangeQueryPredicate.class);
  private Rectangle rect_;
  private static final String TABLE_NOT_SPATIAL_ERROR_MSG = "Table is not a spatial table.";
  private static final String TAG = "tag";
  private static final double ACCEPTED_DATA_RATIO = 0.06;
  private double prunedDataRatio;
  private SlotRef col1;
  private SlotRef col2;
  private boolean rangeQueryColsAreIndexed;
  
  //Global indexes of partitions which either full contained or intersect with the rectangle
  private List<GlobalIndexRecord> GIsIntersectAndFully;

  public RangeQueryPredicate(Rectangle rect, SlotRef col1, SlotRef col2) {
    this.rect_ = rect;
    this.col1 = col1;
    this.col2 = col2;
    rangeQueryColsAreIndexed = false;
    prunedDataRatio = 1;
    this.GIsIntersectAndFully = new ArrayList<GlobalIndexRecord>();
  }
  
  public RangeQueryPredicate(Rectangle rect, SlotRef col1) {
    this.rect_ = rect;
    this.col1 = col1;
    this.col2 = null;
    rangeQueryColsAreIndexed = false;
    prunedDataRatio = 1;
    this.GIsIntersectAndFully = new ArrayList<GlobalIndexRecord>();
  }
  
  public Rectangle getRectangle() {
	  return rect_;
  }
	
  public List<GlobalIndexRecord> getGIs () {
	  return GIsIntersectAndFully;
  }
  
  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
	
	Collection<TupleDescriptor> tupleDescs = analyzer.getTubleDescriptors();
	Iterator itr = tupleDescs.iterator();
	TableName tableName = null;
	SpatialHdfsTable spatialTable = null;
	GlobalIndex globalIndex = null;
	
	
	children_.add(col1);
	
	if (col2 != null) {
		children_.add(col2);
	}
	
	
    super.analyze(analyzer);
    
  //Make sure that the columns type is double in case of 2 columns and shape in case of 1 column
    if (col2 != null) {
    	if (children_.get(0).getType() != ScalarType.createType(PrimitiveType.DOUBLE) || children_.get(1).getType() != ScalarType.createType(PrimitiveType.DOUBLE))
	    {
	    	throw new AnalysisException("Error: Coulmns should be double data type");
    	}
	}
    else {
    	if (!((ScalarType)children_.get(0).getType()).isShapeType())
    	{
	    	throw new AnalysisException("Error: Coulmn should be from Shapes data type " + children_.get(0).getType().toString()+" found!!");
	    }
    }
    
    
    while (itr.hasNext()) {
		TupleDescriptor desc = (TupleDescriptor) itr.next();
		if (desc.getTable() instanceof SpatialHdfsTable) {
			tableName = desc.getTableName();
			spatialTable = (SpatialHdfsTable) desc.getTable(); 
		}
	}
    
    if (spatialTable != null) {
    	globalIndex = spatialTable.getGlobalIndexIfAny();
    	LOG.info("Table is spatial");
    	// Global index shouldn't be null
    	if (globalIndex == null)
			throw new AnalysisException(TABLE_NOT_SPATIAL_ERROR_MSG
					+ " : Table doesn't have global indexes.");
    	
    	checkIfColumnsAreIndexed(globalIndex);
    }
    
    if(rangeQueryColsAreIndexed && spatialTable != null) {
	    
    	LOG.info("The query is on the indexed columns");
		//Now fill the GIsIntersect and GIsFullyContained vectors 
		HashMap<String, GlobalIndexRecord> globalIndexMap = globalIndex.getGlobalIndexMap();
		
		for (GlobalIndexRecord gIRecord : globalIndexMap.values()) {
			if (rect_.intersects(gIRecord.getMBR())) {
				GIsIntersectAndFully.add(gIRecord);
	                       LOG.info("GI is Intersected: " + gIRecord.getTag());
			}
		}
		prunedDataRatio = GIsIntersectAndFully.size() * 1.0 /  globalIndexMap.size();
		LOG.info("Pruned data ratio is : " + Double.toString(prunedDataRatio));
    }
  }
  
  public boolean getRangeQueryColsAreIndexed() {
	  return rangeQueryColsAreIndexed;
  }
  
  public boolean isPrunedDataRatioIsAccepted () {
	  if (prunedDataRatio > ACCEPTED_DATA_RATIO) {
		  return false;
	  }
	  return true;
  }

  public void checkIfColumnsAreIndexed (GlobalIndex globalIndex) {
	  if(col2 == null) {
		  if (globalIndex.isOneOfIndexedCol(col1))
				  rangeQueryColsAreIndexed = true;
	  }
	  else {
		  if (globalIndex.isOneOfIndexedCol(col1) && globalIndex.isOneOfIndexedCol(col2)) 
				  rangeQueryColsAreIndexed = true;
	  }
  }
  
  @Override
  protected void toThrift(TExprNode msg) {
    msg.range_query = new TRangeQuery(rect_.toThrift());
    msg.node_type = TExprNodeType.RANGE_QUERY;
  }

  @Override
  public String toSqlImpl() {
    StringBuilder strBuilder = new StringBuilder();
    strBuilder.append("Inside");
    strBuilder.append("(");
    strBuilder.append(rect_.toString());
    strBuilder.append(")");
    return strBuilder.toString();
  }
  
  @Override
  public Expr clone() { return new RangeQueryPredicate(this.rect_, this.col1, this.col2); }
}