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
  private SlotRef col1;
  private SlotRef col2;
  
  //Global indexes of partitions which either full contained or intersect with the rectangle
  private List<GlobalIndexRecord> GIsIntersectAndFully;

  public RangeQueryPredicate(Rectangle rect, SlotRef col1, SlotRef col2) {
    this.rect_ = rect;
    this.col1 = col1;
    this.col2 = col2;
    this.GIsIntersectAndFully = new ArrayList<GlobalIndexRecord>();
  }
  
  public RangeQueryPredicate(Rectangle rect, SlotRef col1) {
    this.rect_ = rect;
    this.col1 = col1;
    this.col2 = null;
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
    
    
    if(hasXandYColumns() && spatialTable != null) {
    	
    	GlobalIndex globalIndex = spatialTable.getGlobalIndexIfAny();
    	    
	    // Global index shouldn't be null.
		if (globalIndex == null)
			throw new AnalysisException(TABLE_NOT_SPATIAL_ERROR_MSG
					+ " : Table doesn't have global indexes.");
		
		List<String> columnNames = spatialTable.getColumnNames();
		if (!(columnNames.contains(TAG)))
			throw new AnalysisException(TABLE_NOT_SPATIAL_ERROR_MSG
					+ " : Table doesn't have the required columns.");
		
	    
		//Now fill the GIsIntersect and GIsFullyContained vectors 
		HashMap<String, GlobalIndexRecord> globalIndexMap = globalIndex.getGlobalIndexMap();
		
		for (GlobalIndexRecord gIRecord : globalIndexMap.values()) {
			if (rect_.intersects(gIRecord.getMBR())) {
				GIsIntersectAndFully.add(gIRecord);
	                       LOG.info("GI is Intersected: " + gIRecord.getTag());
			}
		}
    }
  }

  public boolean hasXandYColumns () {
	  if(col2 != null && col1.getColumnName().toLowerCase().equals("x") && col2.getColumnName().toLowerCase().equals("y")) {
		  return true;
	  }
	  else {
		  return false;
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