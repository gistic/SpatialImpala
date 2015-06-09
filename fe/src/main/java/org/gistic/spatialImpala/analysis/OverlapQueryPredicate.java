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


public class OverlapQueryPredicate extends Predicate {
  private final static Logger LOG = LoggerFactory.getLogger(RangeQueryPredicate.class);
  private SlotRef col1;
  private SlotRef col2;

  public OverlapQueryPredicate(SlotRef col1, SlotRef col2) {
    this.col1 = col1;
    this.col2 = col2;
  }
    
  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
	
	Collection<TupleDescriptor> tupleDescs = analyzer.getTubleDescriptors();
	Iterator itr = tupleDescs.iterator();
	
	children_.add(col1);
	children_.add(col2);
	
	
    super.analyze(analyzer);
    
  //Make sure that the columns type is shape
    if (!((ScalarType)children_.get(0).getType()).isShapeType() || !((ScalarType)children_.get(1).getType()).isShapeType())
    {
	   	throw new AnalysisException("Error: Overlaps predicate shoud take 2 columns from shapes data type ");
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
  public Expr clone() { return new OverlapQueryPredicate(this.col1, this.col2); }
}