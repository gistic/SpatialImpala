// Copyright 2015 GISTIC.

package org.gistic.spatialImpala.analysis;

import org.gistic.spatialImpala.catalog.*;

import com.cloudera.impala.catalog.Table;
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
import com.cloudera.impala.thrift.TExprNodeType;
import com.cloudera.impala.analysis.Predicate;
import com.cloudera.impala.analysis.CompoundPredicate;
import com.cloudera.impala.analysis.BinaryPredicate;
import com.cloudera.impala.analysis.IsNullPredicate;
import com.cloudera.impala.analysis.StringLiteral;
import com.cloudera.impala.analysis.NumericLiteral;

import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;
import java.math.BigDecimal;
import java.util.Collection;
import java.util.Iterator;

public class RangeQueryPredicate extends Predicate {
  private Rectangle rect_;

  public RangeQueryPredicate(Rectangle rect) {
    this.rect_ = rect;
  }


  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
	
	Collection<TupleDescriptor> tupleDescs = analyzer.getTubleDescriptors();
	Iterator itr = tupleDescs.iterator();
	TableName tableName = null;
	
	while (itr.hasNext()) {
		TupleDescriptor desc = (TupleDescriptor) itr.next();
		if (desc.getTable() instanceof SpatialHdfsTable) {
			tableName = desc.getTableName();
		}
	}
	
	if (tableName == null) {
		String errMsg = "No spatial table found";
	    throw new AnalysisException(errMsg);
	}
	
	children_.add(new SlotRef(tableName, "x"));
	children_.add(new SlotRef(tableName, "y"));
    super.analyze(analyzer);
    
  }

  @Override
  protected void toThrift(TExprNode msg) {
    // Can't serialize a predicate with a subquery
  }

  @Override
  public String toSqlImpl() {
    StringBuilder strBuilder = new StringBuilder();
    return strBuilder.toString();
  }
  
  @Override
  public Expr clone() { return new RangeQueryPredicate(this.rect_); }
}