// Copyright 2014 GISTIC.

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
import com.cloudera.impala.analysis.Expr;
import com.cloudera.impala.analysis.CompoundPredicate;
import com.cloudera.impala.analysis.BinaryPredicate;
import com.cloudera.impala.analysis.IsNullPredicate;
import com.cloudera.impala.analysis.StringLiteral;
import com.cloudera.impala.analysis.NumericLiteral;

import java.util.List;
import java.util.ArrayList;
import java.math.BigDecimal;
import java.util.ListIterator;

/**
 * Represents a Spatial Point Inclusion statement
 */
public class SpatialPointInclusionStmt extends StatementBase {
	private static final String TABLE_NOT_SPATIAL_ERROR_MSG = "Table is not a spatial table.";
	private static final String TAG = "tag";
	private static final String X = "x";
	private static final String Y = "y";
	
	private TableName tableName_;
	
	//The rectangle provided in the query
	private final Rectangle rect_;
	
	/*
	 * If the partitions of the tables ovelap then we will need to
	 * handle this case by addind DISTINCT to our query statement
	 */
	/*public static enum Qualifier {
	   ALL,
	   DISTINCT
	}*/
	
	/*
	 * Each operand should handle one partition of the table. Each statement
	 * in the operand should be easier select statement whith where predicits
	 * in case if the  partition only intersects with the rectanlge in the original
	 * query or select statement without where predicits in case if the 
	 * patition lies inside the rectangle 
	 * 
	 */
	/*public static class SpatialUnionOperand {
	   private final QueryStmt queryStmt_;
	   // Null for the first operand.
	   private Qualifier qualifier_;

	   // Analyzer used for this operand. Set in analyze().
	   private Analyzer analyzer_;

	   // map from UnionStmts result slots to our resultExprs; useful during plan generation
	   private final ExprSubstitutionMap smap_ = new ExprSubstitutionMap();

	   // set if this operand is guaranteed to return an empty result set;
	   // used in planning when assigning conjuncts
	   private boolean isDropped_ = false;

	   public SpatialUnionOperand(QueryStmt queryStmt, Qualifier qualifier) {
	      this.queryStmt_ = queryStmt;
	      this.qualifier_ = qualifier;
	   }

	   public void analyze(Analyzer parent) throws AnalysisException {
	      analyzer_ = new Analyzer(parent);
	      queryStmt_.analyze(analyzer_);
	   }

	   public QueryStmt getQueryStmt() { return queryStmt_; }
	   public Qualifier getQualifier() { return qualifier_; }
	   // Used for propagating DISTINCT.
	   public void setQualifier(Qualifier qualifier) { this.qualifier_ = qualifier; }
	   public Analyzer getAnalyzer() { return analyzer_; }
	   public ExprSubstitutionMap getSmap() { return smap_; }
	   public void drop() { isDropped_ = true; }
	   public boolean isDropped() { return isDropped_; }

	   @Override
	   public SpatialUnionOperand clone() {
	      return new SpatialUnionOperand(queryStmt_.clone(), qualifier_);
	   }
	}*/

	// before analysis, this contains the list of spatial union operands derived verbatim
	// from the query;
	// after analysis, this contains all of distinctOperands followed by allOperands
	//protected final List<SpatialUnionOperand> operands_;
	
	//Global indexes of partitions which only intersect with the rectangle
	List<GlobalIndexRecord> GIsIntersect;
	
	//Global indexes of partitions which fully contained in the rectangle
	List<GlobalIndexRecord> GIsFullyContained;

	// filled during analyze(); contains all operands that need to go through
	// distinct aggregation
	//protected final List<SpatialUnionOperand> distinctOperands_ = Lists.newArrayList();

	// filled during analyze(); contains all operands that can be aggregated with
	// a simple merge without duplicate elimination (also needs to merge the output
	// of the DISTINCT operands)
	//protected final List<SpatialUnionOperand> allOperands_ = Lists.newArrayList();

	protected AggregateInfo distinctAggInfo_;  // only set if we have DISTINCT ops

	// Single tuple materialized by the spatial union. Set in analyze().
	protected TupleId tupleId_;

	// set prior to unnesting
	protected String toSqlString_ = null;

	/*public SpatialPointInclusionStmt(List<UnionOperand> operands) {
	   this.operands_ = operands;
	}*/

	@Override
	public void analyze(Analyzer analyzer) throws AnalysisException {
		super.analyze(analyzer);
		// Getting table and checking for existence.
		Table table;
		if (!tableName_.isFullyQualified()) {
			tableName_ = new TableName(analyzer.getDefaultDb(),
					tableName_.getTbl());
		}
		table = analyzer.getTable(tableName_, Privilege.SELECT);

		// Table should be an instance of a Spatial table.
		if (!(table instanceof SpatialHdfsTable))
			throw new AnalysisException(TABLE_NOT_SPATIAL_ERROR_MSG);

		SpatialHdfsTable spatialTable = (SpatialHdfsTable) table;

		// Global index shouldn't be null.
		GlobalIndex globalIndex = spatialTable.getGlobalIndexIfAny();
		if (globalIndex == null)
			throw new AnalysisException(TABLE_NOT_SPATIAL_ERROR_MSG
					+ " : Table doesn't have global indexes.");
		
		List<String> columnNames = spatialTable.getColumnNames();
		if (!(columnNames.contains(TAG) && columnNames.contains(X) && columnNames
				.contains(Y)))
			throw new AnalysisException(TABLE_NOT_SPATIAL_ERROR_MSG
					+ " : Table doesn't have the required columns.");

		//Now fill the GIsIntersect and GIsFullyContained vectors 
		HashMap<String, GlobalIndexRecord> globalIndexMap = globalIndex.getGlobalIndexMap();
		
		for (GlobalIndexRecord gIRecord : globalIndexMap.values()) {
			if (rect_.contains(gIRecord.getMBR())) {
				GIsFullyContained.add(gIRecord);
			}
			else if (rect_.getMBR().intersects(gIRecord)) {
				GIsIntersect.add(gIRecord);
			}
		}
	}

	@Override
	public String toSql() {
		return "load points from table " + tableName_.getTbl()
				+ " overlaps rectangle" + rect_.toString() + ";";
	}

	/*public SelectStmt getSelectStmtIfAny() {
		return selectStmt_;
	}*/

	/*private Expr createWherePredicate(List<GlobalIndexRecord> globalIndexes) {
		SlotRef globalIndexSlotRef = new SlotRef(tableName_, TAG);
		if (globalIndexes == null) {
			return new IsNullPredicate(globalIndexSlotRef, false);
		}

		// Creating Where predicate.
		Expr wherePredicate = null;

		// Create Global Index predicate.
		wherePredicate = new BinaryPredicate(BinaryPredicate.Operator.EQ,
				globalIndexSlotRef, new StringLiteral("0"));

		for (int i = 0; i < globalIndexes.size(); i++) {
			Expr globalIndexPredicate = new BinaryPredicate(
					BinaryPredicate.Operator.EQ, globalIndexSlotRef,
					new StringLiteral(globalIndexes.get(i).getTag()));

			wherePredicate = new CompoundPredicate(
					CompoundPredicate.Operator.OR, wherePredicate,
					globalIndexPredicate);
		}

		// Create Rectangle predicate.
		SlotRef xSlotRef = new SlotRef(tableName_, X);
		SlotRef ySlotRef = new SlotRef(tableName_, Y);

		wherePredicate = new CompoundPredicate(CompoundPredicate.Operator.AND,
				wherePredicate, new BinaryPredicate(
						BinaryPredicate.Operator.GE, xSlotRef,
						new NumericLiteral(new BigDecimal(rect_.getX1()))));
		
		wherePredicate = new CompoundPredicate(CompoundPredicate.Operator.AND,
				wherePredicate, new BinaryPredicate(
						BinaryPredicate.Operator.LE, xSlotRef,
						new NumericLiteral(new BigDecimal(rect_.getX2()))));
		
		wherePredicate = new CompoundPredicate(CompoundPredicate.Operator.AND,
				wherePredicate, new BinaryPredicate(
						BinaryPredicate.Operator.GE, ySlotRef,
						new NumericLiteral(new BigDecimal(rect_.getY1()))));
		
		wherePredicate = new CompoundPredicate(CompoundPredicate.Operator.AND,
				wherePredicate, new BinaryPredicate(
						BinaryPredicate.Operator.LE, ySlotRef,
						new NumericLiteral(new BigDecimal(rect_.getY2()))));

		return wherePredicate;
	}*/
}
