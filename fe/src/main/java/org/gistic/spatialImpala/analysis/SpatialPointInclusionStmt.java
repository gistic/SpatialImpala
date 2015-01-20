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

/**
 * Represents a Spatial Point Inclusion statement
 */
public class SpatialPointInclusionStmt extends StatementBase {
	private static final String TABLE_NOT_SPATIAL_ERROR_MSG = "Table is not a spatial table.";
	private static final String TAG = "tag";
	private static final String X = "x";
	private static final String Y = "y";
	private TableName tableName_;
	private final Rectangle rect_;

	// Initialized during analysis.
	private SelectStmt selectStmt_;

	public SpatialPointInclusionStmt(TableName tblName, Rectangle rect) {
		this.tableName_ = tblName;
		this.rect_ = rect;
		this.selectStmt_ = null;
	}

	@Override
	public void analyze(Analyzer analyzer) throws AnalysisException {
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

		List<GlobalIndexRecord> globalIndexes = globalIndex
				.getGIsforRectangle(rect_);

		// Preparing data for SelectStmt.
		List<TableRef> tableRefs = new ArrayList<TableRef>();
		tableRefs.add(new TableRef(tableName_, null));

		List<SelectListItem> items = new ArrayList<SelectListItem>();
		items.add(new SelectListItem(new SlotRef(tableName_, X), null));
		items.add(new SelectListItem(new SlotRef(tableName_, Y), null));

		selectStmt_ = new SelectStmt(new SelectList(items), tableRefs,
				createWherePredicate(globalIndexes), null, null, null, null);

		selectStmt_.analyze(analyzer);
	}

	@Override
	public String toSql() {
		return "load points from table " + tableName_.getTbl()
				+ " overlaps rectangle" + rect_.toString() + ";";
	}

	public SelectStmt getSelectStmtIfAny() {
		return selectStmt_;
	}

	private Expr createWherePredicate(List<GlobalIndexRecord> globalIndexes) {
		SlotRef globalIndexSlotRef = new SlotRef(tableName_, TAG);
		if (globalIndexes == null || globalIndexes.size() == 0) {
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
	}
}
