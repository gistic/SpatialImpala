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
import com.cloudera.impala.analysis.Path;

import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;
import java.math.BigDecimal;

/**
 * Represents a Spatial Point Inclusion statement
 */
public class SpatialJoinStmt extends StatementBase {
	private static final String TABLE_NOT_SPATIAL_ERROR_MSG = "Table is not a spatial table.";
	private static final String TAG = "tag";
	private static final String X1 = "x1";
	private static final String Y1 = "y1";
  private static final String X2 = "x2";
  private static final String Y2 = "y2";
	private TableName tableName1_;
	private TableName tableName2_;

	// Initialized during analysis.
	private SelectStmt selectStmt_;

	public SpatialJoinStmt(TableName tblName1, TableName tblName2) {
		this.tableName1_ = tblName1;
		this.tableName2_ = tblName2;
		this.selectStmt_ = null;
	}

	@Override
	public void analyze(Analyzer analyzer) throws AnalysisException {
		// Getting table and checking for existence.
		Table table1;
    Table table2;
		if (!tableName1_.isFullyQualified()) {
			tableName1_ = new TableName(analyzer.getDefaultDb(),
					tableName1_.getTbl());
		}
		table1 = analyzer.getTable(tableName1_, Privilege.SELECT);

    if (!tableName2_.isFullyQualified()) {
      tableName2_ = new TableName(analyzer.getDefaultDb(),
          tableName2_.getTbl());
    }
    table2 = analyzer.getTable(tableName2_, Privilege.SELECT);

		// Table should be an instance of a Spatial table.
		if (!(table1 instanceof SpatialHdfsTable))
			throw new AnalysisException(TABLE_NOT_SPATIAL_ERROR_MSG);

		SpatialHdfsTable spatialTable1 = (SpatialHdfsTable) table1;

    if (!(table2 instanceof SpatialHdfsTable))
      throw new AnalysisException(TABLE_NOT_SPATIAL_ERROR_MSG);

    SpatialHdfsTable spatialTable2 = (SpatialHdfsTable) table2;

		// Global index shouldn't be null.
		GlobalIndex globalIndex1 = spatialTable1.getGlobalIndexIfAny();
		if (globalIndex1 == null)
			throw new AnalysisException(TABLE_NOT_SPATIAL_ERROR_MSG
					+ " : Table doesn't have global indexes.");

    GlobalIndex globalIndex2 = spatialTable2.getGlobalIndexIfAny();
    if (globalIndex2 == null)
      throw new AnalysisException(TABLE_NOT_SPATIAL_ERROR_MSG
          + " : Table doesn't have global indexes.");

		List<String> columnNames1 = spatialTable1.getColumnNames();
		if (!(columnNames1.contains(TAG) && columnNames1.contains(X1) && columnNames1
				.contains(Y1) && columnNames1.contains(X2) && columnNames1.contains(Y2)))
			throw new AnalysisException(TABLE_NOT_SPATIAL_ERROR_MSG
					+ " : Table doesn't have the required columns.");

    List<String> columnNames2 = spatialTable2.getColumnNames();
    if (!(columnNames2.contains(TAG) && columnNames2.contains(X1) && columnNames2
        .contains(Y1) && columnNames2.contains(X2) && columnNames2.contains(Y2)))
      throw new AnalysisException(TABLE_NOT_SPATIAL_ERROR_MSG
          + " : Table doesn't have the required columns.");

		/*HashMap<String, List<GlobalIndexRecord>> globalIndexes = globalIndex1
				.getGIsforJoin(globalIndex2);*/

		// Preparing data for SelectStmt.
		List<TableRef> tableRefs = new ArrayList<TableRef>();
		tableRefs.add(new TableRef(tableName1_.toPath(), null));
    tableRefs.add(new TableRef(tableName2_.toPath(), null));

		List<SelectListItem> items = new ArrayList<SelectListItem>();
		items.add(new SelectListItem(new SlotRef(Path.createRawPath(tableName1_.toString(), X1)), null));
		items.add(new SelectListItem(new SlotRef(Path.createRawPath(tableName1_.toString(), Y1)), null));
    items.add(new SelectListItem(new SlotRef(Path.createRawPath(tableName1_.toString(), X2)), null));
    items.add(new SelectListItem(new SlotRef(Path.createRawPath(tableName1_.toString(), Y2)), null));
    items.add(new SelectListItem(new SlotRef(Path.createRawPath(tableName2_.toString(), X1)), null));
    items.add(new SelectListItem(new SlotRef(Path.createRawPath(tableName2_.toString(), Y1)), null));
    items.add(new SelectListItem(new SlotRef(Path.createRawPath(tableName2_.toString(), X2)), null));
    items.add(new SelectListItem(new SlotRef(Path.createRawPath(tableName2_.toString(), Y2)), null));

		selectStmt_ = new SelectStmt(new SelectList(items), tableRefs,
				createWherePredicate(null), null, null, null, null);

		selectStmt_.analyze(analyzer);
	}

	@Override
	public String toSql() {
		return "join " + tableName1_.getTbl() + " with " + tableName2_.getTbl();
	}

	public SelectStmt getSelectStmtIfAny() {
		return selectStmt_;
	}

	private Expr createWherePredicate(List<GlobalIndexRecord> globalIndexes) { 

		// Create Rectangle predicate.
		SlotRef x11 = new SlotRef(Path.createRawPath(tableName1_.toString(), X1));
    SlotRef y11 = new SlotRef(Path.createRawPath(tableName1_.toString(), Y1));
    SlotRef x12 = new SlotRef(Path.createRawPath(tableName1_.toString(), X2));
    SlotRef y12 = new SlotRef(Path.createRawPath(tableName1_.toString(), Y2));
    SlotRef x21 = new SlotRef(Path.createRawPath(tableName2_.toString(), X1));
    SlotRef y21 = new SlotRef(Path.createRawPath(tableName2_.toString(), Y1));
    SlotRef x22 = new SlotRef(Path.createRawPath(tableName2_.toString(), X2));
    SlotRef y22 = new SlotRef(Path.createRawPath(tableName2_.toString(), Y2));

		Expr wherePredicate = new BinaryPredicate(
						BinaryPredicate.Operator.LE, x11,x22);
		
		wherePredicate = new CompoundPredicate(CompoundPredicate.Operator.AND,
				wherePredicate, new BinaryPredicate(
						BinaryPredicate.Operator.LE, x21, x12));
		
		wherePredicate = new CompoundPredicate(CompoundPredicate.Operator.AND,
				wherePredicate, new BinaryPredicate(
						BinaryPredicate.Operator.LE, y11, y22));
		
		wherePredicate = new CompoundPredicate(CompoundPredicate.Operator.AND,
				wherePredicate, new BinaryPredicate(
						BinaryPredicate.Operator.LE, y21, y12));

		return wherePredicate;
	}
}
