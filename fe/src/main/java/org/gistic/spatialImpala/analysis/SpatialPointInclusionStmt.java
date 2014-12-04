// Copyright 2014 GISTIC.

package org.gistic.spatialImpala.analysis;

import org.gistic.spatialImpala.catalog.*;

import com.cloudera.impala.catalog.Table;
import com.cloudera.impala.authorization.Privilege;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.analysis.Analyzer;
import com.cloudera.impala.analysis.StatementBase;
import com.cloudera.impala.analysis.SelectStmt;
import com.cloudera.impala.analysis.TableName;

import java.util.List;

/**
 * Represents a Spatial Point Inclusion statement
 */
public class SpatialPointInclusionStmt extends StatementBase {
	private static final String TABLE_NOT_SPATIAL_ERROR_MSG
		= "Table is not a spatial table.";
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
		if (! (table instanceof SpatialHdfsTable))
			throw new AnalysisException(TABLE_NOT_SPATIAL_ERROR_MSG);
		
		SpatialHdfsTable spatialTable = (SpatialHdfsTable) table;
		
		// Global index shouldn't be null.
		GlobalIndex globalIndex = spatialTable.getGlobalIndexIfAny();
		if (globalIndex == null)
			throw new AnalysisException(TABLE_NOT_SPATIAL_ERROR_MSG
					+ " : Table doesn't have global indexes.");
		
		List<String> columnNames = spatialTable.getColumnNames();
		if (! (columnNames.contains(TAG) && columnNames.contains(X)
				&& columnNames.contains(Y)))
			throw new AnalysisException(TABLE_NOT_SPATIAL_ERROR_MSG
					+ " : Table doesn't have the required columns.");
		
		List<GlobalIndexRecord> globalIndexes = globalIndex.getGIsforRectangle(rect_);
		if (globalIndexes.size() == 0) {
			// TODO: Create a select stmt to return an empty result.
			return;
		}
		
		// TODO: Create a select stmt containg information about
		// the optimization done for the point inclusion statement.
	}

	@Override
	public String toSql() {
		return "load points from table " + tableName_.getTbl()
				+ " overlaps rectangle" + rect_.toString() + ";";
	}
}