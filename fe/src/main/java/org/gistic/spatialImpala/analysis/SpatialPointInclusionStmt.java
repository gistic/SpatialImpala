// Copyright 2014 GISTIC.

package org.gistic.spatialImpala.analysis;

import org.gistic.spatialImpala.catalog.*;

import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.analysis.Analyzer;
import com.cloudera.impala.analysis.StatementBase;
import com.cloudera.impala.analysis.TableName;

/**
 * Represents a Spatial Point Inclusion statement
 */
public class SpatialPointInclusionStmt extends StatementBase {
	private final TableName tableName_;
	private final Rectangle rect_;
	private String dbName_;

	public SpatialPointInclusionStmt(TableName tblName, Rectangle rect) {
		this.tableName_ = tblName;
		this.rect_ = rect;
	}

	@Override
	public void analyze(Analyzer analyzer) throws AnalysisException {

	}

	@Override
	public String toSql() {
		return "load points from table " + tableName_.getTbl()
				+ " overlaps rectangle" + rect_.toString() + ";";
	}
}