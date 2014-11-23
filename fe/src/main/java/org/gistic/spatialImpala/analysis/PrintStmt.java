// Copyright 2014 GISTIC.

package org.gistic.spatialImpala.analysis;

import com.cloudera.impala.analysis.StatementBase;
import com.cloudera.impala.analysis.Analyzer;
import com.cloudera.impala.common.AnalysisException;

/**
 * Representation of a Print statement.
 */
public class PrintStmt extends StatementBase {
	private final String toPrint;
	
	public PrintStmt(String data) {
		this.toPrint = data;
		isExplain_ = true;
	}
	
	@Override
	public void analyze(Analyzer analyzer) throws AnalysisException {
		if (isExplain_)
			analyzer.setIsExplain();
	}
	
	@Override
	public String toSql() {
		return "print " + "\'" + toPrint + "\'";
	}
	
	public String getDataToPrint() {
		return toPrint;
	}
}