// Copyright 2015 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.cloudera.impala.analysis;

import java.util.ArrayList;
import java.util.List;

import com.cloudera.impala.authorization.Privilege;
import com.cloudera.impala.catalog.HdfsTable;
import com.cloudera.impala.catalog.Table;
import com.cloudera.impala.catalog.Type;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.thrift.TTableName;
import com.cloudera.impala.thrift.TTruncateParams;
import com.google.common.base.Preconditions;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

/**
 * Representation of a TRUNCATE statement.
 * Acceptable syntax:
 *
 * TRUNCATE [TABLE] [database.]table
 *
 */
public class TruncateStmt extends StatementBase {
  private TableName tableName_;

  // Set in analyze().
  private Table table_;

  public TruncateStmt(TableName tableName) {
    Preconditions.checkNotNull(tableName);
    tableName_ = tableName;
    table_ = null;
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    tableName_ = analyzer.getFqTableName(tableName_);
    table_ = analyzer.getTable(tableName_, Privilege.INSERT);
    // We only support truncating hdfs tables now.
    if (!(table_ instanceof HdfsTable)) {
      throw new AnalysisException(String.format(
          "TRUNCATE TABLE not supported on non-HDFS table: %s", table_.getFullName()));
    }
  }

  @Override
  public String toSql() { return "TRUNCATE TABLE " + tableName_; }

  public TTruncateParams toThrift() {
    TTruncateParams params = new TTruncateParams();
    params.setTable_name(tableName_.toThrift());
    return params;
  }
}
