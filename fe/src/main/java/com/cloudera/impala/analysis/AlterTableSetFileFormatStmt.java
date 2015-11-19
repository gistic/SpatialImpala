// Copyright 2012 Cloudera Inc.
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

import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.thrift.TAlterTableParams;
import com.cloudera.impala.thrift.TAlterTableSetFileFormatParams;
import com.cloudera.impala.thrift.TAlterTableType;
import com.cloudera.impala.thrift.THdfsFileFormat;

/**
 * Represents an ALTER TABLE [PARTITION partitionSpec] SET FILEFORMAT statement.
 */
public class AlterTableSetFileFormatStmt extends AlterTableSetStmt {
  private final THdfsFileFormat fileFormat_;

  public AlterTableSetFileFormatStmt(TableName tableName,
      PartitionSpec partitionSpec, THdfsFileFormat fileFormat) {
    super(tableName, partitionSpec);
    this.fileFormat_ = fileFormat;
  }

  public THdfsFileFormat getFileFormat() { return fileFormat_; }

  @Override
  public TAlterTableParams toThrift() {
    TAlterTableParams params = super.toThrift();
    params.setAlter_type(TAlterTableType.SET_FILE_FORMAT);
    TAlterTableSetFileFormatParams fileFormatParams =
        new TAlterTableSetFileFormatParams(fileFormat_);
    if (getPartitionSpec() != null) {
      fileFormatParams.setPartition_spec(getPartitionSpec().toThrift());
    }
    params.setSet_file_format_params(fileFormatParams);
    return params;
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    super.analyze(analyzer);
  }
}