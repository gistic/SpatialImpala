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

#include "exec/hdfs-table-writer.h"

#include "common/names.h"

namespace impala {

HdfsTableWriter::HdfsTableWriter(HdfsTableSink* parent,
                                 RuntimeState* state, OutputPartition* output,
                                 const HdfsPartitionDescriptor* partition_desc,
                                 const HdfsTableDescriptor* table_desc,
                                 const vector<ExprContext*>& output_expr_ctxs)
  : parent_(parent),
    state_(state),
    output_(output),
    table_desc_(table_desc),
    output_expr_ctxs_(output_expr_ctxs) {
  int num_non_partition_cols =
      table_desc_->num_cols() - table_desc_->num_clustering_cols();
  DCHECK_GE(output_expr_ctxs_.size(), num_non_partition_cols) << parent_->DebugString();
}

Status HdfsTableWriter::Write(const uint8_t* data, int32_t len) {
  DCHECK_GE(len, 0);
  int ret = hdfsWrite(output_->hdfs_connection, output_->tmp_hdfs_file, data, len);
  if (ret == -1) {
    string error_msg = GetHdfsErrorMsg("");
    stringstream msg;
    msg << "Failed to write data (length: " << len
        << ") to Hdfs file: " << output_->current_file_name
        << " " << error_msg;
    return Status(msg.str());
  }
  COUNTER_ADD(parent_->bytes_written_counter(), len);
  stats_.bytes_written += len;
  return Status::OK();
}
}
