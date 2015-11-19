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


#ifndef IMPALA_EXEC_HDFS_TABLE_WRITER_H
#define IMPALA_EXEC_HDFS_TABLE_WRITER_H

#include <hdfs.h>
#include <boost/scoped_ptr.hpp>
#include <boost/unordered_map.hpp>

#include "runtime/descriptors.h"
#include "exec/hdfs-table-sink.h"
#include "util/hdfs-util.h"

namespace impala {

/// Pure virtual class for writing to hdfs table partition files.
/// Subclasses implement the code needed to write to a specific file type.
/// A subclass needs to implement functions to format and add rows to the file
/// and to do whatever processing is needed prior to closing the file.
class HdfsTableWriter {
 public:
  /// The implementation of a writer may reference the parameters to the constructor
  /// during the lifetime of the object.
  /// output_partition -- Information on the output partition file.
  /// partition -- the descriptor for the partition being written
  /// table_desc -- the descriptor for the table being written.
  /// output_exprs -- expressions which generate the output values.
  HdfsTableWriter(HdfsTableSink* parent,
                  RuntimeState* state, OutputPartition* output_partition,
                  const HdfsPartitionDescriptor* partition_desc,
                  const HdfsTableDescriptor* table_desc,
                  const std::vector<ExprContext*>& output_expr_ctxs);

  virtual ~HdfsTableWriter() { }

  /// The sequence of calls to this object are:
  /// 1. Init()
  /// 2. InitNewFile()
  /// 3. AppendRowBatch() - called repeatedly
  /// 4. Finalize()
  /// For files formats that are splittable (and therefore can be written to an
  /// arbitrarily large file), 1-4 is called once.
  /// For files formats that are not splittable (i.e. columnar formats, compressed
  /// text), 1) is called once and 2-4) is called repeatedly for each file.

  /// Do initialization of writer.
  virtual Status Init() = 0;

  /// Called when a new file is started.
  virtual Status InitNewFile() = 0;

  /// Appends the current batch of rows to the partition.  If there are multiple
  /// partitions then row_group_indices will contain the rows that are for this
  /// partition, otherwise all rows in the batch are appended.
  /// If the current file is full, the writer stops appending and
  /// returns with *new_file == true.  A new file will be opened and
  /// the same row batch will be passed again.  The writer must track how
  /// much of the batch it had already processed asking for a new file.
  /// Otherwise the writer will return with *newfile == false.
  virtual Status AppendRowBatch(RowBatch* batch,
                                const std::vector<int32_t>& row_group_indices,
                                bool* new_file) = 0;

  /// Finalize this partition. The writer needs to finish processing
  /// all data have written out after the return from this call.
  /// This is called once for each call to InitNewFile()
  virtual Status Finalize() = 0;

  /// Called once when this writer should cleanup any resources.
  virtual void Close() = 0;

  /// Returns the stats for this writer.
  TInsertStats& stats() { return stats_; };

  /// Default block size to use for this file format.  If the file format doesn't
  /// care, it should return 0 and the hdfs config default will be used.
  virtual uint64_t default_block_size() const = 0;

  /// Returns the file extension for this writer.
  virtual std::string file_extension() const = 0;

 protected:
  /// Size to buffer output before calling Write() (which calls hdfsWrite), in bytes
  /// to minimize the overhead of Write()
  static const int HDFS_FLUSH_WRITE_SIZE = 50 * 1024;

  /// Write to the current hdfs file.
  Status Write(const char* data, int32_t len) {
    return Write(reinterpret_cast<const uint8_t*>(data), len);
  }
  Status Write(const uint8_t* data, int32_t len);

  template<typename T>
  Status Write(T v) {
    return Write(reinterpret_cast<uint8_t*>(&v), sizeof(T));
  }

  /// Parent table sink object
  HdfsTableSink* parent_;

  /// Runtime state.
  RuntimeState* state_;

  /// Structure describing partition written to by this writer.
  OutputPartition* output_;

  /// Table descriptor of table to be written.
  const HdfsTableDescriptor* table_desc_;

  /// Expressions that materialize output values.
  std::vector<ExprContext*> output_expr_ctxs_;

  /// Subclass should populate any file format specific stats.
  TInsertStats stats_;

};
}
#endif
