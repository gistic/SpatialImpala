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


#ifndef IMPALA_EXEC_HDFS_PARQUET_TABLE_WRITER_H
#define IMPALA_EXEC_HDFS_PARQUET_TABLE_WRITER_H

#include "exec/data-sink.h"

#include <hdfs.h>
#include <map>
#include <boost/scoped_ptr.hpp>

#include "util/compress.h"
#include "runtime/descriptors.h"
#include "exec/hdfs-table-writer.h"
#include "exec/parquet-common.h"

namespace impala {

class Expr;
struct OutputPartition;
class RuntimeState;
class ThriftSerializer;
class TupleRow;

/// The writer consumes all rows passed to it and writes the evaluated output_exprs
/// as a parquet file in hdfs.
/// TODO: (parts of the format that are not implemented)
/// - group var encoding
/// - compression
/// - multiple row groups per file
/// TODO: we need a mechanism to pass the equivalent of serde params to this class
/// from the FE.  This includes:
/// - compression & codec
/// - type of encoding to use for each type

class HdfsParquetTableWriter : public HdfsTableWriter {
 public:
  HdfsParquetTableWriter(HdfsTableSink* parent,
                         RuntimeState* state, OutputPartition* output_partition,
                         const HdfsPartitionDescriptor* part_desc,
                         const HdfsTableDescriptor* table_desc,
                         const std::vector<ExprContext*>& output_expr_ctxs);

  ~HdfsParquetTableWriter();

  /// Initialize column information.
  virtual Status Init();

  /// Initializes a new file.  This resets the file metadata object and writes
  /// the file header to the output file.
  virtual Status InitNewFile();

  /// Appends parquet representation of rows in the batch to the current file.
  virtual Status AppendRowBatch(RowBatch* batch,
                                const std::vector<int32_t>& row_group_indices,
                                bool* new_file);

  /// Write out all the data.
  virtual Status Finalize();

  virtual void Close();

  /// Returns the target HDFS block size to use.
  virtual uint64_t default_block_size() const;

  virtual std::string file_extension() const { return "parq"; }

 private:
  /// Default data page size. In bytes.
  static const int DEFAULT_DATA_PAGE_SIZE = 64 * 1024;

  /// Max data page size. In bytes.
  /// TODO: May need to be increased after addressing IMPALA-1619.
  static const int64_t MAX_DATA_PAGE_SIZE = 1024 * 1024 * 1024;

  /// Default hdfs block size. In bytes.
  static const int HDFS_BLOCK_SIZE = 256 * 1024 * 1024;

  /// Align block sizes to this constant. In bytes.
  static const int HDFS_BLOCK_ALIGNMENT = 1024 * 1024;

  /// Default row group size.  In bytes.
  static const int ROW_GROUP_SIZE = HDFS_BLOCK_SIZE;

  /// Minimum file size.  If the configured size is less, fail.
  static const int HDFS_MIN_FILE_SIZE = 8 * 1024 * 1024;

  /// Per-column information state.  This contains some metadata as well as the
  /// data buffers.
  class BaseColumnWriter;
  friend class BaseColumnWriter;

  template<typename T> class ColumnWriter;
  template<typename T> friend class ColumnWriter;
  class BoolColumnWriter;
  friend class BoolColumnWriter;

  /// Minimum allowable block size in bytes. This is a function of the number of columns.
  int64_t MinBlockSize() const;

  /// Fills in the schema portion of the file metadata, converting the schema in
  /// table_desc_ into the format in the file metadata
  Status CreateSchema();

  /// Write the file header information to the output file.
  Status WriteFileHeader();

  /// Write the file metadata and footer.
  Status WriteFileFooter();

  /// Flushes the current row group to file.  This will compute the final
  /// offsets of column chunks, updating the file metadata.
  Status FlushCurrentRowGroup();

  /// Adds a row group to the metadata and updates current_row_group_ to the
  /// new row group.  current_row_group_ will be flushed.
  Status AddRowGroup();

  /// Thrift serializer utility object.  Reusing this object allows for
  /// fewer memory allocations.
  boost::scoped_ptr<ThriftSerializer> thrift_serializer_;

  /// File metdata thrift description.
  parquet::FileMetaData file_metadata_;

  /// The current row group being written to.
  parquet::RowGroup* current_row_group_;

  /// array of pointers to column information.
  std::vector<BaseColumnWriter*> columns_;

  /// Number of rows in current file
  int64_t row_count_;

  /// Current estimate of the total size of the file.  The file size estimate includes
  /// the running size of the (uncompressed) dictionary, the size of all finalized
  /// (compressed) data pages and their page headers.
  /// If this size exceeds file_size_limit_, the current data is written and a new file
  /// is started.
  int64_t file_size_estimate_;

  /// Limit on the total size of the file.
  int64_t file_size_limit_;

  /// The file location in the current output file.  This is the number of bytes
  /// that have been written to the file so far.  The metadata uses file offsets
  /// in a few places.
  int64_t file_pos_;

  /// Memory for column/block buffers that are reused for the duration of the
  /// writer (i.e. reused across files).
  boost::scoped_ptr<MemPool> reusable_col_mem_pool_;

  /// Memory for column/block buffers that is allocated per file.  We need to
  /// reset this pool after flushing a file.
  boost::scoped_ptr<MemPool> per_file_mem_pool_;

  /// Current position in the batch being written.  This must be persistent across
  /// calls since the writer may stop in the middle of a row batch and ask for a new
  /// file.
  int row_idx_;

  /// Staging buffer to use to compress data.  This is used only if compression is
  /// enabled and is reused between all data pages.
  std::vector<uint8_t> compression_staging_buffer_;

  /// For each column, the on disk size written.
  TParquetInsertStats parquet_stats_;
};

}
#endif
