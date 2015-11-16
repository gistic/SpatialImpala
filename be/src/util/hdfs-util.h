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


#ifndef IMPALA_UTIL_HDFS_UTIL_H
#define IMPALA_UTIL_HDFS_UTIL_H

#include <string>
#include <hdfs.h>
#include "common/status.h"

namespace impala {

/// Utility function to get error messages from HDFS. This function takes prefix/file and
/// appends errno to it. Note: any stdlib function can reset errno, this should be called
/// immediately following the failed call into libhdfs.
std::string GetHdfsErrorMsg(const std::string& prefix, const std::string& file = "");

/// Return the size, in bytes, of a file from the hdfs connection.
Status GetFileSize(const hdfsFS& connection, const char* filename, int64_t* filesize);

/// Returns the last modification time of 'filename' in seconds.
/// This should not be called in a fast path (e.g., running a UDF).
Status GetLastModificationTime(const hdfsFS& connection, const char* filename,
                               time_t* last_mod_time);

bool IsHiddenFile(const std::string& filename);

/// Copy the file at 'src_path' from 'src_conn' to 'dst_path' in 'dst_conn'.
Status CopyHdfsFile(const hdfsFS& src_conn, const std::string& src_path,
                    const hdfsFS& dst_conn, const std::string& dst_path);

/// Returns true iff the path refers to a location on an HDFS filesystem.
bool IsDfsPath(const char* path);

/// Returns true iff the path refers to a location on an S3A filesystem.
bool IsS3APath(const char* path);
}
#endif // IMPALA_UTIL_HDFS_UTIL_H
