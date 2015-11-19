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

#ifndef IMPALA_SERVICE_SIMPLE_LOGGER_H
#define IMPALA_SERVICE_SIMPLE_LOGGER_H

#include <boost/thread/thread.hpp>
#include <fstream>

#include "common/status.h"

namespace impala {

/// A class that provides basic thread-safe support for logging to a file. Supports
/// creation of the log file and log directories as well as rolling the log file when
/// it has reached a specified number of entries.
class SimpleLogger {
 public:
  SimpleLogger(const std::string& log_dir_, const std::string& log_file_name_prefix_,
      uint64_t max_entries_per_file);

  /// Initializes the logging directory and creates the initial log file. If the log dir
  /// does not already exist, it will be created. This function is not thread safe and
  /// should only be called once.
  Status Init();

  /// Appends the given string to the log file, including a newline. If the log
  /// file already contains the specified entry limit, a new log file will be created.
  /// This function is thread safe and blocks while a Flush() is in progress
  Status AppendEntry(const std::string& entry);

  /// Flushes the log file to disk by closing and re-opening the file. This function is
  /// thread safe and blocks while a WriteEntry() is in progress
  Status Flush();

 private:
  /// Protects log_file_, num_log_file_entries_ and log_file_name_
  boost::mutex log_file_lock_;

  /// Directory to log to
  std::string log_dir_;

  /// Prefix for all log files
  std::string log_file_name_prefix_;

  /// Counts the number of entries written to log_file_; used to decide when to roll the
  /// log file
  uint64_t num_log_file_entries_;

  /// The maximum number of entries for each log file. If this number is reached the log
  /// file will be rolled
  uint64_t max_entries_per_file_;

  /// Log files are written to this stream.
  std::ofstream log_file_;

  /// Current log file name
  std::string log_file_name_;

  /// Generates and sets a new log file name that is based off the log file name prefix
  /// and the current system time. The format will be: PREFIX-<UTC timestamp>
  void GenerateLogFileName();

  /// Flushes the log file to disk (closes and reopens the file). Must be called with the
  /// log_file_lock_ held.
  Status FlushInternal();
};
}
#endif
