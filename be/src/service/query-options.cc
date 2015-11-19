// Copyright 2014 Cloudera Inc.
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

#include "service/query-options.h"

#include "util/debug-util.h"
#include "util/mem-info.h"
#include "util/parse-util.h"
#include "gen-cpp/ImpalaInternalService_types.h"

#include <sstream>
#include <boost/foreach.hpp>
#include <boost/algorithm/string.hpp>
#include <gutil/strings/substitute.h>

#include "common/names.h"

using boost::algorithm::iequals;
using boost::algorithm::is_any_of;
using boost::algorithm::token_compress_on;
using boost::algorithm::split;
using boost::algorithm::trim;
using namespace impala;
using namespace strings;

// Utility method to wrap ParseUtil::ParseMemSpec() by returning a Status instead of an
// int.
static Status ParseMemValue(const string& value, const string& key, int64_t* result) {
  bool is_percent;
  *result = ParseUtil::ParseMemSpec(value, &is_percent, MemInfo::physical_mem());
  if (*result < 0) {
    return Status("Failed to parse " + key + " from '" + value + "'.");
  }
  if (is_percent) {
    return Status("Invalid " + key + " with percent '" + value + "'.");
  }
  return Status::OK();
}

// Returns the TImpalaQueryOptions enum for the given "key". Input is case insensitive.
// Return -1 if the input is an invalid option.
int GetQueryOptionForKey(const string& key) {
  map<int, const char*>::const_iterator itr =
      _TImpalaQueryOptions_VALUES_TO_NAMES.begin();
  for (; itr != _TImpalaQueryOptions_VALUES_TO_NAMES.end(); ++itr) {
    if (iequals(key, (*itr).second)) {
      return itr->first;
    }
  }
  return -1;
}

void impala::TQueryOptionsToMap(const TQueryOptions& query_options,
    map<string, string>* configuration) {
  map<int, const char*>::const_iterator itr =
      _TImpalaQueryOptions_VALUES_TO_NAMES.begin();
  for (; itr != _TImpalaQueryOptions_VALUES_TO_NAMES.end(); ++itr) {
    stringstream val;
    switch (itr->first) {
      case TImpalaQueryOptions::ABORT_ON_ERROR:
        val << query_options.abort_on_error;
        break;
      case TImpalaQueryOptions::MAX_ERRORS:
        val << query_options.max_errors;
        break;
      case TImpalaQueryOptions::DISABLE_CODEGEN:
        val << query_options.disable_codegen;
        break;
      case TImpalaQueryOptions::BATCH_SIZE:
        val << query_options.batch_size;
        break;
      case TImpalaQueryOptions::MEM_LIMIT:
        val << query_options.mem_limit;
        break;
      case TImpalaQueryOptions::NUM_NODES:
        val << query_options.num_nodes;
        break;
      case TImpalaQueryOptions::MAX_SCAN_RANGE_LENGTH:
        val << query_options.max_scan_range_length;
        break;
      case TImpalaQueryOptions::MAX_IO_BUFFERS:
        val << query_options.max_io_buffers;
        break;
      case TImpalaQueryOptions::NUM_SCANNER_THREADS:
        val << query_options.num_scanner_threads;
        break;
      case TImpalaQueryOptions::ALLOW_UNSUPPORTED_FORMATS:
        val << query_options.allow_unsupported_formats;
        break;
      case TImpalaQueryOptions::DEFAULT_ORDER_BY_LIMIT:
        val << query_options.default_order_by_limit;
        break;
      case TImpalaQueryOptions::DEBUG_ACTION:
        val << query_options.debug_action;
        break;
      case TImpalaQueryOptions::ABORT_ON_DEFAULT_LIMIT_EXCEEDED:
        val << query_options.abort_on_default_limit_exceeded;
        break;
      case TImpalaQueryOptions::COMPRESSION_CODEC:
        val << query_options.compression_codec;
        break;
      case TImpalaQueryOptions::SEQ_COMPRESSION_MODE:
        val << query_options.seq_compression_mode;
        break;
      case TImpalaQueryOptions::HBASE_CACHING:
        val << query_options.hbase_caching;
        break;
      case TImpalaQueryOptions::HBASE_CACHE_BLOCKS:
        val << query_options.hbase_cache_blocks;
        break;
      case TImpalaQueryOptions::PARQUET_FILE_SIZE:
        val << query_options.parquet_file_size;
        break;
      case TImpalaQueryOptions::EXPLAIN_LEVEL:
        val << query_options.explain_level;
        break;
      case TImpalaQueryOptions::SYNC_DDL:
        val << query_options.sync_ddl;
        break;
      case TImpalaQueryOptions::REQUEST_POOL:
        val << query_options.request_pool;
        break;
      case TImpalaQueryOptions::V_CPU_CORES:
        val << query_options.v_cpu_cores;
        break;
      case TImpalaQueryOptions::RESERVATION_REQUEST_TIMEOUT:
        val << query_options.reservation_request_timeout;
        break;
      case TImpalaQueryOptions::DISABLE_CACHED_READS:
        val << query_options.disable_cached_reads;
        break;
      case TImpalaQueryOptions::DISABLE_OUTERMOST_TOPN:
        val << query_options.disable_outermost_topn;
        break;
      case TImpalaQueryOptions::RM_INITIAL_MEM:
        val << query_options.rm_initial_mem;
        break;
      case TImpalaQueryOptions::QUERY_TIMEOUT_S:
        val << query_options.query_timeout_s;
        break;
      case TImpalaQueryOptions::MAX_BLOCK_MGR_MEMORY:
        val << query_options.max_block_mgr_memory;
        break;
      case TImpalaQueryOptions::APPX_COUNT_DISTINCT:
        val << query_options.appx_count_distinct;
        break;
      case TImpalaQueryOptions::DISABLE_UNSAFE_SPILLS:
        val << query_options.disable_unsafe_spills;
        break;
      case TImpalaQueryOptions::EXEC_SINGLE_NODE_ROWS_THRESHOLD:
        val << query_options.exec_single_node_rows_threshold;
        break;
      default:
        // We hit this DCHECK(false) if we forgot to add the corresponding entry here
        // when we add a new query option.
        LOG(ERROR) << "Missing exec option implementation: " << itr->second;
        DCHECK(false);
    }
    (*configuration)[itr->second] = val.str();
  }
}

Status impala::SetQueryOption(const string& key, const string& value,
    TQueryOptions* query_options) {
  int option = GetQueryOptionForKey(key);
  if (option < 0) {
    return Status(Substitute("Ignoring invalid configuration option: $0", key));
  } else {
    switch (option) {
      case TImpalaQueryOptions::ABORT_ON_ERROR:
        query_options->__set_abort_on_error(
            iequals(value, "true") || iequals(value, "1"));
        break;
      case TImpalaQueryOptions::MAX_ERRORS:
        query_options->__set_max_errors(atoi(value.c_str()));
        break;
      case TImpalaQueryOptions::DISABLE_CODEGEN:
        query_options->__set_disable_codegen(
            iequals(value, "true") || iequals(value, "1"));
        break;
      case TImpalaQueryOptions::BATCH_SIZE:
        query_options->__set_batch_size(atoi(value.c_str()));
        break;
      case TImpalaQueryOptions::MEM_LIMIT: {
        // Parse the mem limit spec and validate it.
        int64_t bytes_limit;
        RETURN_IF_ERROR(ParseMemValue(value, "query memory limit", &bytes_limit));
        query_options->__set_mem_limit(bytes_limit);
        break;
      }
      case TImpalaQueryOptions::NUM_NODES:
        query_options->__set_num_nodes(atoi(value.c_str()));
        break;
      case TImpalaQueryOptions::MAX_SCAN_RANGE_LENGTH:
        query_options->__set_max_scan_range_length(atol(value.c_str()));
        break;
      case TImpalaQueryOptions::MAX_IO_BUFFERS:
        query_options->__set_max_io_buffers(atoi(value.c_str()));
        break;
      case TImpalaQueryOptions::NUM_SCANNER_THREADS:
        query_options->__set_num_scanner_threads(atoi(value.c_str()));
        break;
      case TImpalaQueryOptions::ALLOW_UNSUPPORTED_FORMATS:
        query_options->__set_allow_unsupported_formats(
            iequals(value, "true") || iequals(value, "1"));
        break;
      case TImpalaQueryOptions::DEFAULT_ORDER_BY_LIMIT:
        query_options->__set_default_order_by_limit(atoi(value.c_str()));
        break;
      case TImpalaQueryOptions::DEBUG_ACTION:
        query_options->__set_debug_action(value.c_str());
        break;
      case TImpalaQueryOptions::SEQ_COMPRESSION_MODE: {
        if (iequals(value, "block")) {
          query_options->__set_seq_compression_mode(THdfsSeqCompressionMode::BLOCK);
        } else if (iequals(value, "record")) {
          query_options->__set_seq_compression_mode(THdfsSeqCompressionMode::RECORD);
        } else {
          stringstream ss;
          ss << "Invalid sequence file compression mode: " << value;
          return Status(ss.str());
        }
        break;
      }
      case TImpalaQueryOptions::COMPRESSION_CODEC: {
        if (value.empty()) break;
        if (iequals(value, "none")) {
          query_options->__set_compression_codec(THdfsCompression::NONE);
        } else if (iequals(value, "gzip")) {
          query_options->__set_compression_codec(THdfsCompression::GZIP);
        } else if (iequals(value, "bzip2")) {
          query_options->__set_compression_codec(THdfsCompression::BZIP2);
        } else if (iequals(value, "default")) {
          query_options->__set_compression_codec(THdfsCompression::DEFAULT);
        } else if (iequals(value, "snappy")) {
          query_options->__set_compression_codec(THdfsCompression::SNAPPY);
        } else if (iequals(value, "snappy_blocked")) {
          query_options->__set_compression_codec(THdfsCompression::SNAPPY_BLOCKED);
        } else {
          stringstream ss;
          ss << "Invalid compression codec: " << value;
          return Status(ss.str());
        }
        break;
      }
      case TImpalaQueryOptions::ABORT_ON_DEFAULT_LIMIT_EXCEEDED:
        query_options->__set_abort_on_default_limit_exceeded(
            iequals(value, "true") || iequals(value, "1"));
        break;
      case TImpalaQueryOptions::HBASE_CACHING:
        query_options->__set_hbase_caching(atoi(value.c_str()));
        break;
      case TImpalaQueryOptions::HBASE_CACHE_BLOCKS:
        query_options->__set_hbase_cache_blocks(
            iequals(value, "true") || iequals(value, "1"));
        break;
      case TImpalaQueryOptions::PARQUET_FILE_SIZE: {
        int64_t file_size;
        RETURN_IF_ERROR(ParseMemValue(value, "parquet file size", &file_size));
        if (file_size > numeric_limits<int32_t>::max()) {
          // Do not allow values greater than or equal to 2GB since hdfsOpenFile() from
          // the HDFS API gets an int32 blocksize parameter (see HDFS-8949).
          stringstream ss;
          ss << "The PARQUET_FILE_SIZE query option must be less than 2GB.";
          return Status(ss.str());
        } else {
          query_options->__set_parquet_file_size(file_size);
        }
        break;
      }
      case TImpalaQueryOptions::EXPLAIN_LEVEL:
        if (iequals(value, "minimal") || iequals(value, "0")) {
          query_options->__set_explain_level(TExplainLevel::MINIMAL);
        } else if (iequals(value, "standard") || iequals(value, "1")) {
          query_options->__set_explain_level(TExplainLevel::STANDARD);
        } else if (iequals(value, "extended") || iequals(value, "2")) {
          query_options->__set_explain_level(TExplainLevel::EXTENDED);
        } else if (iequals(value, "verbose") || iequals(value, "3")) {
          query_options->__set_explain_level(TExplainLevel::VERBOSE);
        } else {
          return Status(Substitute("Invalid explain level '$0'. Valid levels are"
              " MINIMAL(0), STANDARD(1), EXTENDED(2) and VERBOSE(3).", value));
        }
        break;
      case TImpalaQueryOptions::SYNC_DDL:
        query_options->__set_sync_ddl(iequals(value, "true") || iequals(value, "1"));
        break;
      case TImpalaQueryOptions::REQUEST_POOL:
        query_options->__set_request_pool(value);
        break;
      case TImpalaQueryOptions::V_CPU_CORES:
        query_options->__set_v_cpu_cores(atoi(value.c_str()));
        break;
      case TImpalaQueryOptions::RESERVATION_REQUEST_TIMEOUT:
        query_options->__set_reservation_request_timeout(atoi(value.c_str()));
        break;
      case TImpalaQueryOptions::DISABLE_CACHED_READS:
        query_options->__set_disable_cached_reads(
            iequals(value, "true") || iequals(value, "1"));
        break;
      case TImpalaQueryOptions::DISABLE_OUTERMOST_TOPN:
        query_options->__set_disable_outermost_topn(
            iequals(value, "true") || iequals(value, "1"));
        break;
      case TImpalaQueryOptions::RM_INITIAL_MEM: {
        int64_t reservation_size;
        RETURN_IF_ERROR(ParseMemValue(value, "RM memory limit", &reservation_size));
        query_options->__set_rm_initial_mem(reservation_size);
        break;
      }
      case TImpalaQueryOptions::QUERY_TIMEOUT_S:
        query_options->__set_query_timeout_s(atoi(value.c_str()));
        break;
      case TImpalaQueryOptions::MAX_BLOCK_MGR_MEMORY: {
        int64_t mem;
        RETURN_IF_ERROR(ParseMemValue(value, "block mgr memory limit", &mem));
        query_options->__set_max_block_mgr_memory(mem);
        break;
      }
      case TImpalaQueryOptions::APPX_COUNT_DISTINCT: {
        query_options->__set_appx_count_distinct(
            iequals(value, "true") || iequals(value, "1"));
        break;
      }
      case TImpalaQueryOptions::DISABLE_UNSAFE_SPILLS: {
        query_options->__set_disable_unsafe_spills(
            iequals(value, "true") || iequals(value, "1"));
        break;
      }
      case TImpalaQueryOptions::EXEC_SINGLE_NODE_ROWS_THRESHOLD:
        query_options->__set_exec_single_node_rows_threshold(atoi(value.c_str()));
        break;
      default:
        // We hit this DCHECK(false) if we forgot to add the corresponding entry here
        // when we add a new query option.
        LOG(ERROR) << "Missing exec option implementation: " << key;
        DCHECK(false);
        break;
    }
  }
  return Status::OK();
}

Status impala::ParseQueryOptions(const string& options, TQueryOptions* query_options) {
  if (options.length() == 0) return Status::OK();
  vector<string> kv_pairs;
  split(kv_pairs, options, is_any_of(","), token_compress_on);
  BOOST_FOREACH(string& kv_string, kv_pairs) {
    trim(kv_string);
    if (kv_string.length() == 0) continue;
    vector<string> key_value;
    split(key_value, kv_string, is_any_of("="), token_compress_on);
    if (key_value.size() != 2) {
      return Status(Substitute("Ignoring invalid configuration option $0: bad format "
          "(expected 'key=value')", kv_string));
    }
    RETURN_IF_ERROR(SetQueryOption(key_value[0], key_value[1], query_options));
  }
  return Status::OK();
}
