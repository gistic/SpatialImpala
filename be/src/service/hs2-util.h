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

#include "gen-cpp/ImpalaHiveServer2Service.h"
#include "gen-cpp/Frontend_types.h"

namespace impala {

/// Utility methods for converting from Impala (either an Expr result or a TColumnValue) to
/// Hive types (either a thrift::TColumnValue (V1->V5) or a TColumn (V6->).

/// For V6->
void TColumnValueToHS2TColumn(const TColumnValue& col_val, const TColumnType& type,
    uint32_t row_idx, apache::hive::service::cli::thrift::TColumn* column);

/// For V6->
void ExprValueToHS2TColumn(const void* value, const TColumnType& type,
    uint32_t row_idx, apache::hive::service::cli::thrift::TColumn* column);

/// For V1->V5
void TColumnValueToHS2TColumnValue(const TColumnValue& col_val, const TColumnType& type,
    apache::hive::service::cli::thrift::TColumnValue* hs2_col_val);

/// For V1->V5
void ExprValueToHS2TColumnValue(const void* value, const TColumnType& type,
    apache::hive::service::cli::thrift::TColumnValue* hs2_col_val);

/// Combine two null columns by appending 'from' to 'to', starting at 'num_rows_before' in
/// 'from', 'start_idx' in 'to', and proceeding for 'num_rows_added' rows.
void StitchNulls(uint32_t num_rows_before, uint32_t num_rows_added, uint32_t start_idx,
    const std::string& from, std::string* to);

void PrintTColumnValue(const apache::hive::service::cli::thrift::TColumnValue& colval,
    std::stringstream* out);

}
