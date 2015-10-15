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

#include "runtime/types.h"
#include <ostream>
#include <sstream>

using namespace std;
using namespace apache::hive::service::cli::thrift;

namespace impala {

PrimitiveType ThriftToType(TPrimitiveType::type ttype) {
  switch (ttype) {
    case TPrimitiveType::INVALID_TYPE: return INVALID_TYPE;
    case TPrimitiveType::NULL_TYPE: return TYPE_NULL;
    case TPrimitiveType::BOOLEAN: return TYPE_BOOLEAN;
    case TPrimitiveType::TINYINT: return TYPE_TINYINT;
    case TPrimitiveType::SMALLINT: return TYPE_SMALLINT;
    case TPrimitiveType::INT: return TYPE_INT;
    case TPrimitiveType::BIGINT: return TYPE_BIGINT;
    case TPrimitiveType::FLOAT: return TYPE_FLOAT;
    case TPrimitiveType::DOUBLE: return TYPE_DOUBLE;
    case TPrimitiveType::DATE: return TYPE_DATE;
    case TPrimitiveType::DATETIME: return TYPE_DATETIME;
    case TPrimitiveType::TIMESTAMP: return TYPE_TIMESTAMP;
    case TPrimitiveType::STRING: return TYPE_STRING;
    case TPrimitiveType::VARCHAR: return TYPE_VARCHAR;
    case TPrimitiveType::BINARY: return TYPE_BINARY;
    case TPrimitiveType::DECIMAL: return TYPE_DECIMAL;
    case TPrimitiveType::CHAR: return TYPE_CHAR;
    case TPrimitiveType::POINT: return TYPE_POINT;
    case TPrimitiveType::LINE: return TYPE_LINE;
    case TPrimitiveType::RECTANGLE: return TYPE_RECTANGLE;
    case TPrimitiveType::POLYGON: return TYPE_POLYGON;
    default: return INVALID_TYPE;
  }
}

TPrimitiveType::type ToThrift(PrimitiveType ptype) {
  switch (ptype) {
    case INVALID_TYPE: return TPrimitiveType::INVALID_TYPE;
    case TYPE_NULL: return TPrimitiveType::NULL_TYPE;
    case TYPE_BOOLEAN: return TPrimitiveType::BOOLEAN;
    case TYPE_TINYINT: return TPrimitiveType::TINYINT;
    case TYPE_SMALLINT: return TPrimitiveType::SMALLINT;
    case TYPE_INT: return TPrimitiveType::INT;
    case TYPE_BIGINT: return TPrimitiveType::BIGINT;
    case TYPE_FLOAT: return TPrimitiveType::FLOAT;
    case TYPE_DOUBLE: return TPrimitiveType::DOUBLE;
    case TYPE_DATE: return TPrimitiveType::DATE;
    case TYPE_DATETIME: return TPrimitiveType::DATETIME;
    case TYPE_TIMESTAMP: return TPrimitiveType::TIMESTAMP;
    case TYPE_STRING: return TPrimitiveType::STRING;
    case TYPE_VARCHAR: return TPrimitiveType::VARCHAR;
    case TYPE_BINARY: return TPrimitiveType::BINARY;
    case TYPE_DECIMAL: return TPrimitiveType::DECIMAL;
    case TYPE_CHAR: return TPrimitiveType::CHAR;
    case TYPE_POINT: return TPrimitiveType::POINT;
    case TYPE_LINE: return TPrimitiveType::LINE;
    case TYPE_RECTANGLE: return TPrimitiveType::RECTANGLE;
    case TYPE_POLYGON: return TPrimitiveType::POLYGON;
    default: return TPrimitiveType::INVALID_TYPE;
  }
}

string TypeToString(PrimitiveType t) {
  switch (t) {
    case INVALID_TYPE: return "INVALID";
    case TYPE_NULL: return "NULL";
    case TYPE_BOOLEAN: return "BOOL";
    case TYPE_TINYINT: return "TINYINT";
    case TYPE_SMALLINT: return "SMALLINT";
    case TYPE_INT: return "INT";
    case TYPE_BIGINT: return "BIGINT";
    case TYPE_FLOAT: return "FLOAT";
    case TYPE_DOUBLE: return "DOUBLE";
    case TYPE_DATE: return "DATE";
    case TYPE_DATETIME: return "DATETIME";
    case TYPE_TIMESTAMP: return "TIMESTAMP";
    case TYPE_STRING: return "STRING";
    case TYPE_VARCHAR: return "VARCHAR";
    case TYPE_BINARY: return "BINARY";
    case TYPE_DECIMAL: return "DECIMAL";
    case TYPE_CHAR: return "CHAR";
    case TYPE_POINT: return "POINT";
    case TYPE_LINE: return "LINE";
    case TYPE_RECTANGLE: return "RECTANGLE";
    case TYPE_POLYGON: return "POLYGON";
  };
  return "";
}

string TypeToOdbcString(PrimitiveType t) {
  // ODBC driver requires types in lower case
  switch (t) {
    case INVALID_TYPE: return "invalid";
    case TYPE_NULL: return "null";
    case TYPE_BOOLEAN: return "boolean";
    case TYPE_TINYINT: return "tinyint";
    case TYPE_SMALLINT: return "smallint";
    case TYPE_INT: return "int";
    case TYPE_BIGINT: return "bigint";
    case TYPE_FLOAT: return "float";
    case TYPE_DOUBLE: return "double";
    case TYPE_DATE: return "date";
    case TYPE_DATETIME: return "datetime";
    case TYPE_TIMESTAMP: return "timestamp";
    case TYPE_STRING: return "string";
    case TYPE_VARCHAR: return "string";
    case TYPE_BINARY: return "binary";
    case TYPE_DECIMAL: return "decimal";
    case TYPE_CHAR: return "char";
    case TYPE_POINT: return "point";
    case TYPE_LINE: return "line";
    case TYPE_RECTANGLE: return "rectangle";
    case TYPE_POLYGON: return "polygon";
  };
  return "unknown";
}

TTypeId::type TypeToHiveServer2Type(PrimitiveType t) {
  switch (t) {
    // Map NULL_TYPE to BOOLEAN, otherwise Hive's JDBC driver won't
    // work for queries like "SELECT NULL" (IMPALA-914).
    case TYPE_NULL: return TTypeId::BOOLEAN_TYPE;
    case TYPE_BOOLEAN: return TTypeId::BOOLEAN_TYPE;
    case TYPE_TINYINT: return TTypeId::TINYINT_TYPE;
    case TYPE_SMALLINT: return TTypeId::SMALLINT_TYPE;
    case TYPE_INT: return TTypeId::INT_TYPE;
    case TYPE_BIGINT: return TTypeId::BIGINT_TYPE;
    case TYPE_FLOAT: return TTypeId::FLOAT_TYPE;
    case TYPE_DOUBLE: return TTypeId::DOUBLE_TYPE;
    case TYPE_TIMESTAMP: return TTypeId::TIMESTAMP_TYPE;
    case TYPE_STRING: return TTypeId::STRING_TYPE;
    case TYPE_VARCHAR: return TTypeId::STRING_TYPE;
    case TYPE_BINARY: return TTypeId::BINARY_TYPE;
    case TYPE_DECIMAL: return TTypeId::DECIMAL_TYPE;
    // TODO: update when hs2 has char(n)
    case TYPE_CHAR: return TTypeId::STRING_TYPE;
    default:
      // HiveServer2 does not have a type for invalid, date and datetime.
      DCHECK(false) << "bad TypeToTValueType() type: " << TypeToString(t);
      return TTypeId::STRING_TYPE;
  };
}

string ColumnType::DebugString() const {
  stringstream ss;
  switch (type) {
    case TYPE_CHAR:
      ss << "CHAR(" << len << ")";
      return ss.str();
    case TYPE_DECIMAL:
      ss << "DECIMAL(" << precision << ", " << scale << ")";
      return ss.str();
    default:
      return TypeToString(type);
  }
}

ostream& operator<<(ostream& os, const ColumnType& type) {
  os << type.DebugString();
  return os;
}

}
