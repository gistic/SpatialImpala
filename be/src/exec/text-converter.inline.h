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


#ifndef IMPALA_EXEC_TEXT_CONVERTER_INLINE_H
#define IMPALA_EXEC_TEXT_CONVERTER_INLINE_H

#include "text-converter.h"

#include <boost/algorithm/string.hpp>
#include <limits>


#include "runtime/runtime-state.h"
#include "runtime/descriptors.h"
#include "runtime/tuple.h"
#include "util/string-parser.h"
#include "runtime/string-value.h"
#include "runtime/timestamp-value.h"
#include "runtime/mem-pool.h"
#include "runtime/string-value.inline.h"
#include "exprs/string-functions.h"
#include "util/url-coding.h"
#include "exec/point.h"
#include "exec/line.h"
#include "exec/rectangle.h"
#include "exec/polygon.h"

using namespace std;
using namespace spatialimpala;

namespace impala {

// Note: this function has a codegen'd version.  Changing this function requires
// corresponding changes to CodegenWriteSlot.
inline bool TextConverter::WriteSlot(const SlotDescriptor* slot_desc, Tuple* tuple,
    const char* data, int len, bool copy_string, bool need_escape, MemPool* pool) {
  if ((len == 0 && slot_desc->type().type != TYPE_STRING) || data == NULL) {
    tuple->SetNull(slot_desc->null_indicator_offset());
    return true;
  } else if (check_null_ && len == null_col_val_.size() &&
      StringCompare(data, len, null_col_val_.data(), null_col_val_.size(), len) == 0) {
    // We matched the special NULL indicator.
    tuple->SetNull(slot_desc->null_indicator_offset());
    return true;
  }

  StringParser::ParseResult parse_result = StringParser::PARSE_SUCCESS;
  void* slot = tuple->GetSlot(slot_desc->tuple_offset());

  // Parse the raw-text data. Translate the text string to internal format.
  const ColumnType& type = slot_desc->type();
  switch (type.type) {
    case TYPE_STRING:
    case TYPE_VARCHAR:
    case TYPE_CHAR: {
      int buffer_len = len;
      if (type.type == TYPE_VARCHAR || type.type == TYPE_CHAR) buffer_len = type.len;

      bool reuse_data = type.IsVarLen() && !(len != 0 && (copy_string || need_escape));
      if (type.type == TYPE_CHAR) reuse_data &= (buffer_len <= len);

      StringValue str;
      str.len = std::min(buffer_len, len);
      if (reuse_data) {
        str.ptr = const_cast<char*>(data);
      } else {
        str.ptr = type.IsVarLen() ? reinterpret_cast<char*>(pool->Allocate(buffer_len)) :
            reinterpret_cast<char*>(slot);
        if (need_escape) {
          UnescapeString(data, str.ptr, &str.len, buffer_len);
        } else {
          memcpy(str.ptr, data, str.len);
        }
      }

      if (type.type == TYPE_CHAR) {
        StringValue::PadWithSpaces(str.ptr, buffer_len, str.len);
        str.len = type.len;
      }
      // write back to the slot, if !IsVarLen() we already wrote to the slot
      if (type.IsVarLen()) {
        StringValue* str_slot = reinterpret_cast<StringValue*>(slot);
        *str_slot = str;
      }
      break;
    }
    case TYPE_BOOLEAN:
      *reinterpret_cast<bool*>(slot) =
        StringParser::StringToBool(data, len, &parse_result);
      break;
    case TYPE_TINYINT:
      *reinterpret_cast<int8_t*>(slot) =
        StringParser::StringToInt<int8_t>(data, len, &parse_result);
      break;
    case TYPE_SMALLINT:
      *reinterpret_cast<int16_t*>(slot) =
        StringParser::StringToInt<int16_t>(data, len, &parse_result);
      break;
    case TYPE_INT:
      *reinterpret_cast<int32_t*>(slot) =
        StringParser::StringToInt<int32_t>(data, len, &parse_result);
      break;
    case TYPE_BIGINT:
      *reinterpret_cast<int64_t*>(slot) =
        StringParser::StringToInt<int64_t>(data, len, &parse_result);
      break;
    case TYPE_FLOAT:
      *reinterpret_cast<float*>(slot) =
        StringParser::StringToFloat<float>(data, len, &parse_result);
      break;
    case TYPE_DOUBLE:
      *reinterpret_cast<double*>(slot) =
        StringParser::StringToFloat<double>(data, len, &parse_result);
      break;
    case TYPE_POINT: {
      string point_str(data, len);
      boost::algorithm::to_lower(point_str);
      
      std::size_t startingField = point_str.find("point(");
      if (startingField == std::string::npos) {
        VLOG_QUERY << "Error in the well know text format of the point";
        tuple->SetNull(slot_desc->null_indicator_offset());
        return true;
      }
      std::size_t startingData = startingField + 6;
      std::string delimiter = " ";
      std::string Xtoken = point_str.substr(startingData, point_str.find(delimiter, startingData));
      std::string Ytoken = point_str.substr(startingData + Xtoken.size() + 1, point_str.find(")", startingData));
      double x = StringParser::StringToFloat<double>(Xtoken.c_str(), Xtoken.size(), &parse_result);
      double y = StringParser::StringToFloat<double>(Ytoken.c_str(), Ytoken.size(), &parse_result);
      
      Point point_data(x, y);
      Point* point_slot = reinterpret_cast<Point*>(slot);
      *point_slot = point_data;
      break;
    }
    case TYPE_LINE: {
      string line_str(data, len);
      boost::algorithm::to_lower(line_str);
      
      std::size_t startingField = line_str.find("line(");
      if (startingField == std::string::npos) {
        VLOG_QUERY << "Error in the well know text format of the line";
        tuple->SetNull(slot_desc->null_indicator_offset());
        return true;
      }
      std::size_t prefix = startingField + 5;
      std::string delimiter = " ";
      std::string X1token = line_str.substr(prefix, line_str.find(delimiter, prefix));
      prefix += X1token.size() + 1;
      std::string Y1token = line_str.substr(prefix, line_str.find(delimiter, prefix));
      prefix += Y1token.size() + 1;
      std::string X2token = line_str.substr(prefix, line_str.find(delimiter, prefix));
      prefix += X2token.size() + 1;
      std::string Y2token = line_str.substr(prefix, line_str.find(")", prefix));
      double x1 = StringParser::StringToFloat<double>(X1token.c_str(), X1token.size(), &parse_result);
      double y1 = StringParser::StringToFloat<double>(Y1token.c_str(), Y1token.size(), &parse_result);
      double x2 = StringParser::StringToFloat<double>(X2token.c_str(), X2token.size(), &parse_result);
      double y2 = StringParser::StringToFloat<double>(Y2token.c_str(), Y2token.size(), &parse_result);

      Line line_data(x1, y1, x2, y2);
      Line* line_slot = reinterpret_cast<Line*>(slot);
      *line_slot = line_data;
      break;
    }
    case TYPE_RECTANGLE: {
      string rect_str(data, len);
      boost::algorithm::to_lower(rect_str);
      
      std::size_t startingField = rect_str.find("rectangle(");
      if (startingField == std::string::npos) {
        VLOG_QUERY << "Error in the well know text format of the rectangle";
        tuple->SetNull(slot_desc->null_indicator_offset());
        return true;
      }
      std::size_t prefix = startingField + 10;
      std::string delimiter = " ";
      std::string X1token = rect_str.substr(prefix, rect_str.find(delimiter, prefix));
      prefix += X1token.size() + 1;
      std::string Y1token = rect_str.substr(prefix, rect_str.find(delimiter, prefix));
      prefix += Y1token.size() + 1;
      std::string X2token = rect_str.substr(prefix, rect_str.find(delimiter, prefix));
      prefix += X2token.size() + 1;
      std::string Y2token = rect_str.substr(prefix, rect_str.find(")", prefix));
      
      double x1 = StringParser::StringToFloat<double>(X1token.c_str(), X1token.size(), &parse_result);
      double y1 = StringParser::StringToFloat<double>(Y1token.c_str(), Y1token.size(), &parse_result);
      double x2 = StringParser::StringToFloat<double>(X2token.c_str(), X2token.size(), &parse_result);
      double y2 = StringParser::StringToFloat<double>(Y2token.c_str(), Y2token.size(), &parse_result);
      
      Rectangle rect_data(x1, y1, x2, y2);
      Rectangle* rect_slot = reinterpret_cast<Rectangle*>(slot);
      *rect_slot = rect_data;
      break;
    }
    case TYPE_POLYGON: {
      string poly_str(data, len);
      boost::algorithm::to_lower(poly_str);
      std::size_t startingField = poly_str.find("polygon (");
      std::size_t lineStringPrefix;
      int lineStringCount = 0;
      if (startingField == std::string::npos) {
        VLOG_QUERY << "Error in the well know text format of the polygon";
      }
      else {
        lineStringPrefix = startingField + 9;
        lineStringCount = std::count(poly_str.begin() + lineStringPrefix, poly_str.end(), '(');
      }
      
      std::string lineStringToken, Xtoken, Ytoken;
      double x, y;
      int memoryNeeded = 4 * sizeof(double) + sizeof(int32_t);
      for (int i = 0; i < lineStringCount; i++) {
        if (i > 0) {
          lineStringPrefix +=2; //skip the ", " seperator between lineStrings
        }
        memoryNeeded += sizeof(int32_t);
        lineStringPrefix++; //skip the '(' character
        lineStringToken = poly_str.substr(lineStringPrefix, poly_str.find(")", lineStringPrefix) - lineStringPrefix);
        lineStringPrefix += lineStringToken.size() + 1;
        int pointsCount = std::count(lineStringToken.begin(), lineStringToken.end(), ',') + 1;
        memoryNeeded += pointsCount * 2 * sizeof(double);
      }
      Polygon poly_data;
      poly_data.serializedData_ = reinterpret_cast<char*>(pool->Allocate(memoryNeeded));
      poly_data.len_ = memoryNeeded;
      double minX, minY, maxX, maxY;
      minX = minY = numeric_limits<double>::max();
      maxX = maxY = numeric_limits<double>::min();
      int serializedDataIndex = 4 * sizeof(double);
      memcpy(poly_data.serializedData_ + serializedDataIndex, &lineStringCount, sizeof(int32_t));
      serializedDataIndex += sizeof(int32_t);
      lineStringPrefix = startingField + 9;
      for (int i = 0; i < lineStringCount; i++) {
        if (i > 0) {
          lineStringPrefix +=2; //skip the ", " seperator between lineStrings
        }
        lineStringPrefix++; //skip the '(' character
        lineStringToken = poly_str.substr(lineStringPrefix, poly_str.find(")", lineStringPrefix) - lineStringPrefix);
        lineStringPrefix += lineStringToken.size() + 1;
        int pointsCount = std::count(lineStringToken.begin(), lineStringToken.end(), ',') + 1;
        memcpy(poly_data.serializedData_ + serializedDataIndex, &pointsCount, sizeof(int32_t));
        serializedDataIndex += sizeof(int32_t);
        std::size_t pointsPrefix = 0;
        for (int j = 0; j < pointsCount; j++) {
          int position = lineStringToken.find(" ", pointsPrefix);
          Xtoken = lineStringToken.substr(pointsPrefix, position - pointsPrefix);
          pointsPrefix += Xtoken.size() + 1;
          if (j == pointsCount - 1) {
            Ytoken = lineStringToken.substr(pointsPrefix, lineStringToken.find(')', pointsPrefix) - pointsPrefix);
          }
          else {
            int position = lineStringToken.find(", ", pointsPrefix);
            Ytoken = lineStringToken.substr(pointsPrefix, position - pointsPrefix);
          }
          pointsPrefix += Ytoken.size() + 2;
          x = StringParser::StringToFloat<double>(Xtoken.c_str(), Xtoken.size(), &parse_result);
          y = StringParser::StringToFloat<double>(Ytoken.c_str(), Ytoken.size(), &parse_result);
          memcpy(poly_data.serializedData_ + serializedDataIndex, &x, sizeof(double));
          serializedDataIndex += sizeof(double);
          memcpy(poly_data.serializedData_ + serializedDataIndex, &y, sizeof(double));
          serializedDataIndex += sizeof(double);
          
          if (x < minX) {
            minX = x;
          }
          else if (x > maxX) {
            maxX = x;
          }
          if (y < minY) {
            minY = y;
          }
          else if (y > maxY) {
            maxY = y;
          }
        }
      }
      memcpy(poly_data.serializedData_, &minX, sizeof(double));
      memcpy(poly_data.serializedData_ + sizeof(double), &minY, sizeof(double));
      memcpy(poly_data.serializedData_ + 2 * sizeof(double), &maxX, sizeof(double));
      memcpy(poly_data.serializedData_ + 3 * sizeof(double), &maxY, sizeof(double));
      
      StringValue str;
      str.len = poly_data.len_;
      str.ptr = poly_data.serializedData_;
      StringValue* str_slot = reinterpret_cast<StringValue*>(slot);
      *str_slot = str;
      /*Polygon* poly_slot = reinterpret_cast<Polygon*>(slot);
      *poly_slot = poly_data;*/
      break;
    }
    case TYPE_TIMESTAMP: {
      TimestampValue* ts_slot = reinterpret_cast<TimestampValue*>(slot);
      *ts_slot = TimestampValue(data, len);
      if (ts_slot->NotADateTime()) {
        parse_result = StringParser::PARSE_FAILURE;
      }
      break;
    }
    case TYPE_DECIMAL: {
      switch (slot_desc->slot_size()) {
        case 4:
          *reinterpret_cast<Decimal4Value*>(slot) =
              StringParser::StringToDecimal<int32_t>(
                  data, len, slot_desc->type(), &parse_result);
          break;
        case 8:
          *reinterpret_cast<Decimal8Value*>(slot) =
              StringParser::StringToDecimal<int64_t>(
                  data, len, slot_desc->type(), &parse_result);
          break;
        case 12:
          DCHECK(false) << "Planner should not generate this.";
          break;
        case 16:
          *reinterpret_cast<Decimal16Value*>(slot) =
              StringParser::StringToDecimal<int128_t>(
                  data, len, slot_desc->type(), &parse_result);
          break;
        default:
          DCHECK(false) << "Decimal slots can't be this size.";
      }
      if (parse_result != StringParser::PARSE_SUCCESS) {
        // Don't accept underflow and overflow for decimals.
        parse_result = StringParser::PARSE_FAILURE;
      }
      break;
    }
    default:
      DCHECK(false) << "bad slot type: " << slot_desc->type();
      break;
  }

  // TODO: add warning for overflow case
  if (parse_result == StringParser::PARSE_FAILURE) {
    tuple->SetNull(slot_desc->null_indicator_offset());
    return false;
  }

  return true;
}

}

#endif
