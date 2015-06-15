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


#ifndef IMPALA_EXPRS_ANYVAL_UTIL_H
#define IMPALA_EXPRS_ANYVAL_UTIL_H

#include "exec/line.h"
#include "exec/point.h"
#include "exec/rectangle.h"
#include "runtime/timestamp-value.h"
#include "udf/udf-internal.h"
#include "util/hash-util.h"

using namespace impala_udf;
using namespace std;
using namespace spatialimpala;

namespace impala {

class ObjectPool;

// Utilities for AnyVals
class AnyValUtil {
 public:
  static uint32_t Hash(const BooleanVal& v, const FunctionContext::TypeDesc&, int seed) {
    return HashUtil::Hash(&v.val, 1, seed);
  }

  static uint32_t Hash(const TinyIntVal& v, const FunctionContext::TypeDesc&, int seed) {
    return HashUtil::Hash(&v.val, 1, seed);
  }

  static uint32_t Hash(const SmallIntVal& v, const FunctionContext::TypeDesc&, int seed) {
    return HashUtil::Hash(&v.val, 2, seed);
  }

  static uint32_t Hash(const IntVal& v, const FunctionContext::TypeDesc&, int seed) {
    return HashUtil::Hash(&v.val, 4, seed);
  }

  static uint32_t Hash(const BigIntVal& v, const FunctionContext::TypeDesc&, int seed) {
    return HashUtil::Hash(&v.val, 8, seed);
  }

  static uint32_t Hash(const FloatVal& v, const FunctionContext::TypeDesc&, int seed) {
    return HashUtil::Hash(&v.val, 4, seed);
  }

  static uint32_t Hash(const DoubleVal& v, const FunctionContext::TypeDesc&, int seed) {
    return HashUtil::Hash(&v.val, 8, seed);
  }

  static uint32_t Hash(const StringVal& v, const FunctionContext::TypeDesc&, int seed) {
    return HashUtil::Hash(v.ptr, v.len, seed);
  }

  static uint32_t Hash(const TimestampVal& v, const FunctionContext::TypeDesc&,
      int seed) {
    TimestampValue tv = TimestampValue::FromTimestampVal(v);
    return tv.Hash(seed);
  }

  static uint64_t Hash(const DecimalVal& v, const FunctionContext::TypeDesc& t,
      int64_t seed) {
    DCHECK_GT(t.precision, 0);
    switch (ColumnType::GetDecimalByteSize(t.precision)) {
      case 4: return HashUtil::Hash(&v.val4, 4, seed);
      case 8: return HashUtil::Hash(&v.val8, 8, seed);
      case 16: return HashUtil::Hash(&v.val16, 16, seed);
      default:
        DCHECK(false);
        return 0;
    }
  }

  static uint64_t Hash64(const BooleanVal& v, const FunctionContext::TypeDesc&,
      int64_t seed) {
    return HashUtil::FnvHash64(&v.val, 1, seed);
  }

  static uint64_t Hash64(const TinyIntVal& v, const FunctionContext::TypeDesc&,
      int64_t seed) {
    return HashUtil::FnvHash64(&v.val, 1, seed);
  }

  static uint64_t Hash64(const SmallIntVal& v, const FunctionContext::TypeDesc&,
      int64_t seed) {
    return HashUtil::FnvHash64(&v.val, 2, seed);
  }

  static uint64_t Hash64(const IntVal& v, const FunctionContext::TypeDesc&,
      int64_t seed) {
    return HashUtil::FnvHash64(&v.val, 4, seed);
  }

  static uint64_t Hash64(const BigIntVal& v, const FunctionContext::TypeDesc&,
      int64_t seed) {
    return HashUtil::FnvHash64(&v.val, 8, seed);
  }

  static uint64_t Hash64(const FloatVal& v, const FunctionContext::TypeDesc&,
      int64_t seed) {
    return HashUtil::FnvHash64(&v.val, 4, seed);
  }

  static uint64_t Hash64(const DoubleVal& v, const FunctionContext::TypeDesc&,
      int64_t seed) {
    return HashUtil::FnvHash64(&v.val, 8, seed);
  }

  static uint64_t Hash64(const StringVal& v, const FunctionContext::TypeDesc&,
      int64_t seed) {
    return HashUtil::FnvHash64(v.ptr, v.len, seed);
  }

  static uint64_t Hash64(const TimestampVal& v, const FunctionContext::TypeDesc&,
      int64_t seed) {
    TimestampValue tv = TimestampValue::FromTimestampVal(v);
    return HashUtil::FnvHash64(&tv, 12, seed);
  }

  static uint64_t Hash64(const DecimalVal& v, const FunctionContext::TypeDesc& t,
      int64_t seed) {
    switch (ColumnType::GetDecimalByteSize(t.precision)) {
      case 4: return HashUtil::FnvHash64(&v.val4, 4, seed);
      case 8: return HashUtil::FnvHash64(&v.val8, 8, seed);
      case 16: return HashUtil::FnvHash64(&v.val16, 16, seed);
      default:
        DCHECK(false);
        return 0;
    }
  }

  // Returns the byte size of *Val for type t.
  static int AnyValSize(const ColumnType& t) {
    switch (t.type) {
      case TYPE_BOOLEAN: return sizeof(BooleanVal);
      case TYPE_TINYINT: return sizeof(TinyIntVal);
      case TYPE_SMALLINT: return sizeof(SmallIntVal);
      case TYPE_INT: return sizeof(IntVal);
      case TYPE_BIGINT: return sizeof(BigIntVal);
      case TYPE_FLOAT: return sizeof(FloatVal);
      case TYPE_DOUBLE: return sizeof(DoubleVal);
      case TYPE_STRING:
      case TYPE_VARCHAR:
      case TYPE_CHAR:
        return sizeof(StringVal);
      case TYPE_TIMESTAMP: return sizeof(TimestampVal);
      case TYPE_POINT: return sizeof(PointVal);
      case TYPE_LINE: return sizeof(LineVal);
      case TYPE_RECTANGLE: return sizeof(RectangleVal);
      default:
        DCHECK(false) << t;
        return 0;
    }
  }

  static std::string ToString(const StringVal& v) {
    return std::string(reinterpret_cast<char*>(v.ptr), v.len);
  }

  static StringVal FromString(FunctionContext* ctx, const std::string& s) {
    StringVal val = FromBuffer(ctx, s.c_str(), s.size());
    return val;
  }

  static void TruncateIfNecessary(const ColumnType& type, StringVal *val) {
    if (type.type == TYPE_VARCHAR) {
      DCHECK(type.len >= 0);
      val->len = min(val->len, type.len);
    }
  }

  static StringVal FromBuffer(FunctionContext* ctx, const char* ptr, int len) {
    StringVal result(ctx, len);
    memcpy(result.ptr, ptr, len);
    return result;
  }

  static FunctionContext::TypeDesc ColumnTypeToTypeDesc(const ColumnType& type);
  static ColumnType TypeDescToColumnType(const FunctionContext::TypeDesc& type);

  // Utility to put val into an AnyVal struct
  static void SetAnyVal(const void* slot, const ColumnType& type, AnyVal* dst) {
    if (slot == NULL) {
      dst->is_null = true;
      return;
    }

    dst->is_null = false;
    switch (type.type) {
      case TYPE_NULL: return;
      case TYPE_BOOLEAN:
        reinterpret_cast<BooleanVal*>(dst)->val = *reinterpret_cast<const bool*>(slot);
        return;
      case TYPE_TINYINT:
        reinterpret_cast<TinyIntVal*>(dst)->val = *reinterpret_cast<const int8_t*>(slot);
        return;
      case TYPE_SMALLINT:
        reinterpret_cast<SmallIntVal*>(dst)->val = *reinterpret_cast<const int16_t*>(slot);
        return;
      case TYPE_INT:
        reinterpret_cast<IntVal*>(dst)->val = *reinterpret_cast<const int32_t*>(slot);
        return;
      case TYPE_BIGINT:
        reinterpret_cast<BigIntVal*>(dst)->val = *reinterpret_cast<const int64_t*>(slot);
        return;
      case TYPE_FLOAT:
        reinterpret_cast<FloatVal*>(dst)->val = *reinterpret_cast<const float*>(slot);
        return;
      case TYPE_DOUBLE:
        reinterpret_cast<DoubleVal*>(dst)->val = *reinterpret_cast<const double*>(slot);
        return;
      case TYPE_STRING:
      case TYPE_VARCHAR:
      case TYPE_CHAR: {
        if (type.IsVarLen()) {
          reinterpret_cast<const StringValue*>(slot)->ToStringVal(
              reinterpret_cast<StringVal*>(dst));
          if (type.type == TYPE_VARCHAR) {
            StringVal* sv = reinterpret_cast<StringVal*>(dst);
            DCHECK(type.len >= 0);
            DCHECK_LE(sv->len, type.len);
          }
        } else {
          DCHECK_EQ(type.type, TYPE_CHAR);
          StringVal* sv = reinterpret_cast<StringVal*>(dst);
          sv->ptr = const_cast<uint8_t*>(reinterpret_cast<const uint8_t*>(slot));
          sv->len = type.len;
        }
        return;
      }
      case TYPE_TIMESTAMP:
        reinterpret_cast<const TimestampValue*>(slot)->ToTimestampVal(
            reinterpret_cast<TimestampVal*>(dst));
        return;
      case TYPE_DECIMAL:
        switch (type.GetByteSize()) {
          case 4:
            reinterpret_cast<DecimalVal*>(dst)->val4 =
                *reinterpret_cast<const int32_t*>(slot);
            return;
          case 8:
            reinterpret_cast<DecimalVal*>(dst)->val8 =
                *reinterpret_cast<const int64_t*>(slot);
            return;
#if __BYTE_ORDER == __LITTLE_ENDIAN
          case 16:
            memcpy(&reinterpret_cast<DecimalVal*>(dst)->val4, slot, type.GetByteSize());
#else
            DCHECK(false) << "Not implemented.";
#endif
            return;
          default:
            break;
        }
      case TYPE_POINT: {
        const Point* pv = reinterpret_cast<const Point*>(slot);
        PointVal* point_val = reinterpret_cast<PointVal*>(dst);
        point_val->x = pv->x_;
        point_val->y = pv->y_;
        return;
      }
      case TYPE_LINE: {
        const Line* lv = reinterpret_cast<const Line*>(slot);
        LineVal* line_val = reinterpret_cast<LineVal*>(dst);
        line_val->x1 = lv->x1_;
        line_val->y1 = lv->y1_;
        line_val->x2 = lv->x2_;
        line_val->y2 = lv->y2_;
        return;
      }
      case TYPE_RECTANGLE: {
        const Rectangle* rv = reinterpret_cast<const Rectangle*>(slot);
        RectangleVal* rect_val = reinterpret_cast<RectangleVal*>(dst);
        rect_val->x1 = rv->x1_;
        rect_val->y1 = rv->y1_;
        rect_val->x2 = rv->x2_;
        rect_val->y2 = rv->y2_;
        return;
      }
      default:
        DCHECK(false) << "NYI: " << type;
    }
  }
};

// Creates the corresponding AnyVal subclass for type. The object is added to the pool.
impala_udf::AnyVal* CreateAnyVal(ObjectPool* pool, const ColumnType& type);

// Creates the corresponding AnyVal subclass for type. The object is owned by the caller.
impala_udf::AnyVal* CreateAnyVal(const ColumnType& type);

}

#endif
