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


#ifndef IMPALA_EXPRS_DECIMAL_FUNCTIONS_H
#define IMPALA_EXPRS_DECIMAL_FUNCTIONS_H

#include "exprs/decimal-operators.h"
#include "udf/udf.h"

using namespace impala_udf;

namespace impala {

class Expr;
class TupleRow;

class DecimalFunctions {
 public:
  static IntVal Precision(FunctionContext* context, const DecimalVal& val);
  static IntVal Scale(FunctionContext* context, const DecimalVal& val);

  static DecimalVal Abs(FunctionContext* context, const DecimalVal& val);
  static DecimalVal Ceil(FunctionContext* context, const DecimalVal& val);
  static DecimalVal Floor(FunctionContext* context, const DecimalVal& val);

  static DecimalVal Round(FunctionContext* context, const DecimalVal& val);

  static DecimalVal RoundTo(
      FunctionContext* context, const DecimalVal& val, const SmallIntVal& scale);
  static DecimalVal RoundTo(
      FunctionContext* context, const DecimalVal& val, const TinyIntVal& scale);
  static DecimalVal RoundTo(
      FunctionContext* context, const DecimalVal& val, const IntVal& scale);
  static DecimalVal RoundTo(
      FunctionContext* context, const DecimalVal& val, const BigIntVal& scale);

  static DecimalVal Truncate(FunctionContext* context, const DecimalVal& val);

  static DecimalVal TruncateTo(
      FunctionContext* context, const DecimalVal& val, const SmallIntVal& scale);
  static DecimalVal TruncateTo(
      FunctionContext* context, const DecimalVal& val, const TinyIntVal& scale);
  static DecimalVal TruncateTo(
      FunctionContext* context, const DecimalVal& val, const IntVal& scale);
  static DecimalVal TruncateTo(
      FunctionContext* context, const DecimalVal& val, const BigIntVal& scale);

 private:
  /// Implementation of RoundTo
  static DecimalVal RoundTo(FunctionContext* context, const DecimalVal& val, int scale,
                            DecimalOperators::DecimalRoundOp op);
};

}

#endif
