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


#ifndef IMPALA_EXPRS_IN_PREDICATE_H_
#define IMPALA_EXPRS_IN_PREDICATE_H_

#include <string>
#include "exprs/predicate.h"
#include "udf/udf.h"

namespace impala {

/// Predicate for evaluating expressions of the form "val [NOT] IN (x1, x2, x3...)".
//
/// There are two strategies for evaluating the IN predicate:
//
/// 1) SET_LOOKUP: This strategy is for when all the values in the IN list are constant. In
///    the prepare function, we create a set of the constant values from the IN list, and
///    use this set to lookup a given 'val'.
//
/// 2) ITERATE: This is the fallback strategy for when their are non-constant IN list
///    values, or very few values in the IN list. We simply iterate through every
///    expression and compare it to val. This strategy has no prepare function.
//
/// The FE chooses which strategy we should use by choosing the appropriate function (e.g.,
/// InIterate() or InSetLookup()). If it chooses SET_LOOKUP, it also sets the appropriate
/// SetLookupPrepare and SetLookupClose functions.
//
/// TODO: the set lookup logic is not yet implemented for TimestampVals or DecimalVals
class InPredicate : public Predicate {
 public:
  /// Functions for every type
  static impala_udf::BooleanVal InIterate(
      impala_udf::FunctionContext* context, const impala_udf::BooleanVal& val,
      int num_args, const impala_udf::BooleanVal* args);

  static impala_udf::BooleanVal NotInIterate(
      impala_udf::FunctionContext* context, const impala_udf::BooleanVal& val,
      int num_args, const impala_udf::BooleanVal* args);

  static void SetLookupPrepare_boolean(impala_udf::FunctionContext* ctx,
      impala_udf::FunctionContext::FunctionStateScope scope);

  static void SetLookupClose_boolean(impala_udf::FunctionContext* ctx,
      impala_udf::FunctionContext::FunctionStateScope scope);

  static impala_udf::BooleanVal InSetLookup(
      impala_udf::FunctionContext* context, const impala_udf::BooleanVal& val,
      int num_args, const impala_udf::BooleanVal* args);

  static impala_udf::BooleanVal NotInSetLookup(
      impala_udf::FunctionContext* context, const impala_udf::BooleanVal& val,
      int num_args, const impala_udf::BooleanVal* args);

  static impala_udf::BooleanVal InIterate(
      impala_udf::FunctionContext* context, const impala_udf::TinyIntVal& val,
      int num_args, const impala_udf::TinyIntVal* args);

  static impala_udf::BooleanVal NotInIterate(
      impala_udf::FunctionContext* context, const impala_udf::TinyIntVal& val,
      int num_args, const impala_udf::TinyIntVal* args);

  static void SetLookupPrepare_tinyint(impala_udf::FunctionContext* ctx,
      impala_udf::FunctionContext::FunctionStateScope scope);

  static void SetLookupClose_tinyint(impala_udf::FunctionContext* ctx,
      impala_udf::FunctionContext::FunctionStateScope scope);

  static impala_udf::BooleanVal InSetLookup(
      impala_udf::FunctionContext* context, const impala_udf::TinyIntVal& val,
      int num_args, const impala_udf::TinyIntVal* args);

  static impala_udf::BooleanVal NotInSetLookup(
      impala_udf::FunctionContext* context, const impala_udf::TinyIntVal& val,
      int num_args, const impala_udf::TinyIntVal* args);

  static impala_udf::BooleanVal InIterate(
      impala_udf::FunctionContext* context, const impala_udf::SmallIntVal& val,
      int num_args, const impala_udf::SmallIntVal* args);

  static impala_udf::BooleanVal NotInIterate(
      impala_udf::FunctionContext* context, const impala_udf::SmallIntVal& val,
      int num_args, const impala_udf::SmallIntVal* args);

  static void SetLookupPrepare_smallint(impala_udf::FunctionContext* ctx,
      impala_udf::FunctionContext::FunctionStateScope scope);

  static void SetLookupClose_smallint(impala_udf::FunctionContext* ctx,
      impala_udf::FunctionContext::FunctionStateScope scope);

  static impala_udf::BooleanVal InSetLookup(
      impala_udf::FunctionContext* context, const impala_udf::SmallIntVal& val,
      int num_args, const impala_udf::SmallIntVal* args);

  static impala_udf::BooleanVal NotInSetLookup(
      impala_udf::FunctionContext* context, const impala_udf::SmallIntVal& val,
      int num_args, const impala_udf::SmallIntVal* args);

  static impala_udf::BooleanVal InIterate(
      impala_udf::FunctionContext* context, const impala_udf::IntVal& val,
      int num_args, const impala_udf::IntVal* args);

  static impala_udf::BooleanVal NotInIterate(
      impala_udf::FunctionContext* context, const impala_udf::IntVal& val,
      int num_args, const impala_udf::IntVal* args);

  static void SetLookupPrepare_int(impala_udf::FunctionContext* ctx,
      impala_udf::FunctionContext::FunctionStateScope scope);

  static void SetLookupClose_int(impala_udf::FunctionContext* ctx,
      impala_udf::FunctionContext::FunctionStateScope scope);

  static impala_udf::BooleanVal InSetLookup(
      impala_udf::FunctionContext* context, const impala_udf::IntVal& val,
      int num_args, const impala_udf::IntVal* args);

  static impala_udf::BooleanVal NotInSetLookup(
      impala_udf::FunctionContext* context, const impala_udf::IntVal& val,
      int num_args, const impala_udf::IntVal* args);

  static impala_udf::BooleanVal InIterate(
      impala_udf::FunctionContext* context, const impala_udf::BigIntVal& val,
      int num_args, const impala_udf::BigIntVal* args);

  static impala_udf::BooleanVal NotInIterate(
      impala_udf::FunctionContext* context, const impala_udf::BigIntVal& val,
      int num_args, const impala_udf::BigIntVal* args);

  static void SetLookupPrepare_bigint(impala_udf::FunctionContext* ctx,
      impala_udf::FunctionContext::FunctionStateScope scope);

  static void SetLookupClose_bigint(impala_udf::FunctionContext* ctx,
      impala_udf::FunctionContext::FunctionStateScope scope);

  static impala_udf::BooleanVal InSetLookup(
      impala_udf::FunctionContext* context, const impala_udf::BigIntVal& val,
      int num_args, const impala_udf::BigIntVal* args);

  static impala_udf::BooleanVal NotInSetLookup(
      impala_udf::FunctionContext* context, const impala_udf::BigIntVal& val,
      int num_args, const impala_udf::BigIntVal* args);

  static impala_udf::BooleanVal InIterate(
      impala_udf::FunctionContext* context, const impala_udf::FloatVal& val,
      int num_args, const impala_udf::FloatVal* args);

  static impala_udf::BooleanVal NotInIterate(
      impala_udf::FunctionContext* context, const impala_udf::FloatVal& val,
      int num_args, const impala_udf::FloatVal* args);

  static void SetLookupPrepare_float(impala_udf::FunctionContext* ctx,
      impala_udf::FunctionContext::FunctionStateScope scope);

  static void SetLookupClose_float(impala_udf::FunctionContext* ctx,
      impala_udf::FunctionContext::FunctionStateScope scope);

  static impala_udf::BooleanVal InSetLookup(
      impala_udf::FunctionContext* context, const impala_udf::FloatVal& val,
      int num_args, const impala_udf::FloatVal* args);

  static impala_udf::BooleanVal NotInSetLookup(
      impala_udf::FunctionContext* context, const impala_udf::FloatVal& val,
      int num_args, const impala_udf::FloatVal* args);

  static impala_udf::BooleanVal InIterate(
      impala_udf::FunctionContext* context, const impala_udf::DoubleVal& val,
      int num_args, const impala_udf::DoubleVal* args);

  static impala_udf::BooleanVal NotInIterate(
      impala_udf::FunctionContext* context, const impala_udf::DoubleVal& val,
      int num_args, const impala_udf::DoubleVal* args);

  static void SetLookupPrepare_double(impala_udf::FunctionContext* ctx,
      impala_udf::FunctionContext::FunctionStateScope scope);

  static void SetLookupClose_double(impala_udf::FunctionContext* ctx,
      impala_udf::FunctionContext::FunctionStateScope scope);

  static impala_udf::BooleanVal InSetLookup(
      impala_udf::FunctionContext* context, const impala_udf::DoubleVal& val,
      int num_args, const impala_udf::DoubleVal* args);

  static impala_udf::BooleanVal NotInSetLookup(
      impala_udf::FunctionContext* context, const impala_udf::DoubleVal& val,
      int num_args, const impala_udf::DoubleVal* args);

  static impala_udf::BooleanVal InIterate(
      impala_udf::FunctionContext* context, const impala_udf::StringVal& val,
      int num_args, const impala_udf::StringVal* args);

  static impala_udf::BooleanVal NotInIterate(
      impala_udf::FunctionContext* context, const impala_udf::StringVal& val,
      int num_args, const impala_udf::StringVal* args);

  static void SetLookupPrepare_string(impala_udf::FunctionContext* ctx,
      impala_udf::FunctionContext::FunctionStateScope scope);

  static void SetLookupClose_string(impala_udf::FunctionContext* ctx,
      impala_udf::FunctionContext::FunctionStateScope scope);

  static impala_udf::BooleanVal InSetLookup(
      impala_udf::FunctionContext* context, const impala_udf::StringVal& val,
      int num_args, const impala_udf::StringVal* args);

  static impala_udf::BooleanVal NotInSetLookup(
      impala_udf::FunctionContext* context, const impala_udf::StringVal& val,
      int num_args, const impala_udf::StringVal* args);

  static impala_udf::BooleanVal InIterate(
      impala_udf::FunctionContext* context, const impala_udf::TimestampVal& val,
      int num_args, const impala_udf::TimestampVal* args);

  static impala_udf::BooleanVal NotInIterate(
      impala_udf::FunctionContext* context, const impala_udf::TimestampVal& val,
      int num_args, const impala_udf::TimestampVal* args);

  static void SetLookupPrepare_timestamp(impala_udf::FunctionContext* ctx,
      impala_udf::FunctionContext::FunctionStateScope scope);

  static void SetLookupClose_timestamp(impala_udf::FunctionContext* ctx,
      impala_udf::FunctionContext::FunctionStateScope scope);

  static impala_udf::BooleanVal InSetLookup(
      impala_udf::FunctionContext* context, const impala_udf::TimestampVal& val,
      int num_args, const impala_udf::TimestampVal* args);

  static impala_udf::BooleanVal NotInSetLookup(
      impala_udf::FunctionContext* context, const impala_udf::TimestampVal& val,
      int num_args, const impala_udf::TimestampVal* args);

  static impala_udf::BooleanVal InIterate(
      impala_udf::FunctionContext* context, const impala_udf::DecimalVal& val,
      int num_args, const impala_udf::DecimalVal* args);

  static impala_udf::BooleanVal NotInIterate(
      impala_udf::FunctionContext* context, const impala_udf::DecimalVal& val,
      int num_args, const impala_udf::DecimalVal* args);

  static void SetLookupPrepare_decimal(impala_udf::FunctionContext* ctx,
      impala_udf::FunctionContext::FunctionStateScope scope);

  static void SetLookupClose_decimal(impala_udf::FunctionContext* ctx,
      impala_udf::FunctionContext::FunctionStateScope scope);

  static impala_udf::BooleanVal InSetLookup(
      impala_udf::FunctionContext* context, const impala_udf::DecimalVal& val,
      int num_args, const impala_udf::DecimalVal* args);

  static impala_udf::BooleanVal NotInSetLookup(
      impala_udf::FunctionContext* context, const impala_udf::DecimalVal& val,
      int num_args, const impala_udf::DecimalVal* args);

 private:
  friend class InPredicateBenchmark;

  enum Strategy {
    /// Indicates we should use SetLookUp().
    SET_LOOKUP,
    /// Indicates we should use Iterate().
    ITERATE
  };

  template<typename SetType>
  struct SetLookupState {
    /// If true, there is at least one NULL constant in the IN list.
    bool contains_null;

    /// The set of all non-NULL constant values in the IN list.
    /// Note: boost::unordered_set and std::binary_search performed worse based on the
    /// in-predicate-benchmark
    std::set<SetType> val_set;

    /// The type of the arguments
    const FunctionContext::TypeDesc* type;
  };

  /// The templated function that provides the implementation for all the In() and NotIn()
  /// functions.
  template<typename T, typename SetType, bool not_in, Strategy strategy>
  static inline impala_udf::BooleanVal TemplatedIn(
      impala_udf::FunctionContext* context, const T& val, int num_args, const T* args);

  /// Initializes an SetLookupState in ctx.
  template<typename T, typename SetType>
  static void SetLookupPrepare(
      FunctionContext* ctx, FunctionContext::FunctionStateScope scope);

  template<typename SetType>
  static void SetLookupClose(
      FunctionContext* ctx, FunctionContext::FunctionStateScope scope);

  /// Looks up v in state->val_set.
  template<typename T, typename SetType>
  static BooleanVal SetLookup(SetLookupState<SetType>* state, const T& v);

  /// Iterates through each vararg looking for val. 'type' is the type of 'val' and 'args'.
  template<typename T>
  static BooleanVal Iterate(
      const FunctionContext::TypeDesc* type, const T& val, int num_args, const T* args);
};

}

#endif
