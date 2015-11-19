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

#ifndef IMPALA_UDF_TEST_UDFS_H
#define IMPALA_UDF_TEST_UDFS_H

#include "udf/udf.h"

using namespace impala_udf;

void MemTestPrepare(FunctionContext* context, FunctionContext::FunctionStateScope scope);
BigIntVal MemTest(FunctionContext* context, const BigIntVal& bytes);
void MemTestClose(FunctionContext* context, FunctionContext::FunctionStateScope scope);

BigIntVal DoubleFreeTest(FunctionContext* context, BigIntVal bytes);

#endif
