// Copyright 2015 Cloudera Inc.
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

#include <gtest/gtest.h>

#include "runtime/array-value-builder.h"
#include "testutil/desc-tbl-builder.h"

#include "common/names.h"

using namespace impala;

TEST(ArrayValueBuilderTest, MaxBufferSize) {
  ObjectPool obj_pool;
  DescriptorTblBuilder builder(&obj_pool);
  builder.DeclareTuple() << TYPE_TINYINT;
  DescriptorTbl* desc_tbl = builder.Build();
  vector<TupleDescriptor*> descs;
  desc_tbl->GetTupleDescs(&descs);
  DCHECK_EQ(descs.size(), 1);
  const TupleDescriptor& tuple_desc = *descs[0];
  DCHECK_EQ(tuple_desc.byte_size(), 2);

  // Create ArrayValue with buffer size of slightly more than INT_MAX / 2
  ArrayValue array_value;
  MemTracker tracker;
  MemPool pool(&tracker);
  int initial_capacity = (INT_MAX / 4) + 1;
  ArrayValueBuilder array_value_builder(
      &array_value, tuple_desc, &pool, initial_capacity);
  EXPECT_EQ(tracker.consumption(), initial_capacity * 2);

  // Attempt to double the buffer. This should fail due to the new buffer size exceeding
  // INT_MAX.
  DCHECK_GT(tracker.consumption(), INT_MAX / 2);
  array_value_builder.CommitTuples(initial_capacity);
  Tuple* tuple_mem;
  int num_tuples = array_value_builder.GetFreeMemory(&tuple_mem);
  EXPECT_EQ(num_tuples, 0);
  EXPECT_EQ(tracker.consumption(), initial_capacity * 2);

  pool.FreeAll();
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

