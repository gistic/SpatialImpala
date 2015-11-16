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

#include <stdlib.h>
#include <stdio.h>
#include <iostream>
#include <vector>

#include <gtest/gtest.h>

#include "common/compiler-util.h"
#include "exec/hash-table.inline.h"
#include "exprs/expr.h"
#include "exprs/expr-context.h"
#include "exprs/slot-ref.h"
#include "runtime/mem-pool.h"
#include "runtime/mem-tracker.h"
#include "runtime/string-value.h"
#include "runtime/mem-tracker.h"
#include "util/cpu-info.h"
#include "util/runtime-profile.h"

#include "common/names.h"

namespace impala {

class HashTableTest : public testing::Test {
 public:
  HashTableTest() : mem_pool_(&tracker_) {}

 protected:
  ObjectPool pool_;
  MemTracker tracker_;
  MemPool mem_pool_;
  vector<ExprContext*> build_expr_ctxs_;
  vector<ExprContext*> probe_expr_ctxs_;

  virtual void SetUp() {
    RowDescriptor desc;
    Status status;

    // Not very easy to test complex tuple layouts so this test will use the
    // simplest.  The purpose of these tests is to exercise the hash map
    // internals so a simple build/probe expr is fine.
    Expr* expr = pool_.Add(new SlotRef(TYPE_INT, 0));
    build_expr_ctxs_.push_back(pool_.Add(new ExprContext(expr)));
    status = Expr::Prepare(build_expr_ctxs_, NULL, desc, &tracker_);
    EXPECT_TRUE(status.ok());
    status = Expr::Open(build_expr_ctxs_, NULL);
    EXPECT_TRUE(status.ok());

    expr = pool_.Add(new SlotRef(TYPE_INT, 0));
    probe_expr_ctxs_.push_back(pool_.Add(new ExprContext(expr)));
    status = Expr::Prepare(probe_expr_ctxs_, NULL, desc, &tracker_);
    EXPECT_TRUE(status.ok());
    status = Expr::Open(probe_expr_ctxs_, NULL);
    EXPECT_TRUE(status.ok());
  }

  virtual void TearDown() {
    Expr::Close(build_expr_ctxs_, NULL);
    Expr::Close(probe_expr_ctxs_, NULL);
  }

  TupleRow* CreateTupleRow(int32_t val) {
    uint8_t* tuple_row_mem = mem_pool_.Allocate(sizeof(int32_t*));
    Tuple* tuple_mem = Tuple::Create(sizeof(int32_t), &mem_pool_);
    *reinterpret_cast<int32_t*>(tuple_mem) = val;
    TupleRow* row = reinterpret_cast<TupleRow*>(tuple_row_mem);
    row->SetTuple(0, tuple_mem);
    return row;
  }

  // Wrapper to call private methods on HashTable
  // TODO: understand google testing, there must be a more natural way to do this
  void ResizeTable(HashTable* table, int64_t new_size, HashTableCtx* ht_ctx) {
    table->ResizeBuckets(new_size, ht_ctx);
  }

  // Do a full table scan on table.  All values should be between [min,max).  If
  // all_unique, then each key(int value) should only appear once.  Results are
  // stored in results, indexed by the key.  Results must have been preallocated to
  // be at least max size.
  void FullScan(HashTable* table, HashTableCtx* ht_ctx, int min, int max,
      bool all_unique, TupleRow** results, TupleRow** expected) {
    HashTable::Iterator iter = table->Begin(ht_ctx);
    while (!iter.AtEnd()) {
      TupleRow* row = iter.GetRow();
      int32_t val = *reinterpret_cast<int32_t*>(build_expr_ctxs_[0]->GetValue(row));
      EXPECT_GE(val, min);
      EXPECT_LT(val, max);
      if (all_unique) EXPECT_TRUE(results[val] == NULL);
      EXPECT_EQ(row->GetTuple(0), expected[val]->GetTuple(0));
      results[val] = row;
      iter.Next();
    }
  }

  // Validate that probe_row evaluates overs probe_exprs is equal to build_row
  // evaluated over build_exprs
  void ValidateMatch(TupleRow* probe_row, TupleRow* build_row) {
    EXPECT_TRUE(probe_row != build_row);
    int32_t build_val =
        *reinterpret_cast<int32_t*>(build_expr_ctxs_[0]->GetValue(probe_row));
    int32_t probe_val =
        *reinterpret_cast<int32_t*>(probe_expr_ctxs_[0]->GetValue(build_row));
    EXPECT_EQ(build_val, probe_val);
  }

  struct ProbeTestData {
    TupleRow* probe_row;
    vector<TupleRow*> expected_build_rows;
  };

  void ProbeTest(HashTable* table, HashTableCtx* ht_ctx,
      ProbeTestData* data, int num_data, bool scan) {
    uint32_t hash = 0;
    for (int i = 0; i < num_data; ++i) {
      TupleRow* row = data[i].probe_row;

      HashTable::Iterator iter;
      if (ht_ctx->EvalAndHashProbe(row, &hash)) continue;
      iter = table->Find(ht_ctx, hash);

      if (data[i].expected_build_rows.size() == 0) {
        EXPECT_TRUE(iter.AtEnd());
      } else {
        if (scan) {
          map<TupleRow*, bool> matched;
          while (!iter.AtEnd()) {
            EXPECT_EQ(matched.find(iter.GetRow()), matched.end());
            matched[iter.GetRow()] = true;
            iter.Next();
          }
          EXPECT_EQ(matched.size(), data[i].expected_build_rows.size());
          for (int j = 0; i < data[j].expected_build_rows.size(); ++j) {
            EXPECT_TRUE(matched[data[i].expected_build_rows[j]]);
          }
        } else {
          EXPECT_EQ(data[i].expected_build_rows.size(), 1);
          EXPECT_EQ(data[i].expected_build_rows[0]->GetTuple(0),
                    iter.GetRow()->GetTuple(0));
          ValidateMatch(row, iter.GetRow());
        }
      }
    }
  }

  // Constructs and closes a hash table.
  void SetupTest(bool quadratic, int table_size) {
    TupleRow* build_row1 = CreateTupleRow(1);
    TupleRow* build_row2 = CreateTupleRow(2);
    TupleRow* probe_row3 = CreateTupleRow(3);
    TupleRow* probe_row4 = CreateTupleRow(4);

    int32_t* val_row1 =
        reinterpret_cast<int32_t*>(build_expr_ctxs_[0]->GetValue(build_row1));
    EXPECT_EQ(*val_row1, 1);
    int32_t* val_row2 =
        reinterpret_cast<int32_t*>(build_expr_ctxs_[0]->GetValue(build_row2));
    EXPECT_EQ(*val_row2, 2);
    int32_t* val_row3 =
        reinterpret_cast<int32_t*>(probe_expr_ctxs_[0]->GetValue(probe_row3));
    EXPECT_EQ(*val_row3, 3);
    int32_t* val_row4 =
        reinterpret_cast<int32_t*>(probe_expr_ctxs_[0]->GetValue(probe_row4));
    EXPECT_EQ(*val_row4, 4);

    // Create and close the hash table.
    HashTable hash_table(&mem_pool_, quadratic, table_size);
    hash_table.Close();
    mem_pool_.FreeAll();
  }

  // This test inserts the build rows [0->5) to hash table. It validates that they
  // are all there using a full table scan. It also validates that Find() is correct
  // testing for probe rows that are both there and not.
  // The hash table is resized a few times and the scans/finds are tested again.
  void BasicTest(bool quadratic, int table_size) {
    TupleRow* build_rows[5];
    TupleRow* scan_rows[5] = {0};
    for (int i = 0; i < 5; ++i) build_rows[i] = CreateTupleRow(i);

    ProbeTestData probe_rows[10];
    for (int i = 0; i < 10; ++i) {
      probe_rows[i].probe_row = CreateTupleRow(i);
      if (i < 5) probe_rows[i].expected_build_rows.push_back(build_rows[i]);
    }

    // Create the hash table and insert the build rows
    HashTable hash_table(&mem_pool_, quadratic, table_size);
    HashTableCtx ht_ctx(build_expr_ctxs_, probe_expr_ctxs_, false, false, 1, 0, 1);

    uint32_t hash = 0;
    bool success = hash_table.CheckAndResize(5, &ht_ctx);
    EXPECT_TRUE(success);
    for (int i = 0; i < 5; ++i) {
      if (!ht_ctx.EvalAndHashBuild(build_rows[i], &hash)) continue;
      bool inserted = hash_table.Insert(&ht_ctx, build_rows[i]->GetTuple(0), hash);
      EXPECT_TRUE(inserted);
    }
    EXPECT_EQ(hash_table.size(), 5);

    // Do a full table scan and validate returned pointers
    FullScan(&hash_table, &ht_ctx, 0, 5, true, scan_rows, build_rows);
    ProbeTest(&hash_table, &ht_ctx, probe_rows, 10, false);

    // Double the size of the hash table and scan again.
    ResizeTable(&hash_table, 2048, &ht_ctx);
    EXPECT_EQ(hash_table.num_buckets(), 2048);
    EXPECT_EQ(hash_table.size(), 5);
    memset(scan_rows, 0, sizeof(scan_rows));
    FullScan(&hash_table, &ht_ctx, 0, 5, true, scan_rows, build_rows);
    ProbeTest(&hash_table, &ht_ctx, probe_rows, 10, false);

    // Try to shrink and scan again.
    ResizeTable(&hash_table, 64, &ht_ctx);
    EXPECT_EQ(hash_table.num_buckets(), 64);
    EXPECT_EQ(hash_table.size(), 5);
    memset(scan_rows, 0, sizeof(scan_rows));
    FullScan(&hash_table, &ht_ctx, 0, 5, true, scan_rows, build_rows);
    ProbeTest(&hash_table, &ht_ctx, probe_rows, 10, false);

    // Resize to 8, which is the smallest value to fit the number of filled buckets.
    ResizeTable(&hash_table, 8, &ht_ctx);
    EXPECT_EQ(hash_table.num_buckets(), 8);
    EXPECT_EQ(hash_table.size(), 5);
    memset(scan_rows, 0, sizeof(scan_rows));
    FullScan(&hash_table, &ht_ctx, 0, 5, true, scan_rows, build_rows);
    ProbeTest(&hash_table, &ht_ctx, probe_rows, 10, false);

    hash_table.Close();
    mem_pool_.FreeAll();
  }

  void ScanTest(bool quadratic, int initial_size, int rows_to_insert,
                int additional_rows) {
    int total_rows = rows_to_insert + additional_rows;
    int target_size = BitUtil::NextPowerOfTwo(initial_size);
    HashTable hash_table(&mem_pool_, quadratic, target_size);
    HashTableCtx ht_ctx(build_expr_ctxs_, probe_expr_ctxs_, false, false, 1, 0, 1);

    // Add 1 row with val 1, 2 with val 2, etc
    vector<TupleRow*> build_rows;
    ProbeTestData* probe_rows = new ProbeTestData[total_rows];
    probe_rows[0].probe_row = CreateTupleRow(0);
    uint32_t hash = 0;
    for (int val = 1; val <= rows_to_insert; ++val) {
      bool success = hash_table.CheckAndResize(val, &ht_ctx);
      EXPECT_TRUE(success) << " failed to resize: " << val;
      probe_rows[val].probe_row = CreateTupleRow(val);
      for (int i = 0; i < val; ++i) {
        TupleRow* row = CreateTupleRow(val);
        if (!ht_ctx.EvalAndHashBuild(row, &hash)) continue;
        hash_table.Insert(&ht_ctx, row->GetTuple(0), hash);
        build_rows.push_back(row);
        probe_rows[val].expected_build_rows.push_back(row);
      }
    }

    // Add some more probe rows that aren't there.
    for (int val = rows_to_insert; val < rows_to_insert + additional_rows; ++val) {
      probe_rows[val].probe_row = CreateTupleRow(val);
    }

    // Test that all the builds were found.
    ProbeTest(&hash_table, &ht_ctx, probe_rows, total_rows, true);

    // Resize and try again.
    target_size = BitUtil::NextPowerOfTwo(2 * total_rows);
    ResizeTable(&hash_table, target_size, &ht_ctx);
    EXPECT_EQ(hash_table.num_buckets(), target_size);
    ProbeTest(&hash_table, &ht_ctx, probe_rows, total_rows, true);

    target_size = BitUtil::NextPowerOfTwo(total_rows + 1);
    ResizeTable(&hash_table, target_size, &ht_ctx);
    EXPECT_EQ(hash_table.num_buckets(), target_size);
    ProbeTest(&hash_table, &ht_ctx, probe_rows, total_rows, true);

    delete [] probe_rows;
    hash_table.Close();
    mem_pool_.FreeAll();
  }

  // This test continues adding tuples to the hash table and exercises the resize code
  // paths.
  void GrowTableTest(bool quadratic) {
    uint64_t num_to_add = 4;
    int expected_size = 0;

    // Allocate a new pool to test OOM.
    MemTracker tracker(100 * 1024 * 1024);
    MemPool pool(&tracker);
    HashTable hash_table(&pool, quadratic, num_to_add);
    HashTableCtx ht_ctx(build_expr_ctxs_, probe_expr_ctxs_, false, false, 1, 0, 1);

    // Inserts num_to_add + (num_to_add^2) + (num_to_add^4) + ... + (num_to_add^20)
    // entries. When num_to_add == 4, then the total number of inserts is 4194300.
    int build_row_val = 0;
    uint32_t hash = 0;
    for (int i = 0; i < 20; ++i) {
      // Currently the mem used for the bucket is not being tracked by the mem tracker.
      // Thus the resize is expected to be successful.
      // TODO: Keep track of the mem used for the buckets and test cases where we
      // actually hit OOM.
      bool success = hash_table.CheckAndResize(num_to_add, &ht_ctx);
      EXPECT_TRUE(success) << " failed to resize: " << num_to_add;
      for (int j = 0; j < num_to_add; ++build_row_val, ++j) {
        TupleRow* row = CreateTupleRow(build_row_val);
        if (!ht_ctx.EvalAndHashBuild(row, &hash)) continue;
        bool inserted = hash_table.Insert(&ht_ctx, row->GetTuple(0), hash);
        if (!inserted) goto done_inserting;
      }
      expected_size += num_to_add;
      num_to_add *= 2;
    }
 done_inserting:
    EXPECT_FALSE(tracker.LimitExceeded());
    EXPECT_EQ(hash_table.size(), 4194300);
    // Validate that we can find the entries before we went over the limit
    for (int i = 0; i < expected_size * 5; i += 100000) {
      TupleRow* probe_row = CreateTupleRow(i);
      if (!ht_ctx.EvalAndHashProbe(probe_row, &hash)) continue;
      HashTable::Iterator iter = hash_table.Find(&ht_ctx, hash);
      if (i < hash_table.size()) {
        EXPECT_TRUE(!iter.AtEnd()) << " i: " << i;
        ValidateMatch(probe_row, iter.GetRow());
      } else {
        EXPECT_TRUE(iter.AtEnd()) << " i: " << i;
      }
    }
    hash_table.Close();
    pool.FreeAll();
    mem_pool_.FreeAll();
  }

  // This test inserts and probes as many elements as the size of the hash table without
  // calling resize. All the inserts and probes are expected to succeed, because there is
  // enough space in the hash table (it is also expected to be slow). It also expects that
  // a probe for a N+1 element will return BUCKET_NOT_FOUND.
  void InsertFullTest(bool quadratic, int table_size) {
    // Allocate a new pool to test OOM.
    MemTracker tracker(100 * 1024 * 1024);
    MemPool pool(&tracker);
    HashTable hash_table(&pool, quadratic, table_size);
    HashTableCtx ht_ctx(build_expr_ctxs_, probe_expr_ctxs_, false, false, 1, 0, 1);
    EXPECT_EQ(hash_table.EmptyBuckets(), table_size);

    // Insert and probe table_size different tuples. All of them are expected to be
    // successfully inserted and probed.
    uint32_t hash = 0;
    HashTable::Iterator iter;
    for (int build_row_val = 0; build_row_val < table_size; ++build_row_val) {
      TupleRow* row = CreateTupleRow(build_row_val);
      bool passes = ht_ctx.EvalAndHashBuild(row, &hash);
      EXPECT_TRUE(passes);
      bool inserted = hash_table.Insert(&ht_ctx, row->GetTuple(0), hash);
      EXPECT_TRUE(inserted);
      EXPECT_EQ(hash_table.EmptyBuckets(), table_size - build_row_val - 1);

      passes = ht_ctx.EvalAndHashProbe(row, &hash);
      EXPECT_TRUE(passes);
      iter = hash_table.Find(&ht_ctx, hash);
      EXPECT_FALSE(iter.AtEnd());
    }

    // Probe for a tuple that does not exist. This should exercise the probe of a full
    // hash table code path.
    EXPECT_EQ(hash_table.EmptyBuckets(), 0);
    TupleRow* probe_row = CreateTupleRow(table_size);
    bool passes = ht_ctx.EvalAndHashProbe(probe_row, &hash);
    EXPECT_TRUE(passes);
    iter = hash_table.Find(&ht_ctx, hash);
    EXPECT_TRUE(iter.AtEnd());
    hash_table.Close();
    pool.FreeAll();
    mem_pool_.FreeAll();
  }
};

TEST_F(HashTableTest, LinearSetupTest) {
  SetupTest(false, 1);
  SetupTest(false, 1024);
  SetupTest(false, 65536);
}

TEST_F(HashTableTest, QuadraticSetupTest) {
  SetupTest(true, 1);
  SetupTest(true, 1024);
  SetupTest(true, 65536);
}

// TODO: In order to test IMPALA-2065, after IMPALA-1656 is delivered add a test that
// tries to Init a hash table with very large (>2^31) number of buckets.
//TEST_F(HashTableTest, InitTest)

TEST_F(HashTableTest, LinearBasicTest) {
  BasicTest(false, 1);
  BasicTest(false, 1024);
  BasicTest(false, 65536);
}

TEST_F(HashTableTest, QuadraticBasicTest) {
  BasicTest(true, 1);
  BasicTest(true, 1024);
  BasicTest(true, 65536);
}

// This test makes sure we can scan ranges of buckets.
TEST_F(HashTableTest, LinearScanTest) {
  ScanTest(false, 1, 10, 5);
  ScanTest(false, 1024, 1000, 5);
  ScanTest(false, 1024, 1000, 500);
}

TEST_F(HashTableTest, QuadraticScanTest) {
  ScanTest(true, 1, 10, 5);
  ScanTest(true, 1024, 1000, 5);
  ScanTest(true, 1024, 1000, 500);
}

TEST_F(HashTableTest, LinearGrowTableTest) {
  GrowTableTest(false);
}

TEST_F(HashTableTest, QuadraticGrowTableTest) {
  GrowTableTest(true);
}

TEST_F(HashTableTest, LinearInsertFullTest) {
  InsertFullTest(false, 1);
  InsertFullTest(false, 4);
  InsertFullTest(false, 64);
  InsertFullTest(false, 1024);
  InsertFullTest(false, 65536);
}

TEST_F(HashTableTest, QuadraticInsertFullTest) {
  InsertFullTest(true, 1);
  InsertFullTest(true, 4);
  InsertFullTest(true, 64);
  InsertFullTest(true, 1024);
  InsertFullTest(true, 65536);
}

// Test that hashing empty string updates hash value.
TEST_F(HashTableTest, HashEmpty) {
  HashTableCtx ht_ctx(build_expr_ctxs_, probe_expr_ctxs_, false, false, 1, 2, 1);
  uint32_t seed = 9999;
  ht_ctx.set_level(0);
  EXPECT_NE(seed, ht_ctx.Hash(NULL, 0, seed));
  // TODO: level 0 uses CRC hash, which only swaps bytes around on empty input.
  // EXPECT_NE(seed, ht_ctx.Hash(NULL, 0, ht_ctx.Hash(NULL, 0, seed)));
  ht_ctx.set_level(1);
  EXPECT_NE(seed, ht_ctx.Hash(NULL, 0, seed));
  EXPECT_NE(seed, ht_ctx.Hash(NULL, 0, ht_ctx.Hash(NULL, 0, seed)));
}

}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  impala::CpuInfo::Init();
  return RUN_ALL_TESTS();
}
