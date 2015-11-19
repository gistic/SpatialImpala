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


#ifndef IMPALA_RUNTIME_TUPLE_ROW_H
#define IMPALA_RUNTIME_TUPLE_ROW_H

#include "runtime/descriptors.h"
#include "runtime/mem-pool.h"
#include "runtime/row-batch.h"
#include "runtime/tuple.h"

namespace impala {

/// A TupleRow encapsulates a contiguous sequence of Tuple pointers which
/// together make up a row.
class TupleRow {
 public:
  Tuple* GetTuple(int tuple_idx) {
    return tuples_[tuple_idx];
  }

  void SetTuple(int tuple_idx, Tuple* tuple) {
    tuples_[tuple_idx] = tuple;
  }

  /// Create a deep copy of this TupleRow.  DeepCopy will allocate from  the pool.
  TupleRow* DeepCopy(const std::vector<TupleDescriptor*>& descs, MemPool* pool) {
    int size = descs.size() * sizeof(Tuple*);
    TupleRow* result = reinterpret_cast<TupleRow*>(pool->Allocate(size));
    DeepCopy(result, descs, pool, false);
    return result;
  }

  /// Create a deep copy of this TupleRow into 'dst'.  DeepCopy will allocate from
  /// the MemPool and copy the tuple pointers, the tuples and the string data in the
  /// tuples.
  /// If reuse_tuple_mem is true, it is assumed the dst TupleRow has already allocated
  /// tuple memory and that memory will be reused.  Otherwise, new tuples will be allocated
  /// and stored in 'dst'.
  void DeepCopy(TupleRow* dst, const std::vector<TupleDescriptor*>& descs, MemPool* pool,
      bool reuse_tuple_mem) {
    for (int i = 0; i < descs.size(); ++i) {
      if (this->GetTuple(i) != NULL) {
        if (reuse_tuple_mem && dst->GetTuple(i) != NULL) {
          this->GetTuple(i)->DeepCopy(dst->GetTuple(i), *descs[i], pool);
        } else {
          dst->SetTuple(i, this->GetTuple(i)->DeepCopy(*descs[i], pool));
        }
      } else {
        // TODO: this is wasteful.  If we have 'reuse_tuple_mem', we should be able to
        // save the tuple buffer and reuse it (i.e. freelist).
        dst->SetTuple(i, NULL);
      }
    }
  }

  inline TupleRow* next_row(RowBatch* batch) const {
    uint8_t* mem = const_cast<uint8_t*>(reinterpret_cast<const uint8_t*>(this));
    return reinterpret_cast<TupleRow*>(mem + batch->row_byte_size());
  }

  /// TODO: make a macro for doing thisf
  /// For C++/IR interop, we need to be able to look up types by name.
  static const char* LLVM_CLASS_NAME;

 private:
  Tuple* tuples_[1];
};

}

#endif
