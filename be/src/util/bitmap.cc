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

#include "util/bitmap.h"

#include <sstream>

#include "common/names.h"

using namespace impala;

string Bitmap::DebugString(bool print_bits) {
  int64_t words = BitUtil::RoundUp(num_bits_, 64) / 64;
  stringstream ss;
  ss << "Size (" << num_bits_ << ") words (" << words << ") ";
  if (print_bits) {
    for (int i = 0; i < num_bits(); ++i) {
      if (Get<false>(i)) {
        ss << "1";
      } else {
        ss << "0";
      }
    }
  } else {
    for (vector<uint64_t>::iterator it = buffer_.begin(); it != buffer_.end(); ++it) {
      ss << *it << ".";
    }
  }
  ss << endl;
  return ss.str();
}
