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


#ifndef IMPALA_UTIL_COMPRESS_H
#define IMPALA_UTIL_COMPRESS_H

/// We need zlib.h here to declare stream_ below.
#include <zlib.h>

#include "util/codec.h"
#include "exec/hdfs-scanner.h"
#include "runtime/mem-pool.h"

namespace impala {

/// Different compression classes.  The classes all expose the same API and
/// abstracts the underlying calls to the compression libraries.
/// TODO: reconsider the abstracted API

class GzipCompressor : public Codec {
 public:
  /// Compression formats supported by the zlib library
  enum Format {
    ZLIB,
    DEFLATE,
    GZIP,
  };

  virtual ~GzipCompressor();
  virtual int64_t MaxOutputLen(int64_t input_len, const uint8_t* input = NULL);
  virtual Status ProcessBlock(bool output_preallocated, int64_t input_length,
      const uint8_t* input, int64_t* output_length, uint8_t** output);

  virtual std::string file_extension() const { return "gz"; }

 private:
  friend class Codec;
  GzipCompressor(Format format, MemPool* mem_pool = NULL, bool reuse_buffer = false);
  virtual Status Init();

  Format format_;

  /// Structure used to communicate with the library.
  z_stream stream_;

  /// These are magic numbers from zlib.h.  Not clear why they are not defined there.
  const static int WINDOW_BITS = 15;    // Maximum window size
  const static int GZIP_CODEC = 16;     // Output Gzip.

  /// Compresses 'input' into 'output'.  Output must be preallocated and
  /// at least big enough.
  /// *output_length should be called with the length of the output buffer and on return
  /// is the length of the output.
  Status Compress(int64_t input_length, const uint8_t* input,
      int64_t* output_length, uint8_t* output);
};

class BzipCompressor : public Codec {
 public:
  virtual ~BzipCompressor() { }
  virtual int64_t MaxOutputLen(int64_t input_len, const uint8_t* input = NULL);
  virtual Status ProcessBlock(bool output_preallocated, int64_t input_length,
      const uint8_t* input, int64_t* output_length, uint8_t** output);
  virtual std::string file_extension() const { return "bz2"; }

 private:
  friend class Codec;
  BzipCompressor(MemPool* mem_pool, bool reuse_buffer);
  virtual Status Init() { return Status::OK(); }
};

class SnappyBlockCompressor : public Codec {
 public:
  virtual ~SnappyBlockCompressor() { }
  virtual int64_t MaxOutputLen(int64_t input_len, const uint8_t* input = NULL);
  virtual Status ProcessBlock(bool output_preallocated, int64_t input_length,
      const uint8_t* input, int64_t* output_length, uint8_t** output);
  virtual std::string file_extension() const { return "snappy"; }

 private:
  friend class Codec;
  SnappyBlockCompressor(MemPool* mem_pool, bool reuse_buffer);
  virtual Status Init() { return Status::OK(); }
};

class SnappyCompressor : public Codec {
 public:
  virtual ~SnappyCompressor() { }
  virtual int64_t MaxOutputLen(int64_t input_len, const uint8_t* input = NULL);
  virtual Status ProcessBlock(bool output_preallocated, int64_t input_length,
      const uint8_t* input, int64_t* output_length, uint8_t** output);
  virtual std::string file_extension() const { return "snappy"; }

  /// Computes the crc checksum that snappy expects when used in a framing format.
  /// This checksum needs to come after the compressed data.
  /// http://code.google.com/p/snappy/source/browse/trunk/framing_format.txt
  static uint32_t ComputeChecksum(int64_t input_len, const uint8_t* input);

 private:
  friend class Codec;
  SnappyCompressor(MemPool* mem_pool = NULL, bool reuse_buffer = false);
  virtual Status Init() { return Status::OK(); }
};

/// Lz4 is a compression codec with similar compression ratios as snappy
/// but much faster decompression. This compressor is not able to compress
/// unless the output buffer is allocated and will cause an error if
/// asked to do so.
class Lz4Compressor : public Codec {
 public:
  virtual ~Lz4Compressor() { }
  virtual int64_t MaxOutputLen(int64_t input_len, const uint8_t* input = NULL);
  virtual Status ProcessBlock(bool output_preallocated, int64_t input_length,
      const uint8_t* input, int64_t* output_length, uint8_t** output);
  virtual std::string file_extension() const { return "lz4"; }

 private:
  friend class Codec;
  Lz4Compressor(MemPool* mem_pool = NULL, bool reuse_buffer = false);
  virtual Status Init() { return Status::OK(); }
};

}
#endif
