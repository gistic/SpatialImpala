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

#include "util/codec.h"

#include <boost/assign/list_of.hpp>
#include <limits> // for std::numeric_limits
#include <gutil/strings/substitute.h>

#include "util/compress.h"
#include "util/decompress.h"

#include "gen-cpp/CatalogObjects_types.h"
#include "gen-cpp/CatalogObjects_constants.h"

#include "common/names.h"

using boost::assign::map_list_of;
using namespace impala;
using namespace strings;

const char* const Codec::DEFAULT_COMPRESSION =
    "org.apache.hadoop.io.compress.DefaultCodec";
const char* const Codec::GZIP_COMPRESSION = "org.apache.hadoop.io.compress.GzipCodec";
const char* const Codec::BZIP2_COMPRESSION = "org.apache.hadoop.io.compress.BZip2Codec";
const char* const Codec::SNAPPY_COMPRESSION = "org.apache.hadoop.io.compress.SnappyCodec";
const char* const Codec::UNKNOWN_CODEC_ERROR =
    "This compression codec is currently unsupported: ";
const char* const NO_LZO_MSG = "LZO codecs may not be created via the Codec interface. "
    "Instead the LZO library is directly invoked.";

const Codec::CodecMap Codec::CODEC_MAP = map_list_of
  ("", THdfsCompression::NONE)
  (DEFAULT_COMPRESSION, THdfsCompression::DEFAULT)
  (GZIP_COMPRESSION, THdfsCompression::GZIP)
  (BZIP2_COMPRESSION, THdfsCompression::BZIP2)
  (SNAPPY_COMPRESSION, THdfsCompression::SNAPPY_BLOCKED);

string Codec::GetCodecName(THdfsCompression::type type) {
  BOOST_FOREACH(const CodecMap::value_type& codec,
      g_CatalogObjects_constants.COMPRESSION_MAP) {
    if (codec.second == type) return codec.first;
  }
  DCHECK(false) << "Missing codec in COMPRESSION_MAP: " << type;
  return "INVALID";
}

Status Codec::GetHadoopCodecClassName(THdfsCompression::type type, string* out_name) {
  BOOST_FOREACH(const CodecMap::value_type& codec, CODEC_MAP) {
    if (codec.second == type) {
      out_name->assign(codec.first);
      return Status::OK();
    }
  }
  return Status(Substitute("Unsupported codec for given file type: $0",
      _THdfsCompression_VALUES_TO_NAMES.find(type)->second));
}

Status Codec::CreateCompressor(MemPool* mem_pool, bool reuse, const string& codec,
    scoped_ptr<Codec>* compressor) {
  CodecMap::const_iterator type = CODEC_MAP.find(codec);
  if (type == CODEC_MAP.end()) {
    return Status(Substitute("$0$1", UNKNOWN_CODEC_ERROR, codec));
  }

  RETURN_IF_ERROR(
      CreateCompressor(mem_pool, reuse, type->second, compressor));
  return Status::OK();
}

Status Codec::CreateCompressor(MemPool* mem_pool, bool reuse,
    THdfsCompression::type format, scoped_ptr<Codec>* compressor) {
  switch (format) {
    case THdfsCompression::NONE:
      compressor->reset(NULL);
      return Status::OK();
    case THdfsCompression::GZIP:
      compressor->reset(new GzipCompressor(GzipCompressor::GZIP, mem_pool, reuse));
      break;
    case THdfsCompression::DEFAULT:
      compressor->reset(new GzipCompressor(GzipCompressor::ZLIB, mem_pool, reuse));
      break;
    case THdfsCompression::DEFLATE:
      compressor->reset(new GzipCompressor(GzipCompressor::DEFLATE, mem_pool, reuse));
      break;
    case THdfsCompression::BZIP2:
      compressor->reset(new BzipCompressor(mem_pool, reuse));
      break;
    case THdfsCompression::SNAPPY_BLOCKED:
      compressor->reset(new SnappyBlockCompressor(mem_pool, reuse));
      break;
    case THdfsCompression::SNAPPY:
      compressor->reset(new SnappyCompressor(mem_pool, reuse));
      break;
    case THdfsCompression::LZ4:
      compressor->reset(new Lz4Compressor(mem_pool, reuse));
      break;
    default: {
      if (format == THdfsCompression::LZO) return Status(NO_LZO_MSG);
      return Status(Substitute("Unsupported codec: $0", format));
    }
  }

  return (*compressor)->Init();
}

Status Codec::CreateDecompressor(MemPool* mem_pool, bool reuse, const string& codec,
    scoped_ptr<Codec>* decompressor) {
  CodecMap::const_iterator type = CODEC_MAP.find(codec);
  if (type == CODEC_MAP.end()) {
    return Status(Substitute("$0$1", UNKNOWN_CODEC_ERROR, codec));
  }

  RETURN_IF_ERROR(
      CreateDecompressor(mem_pool, reuse, type->second, decompressor));
  return Status::OK();
}

Status Codec::CreateDecompressor(MemPool* mem_pool, bool reuse,
    THdfsCompression::type format, scoped_ptr<Codec>* decompressor) {
  switch (format) {
    case THdfsCompression::NONE:
      decompressor->reset(NULL);
      return Status::OK();
    case THdfsCompression::DEFAULT:
    case THdfsCompression::GZIP:
      decompressor->reset(new GzipDecompressor(mem_pool, reuse, false));
      break;
    case THdfsCompression::DEFLATE:
      decompressor->reset(new GzipDecompressor(mem_pool, reuse, true));
      break;
    case THdfsCompression::BZIP2:
      decompressor->reset(new BzipDecompressor(mem_pool, reuse));
      break;
    case THdfsCompression::SNAPPY_BLOCKED:
      decompressor->reset(new SnappyBlockDecompressor(mem_pool, reuse));
      break;
    case THdfsCompression::SNAPPY:
      decompressor->reset(new SnappyDecompressor(mem_pool, reuse));
      break;
    case THdfsCompression::LZ4:
      decompressor->reset(new Lz4Decompressor(mem_pool, reuse));
      break;
    default: {
      if (format == THdfsCompression::LZO) return Status(NO_LZO_MSG);
      return Substitute("Unsupported codec: $0", format);
    }
  }

  return (*decompressor)->Init();
}

Codec::Codec(MemPool* mem_pool, bool reuse_buffer)
  : memory_pool_(mem_pool),
    reuse_buffer_(reuse_buffer),
    out_buffer_(NULL),
    buffer_length_(0) {
  if (memory_pool_ != NULL) {
    temp_memory_pool_.reset(new MemPool(memory_pool_->mem_tracker()));
  }
}

void Codec::Close() {
  if (temp_memory_pool_.get() != NULL) {
    DCHECK(memory_pool_ != NULL);
    memory_pool_->AcquireData(temp_memory_pool_.get(), false);
  }
}

Status Codec::ProcessBlock32(bool output_preallocated, int input_length,
    const uint8_t* input, int* output_length, uint8_t** output) {
  int64_t input_len64 = input_length;
  int64_t output_len64 = *output_length;
  RETURN_IF_ERROR(ProcessBlock(output_preallocated, input_len64, input, &output_len64,
                               output));
  // Check whether we are going to have an overflow if we are going to cast from int64_t
  // to int.
  // TODO: Is there a faster way to do this check?
  if (UNLIKELY(output_len64 > numeric_limits<int>::max())) {
    return Status(Substitute("Arithmetic overflow in codec function. Output length is $0",
        output_len64));;
  }
  *output_length = static_cast<int32_t>(output_len64);
  return Status::OK();
}
