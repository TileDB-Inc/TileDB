/**
 * @file   bzip_compressor.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2021 TileDB, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 *
 * @section DESCRIPTION
 *
 * This file implements the bzip compressor class.
 */

#include "tiledb/sm/compressors/bzip_compressor.h"
#include "tiledb/common/logger.h"
#include "tiledb/sm/buffer/buffer.h"

#include <bzlib.h>
#include <cmath>

using namespace tiledb::common;

namespace tiledb {
namespace sm {

class BZipException : public StatusException {
 public:
  explicit BZipException(const std::string& message)
      : StatusException("BZipException", message) {
  }
};

void BZip::compress(
    int level, ConstBuffer* input_buffer, Buffer* output_buffer) {
  // Sanity check
  if (input_buffer->data() == nullptr || output_buffer->data() == nullptr)
    throw BZipException("Failed compressing with BZip; invalid buffer format");

  // Compress
  auto in_size = (unsigned int)input_buffer->size();
  auto out_size = (unsigned int)output_buffer->free_space();
  int rc = BZ2_bzBuffToBuffCompress(
      static_cast<char*>(output_buffer->cur_data()),
      &out_size,
      (char*)input_buffer->data(),
      in_size,
      level < level_limit_ ? BZip::default_level() : level,  // block size 100k
      0,                                                     // verbosity
      0);                                                    // work factor

  // Handle error
  if (rc != BZ_OK) {
    switch (rc) {
      case BZ_CONFIG_ERROR:
        throw BZipException(
            "BZip compression error: library has been miscompiled");
      case BZ_PARAM_ERROR:
        throw BZipException(
            "BZip compression error: 'output_buffer' or 'output_buffer_size' "
            "is NULL");
      case BZ_MEM_ERROR:
        throw BZipException("BZip compression error: insufficient memory");
      case BZ_OUTBUFF_FULL:
        throw BZipException(
            "BZip compression error: compressed size exceeds limits for "
            "'output_buffer_size'");
      default:
        throw BZipException("BZip compression error: unknown error code");
    }
  }

  // Set size of compressed data
  output_buffer->advance_size(out_size);
  output_buffer->advance_offset(out_size);
}

void BZip::compress(ConstBuffer* input_buffer, Buffer* output_buffer) {
  return BZip::compress(BZip::default_level(), input_buffer, output_buffer);
}

void BZip::decompress(
    ConstBuffer* input_buffer, PreallocatedBuffer* output_buffer) {
  // Sanity check
  if (input_buffer->data() == nullptr || output_buffer->data() == nullptr)
    throw BZipException(
        "Failed decompressing with BZip; invalid buffer format");

  // Decompress
  auto out_size = (unsigned int)output_buffer->free_space();
  int rc = BZ2_bzBuffToBuffDecompress(
      static_cast<char*>(output_buffer->cur_data()),
      &out_size,
      (char*)input_buffer->data(),
      (unsigned int)input_buffer->size(),
      0,   // small bzip data format stream
      0);  // verbositiy

  // Handle error
  if (rc != BZ_OK) {
    switch (rc) {
      case BZ_CONFIG_ERROR:
        throw BZipException(
            "BZip decompression error: library has been miscompiled");
      case BZ_PARAM_ERROR:
        throw BZipException(
            "BZip decompression error: 'output_buffer' or 'output_buffer_size' "
            "is NULL");
      case BZ_MEM_ERROR:
        throw BZipException("BZip decompression error: insufficient memory");
      case BZ_DATA_ERROR:
      case BZ_DATA_ERROR_MAGIC:
      case BZ_UNEXPECTED_EOF:
        throw BZipException(
            "BZip decompression error: compressed data is corrupted");
      default:
        throw BZipException("BZip decompression error: unknown error code ");
    }
  }

  // Set size of compressed data
  output_buffer->advance_offset(out_size);
}

uint64_t BZip::overhead(uint64_t nbytes) {
  // From the BZip2 documentation:
  // To guarantee that the compressed data will fit in its buffer, allocate an
  // output buffer of size 1% larger than the uncompressed data, plus six
  // hundred extra bytes.
  return uint64_t(std::ceil(nbytes * 0.01) + 600);
}

}  // namespace sm
}  // namespace tiledb
