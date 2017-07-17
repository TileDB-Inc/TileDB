/**
 * @file   blosc_compressor.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017 TileDB, Inc.
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
 * This file implements the blosc compressor class.
 */

#include <blosc.h>
#include <cassert>
#include <limits>

#include "blosc_compressor.h"

namespace tiledb {

size_t Blosc::compress_bound(size_t nbytes) {
  // TODO: this may overflow
  return nbytes + BLOSC_MAX_OVERHEAD;
}

Status Blosc::compress(
    const char* compressor,
    size_t type_size,
    int level,
    void* input_buffer,
    size_t input_buffer_size,
    void* output_buffer,
    size_t output_buffer_size,
    size_t* compressed_size) {
  *compressed_size = 0;
  assert(input_buffer_size <= std::numeric_limits<int>::max());
  assert(output_buffer_size <= std::numeric_limits<int>::max());
  blosc_init();
  if (blosc_set_compressor(compressor) < 0) {
    return Status::CompressionError(
        std::string(
            "Blosc compression error, failed to set Blosc compressor ") +
        compressor);
  }
  int rc = blosc_compress(
      level < 0 ? Blosc::default_level() : level,
      1,  // shuffle
      type_size,
      input_buffer_size,
      input_buffer,
      output_buffer,
      output_buffer_size);
  blosc_destroy();
  if (rc < 0) {
    return Status::CompressionError("Blosc compression error");
  }
  *compressed_size = rc;
  return Status::Ok();
}

Status Blosc::decompress(
    size_t type_size,
    void* input_buffer,
    size_t input_buffer_size,
    void* output_buffer,
    size_t output_buffer_size,
    size_t* decompressed_size) {
  *decompressed_size = 0;
  assert(input_buffer_size <= std::numeric_limits<int>::max());
  assert(output_buffer_size <= std::numeric_limits<int>::max());
  // TODO: this should be performed once
  blosc_init();
  int rc = blosc_decompress(
      static_cast<char*>(input_buffer), output_buffer, output_buffer_size);
  blosc_destroy();
  if (rc <= 0) {
    return Status::CompressionError("Blosc decompress error");
  }
  *decompressed_size = static_cast<int>(rc);
  return Status::Ok();
}

};  // namespace tiledb