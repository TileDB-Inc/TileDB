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
#include "logger.h"

namespace tiledb {

uint64_t Blosc::compress_bound(uint64_t nbytes) {
  // TODO: this may overflow
  return nbytes + BLOSC_MAX_OVERHEAD;
}

Status Blosc::compress(
    const char* compressor,
    uint64_t type_size,
    int level,
    const Buffer* input_buffer,
    Buffer* output_buffer) {
  // Sanity check
  if (input_buffer->data() == nullptr || output_buffer->data() == nullptr)
    return LOG_STATUS(Status::CompressionError(
        "Failed compressing with Blosc; invalid buffer format"));

  // TODO: put something better than assertion here
  assert(input_buffer->offset() <= std::numeric_limits<int>::max());
  assert(output_buffer->size() <= std::numeric_limits<int>::max());

  // Initialize Blosc
  // TODO: this should be performed once
  blosc_init();
  if (blosc_set_compressor(compressor) < 0) {
    return Status::CompressionError(
        std::string(
            "Blosc compression error, failed to set Blosc compressor ") +
        compressor);
  }

  // Compress
  int rc = blosc_compress(
      level < 0 ? Blosc::default_level() : level,
      1,  // shuffle
      type_size,
      input_buffer->offset(),
      input_buffer->data(),
      output_buffer->data(),
      output_buffer->size());

  // Destroy Blosc
  blosc_destroy();

  // Handle error
  if (rc < 0)
    return Status::CompressionError("Blosc compression error");

  // Set size of compressed data
  output_buffer->set_offset(rc);

  return Status::Ok();
}

Status Blosc::decompress(const Buffer* input_buffer, Buffer* output_buffer) {
  // Sanity check
  if (input_buffer->data() == nullptr || output_buffer->data() == nullptr)
    return LOG_STATUS(Status::CompressionError(
        "Failed decompressing with Blosc; invalid buffer format"));

  // TODO: put something better than assertion here
  assert(input_buffer->size() <= std::numeric_limits<int>::max());
  assert(output_buffer->size() <= std::numeric_limits<int>::max());

  // Initialize Blosc
  // TODO: this should be performed once
  blosc_init();

  // Decompress
  int rc = blosc_decompress(
      static_cast<char*>(input_buffer->data()),
      output_buffer->data(),
      output_buffer->size());

  // Destroy Blosc
  blosc_destroy();

  // Handle error
  if (rc <= 0)
    return Status::CompressionError("Blosc decompress error");

  return Status::Ok();
}

};  // namespace tiledb