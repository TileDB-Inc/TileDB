/**
 * @file   blosc_compressor.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2018 TileDB, Inc.
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
#include <limits>

#include "tiledb/sm/compressors/blosc_compressor.h"
#include "tiledb/sm/misc/logger.h"
#include "tiledb/sm/misc/stats.h"

namespace tiledb {
namespace sm {

Status Blosc::compress(
    const char* compressor,
    uint64_t type_size,
    int level,
    ConstBuffer* input_buffer,
    Buffer* output_buffer) {
  STATS_FUNC_IN(compressor_blosc_compress);

  // Sanity check
  if (input_buffer->data() == nullptr || output_buffer->data() == nullptr)
    return LOG_STATUS(Status::CompressionError(
        "Failed compressing with Blosc; invalid buffer format"));

  // Compress
  int rc = blosc_compress_ctx(
      level < 0 ? Blosc::default_level() : level,
      1,  // shuffle
      type_size,
      input_buffer->size(),
      input_buffer->data(),
      output_buffer->cur_data(),
      output_buffer->free_space(),
      compressor,
      0,  // blocksize (0 lets BLOSC choose automatically)
      1   // disable BLOSC thread pool
  );

  // Handle error
  if (rc < 0)
    return LOG_STATUS(Status::CompressionError("Blosc compression error"));

  // Set size of compressed data
  output_buffer->advance_size(uint64_t(rc));
  output_buffer->advance_offset(uint64_t(rc));

  return Status::Ok();

  STATS_FUNC_OUT(compressor_blosc_compress);
}

Status Blosc::decompress(
    ConstBuffer* input_buffer, PreallocatedBuffer* output_buffer) {
  STATS_FUNC_IN(compressor_blosc_decompress);

  // Sanity check
  if (input_buffer->data() == nullptr || output_buffer->data() == nullptr)
    return LOG_STATUS(Status::CompressionError(
        "Failed decompressing with Blosc; invalid buffer format"));

  // Decompress
  int rc = blosc_decompress_ctx(
      input_buffer->data(),
      output_buffer->cur_data(),
      output_buffer->free_space(),
      1  // disable BLOSC thread pool
  );

  // Handle error
  if (rc <= 0)
    return LOG_STATUS(Status::CompressionError("Blosc decompress error"));

  // Set size of decompressed data
  output_buffer->advance_offset(uint64_t(rc));

  return Status::Ok();

  STATS_FUNC_OUT(compressor_blosc_decompress);
}

uint64_t Blosc::overhead(uint64_t nbytes) {
  // Blosc has a fixed overhead
  (void)nbytes;
  return BLOSC_MAX_OVERHEAD;
}

}  // namespace sm
}  // namespace tiledb
