/**
 * @file   zstd_compressor.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2020 TileDB, Inc.
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
 * This file implements the zstd compressor class.
 */

#include "tiledb/sm/compressors/zstd_compressor.h"
#include "tiledb/common/logger.h"
#include "tiledb/sm/buffer/buffer.h"
#include "tiledb/sm/buffer/const_buffer.h"
#include "tiledb/sm/buffer/preallocated_buffer.h"

#include <zstd.h>
#include <iostream>

using namespace tiledb::common;

namespace tiledb {
namespace sm {

Status ZStd::compress(
    int level, ConstBuffer* input_buffer, Buffer* output_buffer) {
  // Sanity check
  if (input_buffer->data() == nullptr || output_buffer->data() == nullptr)
    return LOG_STATUS(Status::CompressionError(
        "Failed compressing with ZStd; invalid buffer format"));

  // Create context
  std::unique_ptr<ZSTD_CCtx, decltype(&ZSTD_freeCCtx)> ctx(
      ZSTD_createCCtx(), ZSTD_freeCCtx);
  if (ctx.get() == nullptr)
    return LOG_STATUS(Status::CompressionError(
        std::string("ZStd compression failed; could not allocate context.")));

  // Compress
  uint64_t zstd_ret = ZSTD_compressCCtx(
      ctx.get(),
      output_buffer->cur_data(),
      output_buffer->free_space(),
      input_buffer->data(),
      input_buffer->size(),
      level < 0 ? ZStd::default_level() : level);

  // Handle error
  if (ZSTD_isError(zstd_ret) != 0) {
    const char* msg = ZSTD_getErrorName(zstd_ret);
    return LOG_STATUS(Status::CompressionError(
        std::string("ZStd compression failed: ") + msg));
  }

  // Set size of compressed data
  output_buffer->advance_size(zstd_ret);
  output_buffer->advance_offset(zstd_ret);

  return Status::Ok();
}

Status ZStd::decompress(
    ConstBuffer* input_buffer, PreallocatedBuffer* output_buffer) {
  // Sanity check
  if (input_buffer->data() == nullptr || output_buffer->data() == nullptr)
    return LOG_STATUS(Status::CompressionError(
        "Failed decompressing with ZStd; invalid buffer format"));

  // Create context
  std::unique_ptr<ZSTD_DCtx, decltype(&ZSTD_freeDCtx)> ctx(
      ZSTD_createDCtx(), ZSTD_freeDCtx);
  if (ctx.get() == nullptr)
    return LOG_STATUS(Status::CompressionError(
        std::string("ZStd decompression failed; could not allocate context.")));

  // Decompress
  uint64_t zstd_ret = ZSTD_decompressDCtx(
      ctx.get(),
      output_buffer->cur_data(),
      output_buffer->free_space(),
      input_buffer->data(),
      input_buffer->size());

  // Check error
  if (ZSTD_isError(zstd_ret) != 0) {
    const char* msg = ZSTD_getErrorName(zstd_ret);
    return LOG_STATUS(Status::CompressionError(
        std::string("ZStd decompression failed: ") + msg));
  }

  // Set size decompressed data
  output_buffer->advance_offset(zstd_ret);

  return Status::Ok();
}

uint64_t ZStd::overhead(uint64_t nbytes) {
  return ZSTD_compressBound(nbytes) - nbytes;
}

}  // namespace sm
}  // namespace tiledb
