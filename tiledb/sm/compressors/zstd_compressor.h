/**
 * @file   zstd_compressor.h
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
 * This file defines the zstd compressor class.
 */

#ifndef TILEDB_ZSTD_H
#define TILEDB_ZSTD_H

#include "tiledb/common/status.h"

#include "tiledb/sm/misc/resource_pool.h"

#include <zstd.h>

using namespace tiledb::common;

namespace tiledb {
namespace sm {

class Buffer;
class ConstBuffer;
class PreallocatedBuffer;

/** Handles compression/decompression with the zstd library. */
class ZStd {
 public:
  /** wrapper around the decompress ZSTD context so that it can be used in a
   * resource pool */
  class ZSTD_Decompress_Context {
   public:
    ZSTD_Decompress_Context()
        : ctx_(ZSTD_createDCtx(), ZSTD_freeDCtx) {
    }

    ZSTD_DCtx* ptr() {
      return ctx_.get();
    }

   private:
    std::unique_ptr<ZSTD_DCtx, decltype(&ZSTD_freeDCtx)> ctx_;
  };

  /**
   * Compression function.
   *
   * @param level Compression level.
   * @param input_buffer Input buffer to read from.
   * @param output_buffer Output buffer to write to the compressed data.
   * @return Status
   */
  static Status compress(
      int level, ConstBuffer* input_buffer, Buffer* output_buffer);

  /**
   * Overloaded compression function with default compression level.
   *
   * @param input_buffer Input buffer to read from.
   * @param output_buffer Output buffer to write to the compressed data.
   * @return Status
   */
  static Status compress(ConstBuffer* input_buffer, Buffer* output_buffer);

  /**
   * Decompression function.
   *
   * @param decompress_ctx_pool Resource pool to manage decompression context
   * reuse
   * @param input_buffer Input buffer to read from.
   * @param output_buffer Output buffer to write the decompressed data to.
   * @return Status
   */
  static Status decompress(
      std::shared_ptr<ResourcePool<ZStd::ZSTD_Decompress_Context>>
          decompress_ctx_pool,
      ConstBuffer* input_buffer,
      PreallocatedBuffer* output_buffer);

  /** Returns the default compression level. */
  static int default_level() {
    return 3;
  }

  /** Returns the compression overhead for the given input. */
  static uint64_t overhead(uint64_t nbytes);
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_ZSTD_H
