/**
 * @file   rle_compressor.h
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
 * This file defines the rle compressor class.
 */

#ifndef TILEDB_RLE_COMPRESSOR_H
#define TILEDB_RLE_COMPRESSOR_H

#include "tiledb/common/status.h"

#include "span.hpp"

#include <vector>

using namespace tiledb::common;

namespace tiledb {
namespace sm {

class Buffer;
class ConstBuffer;
class PreallocatedBuffer;

/** Handles compression/decompression Run-Length-Encoding. */
class RLE {
 public:
  /**
   * Compression function.
   *
   * @param value_size The size of a single value.
   * @param input_buffer Input buffer to read from.
   * @param output_buffer Output buffer to write the compressed data to.
   * @return Status
   */
  static Status compress(
      uint64_t value_size, ConstBuffer* input_buffer, Buffer* output_buffer);

  /**
   * Decompression function.
   *
   * @param value_size The size of a single.
   * @param input_buffer Input buffer to read from.
   * @param output_buffer Output buffer to write to the decompressed data.
   * @return Status
   */
  static Status decompress(
      uint64_t value_size,
      ConstBuffer* input_buffer,
      PreallocatedBuffer* output_buffer);

  /** Returns the compression overhead for the given input. */
  static uint64_t overhead(uint64_t nbytes, uint64_t value_size);

  /**
   * Override max_run_length_. Used in tests only.
   *
   * @param max_run_length The maximum length of a run in strings RLE
   */
  static void set_max_run_length(const uint64_t max_tile_chunk_size);

  /**
   * Collection of strings compression function.
   *
   * @param input_buffer Input in form of a contiguous sequence of buffers to
   * compress
   * @return RLE encoded output as a vector of strings
   */
  static std::vector<std::string> compress(
      nonstd::span<std::string_view> input);

  /**
   * Collection of strings decompression function.
   *
   * @param input_buffer Input in form of a contiguous sequence of buffers in
   * RLE format to decompress
   * @return RLE decoded output as a vector of strings
   */
  static std::vector<std::string> decompress(
      nonstd::span<std::string_view> input);

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /* The maximum length of a run in strings RLE */
  static inline uint64_t max_run_length_ = UINT64_MAX;
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_RLE_COMPRESSOR_H
