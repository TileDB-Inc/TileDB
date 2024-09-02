/**
 * @file   delta_compressor.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2023 TileDB, Inc.
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
 * This file defines the delta compressor class.
 */

#ifndef TILEDB_DELTA_H
#define TILEDB_DELTA_H

#include "tiledb/common/common.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

class Buffer;
class ConstBuffer;
class PreallocatedBuffer;
enum class Datatype : uint8_t;

/** Implements a double delta compressor. */
class Delta {
 public:
  /**
   * Constant overhead (equal to 8 bytes for the number of cells).
   */
  static constexpr uint64_t OVERHEAD = sizeof(uint64_t);

  /* ****************************** */
  /*               API              */
  /* ****************************** */

  /**
   * Compression function. The algorithm works as follows:
   *
   * Let the input buffer contain the following values:
   *
   * in_0 | in_1 | in_2 |      ...      | in_n
   *
   * The output buffer will contain the following after compression:
   *
   * n | in_0 | in_1 - in_0 | in_2 - in_1 | ... | in_n - in_{n-1}
   *
   * where:
   *  - *n* (uint64_t) is the number of values in the input buffer.
   *
   * @param type The type of the input values.
   * @param input_buffer Input buffer to read from.
   * @param output_buffer Output buffer to write to the compressed data.
   *
   * @note The function will fail with an error if:
   *    Float or otherwise unsupported datatype is used.
   *    Failure to write / read from allocated buffers.
   */

  static void compress(
      Datatype type, ConstBuffer* input_buffer, Buffer* output_buffer);

  /**
   * Decompression function.
   *
   * @param type The type of the original decompressed values.
   * @param input_buffer Input buffer to read from.
   * @param output_buffer Output buffer to write the decompressed data to.
   */
  static void decompress(
      Datatype type,
      ConstBuffer* input_buffer,
      PreallocatedBuffer* output_buffer);

 private:
  /* ****************************** */
  /*         PRIVATE METHODS        */
  /* ****************************** */

  /** Templated version of *compress* on the type of buffer values. */
  template <class T>
  static void compress(ConstBuffer* input_buffer, Buffer* output_buffer);

  /**
   * Decompression function.
   *
   * @tparam T The datatype of the values.
   * @param input_buffer Input buffer to read from.
   * @param output_buffer Output buffer to write the decompressed data to.
   */
  template <class T>
  static void decompress(
      ConstBuffer* input_buffer, PreallocatedBuffer* output_buffer);
};
}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_GZIP_H
