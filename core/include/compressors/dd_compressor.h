/**
 * @file   dd_compressor.h
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
 * This file defines the double delta compressor class.
 */

#ifndef TILEDB_DOUBLE_DELTA_H
#define TILEDB_DOUBLE_DELTA_H

#include "buffer.h"
#include "const_buffer.h"
#include "datatype.h"
#include "status.h"

namespace tiledb {

/** Implements a double delta compressor. */
class DoubleDelta {
 public:
  /**
   * Constant overhead (equal to 1 byte for the bitsize, 8 bytes for
   * the number of cells, and 8 bytes for a potential extra 64-bit chunk).
   */
  static const uint64_t OVERHEAD;

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
   * bitsize | n | in_0 | in_1 | b_2 | abs(dd_2) | b_3 | abs(dd_3) | ...
   *   ...   | b_n | abs(dd_n)
   *
   * where:
   *  - *bitsize* (char) is the minimum number of bits required to represent
   *    any abs(dd_i).
   *  - *n* (uint64_t) is the number of values in the input buffer.
   *  - **b_i** is the sign of dd_i
   *  - **dd_i** is equal to (in_{i} - in_{i-1}) - (in_{i-1} - in_{i-2})
   *
   *  In case the bitsize is equal to the size of the data type plus one (for
   *  the sign), then it does not make sense to compress and, thus, the
   *  algorithm simply copies the input to the output, though after *bitsize*
   *  and *n* that are always written to the output buffer.
   *
   *  The algorithm populates and writes to the output buffer a 64-bit chunk
   *  at a time. Therefore, the output buffer may end up having a worst-case
   *  overhead of 1 (bitsize) + 8 (n) + 8 (last, potentially almost empty chunk)
   *  bytes.
   *
   * @param type The type of the input values.
   * @param input_buffer Input buffer to read from.
   * @param output_buffer Output buffer to write to the compressed data.
   * @return Status
   *
   * @note The function will fail with an error in two cases: (i) the output
   *     buffer incurs a memory allocation error, and (ii) some double delta
   *     value is out of bounds. Note that all double deltas are represented
   *     as int64_t. Therefore, the out of bounds case occurs when adding
   *     two huge positive numbers (e.g., INT64_MAX) resulting in a negative
   *     value, or adding to very small negative numbers (e.g., -INT64_MAX),
   *     resulting in a positive number. However, both these two cases are
   *     extreme.
   */
  static Status compress(
      Datatype type, ConstBuffer* input_buffer, Buffer* output_buffer);

  /**
   * Decompression function.
   *
   * @param type The type of the original decompressed values.
   * @param input_buffer Input buffer to read from.
   * @param output_buffer Output buffer to write the decompressed data to.
   * @return Status
   */
  static Status decompress(
      Datatype type, ConstBuffer* input_buffer, Buffer* output_buffer);

  /** Returns the compression overhead for the given input. */
  static uint64_t overhead(uint64_t nbytes);

 private:
  /* ****************************** */
  /*         PRIVATE METHODS        */
  /* ****************************** */

  /** Templated version of *compress* on the type of buffer values. */
  template <class T>
  static Status compress(ConstBuffer* input_buffer, Buffer* output_buffer);

  /**
   * Calculates the bitsize all the double deltas will have. Note that
   * the sign bit is not counted.
   *
   * @tparam The datatype of the values.
   * @param in The input buffer.
   * @param num The number of values in the buffer.
   * @param bitsize The bitsize of the double deltas to be retrieved.
   * @return Status
   */
  template <class T>
  static Status compute_bitsize(T* in, uint64_t num, unsigned int* bitsize);

  /**
   * Decompression function.
   *
   * @tparam The datatype of the values.
   * @param input_buffer Input buffer to read from.
   * @param output_buffer Output buffer to write the decompressed data to.
   * @return Status
   */
  template <class T>
  static Status decompress(ConstBuffer* input_buffer, Buffer* output_buffer);

  /**
   * Reads/reconstructs a double delta value from a compressed buffer.
   *
   * @param buff The input buffer.
   * @param double_delta The double delta value to be retrieved.
   * @param bitsize The bitsize of the double delta compression.
   * @param chunk The function reads from the buffer in whole 64-bit chunks.
   *     This parameters represents some state about the current chunk.
   * @param bit_in_chunk Part of the chunk state. Stores the current bit
   *     in the chunk the next read will start from.
   * @return Status
   */
  static Status read_double_delta(
      ConstBuffer* buff,
      int64_t* double_delta,
      int bitsize,
      uint64_t* chunk,
      int* bit_in_chunk);

  /**
   * Writes a double delta value to a buffer, after reducing its bitsize
   * to match the input one.
   *
   * @param buff The output buffer.
   * @param double_delta The double delta value to write.
   * @param bitsize The bitsize the double delta value will reduce to
   *     before being written to the buffer.
   * @param chunk The function writes to the buffer in whole 64-bit chunks.
   *     This parameters represents some state about the current chunk.
   * @param bit_in_chunk Part of the chunk state. Stores the current bit
   *     in the chunk the next write will start from.
   * @return Status
   */
  static Status write_double_delta(
      Buffer* buff,
      int64_t double_delta,
      int bitsize,
      uint64_t* chunk,
      int* bit_in_chunk);
};

}  // namespace tiledb

#endif  // TILEDB_GZIP_H
