/**
 * @file   dd_compressor.h
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
 * This file defines the double delta compressor class.
 */

#ifndef TILEDB_DOUBLE_DELTA_H
#define TILEDB_DOUBLE_DELTA_H

#include "tiledb/common/common.h"

#include <type_traits>

using namespace tiledb::common;

namespace tiledb {
namespace sm {

class Buffer;
class ConstBuffer;
class PreallocatedBuffer;
enum class Datatype : uint8_t;

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

  /** Returns the compression overhead for the given input. */
  static uint64_t overhead(uint64_t nbytes);

 private:
  /* ****************************** */
  /*         PRIVATE METHODS        */
  /* ****************************** */

  /** Templated version of *compress* on the type of buffer values. */
  template <class T>
  static void compress(ConstBuffer* input_buffer, Buffer* output_buffer);

  /**
   * Calculates the bitsize all the double deltas will have. Note that
   * the sign bit is not counted.
   *
   * @tparam The datatype of the values.
   * @param in The input buffer.
   * @param num The number of values in the buffer.
   * @param bitsize The bitsize of the double deltas to be retrieved.
   */
  template <class T>
  static void compute_bitsize(T* in, uint64_t num, unsigned int* bitsize);

  /**
   * Decompression function.
   *
   * @tparam The datatype of the values.
   * @param input_buffer Input buffer to read from.
   * @param output_buffer Output buffer to write the decompressed data to.
   */
  template <class T>
  static void decompress(
      ConstBuffer* input_buffer, PreallocatedBuffer* output_buffer);

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
   */
  static void read_double_delta(
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
   */
  static void write_double_delta(
      Buffer* buff,
      int64_t double_delta,
      int bitsize,
      uint64_t* chunk,
      int* bit_in_chunk);
};

namespace double_delta {

template <typename T>
struct Integral64;

template <typename T>
  requires std::is_unsigned_v<T> || std::is_same_v<T, std::byte>
struct Integral64<T> {
  using type = uint64_t;

  /**
   * @return `a - b` if it can be represented as an `int64_t` without undefined
   * behavior, `std::nullopt` otherwise
   */
  static std::optional<int64_t> delta(uint64_t a, uint64_t b) {
    if (a >= b) {
      // since `b >= 0` due to unsigned-ness this will not underflow
      // but we do have to check if it can be represented as `int64_t`
      if (a - b > static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
        return std::nullopt;
      } else {
        return a - b;
      }
    }

    // NB: a traditional and "obvious" way of detecting overflow
    // is using the signed-ness of the subtraction result compared
    // against the inputs.  This is actually undefined behavior
    // and could also possibly be optimized out by a compiler
    // which would make us sad.
    //
    // We will instead rely on `a - b == -(b - a)` which decays to the
    // above case, and we can check for the only possible
    // result which we don't have to negate.
    const auto negative_delta = delta(b, a);
    if (!negative_delta.has_value()) {
      // edge case: the delta is exactly the non-negatable twos complement value
      if (b - a ==
          static_cast<uint64_t>(std::numeric_limits<int64_t>::max()) + 1) {
        return std::numeric_limits<int64_t>::min();
      } else {
        return std::nullopt;
      }
    } else {
      return -negative_delta.value();
    }
  }
};

template <typename T>
  requires std::is_signed_v<T>
struct Integral64<T> {
  using type = int64_t;

  /**
   * @return `a - b` if it can be represented as an `int64_t` without
   * undefined behavior, `std::nullopt` otherwise
   */
  static std::optional<int64_t> delta(int64_t a, int64_t b) {
    if (a < b) {
      // negate so we can apply logic with generality
      const auto negated = delta(b, a);
      if (!negated.has_value()) {
        if (a - b == std::numeric_limits<int64_t>::min()) {
          return std::numeric_limits<int64_t>::min();
        } else {
          return std::nullopt;
        }
      } else if (negated.value() == std::numeric_limits<int64_t>::min()) {
        return std::nullopt;
      } else {
        return -negated.value();
      }
    } else {
      // overflow condition `a - b > i64::MAX`
      // is equivalent to `a > i64::MAX + b`
      if (b >= 0) {
        return a - b;
      } else if (a > std::numeric_limits<int64_t>::max() + b) {
        return std::nullopt;
      } else {
        return a - b;
      }
    }
  }
};

}  // namespace double_delta

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_GZIP_H
