/**
 * @file   rle_compressor.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2022 TileDB, Inc.
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

#include "tiledb/common/common.h"
#include "tiledb/sm/misc/constants.h"
#include "tiledb/sm/misc/endian.h"

#include <limits>
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
   */
  static void compress(
      uint64_t value_size, ConstBuffer* input_buffer, Buffer* output_buffer);

  /**
   * Decompression function.
   *
   * @param value_size The size of a single.
   * @param input_buffer Input buffer to read from.
   * @param output_buffer Output buffer to write to the decompressed data.
   */
  static void decompress(
      uint64_t value_size,
      ConstBuffer* input_buffer,
      PreallocatedBuffer* output_buffer);

  /** Returns the compression overhead for the given input. */
  static uint64_t overhead(uint64_t nbytes, uint64_t value_size);

  /**
   * Compress variable sized strings in contiguous memory to RLE format
   *
   * @tparam T Type of integer to store run legths, must fit max num of
   * repetitions in input
   * @tparam P Type of integer to store string sizes, must fit max length input
   * string
   * @param input Input in form of a memory contiguous sequence of strings
   * @param output RLE-encoded output as a series of [run
   * length|string_size|string] items. Memory is allocated and owned by the
   * caller
   */
  template <class T, class P>
  static void compress(
      const span<std::string_view> input, span<std::byte> output) {
    if (input.empty() || output.empty())
      return;

    const uint64_t run_size = sizeof(T);
    const uint64_t str_len_size = sizeof(P);
    const uint64_t max_run_length = std::numeric_limits<T>::max();

    T run_length = 1;
    auto out_offset = &output[0];
    auto previous = input[0];
    for (decltype(input.size()) in_index = 1; in_index < input.size();
         in_index++) {
      if (input[in_index] == previous && run_length < max_run_length) {
        run_length++;
      } else {
        // End of a run, write [run length|string_size|string] to output
        utils::endianness::encode_be<T>(run_length, out_offset);
        out_offset += run_size;
        utils::endianness::encode_be<P>(
            static_cast<P>(previous.size()), out_offset);
        out_offset += str_len_size;
        memcpy(out_offset, previous.data(), previous.size());
        out_offset += previous.size();
        previous = input[in_index];
        run_length = 1;
      }
    }

    // encode the runs of the last string
    utils::endianness::encode_be<T>(run_length, out_offset);
    utils::endianness::encode_be<P>(
        static_cast<P>(previous.size()), out_offset + run_size);
    memcpy(
        out_offset + run_size + str_len_size, previous.data(), previous.size());
  }

  /**
   * Decompress strings encoded in RLE format
   *
   * @tparam T Type of integer to store run legths, must be the same used for
   * encoding
   * @tparam P Type of integer to store string sizes, must be the same used for
   * encoding
   * @param input Input in [run length|string_size|string] RLE format to
   * decompress
   * @param output Decoded output as a series of strings in contiguous memory.
   * Memory is allocated and owned by the caller
   * @param output_offsets Output offsets reconstructed from decoding the RLE
   * compressed input. Memory is allocated and owned by the caller
   */
  template <class T, class P>
  static void decompress(
      const span<const std::byte> input,
      span<std::byte> output,
      span<uint64_t> output_offsets) {
    if (input.empty() || output_offsets.empty())
      return;

    const uint64_t run_size = sizeof(T);
    const uint64_t str_len_size = sizeof(P);

    T run_length = 0;
    P string_length = 0;
    uint64_t out_offset = 0;
    size_t offset_index = 0;
    // Iterate input to read [run length|string size|string] items
    uint64_t in_index = 0;
    while (in_index < input.size()) {
      run_length = utils::endianness::decode_be<T>(&input[in_index]);
      in_index += run_size;
      string_length = utils::endianness::decode_be<P>(&input[in_index]);
      in_index += str_len_size;
      for (uint64_t j = 0; j < run_length; j++) {
        if (string_length > 0) {
          memcpy(&output[out_offset], &input[in_index], string_length);
        }
        output_offsets[offset_index++] = out_offset;
        out_offset += string_length;
      }
      in_index += string_length;
    }
  }

  /**
   * Return the number of bytes required to store an integer
   *
   * @param param_length Number to calculate the bytesize
   * @return Number of bytes required to store the input number
   */
  static uint8_t compute_bytesize(uint64_t param_length);

  /**
   * Calculate RLE parameters prior to compressing a series of var-sized strings
   *
   * @param input Input in form of a memory contiguous sequence of strings
   * @return {max_run_value, max_string_size, num_of_runs, output_strings_size}
   */
  static tuple<uint64_t, uint64_t, uint64_t, uint64_t>
  calculate_compression_params(const span<std::string_view> input);

  /**
   * Compress variable sized strings in contiguous memory to RLE format
   *
   * @param input Input in form of a memory contiguous sequence of strings
   * @param rle_len_size Bytesize to use to store run legths
   * @param string_len_size Bytesize to use to store string sizes
   * @param output RLE-encoded output as a series of [run
   * length|string_size|string] items. Memory is allocated and owned by the
   * caller
   */
  static void compress(
      const span<std::string_view> input,
      uint64_t rle_len_size,
      uint64_t string_len_size,
      span<std::byte> output);

  /**
   * Decompress strings encoded in RLE format
   *
   * @param input Input in [run length|string_size|string] RLE format to
   * decompress
   * @param rle_len_size Bytesize to use to read run legths
   * @param string_len_size Bytesize to use to read string sizes
   * @param output Decoded output as a series of strings in contiguous memory.
   * @param output_offsets Output offsets reconstructed from input
   * Memory is allocated and owned by the caller
   */
  static void decompress(
      const span<const std::byte> input,
      const uint8_t rle_len_size,
      const uint8_t string_len_size,
      span<std::byte> output,
      span<uint64_t> output_offsets);

  /**
   * Compress numbers in contiguous memory to RLE format
   *
   * @tparam T Type of integer in input
   * @param input Input in form of a memory contiguous sequence of numbers
   * @param output RLE-encoded as a series of [run length|value] items. Memory
   * is allocated and owned by the caller
   */
  template <class T>
  static void compress(const span<T> input, span<T> output) {
    if (input.empty() || output.empty())
      return;

    const T max_run_length = std::numeric_limits<T>::max();
    uint64_t run_length = 1;
    auto out_index = 0;
    auto previous = input[0];
    for (uint64_t in_index = 1; in_index < input.size(); in_index++) {
      if (input[in_index] == previous && run_length < max_run_length) {
        run_length++;
      } else {
        // End of a run, write [run length|value] to output
        output[out_index++] = run_length;
        output[out_index++] = previous;
        previous = input[in_index];
        run_length = 1;
      }
    }

    // encode the runs of the last value
    output[out_index++] = run_length;
    output[out_index] = previous;
  }

  /**
   * Decompress numbers in contiguous memory encoded in RLE format
   *
   * @tparam T Type of integer in input
   * @param input Input in [run length|value] RLE format to decompress
   * @param output Decoded output as a series of values in contiguous memory.
   * Memory is allocated and owned by the caller
   */
  template <class T>
  static void decompress(const span<T> input, span<T> output) {
    if (input.empty() || output.empty())
      return;

    uint64_t run_length = 0;
    uint64_t out_index = 0;
    // Iterate input to read [run length|value] items
    uint64_t in_index = 0;
    while (in_index < input.size()) {
      run_length = input[in_index++];
      auto value = input[in_index++];
      for (uint64_t j = 0; j < run_length; j++) {
        output[out_index++] = value;
      }
    }
  }
};
}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_RLE_COMPRESSOR_H
