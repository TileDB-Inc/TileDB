/**
 * @file   dict_compressor.h
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
 * This file defines the dictionary compression class for strings.
 */

#ifndef TILEDB_DICT_COMPRESSOR_H
#define TILEDB_DICT_COMPRESSOR_H

#include "tiledb/common/common.h"
#include "tiledb/sm/misc/endian.h"

#include <map>
#include <vector>

using namespace tiledb::common;

namespace tiledb {
namespace sm {

/** Handles dictionary compression/decompression of variable sized strings. */
class DictEncoding {
 public:
  /**
   * Compress variable sized strings into dictionary encoded format
   *
   * @tparam T Type of integer that can fit the word ids
   * @param input_buffer Input in form of a memory contiguous sequence of
   * strings
   * @param Dictionary-encoded output in ids of type T. Memory is allocated and
   * owned by the caller
   * @return A dictionary in the form of a vector of strings, where indices are
   * the word_ids of each word
   */
  template <class T>
  static std::vector<std::string> compress(
      const span<std::string_view> input, span<T> output) {
    if (input.empty() || output.empty()) {
      return {};
    }

    std::vector<std::string> dict;
    dict.reserve(input.size());
    T word_id = 0;
    // TODO: I can't use unordered_map with string_view before C++20 (I think),
    // more details here:
    // https://stackoverflow.com/questions/34596768/stdunordered-mapfind-using-a-type-different-than-the-key-type/53530846#53530846
    std::map<std::string_view, T, std::less<>> word_ids;
    for (uint64_t i = 0; i < input.size(); i++) {
      // if we haven't seen that string before, add it to the dictionary
      if (word_ids.find(input[i]) == word_ids.end()) {
        dict.emplace_back(std::string(input[i]));
        word_ids[input[i]] = word_id++;
      }

      utils::endianness::encode_be<T>(word_ids[input[i]], &output[i]);
    }

    // TODO: copy happens here: improve? take it as input?
    return dict;
  }

  /**
   * Decompress strings that are encoded in dictionary format
   *
   * @tparam T Type of integer that can fit the word ids
   * @param input_buffer Input dictionary-encoded format of ids of type T
   * @param The dictionary to be used for decoding a word id into a string.
   * Expected type is a vector of strings, where indices are the ids of
   * each word
   * @param Dictionary-encoded output as a series of ids of size T. Memory is
   * allocated and owned by the caller
   */
  template <class T>
  static void decompress(
      const span<T> input,
      const std::vector<std::string>& dict,
      span<std::byte> output) {
    if (input.empty() || output.empty() || dict.size() == 0) {
      return;
    }

    T word_id = 0;
    uint64_t in_index = 0, out_index = 0;
    while (in_index < input.size()) {
      word_id = utils::endianness::decode_be<T>(&input[in_index++]);
      assert(word_id < dict.size());
      auto word = std::string_view(dict[word_id]);
      memcpy(&output[out_index], word.data(), word.size());
      out_index += word.size();
    }
  }
};
}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_DICT_COMPRESSOR_H