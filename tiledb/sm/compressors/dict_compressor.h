/**
 * @file   dict_compressor.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022 TileDB, Inc.
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

#include "tiledb/common/assert.h"
#include "tiledb/common/common.h"
#include "tiledb/sm/misc/endian.h"

#include <map>
#include <unordered_map>
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
   * @param input Input in form of a memory contiguous sequence of
   * strings
   * @param word_id_size Bytesize to use to store word ids
   * @param output Dictionary-encoded output in ids. Memory is allocated and
   * owned by the caller
   * @return A dictionary in the form of a vector of string_views, where indices
   * are the word_ids of each word. Those string_views point to strings in
   * input, so input memory should not be freed before dictionary data is
   * consumed
   */
  static std::vector<std::string_view> compress(
      const span<std::string_view> input,
      const uint8_t word_id_size,
      span<std::byte> output);

  /**
   * Decompress strings that are encoded in dictionary format
   *
   * @param input Input dictionary-encoded format of ids of type T
   * @param dict The dictionary to be used for decoding a word id into a string.
   * Expected type is a vector of strings, where indices are the ids of
   * each word
   * @param word_id_size Bytesize to use to read word ids
   * @param output Dictionary-encoded output as a series of ids of size T.
   * Memory is allocated and owned by the caller
   * @param output_offsets Output offsets reconstructed from decoding the
   * Dictionary compressed input. Memory is allocated and owned by the caller
   */
  static void decompress(
      const span<const std::byte> input,
      const span<const std::string> dict,
      const uint8_t word_id_size,
      span<std::byte> output,
      span<uint64_t> output_offsets);

  /**
   * Serialize string-id dictionary to store it in memory
   *
   * @param dict The dictionary to be used for decoding a word id into a string.
   * Expected type is a vector of strings, where indices are the ids of
   * each word
   * @param strlen_bytesize Bytesize to use to serialize the lenghts of strings
   * @param dict_size Estimated size of the serialized dict
   * @return The serialized dictionary in format
   * [size_str1|str1|...|size_strN|strN]
   */
  static std::vector<std::byte> serialize_dictionary(
      const span<std::string_view> dict,
      const size_t strlen_bytesize,
      const size_t dict_size);

  /**
   * Deserialize string-id dictionary to restore it from memory
   *
   * @param serialized_dict The dictionary to restore in serialized format
   * @param strlen_bytesize Bytesize to use to serialize the lenghts of strings
   * @return The dictionary in the original format of vector of strings, where
   * indices are the ids of each word
   */
  static std::vector<std::string> deserialize_dictionary(
      const span<std::byte> serialized_dict, size_t strlen_bytesize);

  /**
   * Compress variable sized strings into dictionary encoded format
   *
   * @tparam T Type of integer that can fit the word ids
   * @param input Input in form of a memory contiguous sequence of
   * strings
   * @param output Dictionary-encoded output in ids of type T. Memory is
   * allocated and owned by the caller
   * @return A dictionary in the form of a vector of string_views, where indices
   * are the word_ids of each word. Those string_views point to strings in
   * input, so input memory should not be freed before dictionary data is
   * consumed
   */
  template <class T>
  static std::vector<std::string_view> compress(
      const span<std::string_view> input, span<std::byte> output) {
    if (input.empty() || output.empty()) {
      throw std::logic_error(
          "Empty arguments when compressing strings with dictionary encoding.");
    }

    if (output.size() < input.size() * sizeof(T)) {
      throw std::logic_error(
          "Output buffer too small to fit the compressed input.");
    }

    // We use string_views in the dictionary, which point to input strings, so
    // input should not be freed before dictionary data is consumed
    std::vector<std::string_view> dict;
    dict.reserve(input.size());
    T word_id = 0;
    const auto word_id_size = sizeof(T);
    // hash table to store string - unique id associations
    std::unordered_map<std::string_view, T> word_ids;
    auto out_index = 0;

    for (uint64_t i = 0; i < input.size(); i++) {
      // If we haven't seen that string before, add it to the dictionary
      if (word_ids.find(input[i]) == word_ids.end()) {
        dict.emplace_back(input[i]);
        word_ids[input[i]] = word_id++;
      }

      utils::endianness::encode_be<T>(word_ids[input[i]], &output[out_index]);
      out_index += word_id_size;
    }

    return dict;
  }

  /**
   * Decompress strings that are encoded in dictionary format
   *
   * @tparam T Type of integer that can fit the word ids
   * @param input Input dictionary-encoded format of ids of type T
   * @param dict The dictionary to be used for decoding a word id into a string.
   * Expected type is a vector of strings, where indices are the ids of
   * each word
   * @param output Dictionary-encoded output as a series of ids of size T.
   * Memory is allocated and owned by the caller
   * @param output_offsets Output offsets reconstructed from decoding the
   * Dictionary compressed input. Memory is allocated and owned by the caller
   */
  template <class T>
  static void decompress(
      const span<const std::byte> input,
      const span<const std::string> dict,
      span<std::byte> output,
      span<uint64_t> output_offsets) {
    if (input.empty() || dict.size() == 0) {
      throw std::logic_error(
          "Empty arguments when decompressing dictionary encoded strings.");
    }

    T word_id = 0;
    size_t in_index = 0, out_index = 0, offset_index = 0;

    while (in_index < input.size()) {
      word_id = utils::endianness::decode_be<T>(&input[in_index]);
      in_index += sizeof(T);
      iassert(
          word_id < dict.size(),
          "word_id = {}, dict.size() = {}",
          word_id,
          dict.size());
      const auto& word = dict[word_id];
      if (word.size() > 0) {
        memcpy(&output[out_index], word.data(), word.size());
      }
      output_offsets[offset_index++] = out_index;
      out_index += word.size();
    }
  }

  template <class T>
  static std::vector<std::byte> serialize_dictionary(
      const span<std::string_view> dict, const size_t dict_size) {
    if (dict.empty() || dict_size == 0) {
      throw std::logic_error(
          "Empty arguments when serializing dictionary for dictionary "
          "encoding.");
    }

    std::vector<std::byte> serialized_dict(dict_size);
    size_t out_index = 0;
    for (const auto& dict_entry : dict) {
      utils::endianness::encode_be<T>(
          static_cast<T>(dict_entry.size()), &serialized_dict[out_index]);
      out_index += sizeof(T);
      memcpy(&serialized_dict[out_index], dict_entry.data(), dict_entry.size());
      out_index += dict_entry.size();
    }

    serialized_dict.resize(out_index);

    return serialized_dict;
  }

  template <class T>
  static std::vector<std::string> deserialize_dictionary(
      const span<std::byte> serialized_dict) {
    if (serialized_dict.empty()) {
      throw std::logic_error(
          "Empty arguments when deserializing dictionary for dictionary "
          "decoding.");
    }

    std::vector<std::string> dict;
    dict.reserve(serialized_dict.size());
    // T is uint{8,16,32,64} specified at the call-site
    T str_len = 0;

    size_t in_index = 0;
    while (in_index < serialized_dict.size()) {
      str_len = utils::endianness::decode_be<T>(&serialized_dict[in_index]);
      // increment past the size element to the per-word data block
      in_index += sizeof(T);
      // construct string in place
      if (str_len > 0) {
        dict.emplace_back(
            reinterpret_cast<const char*>(&serialized_dict[in_index]), str_len);
      } else {
        dict.emplace_back();
      }
      in_index += str_len;
    }

    return dict;
  }
};
}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_DICT_COMPRESSOR_H
