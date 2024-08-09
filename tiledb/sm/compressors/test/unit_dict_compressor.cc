/**
 * @file   unit_dict_compressor.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022 TileDB Inc.
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
 * Tests for the dictionary encoding of strings.
 */

#include <catch2/catch_test_macros.hpp>
#include <catch2/catch_template_test_macros.hpp>

#include "../dict_compressor.h"

#include <algorithm>
#include <cstring>
#include <iterator>
#include <sstream>

using namespace tiledb::common;
using namespace tiledb::sm;

std::string random_string(const uint64_t l) {
  static const char char_set[] =
      "0123456789"
      "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
      "abcdefghijklmnopqrstuvwxyz";
  std::string s;
  s.reserve(l);

  for (uint64_t i = 0; i < l; ++i) {
    s += char_set[rand() % (sizeof(char_set) - 1)];
  }

  return s;
}

TEST_CASE(
    "Compression-Dictionary: Test compression with repetitive words in input",
    "[compression][dict]") {
  std::string str1 = "HG543232";
  std::string str2 = "HG54";
  std::string str3 = "A";
  uint8_t num_of_strings = 8;
  std::string_view uncompressed[] = {
      str1, str1, str1, str2, str2, str3, str1, str2};

  std::vector<std::string_view> exp_dict{str1, str2, str3};

  // Allocate the compressed array - we know the size will be equal to input
  std::vector<std::byte> compressed(num_of_strings);
  auto dict =
      tiledb::sm::DictEncoding::compress<uint8_t>(uncompressed, compressed);
  CHECK(dict.size() == exp_dict.size());
  CHECK(dict == exp_dict);

  std::vector<uint8_t> exp_compressed{0, 0, 0, 1, 1, 2, 0, 1};
  CHECK(
      memcmp(
          exp_compressed.data(),
          reinterpret_cast<uint8_t*>(compressed.data()),
          exp_compressed.size()) == 0);

  // Decompress the previously compressed array
  const char* exp_decompressed =
      "HG543232"
      "HG543232"
      "HG543232"
      "HG54"
      "HG54"
      "A"
      "HG543232"
      "HG54";
  const auto exp_decomp_size = strlen(exp_decompressed);
  std::vector<std::byte> decompressed(exp_decomp_size);
  auto num_strings = 8;
  std::vector<uint64_t> decompressed_offsets(num_strings);
  std::vector<std::string> input_dict{str1, str2, str3};
  tiledb::sm::DictEncoding::decompress<uint8_t>(
      compressed, input_dict, decompressed, decompressed_offsets);

  // In decompressed array there are only chars, so compare using memcpy
  CHECK(
      memcmp(
          exp_decompressed,
          reinterpret_cast<const char*>(decompressed.data()),
          decompressed.size()) == 0);

  std::vector<uint64_t> expected_offsets{0, 8, 16, 24, 28, 32, 33, 41};
  for (uint32_t i = 0; i < expected_offsets.size(); i++) {
    CHECK(expected_offsets[i] == decompressed_offsets[i]);
  }
}

typedef tuple<uint16_t, uint32_t, uint64_t> FixedTypesUnderTest;
TEMPLATE_LIST_TEST_CASE(
    "Compression-Dictionary: Test compression of single string repeated many "
    "times",
    "[compression][dict]",
    FixedTypesUnderTest) {
  typedef TestType T;
  // pick values of at least 2 bytes
  uint64_t num_strings = std::numeric_limits<uint8_t>::max() + 1;
  uint64_t string_len = std::numeric_limits<uint8_t>::max() + 1;
  // a single string repeated
  std::srand(10);
  std::string string_rand = random_string(string_len);
  std::vector<std::string> uncompressed_v(num_strings, string_rand);
  std::vector<std::string_view> uncompressed;
  // the reference here is crucial, otherwise a temp string is created and
  // therefore string_view will outlive it
  for (const std::string& str : uncompressed_v) {
    uncompressed.emplace_back(str);
  }

  // Allocate the compressed array - we know the number of elements will be
  // equal to input
  std::vector<std::byte> compressed(num_strings * sizeof(T));
  std::string_view sv{string_rand};
  std::vector<std::string_view> exp_dict{sv};
  // Compress the input array
  auto dict = tiledb::sm::DictEncoding::compress<T>(uncompressed, compressed);
  CHECK(dict == exp_dict);

  std::vector<T> exp_compressed(num_strings, 0);
  auto data = reinterpret_cast<const char*>(compressed.data());
  for (uint32_t i = 0; i < exp_compressed.size(); i++) {
    CHECK(exp_compressed[i] == utils::endianness::decode_be<T>(data));
    data += sizeof(T);
  }

  // Decompress the previously compressed array
  std::ostringstream repeated2;
  std::fill_n(
      std::ostream_iterator<std::string>(repeated2), num_strings, string_rand);
  auto strout = repeated2.str();
  const char* exp_decompressed = strout.data();
  const auto exp_decomp_size = strout.size();
  std::vector<std::byte> decompressed(exp_decomp_size);
  std::vector<uint64_t> decompressed_offsets(num_strings);
  std::vector<std::string> input_dict{string_rand};
  tiledb::sm::DictEncoding::decompress<T>(
      compressed, input_dict, decompressed, decompressed_offsets);

  // In decompressed array there are only chars, so compare using memcpy
  CHECK(
      memcmp(
          exp_decompressed,
          reinterpret_cast<const char*>(decompressed.data()),
          decompressed.size()) == 0);

  std::vector<uint64_t> expected_offsets(num_strings);
  auto len = string_rand.size();
  auto start = -1 * len;
  std::generate(expected_offsets.begin(), expected_offsets.end(), [&] {
    return start += len;
  });
  for (uint32_t i = 0; i < expected_offsets.size(); i++) {
    CHECK(expected_offsets[i] == decompressed_offsets[i]);
  }
}

TEST_CASE(
    "Compression-Dictionary: Test compression of unique strings (worst case)",
    "[compression][dict]") {
  std::vector<std::string> uncompressed_v = {
      "HG543232", "ATG", "AT", "A", "TGC", "HG54", "HG5"};
  std::vector<std::string_view> uncompressed;
  // the reference here is crucial, otherwise a temp string is created and
  // therefore string_view will outlive it
  for (const std::string& str : uncompressed_v) {
    uncompressed.emplace_back(str);
  }

  // Allocate the compressed array - we know the size will be equal to input
  std::vector<std::byte> compressed(uncompressed.size());
  auto dict =
      tiledb::sm::DictEncoding::compress<uint8_t>(uncompressed, compressed);
  CHECK(dict == uncompressed);

  std::vector<uint8_t> exp_compressed{0, 1, 2, 3, 4, 5, 6};
  CHECK(
      memcmp(
          exp_compressed.data(),
          reinterpret_cast<uint8_t*>(compressed.data()),
          exp_compressed.size()) == 0);

  // Decompress the previously compressed array
  const char* exp_decompressed =
      "HG543232"
      "ATG"
      "AT"
      "A"
      "TGC"
      "HG54"
      "HG5";
  const auto exp_decomp_size = strlen(exp_decompressed);
  std::vector<std::byte> decompressed(exp_decomp_size);
  const auto num_strings = 7;
  std::vector<uint64_t> decompressed_offsets(num_strings);
  tiledb::sm::DictEncoding::decompress<uint8_t>(
      compressed, uncompressed_v, decompressed, decompressed_offsets);

  // In decompressed array there are only chars, so compare using memcpy
  CHECK(
      memcmp(
          exp_decompressed,
          reinterpret_cast<const char*>(decompressed.data()),
          decompressed.size()) == 0);

  std::vector<uint64_t> expected_offsets{0, 8, 11, 13, 14, 17, 21};
  for (uint32_t i = 0; i < expected_offsets.size(); i++) {
    CHECK(expected_offsets[i] == decompressed_offsets[i]);
  }
}

typedef tuple<uint8_t, uint16_t, uint32_t, uint64_t> AllTypesUnderTest;
TEMPLATE_LIST_TEST_CASE(
    "Compression-Dictionary: Test dictionary serialization",
    "[compression][dict][dict-serialization]",
    AllTypesUnderTest) {
  typedef TestType T;
  std::vector<std::string> dictionary_ref = {
      "HG543232", "ATG", "AT", "A", "TGC", "HG54", "HG5"};
  std::vector<std::string_view> dictionary;
  // the reference here is crucial, otherwise a temp string is created and
  // therefore string_view will outlive it
  for (const std::string& str : dictionary_ref) {
    dictionary.emplace_back(str);
  }

  // Serialization
  std::vector<uint8_t> exp_serialized_len{8, 3, 2, 1, 3, 4, 3};
  // dict_size: T bytes for each length + the length of all strings
  auto serialized_dict = tiledb::sm::DictEncoding::serialize_dictionary<T>(
      dictionary, exp_serialized_len.size() * sizeof(T) + 24);

  // Check results
  auto serialized_dict_data =
      reinterpret_cast<const char*>(serialized_dict.data());
  for (size_t i = 0; i < exp_serialized_len.size(); i++) {
    auto string_len = utils::endianness::decode_be<T>(serialized_dict_data);
    CHECK(exp_serialized_len[i] == string_len);
    serialized_dict_data += sizeof(T);
    CHECK(strncmp(dictionary[i].data(), serialized_dict_data, string_len) == 0);
    serialized_dict_data += string_len;
  }

  // Deserialization
  auto dict =
      tiledb::sm::DictEncoding::deserialize_dictionary<T>(serialized_dict);
  // Check results
  CHECK(dict == dictionary_ref);
}

TEST_CASE(
    "Compression-Dictionary: Test compression of empty strings",
    "[compression][dict]") {
  std::vector<std::string> uncompressed_v = {""};
  std::vector<std::string_view> uncompressed;
  // the reference here is crucial, otherwise a temp string is created and
  // therefore string_view will outlive it
  for (const std::string& str : uncompressed_v) {
    uncompressed.emplace_back(str);
  }

  // Allocate the compressed array - we know the size will be equal to input
  std::vector<std::byte> compressed(uncompressed.size());
  auto dict =
      tiledb::sm::DictEncoding::compress<uint8_t>(uncompressed, compressed);
  CHECK(dict == uncompressed);

  std::vector<uint8_t> exp_compressed{0};
  CHECK(
      memcmp(
          exp_compressed.data(),
          reinterpret_cast<uint8_t*>(compressed.data()),
          exp_compressed.size()) == 0);

  // Decompress the previously compressed array
  const char* exp_decompressed = "";

  // In this test we allocate atleast 1 byte to avoid empty input buffer
  std::vector<std::byte> decompressed(1);
  const auto num_strings = 1;
  std::vector<uint64_t> decompressed_offsets(num_strings);
  tiledb::sm::DictEncoding::decompress<uint8_t>(
      compressed, uncompressed_v, decompressed, decompressed_offsets);

  // In decompressed array there are only chars, so compare using memcpy
  CHECK(
      memcmp(
          exp_decompressed,
          reinterpret_cast<const char*>(decompressed.data()),
          decompressed.size()) == 0);

  std::vector<uint64_t> expected_offsets{0};
  for (uint32_t i = 0; i < expected_offsets.size(); i++) {
    CHECK(expected_offsets[i] == decompressed_offsets[i]);
  }
}

TEST_CASE(
    "Compression-Dictionary: Test compression of mixed empty strings",
    "[compression][dict]") {
  std::vector<std::string> uncompressed_v = {"", "a"};
  std::vector<std::string_view> uncompressed;
  // the reference here is crucial, otherwise a temp string is created and
  // therefore string_view will outlive it
  for (const std::string& str : uncompressed_v) {
    uncompressed.emplace_back(str);
  }

  // Allocate the compressed array - we know the size will be equal to input
  std::vector<std::byte> compressed(uncompressed.size());
  auto dict =
      tiledb::sm::DictEncoding::compress<uint8_t>(uncompressed, compressed);
  CHECK(dict == uncompressed);

  std::vector<uint8_t> exp_compressed{0, 1};
  CHECK(
      memcmp(
          exp_compressed.data(),
          reinterpret_cast<uint8_t*>(compressed.data()),
          exp_compressed.size()) == 0);

  // Decompress the previously compressed array
  const char* exp_decompressed =
      ""
      "a";

  const auto exp_decomp_size = strlen(exp_decompressed);
  std::vector<std::byte> decompressed(exp_decomp_size);
  const auto num_strings = 2;
  std::vector<uint64_t> decompressed_offsets(num_strings);
  tiledb::sm::DictEncoding::decompress<uint8_t>(
      compressed, uncompressed_v, decompressed, decompressed_offsets);

  // In decompressed array there are only chars, so compare using memcpy
  CHECK(
      memcmp(
          exp_decompressed,
          reinterpret_cast<const char*>(decompressed.data()),
          decompressed.size()) == 0);

  std::vector<uint64_t> expected_offsets{0, 0};
  for (uint32_t i = 0; i < expected_offsets.size(); i++) {
    CHECK(expected_offsets[i] == decompressed_offsets[i]);
  }
}

TEST_CASE(
    "Compression-Dictionary: Test compression of UTF-8 strings",
    "[compression][dict][utf-8]") {
  std::vector<std::string> uncompressed_v = {
      "föö", "föö", "fööbär", "bär", "bär", "bär", "bär"};
  std::vector<std::string_view> uncompressed;
  // the reference here is crucial, otherwise a temp string is created and
  // therefore string_view will outlive it
  for (const std::string& str : uncompressed_v) {
    uncompressed.emplace_back(str);
  }

  // The dictionary is created sequentially in order of appearance
  std::vector<std::string_view> dict_expected{
      uncompressed_v[0], uncompressed_v[2], uncompressed_v[3]};

  // Allocate the compressed array - we know the size will be 7
  std::vector<std::byte> compressed(7);
  auto dict =
      tiledb::sm::DictEncoding::compress<uint8_t>(uncompressed, compressed);
  CHECK(dict == dict_expected);

  std::vector<uint8_t> exp_compressed{0, 0, 1, 2, 2, 2, 2};
  CHECK(
      memcmp(
          exp_compressed.data(),
          reinterpret_cast<uint8_t*>(compressed.data()),
          exp_compressed.size()) == 0);

  // Decompress the previously compressed array
  const char* exp_decompressed =
      "föö"
      "föö"
      "fööbär"
      "bär"
      "bär"
      "bär"
      "bär";

  std::vector<std::string> dict_orig = {"föö", "fööbär", "bär"};

  const auto exp_decomp_size = strlen(exp_decompressed);
  std::vector<std::byte> decompressed(exp_decomp_size);
  const auto num_strings = 7;
  std::vector<uint64_t> decompressed_offsets(num_strings);
  tiledb::sm::DictEncoding::decompress<uint8_t>(
      compressed, dict_orig, decompressed, decompressed_offsets);

  // In decompressed array there are only chars, so compare using memcpy
  CHECK(
      memcmp(
          exp_decompressed,
          reinterpret_cast<const char*>(decompressed.data()),
          decompressed.size()) == 0);

  std::vector<uint64_t> expected_offsets{0, 5, 10, 19, 23, 27, 31};
  for (uint32_t i = 0; i < expected_offsets.size(); i++) {
    CHECK(expected_offsets[i] == decompressed_offsets[i]);
  }
}
