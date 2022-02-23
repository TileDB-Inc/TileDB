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

#include "catch.hpp"

#include "../dict_compressor.h"

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
  std::vector<uint8_t> compressed(num_of_strings);
  auto dict =
      tiledb::sm::DictEncoding::compress<uint8_t>(uncompressed, compressed);
  CHECK(dict.size() == exp_dict.size());
  CHECK(dict == exp_dict);

  std::vector<uint8_t> exp_compressed{0, 0, 0, 1, 1, 2, 0, 1};
  CHECK(exp_compressed == compressed);
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
  tiledb::sm::DictEncoding::decompress<uint8_t>(compressed, dict, decompressed);

  // In decompressed array there are only chars, so compare using memcpy
  CHECK(
      memcmp(
          exp_decompressed,
          reinterpret_cast<const char*>(decompressed.data()),
          decompressed.size()) == 0);
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
  auto string_rand = random_string(string_len);
  std::vector<std::string> uncompressed_v(num_strings, string_rand);
  std::vector<std::string_view> uncompressed;
  // the reference here is crucial, otherwise a temp string is created and
  // therefore string_view will outlive it
  for (const std::string& str : uncompressed_v) {
    uncompressed.emplace_back(str);
  }

  // Allocate the compressed array - we know the size will be equal to input
  std::vector<T> compressed(num_strings);
  std::vector<std::string_view> exp_dict{string_rand};
  // Compress the input array
  auto dict = tiledb::sm::DictEncoding::compress<T>(uncompressed, compressed);
  CHECK(dict == exp_dict);

  std::vector<T> exp_compressed(num_strings, 0);
  CHECK(exp_compressed == compressed);

  // Decompress the previously compressed array
  std::ostringstream repeated2;
  std::fill_n(
      std::ostream_iterator<std::string>(repeated2), num_strings, string_rand);
  auto strout = repeated2.str();
  const char* exp_decompressed = strout.data();
  const auto exp_decomp_size = strout.size();
  std::vector<std::byte> decompressed(exp_decomp_size);
  tiledb::sm::DictEncoding::decompress<T>(compressed, dict, decompressed);

  // In decompressed array there are only chars, so compare using memcpy
  CHECK(
      memcmp(
          exp_decompressed,
          reinterpret_cast<const char*>(decompressed.data()),
          decompressed.size()) == 0);
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
  std::vector<uint8_t> compressed(uncompressed.size());
  auto dict =
      tiledb::sm::DictEncoding::compress<uint8_t>(uncompressed, compressed);
  CHECK(dict == uncompressed);

  std::vector<uint8_t> exp_compressed{0, 1, 2, 3, 4, 5, 6};
  CHECK(exp_compressed == compressed);

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
  tiledb::sm::DictEncoding::decompress<uint8_t>(compressed, dict, decompressed);

  // In decompressed array there are only chars, so compare using memcpy
  CHECK(
      memcmp(
          exp_decompressed,
          reinterpret_cast<const char*>(decompressed.data()),
          decompressed.size()) == 0);
}