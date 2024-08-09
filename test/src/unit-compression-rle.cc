/**
 * @file   unit-compression-rle.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2021 TileDB Inc.
 * @copyright Copyright (c) 2016 MIT and Intel Corporation
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
 * Tests for the RLE compression.
 */

#include <catch2/catch_test_macros.hpp>
#include <catch2/catch_template_test_macros.hpp>

#include "test/support/src/helpers.h"
#include "tiledb/sm/buffer/buffer.h"
#include "tiledb/sm/compressors/rle_compressor.h"

#include <cstring>
#include <iostream>
#include <iterator>
#include <sstream>

using namespace tiledb::common;
using namespace tiledb::sm;

TEST_CASE("Compression-RLE: Test invalid format", "[compression][rle]") {
  // Initializations
  auto input = new ConstBuffer(nullptr, 0);
  auto compressed = new Buffer();

  // Test empty buffer
  REQUIRE_THROWS(tiledb::sm::RLE::compress(sizeof(int), input, compressed));
  delete input;

  // Test input buffer invalid format
  auto buff = new Buffer();
  int int_v = 0;
  auto st = buff->write(&int_v, sizeof(int));
  REQUIRE(st.ok());
  char char_v = 'a';
  st = buff->write(&char_v, sizeof(char));
  REQUIRE(st.ok());
  CHECK(buff->size() == 5);
  st = compressed->realloc(1000000);
  REQUIRE(st.ok());
  input = new ConstBuffer(buff->data(), buff->size());
  REQUIRE_THROWS(tiledb::sm::RLE::compress(sizeof(int), input, compressed));

  delete input;
  delete compressed;
  delete buff;
}

TEST_CASE("Compression-RLE: Test all values unique", "[compression][rle]") {
  // Populate data
  int data[100];
  for (int i = 0; i < 100; ++i)
    data[i] = i;

  // Allocate space for the compressed buffer
  uint64_t compressed_size =
      tiledb::sm::RLE::overhead(sizeof(data), sizeof(int)) + sizeof(data);
  auto compressed = new Buffer();
  Status st = compressed->realloc(compressed_size);
  REQUIRE(st.ok());

  // Create an input buffer and compress
  auto input = new ConstBuffer(data, sizeof(data));
  REQUIRE_NOTHROW(tiledb::sm::RLE::compress(sizeof(int), input, compressed));
  delete input;

  // Decompress
  input = new ConstBuffer(compressed->data(), compressed->size());
  auto decompressed = new Buffer();
  st = decompressed->realloc(sizeof(data));
  PreallocatedBuffer prealloc_buf(
      decompressed->data(), decompressed->alloced_size());
  REQUIRE(st.ok());
  REQUIRE_NOTHROW(
      tiledb::sm::RLE::decompress(sizeof(int), input, &prealloc_buf));
  CHECK_FALSE(memcmp(data, decompressed->data(), sizeof(data)));

  delete input;
  delete compressed;
  delete decompressed;
}

TEST_CASE("Compression-RLE: Test all values the same", "[compression][rle]") {
  // Initializations
  uint64_t run_size = 6;
  auto compressed = new Buffer();
  auto decompressed = new Buffer();
  Status st;

  int data[100];
  REQUIRE(st.ok());
  uint64_t compressed_size =
      tiledb::sm::RLE::overhead(sizeof(data), sizeof(int)) + sizeof(data);
  st = compressed->realloc(compressed_size);
  REQUIRE(st.ok());

  // Prepare data
  for (int& i : data)
    i = 111;

  // Compress data
  auto input = new ConstBuffer(data, sizeof(data));
  REQUIRE_NOTHROW(tiledb::sm::RLE::compress(sizeof(int), input, compressed));
  CHECK(compressed->size() == run_size);
  delete input;

  // Decompress data
  st = decompressed->realloc(sizeof(data));
  REQUIRE(st.ok());
  PreallocatedBuffer prealloc_buf(
      decompressed->data(), decompressed->alloced_size());
  input = new ConstBuffer(compressed->data(), compressed->size());
  REQUIRE_NOTHROW(
      tiledb::sm::RLE::decompress(sizeof(int), input, &prealloc_buf));
  CHECK_FALSE(memcmp(data, decompressed->data(), sizeof(data)));

  delete input;
  delete compressed;
  delete decompressed;
}

TEST_CASE(
    "Compression-RLE: Test a mix of short and long runs",
    "[compression][rle]") {
  // Initializations
  uint64_t run_size = 6;
  Status st;

  // Prepare data
  int data[110];
  for (int i = 0; i < 10; ++i)
    data[i] = i;
  for (int i = 10; i < 100; ++i)
    data[i] = 110;
  for (int i = 100; i < 110; ++i)
    data[i] = i;

  // Compress
  uint64_t compressed_size =
      tiledb::sm::RLE::overhead(sizeof(data), sizeof(int)) + sizeof(data);
  auto compressed = new Buffer();
  st = compressed->realloc(compressed_size);
  REQUIRE(st.ok());
  auto input = new ConstBuffer(data, sizeof(data));
  REQUIRE_NOTHROW(tiledb::sm::RLE::compress(sizeof(int), input, compressed));
  CHECK(compressed->size() == 21 * run_size);
  delete input;

  // Decompress data
  auto decompressed = new Buffer();
  st = decompressed->realloc(sizeof(data));
  REQUIRE(st.ok());
  PreallocatedBuffer prealloc_buf(
      decompressed->data(), decompressed->alloced_size());
  input = new ConstBuffer(compressed->data(), compressed->size());
  REQUIRE_NOTHROW(
      tiledb::sm::RLE::decompress(sizeof(int), input, &prealloc_buf));
  CHECK_FALSE(memcmp(data, decompressed->data(), sizeof(int)));

  delete input;
  delete compressed;
  delete decompressed;
}

TEST_CASE(
    "Compression-RLE: Test when a run exceeds max run length",
    "[compression][rle]") {
  // Initializations
  uint64_t run_size = 6;
  auto decompressed = new Buffer();
  Status st;

  // Prepare data
  int data[70030];
  for (int i = 0; i < 10; ++i)
    data[i] = i;
  for (int i = 10; i < 70010; ++i)
    data[i] = 20;
  for (int i = 70010; i < 70030; ++i)
    data[i] = i;

  // Compress data
  uint64_t compressed_size =
      tiledb::sm::RLE::overhead(sizeof(data), sizeof(int)) + sizeof(data);
  auto compressed = new Buffer();
  st = compressed->realloc(compressed_size);
  REQUIRE(st.ok());
  auto input = new ConstBuffer(data, sizeof(data));
  REQUIRE_NOTHROW(tiledb::sm::RLE::compress(sizeof(int), input, compressed));
  CHECK(compressed->size() == 32 * run_size);
  delete input;

  // Decompress data
  st = decompressed->realloc(sizeof(data));
  REQUIRE(st.ok());
  PreallocatedBuffer prealloc_buf(
      decompressed->data(), decompressed->alloced_size());
  input = new ConstBuffer(compressed->data(), compressed->size());
  REQUIRE_NOTHROW(
      tiledb::sm::RLE::decompress(sizeof(int), input, &prealloc_buf));
  CHECK_FALSE(memcmp(data, decompressed->data(), sizeof(data)));

  delete input;
  delete compressed;
  delete decompressed;
}

TEST_CASE(
    "Compression-RLE: Test compression/decompression with type double:2",
    "[compression][rle]") {
  // Initializations
  Status st;
  uint64_t value_size = 2 * sizeof(double);
  uint64_t run_size = value_size + 2;

  // Prepare data
  double data[220];
  double j = 0.1, k = 0.2;
  for (int i = 0; i < 10; ++i) {
    j += 10000.12;
    k += 1000.12;
    data[2 * i] = j;
    data[2 * i + 1] = k;
  }
  j += 10000.12;
  k += 1000.12;
  for (int i = 10; i < 100; ++i) {
    data[2 * i] = j;
    data[2 * i + 1] = j;
  }
  for (int i = 100; i < 110; ++i) {
    j += 10000.12;
    k += 1000.12;
    data[2 * i] = j;
    data[2 * i + 1] = k;
  }

  // Compress data
  uint64_t compressed_size =
      tiledb::sm::RLE::overhead(sizeof(data), value_size) + sizeof(data);
  auto compressed = new Buffer();
  st = compressed->realloc(compressed_size);
  REQUIRE(st.ok());
  auto input = new ConstBuffer(data, sizeof(data));
  REQUIRE_NOTHROW(tiledb::sm::RLE::compress(value_size, input, compressed));
  CHECK(compressed->size() == 21 * run_size);
  delete input;

  // Decompress data
  auto decompressed = new Buffer();
  st = decompressed->realloc(sizeof(data));
  REQUIRE(st.ok());
  PreallocatedBuffer prealloc_buf(
      decompressed->data(), decompressed->alloced_size());
  input = new ConstBuffer(compressed->data(), compressed->size());
  REQUIRE_NOTHROW(
      tiledb::sm::RLE::decompress(value_size, input, &prealloc_buf));
  CHECK_FALSE(memcmp(data, decompressed->data(), sizeof(data)));

  delete input;
  delete compressed;
  delete decompressed;
}

TEST_CASE(
    "Compression-RLE: Test bytesize computation",
    "[compression][rle][rle-strings]") {
  CHECK(RLE::compute_bytesize(0) == 1);
  CHECK(RLE::compute_bytesize(1) == 1);
  CHECK(RLE::compute_bytesize(0xff) == 1);
  CHECK(RLE::compute_bytesize(0x100) == 2);
  CHECK(RLE::compute_bytesize(0xffff) == 2);
  CHECK(RLE::compute_bytesize(0x10000) == 4);
  CHECK(RLE::compute_bytesize(0xffffffff) == 4);
  CHECK(RLE::compute_bytesize(0x100000000) == 8);
  CHECK(RLE::compute_bytesize(0xffffffffffffffff) == 8);
}

TEST_CASE(
    "Compression-RLE: Test compression parameter calculation of strings",
    "[compression][rle][rle-strings]") {
  // initialize the seed for generating pseudorandom strings
  std::srand(10);
  std::string s15 = tiledb::test::random_string(15);
  std::string s8 = tiledb::test::random_string(8);
  std::string s4 = tiledb::test::random_string(4);
  std::string s3 = tiledb::test::random_string(3);
  std::string s1 = tiledb::test::random_string(1);

  std::vector<std::string_view> simple{s8, s8, s8, s8, s8, s1, s4, s4};
  CHECK(
      tiledb::sm::RLE::calculate_compression_params(simple) ==
      std::tuple(1, 1, 3, 13));

  std::vector<std::string_view> last_unique{s8, s8, s8, s8, s4, s4, s1};
  CHECK(
      tiledb::sm::RLE::calculate_compression_params(last_unique) ==
      std::tuple(1, 1, 3, 13));

  std::vector<std::string_view> first_unique{s1, s15, s15, s8, s8, s1, s4, s4};
  CHECK(
      tiledb::sm::RLE::calculate_compression_params(first_unique) ==
      std::tuple(1, 1, 5, 29));

  std::vector<std::string_view> all_unique{s8, s15, s1, s3, s4};
  CHECK(
      tiledb::sm::RLE::calculate_compression_params(all_unique) ==
      std::tuple(1, 1, 5, 31));

  std::vector<std::string_view> single_item{s4};
  CHECK(
      tiledb::sm::RLE::calculate_compression_params(single_item) ==
      std::tuple(1, 1, 1, 4));

  std::vector<std::string_view> empty_in{};
  CHECK(
      tiledb::sm::RLE::calculate_compression_params(empty_in) ==
      std::tuple(0, 0, 0, 0));

  std::vector<std::string> all_same_v(
      300000, tiledb::test::random_string(5000));
  std::vector<std::string_view> all_same;
  // the reference here is crucial, otherwise a temp string is created and
  // therefore string_view will outlive it
  for (const std::string& str : all_same_v) {
    all_same.emplace_back(str);
  }
  CHECK(
      tiledb::sm::RLE::calculate_compression_params(all_same) ==
      std::tuple(4, 2, 1, 5000));
}

TEST_CASE(
    "Compression-RLE: Test compression of strings, small run lengths and "
    "string sizes",
    "[compression][rle][rle-strings]") {
  std::string str1 = "HG543232";
  std::string str2 = "HG54";
  std::string str3 = "A";
  std::string_view uncompressed[] = {
      str1, str1, str1, str1, str1, str2, str2, str3};
  std::vector<std::string> unique_values = {str1, str2, str3};
  const auto strings_size = str1.size() + str2.size() + str3.size();
  const auto exp_size = unique_values.size() * sizeof(uint8_t) +
                        unique_values.size() * sizeof(uint8_t) + strings_size;

  // Compress the input array
  std::vector<std::byte> compressed(exp_size);
  tiledb::sm::RLE::compress<uint8_t, uint8_t>(uncompressed, compressed);
  CHECK(compressed.size() == exp_size);

  // All values here are 1 byte long, so endianness is not an issue and we can
  // just read using memcpy
  uint32_t run_length[] = {5, 2, 1};
  uint16_t size[] = {8, 4, 1};
  auto data = reinterpret_cast<const char*>(compressed.data());
  for (uint32_t i = 0; i < unique_values.size(); i++) {
    CHECK(memcmp(&run_length[i], data, sizeof(uint8_t)) == 0);
    data += sizeof(uint8_t);
    CHECK(memcmp(&size[i], data, sizeof(uint8_t)) == 0);
    data += sizeof(uint8_t);
    CHECK(strncmp(unique_values[i].c_str(), data, size[i] * sizeof(char)) == 0);
    data += size[i];
  }

  // Decompress the previously compressed array
  const char* unc =
      "HG543232"
      "HG543232"
      "HG543232"
      "HG543232"
      "HG543232"
      "HG54"
      "HG54"
      "A";
  const auto exp_decomp_size = strlen(unc);
  std::vector<std::byte> decompressed(exp_decomp_size);
  auto num_strings = 8;
  std::vector<uint64_t> decompressed_offsets(num_strings);
  tiledb::sm::RLE::decompress<uint8_t, uint8_t>(
      compressed, decompressed, decompressed_offsets);

  // In decompressed array there are only chars, so compare using memcpy
  CHECK(
      memcmp(
          unc,
          reinterpret_cast<const char*>(decompressed.data()),
          decompressed.size()) == 0);

  std::vector<uint64_t> expected_offsets{0, 8, 16, 24, 32, 40, 44};
  for (uint32_t i = 0; i < expected_offsets.size(); i++) {
    CHECK(expected_offsets[i] == decompressed_offsets[i]);
  }
}

typedef tuple<uint16_t, uint32_t, uint64_t> FixedTypesUnderTest;
TEMPLATE_LIST_TEST_CASE(
    "Compression-RLE: Test compression of single large run length/long string",
    "[compression][rle][rle-strings]",
    FixedTypesUnderTest) {
  typedef TestType T;
  // pick values of at least 2 bytes
  uint64_t num_strings = std::numeric_limits<uint8_t>::max() + 1;
  uint64_t string_len = std::numeric_limits<uint8_t>::max() + 1;

  // initialize the seed for generating pseudorandom string
  std::srand(10);
  auto string_rand = tiledb::test::random_string(string_len);
  std::vector<std::string> uncompressed_v(num_strings, string_rand);
  std::vector<std::string_view> uncompressed;
  // the reference here is crucial, otherwise a temp string is created and
  // therefore string_view will outlive it
  for (const std::string& str : uncompressed_v) {
    uncompressed.emplace_back(str);
  }

  const auto exp_size = sizeof(T) + sizeof(T) + string_rand.size();

  std::vector<std::byte> compressed(exp_size);
  tiledb::sm::RLE::compress<T, T>(uncompressed, compressed);

  auto data = reinterpret_cast<const char*>(compressed.data());
  CHECK(num_strings == utils::endianness::decode_be<T>(data));
  data += sizeof(T);
  CHECK(string_len == utils::endianness::decode_be<T>(data));
  data += sizeof(T);
  CHECK(strncmp(string_rand.c_str(), data, string_len * sizeof(char)) == 0);

  // Decompress
  std::ostringstream repeated2;
  std::fill_n(
      std::ostream_iterator<std::string>(repeated2), num_strings, string_rand);
  auto strout = repeated2.str();
  const char* unc = strout.data();
  const auto exp_decomp_size = strlen(unc);
  std::vector<std::byte> decompressed(exp_decomp_size);
  std::vector<uint64_t> decompressed_offsets(num_strings);
  tiledb::sm::RLE::decompress<T, T>(
      compressed, decompressed, decompressed_offsets);

  CHECK(
      memcmp(
          unc,
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
    "Compression-RLE: Test input of unique strings (worst case)",
    "[compression][rle][rle-strings]") {
  std::vector<std::string> uncompressed_v = {
      "HG543232", "ATG", "AT", "A", "TGC", "HG54", "HG5"};
  std::vector<std::string_view> uncompressed;
  uint64_t strings_size = 0;
  // the reference here is crucial, otherwise a temp string is created and
  // therefore string_view will outlive it
  for (const std::string& str : uncompressed_v) {
    uncompressed.emplace_back(str);
    strings_size += str.size();
  }

  const auto exp_size = uncompressed.size() * sizeof(uint8_t) +
                        uncompressed.size() * sizeof(uint8_t) + strings_size;

  // Compress the input array
  std::vector<std::byte> compressed(exp_size);
  tiledb::sm::RLE::compress<uint8_t, uint8_t>(uncompressed, compressed);

  // When all elements are unique the compressed output is always larger
  CHECK(compressed.size() > strings_size);

  // All values here are 1 byte long, so endianness is not an issue and we can
  // just read using memcpy
  uint32_t run_length[] = {1, 1, 1, 1, 1, 1, 1};
  uint16_t size[] = {8, 3, 2, 1, 3, 4, 3};
  auto data = reinterpret_cast<const char*>(compressed.data());
  for (uint32_t i = 0; i < uncompressed.size(); i++) {
    CHECK(memcmp(&run_length[i], data, sizeof(uint8_t)) == 0);
    data += sizeof(uint8_t);
    CHECK(memcmp(&size[i], data, sizeof(uint8_t)) == 0);
    data += sizeof(uint8_t);
    CHECK(strncmp(uncompressed[i].data(), data, size[i] * sizeof(char)) == 0);
    data += size[i];
  }

  // Decompress the previously compressed array
  const char* unc =
      "HG543232"
      "ATG"
      "AT"
      "A"
      "TGC"
      "HG54"
      "HG5";
  const auto exp_decomp_size = strlen(unc);
  std::vector<std::byte> decompressed(exp_decomp_size);
  auto num_strings = 7;
  std::vector<uint64_t> decompressed_offsets(num_strings);
  tiledb::sm::RLE::decompress<uint8_t, uint8_t>(
      compressed, decompressed, decompressed_offsets);

  // In decompressed array there are only chars, so compare using memcpy
  CHECK(
      memcmp(
          unc,
          reinterpret_cast<const char*>(decompressed.data()),
          decompressed.size()) == 0);

  std::vector<uint64_t> expected_offsets{0, 8, 11, 13, 14, 17, 21};
  for (uint32_t i = 0; i < expected_offsets.size(); i++) {
    CHECK(expected_offsets[i] == decompressed_offsets[i]);
  }
}

typedef tuple<uint8_t, uint16_t, uint32_t, uint64_t> UnsignedIntegerTypes;
TEMPLATE_LIST_TEST_CASE(
    "Compression-RLE: Test input of multiple runs of unsigned integers",
    "[compression][rle][rle-num]",
    UnsignedIntegerTypes) {
  typedef TestType T;
  std::vector<T> uncompressed = {
      1,
      1,
      1,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      2,
      1,
      1,
      std::numeric_limits<T>::max(),
      127,
      127};

  // Compress the input array
  const auto num_of_unique_runs = 6;
  const auto exp_size = num_of_unique_runs * 2;
  std::vector<T> compressed(exp_size);
  tiledb::sm::RLE::compress<T>(uncompressed, compressed);
  CHECK(
      compressed ==
      std::vector<T>{
          3, 1, 8, 0, 1, 2, 2, 1, 1, std::numeric_limits<T>::max(), 2, 127});

  // Decompress the previously compressed array
  std::vector<T> decompressed(uncompressed.size());
  tiledb::sm::RLE::decompress<T>(compressed, decompressed);
  CHECK(decompressed == uncompressed);
}

typedef tuple<int8_t, int16_t, int32_t, int64_t> SignedIntegerTypes;
TEMPLATE_LIST_TEST_CASE(
    "Compression-RLE: Test input of multiple runs of signed integers",
    "[compression][rle][rle-num]",
    SignedIntegerTypes) {
  typedef TestType T;
  std::vector<T> uncompressed = {
      -1,
      -1,
      -1,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      2,
      1,
      1,
      std::numeric_limits<T>::min(),
      127,
      127};
  std::vector<T> unique_runs = {
      -1, 0, 2, 1, 127, std::numeric_limits<T>::min()};

  // Compress the input array
  const auto num_of_unique_runs = 6;
  const auto exp_size = num_of_unique_runs * 2;
  std::vector<T> compressed(exp_size);
  tiledb::sm::RLE::compress<T>(uncompressed, compressed);
  CHECK(
      compressed ==
      std::vector<T>{
          3, -1, 8, 0, 1, 2, 2, 1, 1, std::numeric_limits<T>::min(), 2, 127});

  // Decompress the previously compressed array
  std::vector<T> decompressed(uncompressed.size());
  tiledb::sm::RLE::decompress<T>(compressed, decompressed);
  CHECK(decompressed == uncompressed);
}

typedef tuple<float, double> FloatingPointTypes;
TEMPLATE_LIST_TEST_CASE(
    "Compression-RLE: Test input of multiple runs of floating point numbers",
    "[compression][rle][rle-num]",
    FloatingPointTypes) {
  typedef TestType T;
  std::vector<T> uncompressed = {
      (T)-1.2,
      (T)-1.2,
      (T)-1.2,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      2,
      (T)1.8,
      (T)1.8,
      std::numeric_limits<T>::max(),
      (T)127};

  // Compress the input array
  const auto num_of_unique_runs = 6;
  const auto exp_size = num_of_unique_runs * 2;
  std::vector<T> compressed(exp_size);
  tiledb::sm::RLE::compress<T>(uncompressed, compressed);
  CHECK(
      compressed == std::vector<T>{
                        3,
                        (T)-1.2,
                        8,
                        0,
                        1,
                        2,
                        2,
                        (T)1.8,
                        1,
                        std::numeric_limits<T>::max(),
                        1,
                        (T)127});

  // Decompress the previously compressed array
  std::vector<T> decompressed(uncompressed.size());
  tiledb::sm::RLE::decompress<T>(compressed, decompressed);
  CHECK(decompressed == uncompressed);
}

TEST_CASE(
    "Compression-RLE: Test runs that exceed max size of input type",
    "[compression][rle][rle-num]") {
  uint64_t num_values = std::numeric_limits<uint8_t>::max() + 1;
  std::vector<uint8_t> uncompressed(num_values, 10);

  // Compress the input array
  // the repetitions are too many to fit in one run, so we expect an additional
  // pair of [run|value]
  const auto num_of_unique_runs = 1;
  const auto exp_size = (num_of_unique_runs + 1) * 2;
  std::vector<uint8_t> compressed(exp_size);
  tiledb::sm::RLE::compress<uint8_t>(uncompressed, compressed);
  CHECK(
      compressed ==
      std::vector<uint8_t>{std::numeric_limits<uint8_t>::max(), 10, 1, 10});

  // Decompress the previously compressed array
  std::vector<uint8_t> decompressed(uncompressed.size());
  tiledb::sm::RLE::decompress<uint8_t>(compressed, decompressed);
  CHECK(decompressed == uncompressed);
}

TEST_CASE(
    "Compression-RLE: Test all numeric unique runs (worst case)",
    "[compression][rle][rle-num]") {
  std::vector<uint64_t> uncompressed = {1, 5, 12, 123, 1, 2, 5, 12, 8};

  // Compress the input array
  const auto num_of_unique_runs = 9;
  const auto exp_size = num_of_unique_runs * 2;
  std::vector<uint64_t> compressed(exp_size);
  tiledb::sm::RLE::compress<uint64_t>(uncompressed, compressed);
  CHECK(
      compressed ==
      std::vector<uint64_t>{
          1, 1, 1, 5, 1, 12, 1, 123, 1, 1, 1, 2, 1, 5, 1, 12, 1, 8});

  // Decompress the previously compressed array
  std::vector<uint64_t> decompressed(uncompressed.size());
  tiledb::sm::RLE::decompress<uint64_t>(compressed, decompressed);
  CHECK(decompressed == uncompressed);
}

TEST_CASE("Compression-RLE: Test empty input", "[compression][rle][rle-num]") {
  std::vector<uint64_t> uncompressed = {};

  // Compress the input array
  std::vector<uint64_t> compressed;
  tiledb::sm::RLE::compress<uint64_t>(uncompressed, compressed);
  CHECK(compressed == std::vector<uint64_t>{});

  // Decompress the previously compressed array
  std::vector<uint64_t> decompressed(uncompressed.size());
  tiledb::sm::RLE::decompress<uint64_t>(compressed, decompressed);
  CHECK(decompressed == uncompressed);
}
