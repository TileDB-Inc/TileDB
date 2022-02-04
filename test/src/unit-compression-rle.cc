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

#include "catch.hpp"
#include "helpers.h"
#include "tiledb/sm/buffer/buffer.h"
#include "tiledb/sm/compressors/rle_compressor.h"

#include <cstring>
#include <iostream>
#include <sstream>

using namespace tiledb::common;
using namespace tiledb::sm;

TEST_CASE("Compression-RLE: Test invalid format", "[compression][rle]") {
  // Initializations
  auto input = new ConstBuffer(nullptr, 0);
  auto compressed = new Buffer();

  // Test empty buffer
  auto st = tiledb::sm::RLE::compress(sizeof(int), input, compressed);
  CHECK(!st.ok());
  delete input;

  // Test input buffer invalid format
  auto buff = new Buffer();
  int int_v = 0;
  st = buff->write(&int_v, sizeof(int));
  REQUIRE(st.ok());
  char char_v = 'a';
  st = buff->write(&char_v, sizeof(char));
  REQUIRE(st.ok());
  CHECK(buff->size() == 5);
  st = compressed->realloc(1000000);
  REQUIRE(st.ok());
  input = new ConstBuffer(buff->data(), buff->size());
  st = tiledb::sm::RLE::compress(sizeof(int), input, compressed);
  CHECK(!st.ok());

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
  st = tiledb::sm::RLE::compress(sizeof(int), input, compressed);
  CHECK(st.ok());
  delete input;

  // Decompress
  input = new ConstBuffer(compressed->data(), compressed->size());
  auto decompressed = new Buffer();
  st = decompressed->realloc(sizeof(data));
  PreallocatedBuffer prealloc_buf(
      decompressed->data(), decompressed->alloced_size());
  REQUIRE(st.ok());
  st = tiledb::sm::RLE::decompress(sizeof(int), input, &prealloc_buf);
  CHECK(st.ok());
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
  st = tiledb::sm::RLE::compress(sizeof(int), input, compressed);
  CHECK(st.ok());
  CHECK(compressed->size() == run_size);
  delete input;

  // Decompress data
  st = decompressed->realloc(sizeof(data));
  REQUIRE(st.ok());
  PreallocatedBuffer prealloc_buf(
      decompressed->data(), decompressed->alloced_size());
  input = new ConstBuffer(compressed->data(), compressed->size());
  st = tiledb::sm::RLE::decompress(sizeof(int), input, &prealloc_buf);
  CHECK(st.ok());
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
  st = tiledb::sm::RLE::compress(sizeof(int), input, compressed);
  CHECK(st.ok());
  CHECK(compressed->size() == 21 * run_size);
  delete input;

  // Decompress data
  auto decompressed = new Buffer();
  st = decompressed->realloc(sizeof(data));
  REQUIRE(st.ok());
  PreallocatedBuffer prealloc_buf(
      decompressed->data(), decompressed->alloced_size());
  input = new ConstBuffer(compressed->data(), compressed->size());
  st = tiledb::sm::RLE::decompress(sizeof(int), input, &prealloc_buf);
  CHECK(st.ok());
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
  st = tiledb::sm::RLE::compress(sizeof(int), input, compressed);
  CHECK(st.ok());
  CHECK(compressed->size() == 32 * run_size);
  delete input;

  // Decompress data
  st = decompressed->realloc(sizeof(data));
  REQUIRE(st.ok());
  PreallocatedBuffer prealloc_buf(
      decompressed->data(), decompressed->alloced_size());
  input = new ConstBuffer(compressed->data(), compressed->size());
  st = tiledb::sm::RLE::decompress(sizeof(int), input, &prealloc_buf);
  CHECK(st.ok());
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
  st = tiledb::sm::RLE::compress(value_size, input, compressed);
  CHECK(st.ok());
  CHECK(compressed->size() == 21 * run_size);
  delete input;

  // Decompress data
  auto decompressed = new Buffer();
  st = decompressed->realloc(sizeof(data));
  REQUIRE(st.ok());
  PreallocatedBuffer prealloc_buf(
      decompressed->data(), decompressed->alloced_size());
  input = new ConstBuffer(compressed->data(), compressed->size());
  st = tiledb::sm::RLE::decompress(value_size, input, &prealloc_buf);
  CHECK(st.ok());
  CHECK_FALSE(memcmp(data, decompressed->data(), sizeof(data)));

  delete input;
  delete compressed;
  delete decompressed;
}

TEST_CASE(
    "Compression-RLE: Test compression parameter calculation of strings",
    "[compression][rle][rle-strings]") {
  std::string s15 = tiledb::test::random_string(15);
  std::string s8 = tiledb::test::random_string(8);
  std::string s4 = tiledb::test::random_string(4);
  std::string s3 = tiledb::test::random_string(3);
  std::string s1 = tiledb::test::random_string(1);
  std::vector<std::string_view> all_same(
      300000, tiledb::test::random_string(5000));

  // Generate {input, expected_output = {max_run_value, max_string_size,
  // num_of_runs, output_strings_size}} pairs
  auto data = GENERATE_REF(table<
                           std::vector<std::string_view>,
                           tuple<uint64_t, uint64_t, uint64_t, uint64_t>>(
      {{{s8, s8, s8, s8, s8, s1, s4, s4}, {5, 8, 3, 13}},
       {{s8, s8, s8, s8, s4, s4, s1}, {4, 8, 3, 13}},
       // s1 shows up in 2 different runs, so needs to be counted separately
       {{s1, s15, s15, s8, s8, s1, s4, s4}, {2, 15, 5, 29}},
       {{s8, s15, s1, s3, s4}, {1, 15, 5, 31}},
       {all_same, {300000, 5000, 1, 5000}},
       {{s4}, {1, 4, 1, 4}},
       {{}, {0, 0, 0, 0}}}));

  REQUIRE(
      tiledb::sm::RLE::calculate_compression_params(std::get<0>(data)) ==
      std::get<1>(data));
}

TEST_CASE(
    "Compression-RLE: Test compression of strings, small run lengths and "
    "string sizes",
    "[compression][rle][rle-strings]") {
  std::string_view uncompressed[] = {"HG543232",
                                     "HG543232",
                                     "HG543232",
                                     "HG543232",
                                     "HG543232",
                                     "HG54",
                                     "HG54",
                                     "A"};
  std::vector<std::string> unique_values = {"HG543232", "HG54", "A"};
  const auto strings_size = accumulate(
      unique_values.begin(),
      unique_values.end(),
      0,
      [](size_t sum, const std::string& str) { return sum + str.size(); });
  const auto exp_size = unique_values.size() * sizeof(uint8_t) +
                        unique_values.size() * sizeof(uint8_t) + strings_size;

  // Compress the input array
  std::vector<byte> compressed(exp_size);
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
  std::vector<byte> decompressed(exp_decomp_size);
  tiledb::sm::RLE::decompress<uint8_t, uint8_t>(compressed, decompressed);

  // In decompressed array there are only chars, so compare using memcpy
  CHECK(
      memcmp(
          unc,
          reinterpret_cast<const char*>(decompressed.data()),
          decompressed.size()) == 0);
}

TEST_CASE(
    "Compression-RLE: Test compression of single large run length/long string",
    "[compression][rle][rle-strings]") {
  uint32_t num_strings_32 = std::numeric_limits<uint16_t>::max() + 1;
  uint16_t string_16_len = std::numeric_limits<uint8_t>::max() + 1;
  auto string_16 = tiledb::test::random_string(string_16_len);
  std::vector<std::string_view> uncompressed(num_strings_32, string_16);

  const auto exp_size = sizeof(uint32_t) + sizeof(uint16_t) + string_16.size();

  std::vector<byte> compressed(exp_size);
  tiledb::sm::RLE::compress<uint32_t, uint16_t>(uncompressed, compressed);

  auto data = reinterpret_cast<const char*>(compressed.data());
  CHECK(num_strings_32 == utils::endianness::decode_be<uint32_t>(data));
  data += sizeof(uint32_t);
  CHECK(string_16_len == utils::endianness::decode_be<uint16_t>(data));
  data += sizeof(uint16_t);
  CHECK(strncmp(string_16.c_str(), data, string_16_len * sizeof(char)) == 0);

  // Decompress
  std::ostringstream repeated2;
  std::fill_n(
      std::ostream_iterator<std::string>(repeated2), num_strings_32, string_16);
  auto strout = repeated2.str();
  const char* unc = strout.data();
  const auto exp_decomp_size = strlen(unc);
  std::vector<byte> decompressed(exp_decomp_size);
  tiledb::sm::RLE::decompress<uint32_t, uint16_t>(compressed, decompressed);

  CHECK(
      memcmp(
          unc,
          reinterpret_cast<const char*>(decompressed.data()),
          decompressed.size()) == 0);
}

TEST_CASE(
    "Compression-RLE: Test input of unique strings (worst case)",
    "[compression][rle][rle-strings]") {
  std::vector<std::string_view> uncompressed = {
      "HG543232", "ATG", "AT", "A", "TGC", "HG54", "HG5"};
  const uint64_t uncompressed_bytesize = accumulate(
      uncompressed.begin(),
      uncompressed.end(),
      0,
      [](size_t sum, const std::string_view& str) { return sum + str.size(); });
  const auto exp_size = uncompressed.size() * sizeof(uint8_t) +
                        uncompressed.size() * sizeof(uint8_t) +
                        uncompressed_bytesize;

  // Compress the input array
  std::vector<byte> compressed(exp_size);
  tiledb::sm::RLE::compress<uint8_t, uint8_t>(uncompressed, compressed);

  // When all elements are unique the compressed output is always larger
  CHECK(compressed.size() > uncompressed_bytesize);

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
  std::vector<byte> decompressed(exp_decomp_size);
  tiledb::sm::RLE::decompress<uint8_t, uint8_t>(compressed, decompressed);

  // In decompressed array there are only chars, so compare using memcpy
  CHECK(
      memcmp(
          unc,
          reinterpret_cast<const char*>(decompressed.data()),
          decompressed.size()) == 0);
}
