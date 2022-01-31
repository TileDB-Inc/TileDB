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
#include "tiledb/sm/buffer/buffer.h"
#include "tiledb/sm/compressors/rle_compressor.h"

#include <cstring>
#include <iostream>

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
    "Compression-RLE: Test compression of strings",
    "[compression][rle][rle-strings]") {
  SECTION("Compress") {
    std::string_view uncompressed[] = {
        "HG543232", "HG543232", "HG543232", "A", "TGC", "HG54", "HG54", "A"};
    std::string ref_compressed[] = {
        "3", "HG543232", "1", "A", "1", "TGC", "2", "HG54", "1", "A"};
    auto compressed = tiledb::sm::RLE::compress(uncompressed);
    CHECK(equal(
        begin(compressed),
        end(compressed),
        begin(ref_compressed),
        end(ref_compressed)));
  }

  SECTION("Decompress") {
    std::string_view compressed[] = {
        "3", "HG543232", "1", "A", "1", "TGC", "2", "HG54", "1", "A"};
    std::string ref_uncompressed[] = {
        "HG543232", "HG543232", "HG543232", "A", "TGC", "HG54", "HG54", "A"};
    auto uncompressed = tiledb::sm::RLE::decompress(compressed);
    CHECK(equal(
        begin(uncompressed),
        end(uncompressed),
        begin(ref_uncompressed),
        end(ref_uncompressed)));
  }
}

TEST_CASE(
    "Compression-RLE: Test too long runs", "[compression][rle][rle-strings]") {
  tiledb::sm::RLE::set_max_run_length(2);

  SECTION("Compress/Uncompress single duplicate run") {
    // compress
    std::string_view input[] = {
        "HG543232", "HG543232", "HG543232", "A", "TGC", "HG54", "HG54"};
    std::string expected_output[] = {
        "2", "HG543232", "1", "HG543232", "1", "A", "1", "TGC", "2", "HG54"};
    auto output = tiledb::sm::RLE::compress(input);
    CHECK(equal(
        begin(output),
        end(output),
        begin(expected_output),
        end(expected_output)));

    // decompress
    std::string_view compressed[] = {
        "2", "HG543232", "1", "HG543232", "1", "A", "1", "TGC", "2", "HG54"};
    std::string ref_uncompressed[] = {
        "HG543232", "HG543232", "HG543232", "A", "TGC", "HG54", "HG54"};
    auto uncompressed = tiledb::sm::RLE::decompress(compressed);
    CHECK(equal(
        begin(uncompressed),
        end(uncompressed),
        begin(ref_uncompressed),
        end(ref_uncompressed)));
  }

  SECTION("Compress/Uncompress double duplicate run") {
    // compress
    std::string_view input[] = {
        "HG543232", "HG543232", "A", "A", "A", "A", "A", "TGC", "HG54", "HG54"};
    std::string expected_output[] = {
        "2", "HG543232", "2", "A", "2", "A", "1", "A", "1", "TGC", "2", "HG54"};
    auto output = tiledb::sm::RLE::compress(nonstd::span{input});
    CHECK(equal(
        begin(output),
        end(output),
        begin(expected_output),
        end(expected_output)));

    // decompress
    std::string_view compressed[] = {
        "2", "HG543232", "2", "A", "2", "A", "1", "A", "1", "TGC", "2", "HG54"};
    std::string ref_uncompressed[] = {
        "HG543232", "HG543232", "A", "A", "A", "A", "A", "TGC", "HG54", "HG54"};
    auto uncompressed = tiledb::sm::RLE::decompress(nonstd::span{compressed});
    CHECK(equal(
        begin(uncompressed),
        end(uncompressed),
        begin(ref_uncompressed),
        end(ref_uncompressed)));
  }
}

TEST_CASE(
    "Compression-RLE: Test empty input", "[compression][rle][rle-strings]") {
  SECTION("Compress") {
    std::vector<std::string_view> uncompressed;
    std::vector<std::string_view> ref_compressed;
    auto compressed = tiledb::sm::RLE::compress(nonstd::span{uncompressed});
    CHECK(equal(
        begin(compressed),
        end(compressed),
        begin(ref_compressed),
        end(ref_compressed)));
  }

  SECTION("Decompress") {
    std::vector<std::string_view> compressed;
    std::vector<std::string_view> ref_uncompressed;
    auto uncompressed = tiledb::sm::RLE::decompress(compressed);
    CHECK(equal(
        begin(uncompressed),
        end(uncompressed),
        begin(ref_uncompressed),
        end(ref_uncompressed)));
  }
}

TEST_CASE(
    "Compression-RLE: Test input of unique elements (worst case)",
    "[compression][rle][rle-strings]") {
  unsigned long unique_elem_len = 7;

  SECTION("Compress") {
    std::string_view uncompressed[] = {
        "HG543232", "ATG", "AT", "A", "TGC", "HG54", "HG5"};
    std::string ref_compressed[] = {"1",
                                    "HG543232",
                                    "1",
                                    "ATG",
                                    "1",
                                    "AT",
                                    "1",
                                    "A",
                                    "1",
                                    "TGC",
                                    "1",
                                    "HG54",
                                    "1",
                                    "HG5"};
    auto compressed = tiledb::sm::RLE::compress(uncompressed);
    CHECK(compressed.size() == 2 * unique_elem_len);
    CHECK(equal(
        begin(compressed),
        end(compressed),
        begin(ref_compressed),
        end(ref_compressed)));
  }

  SECTION("Decompress") {
    std::string_view compressed[] = {"1",
                                     "HG543232",
                                     "1",
                                     "ATG",
                                     "1",
                                     "AT",
                                     "1",
                                     "A",
                                     "1",
                                     "TGC",
                                     "1",
                                     "HG54",
                                     "1",
                                     "HG5"};
    std::string ref_uncompressed[] = {
        "HG543232", "ATG", "AT", "A", "TGC", "HG54", "HG5"};
    auto uncompressed = tiledb::sm::RLE::decompress(compressed);
    CHECK(uncompressed.size() == unique_elem_len);
    CHECK(equal(
        begin(uncompressed),
        end(uncompressed),
        begin(ref_uncompressed),
        end(ref_uncompressed)));
  }
}