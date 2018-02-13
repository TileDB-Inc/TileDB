/**
 * @file   unit-compression-rle.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2018 TileDB Inc.
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

#include <cstring>
#include <iostream>

#include "catch.hpp"
#include "tiledb/sm/compressors/rle_compressor.h"

using namespace tiledb::sm;

TEST_CASE("Compression-RLE: Test invalid format", "[compression], [rle]") {
  // Initializations
  auto input = new ConstBuffer(nullptr, 0);
  auto compressed = new Buffer();
  tiledb::sm::Status st;

  // Test empty buffer
  st = tiledb::sm::RLE::compress(sizeof(int), input, compressed);
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

TEST_CASE("Compression-RLE: Test all values unique", "[compression], [rle]") {
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
  REQUIRE(st.ok());
  st = tiledb::sm::RLE::decompress(sizeof(int), input, decompressed);
  CHECK(st.ok());
  CHECK_FALSE(memcmp(data, decompressed->data(), sizeof(data)));

  delete input;
  delete compressed;
  delete decompressed;
}

TEST_CASE("Compression-RLE: Test all values the same", "[compression], [rle]") {
  // Initializations
  uint64_t run_size = 6;
  auto compressed = new Buffer();
  auto decompressed = new Buffer();
  tiledb::sm::Status st;

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
  input = new ConstBuffer(compressed->data(), compressed->size());
  st = tiledb::sm::RLE::decompress(sizeof(int), input, decompressed);
  CHECK(st.ok());
  CHECK_FALSE(memcmp(data, decompressed->data(), sizeof(data)));

  delete input;
  delete compressed;
  delete decompressed;
}

TEST_CASE(
    "Compression-RLE: Test a mix of short and long runs",
    "[compression], [rle]") {
  // Initializations
  uint64_t run_size = 6;
  tiledb::sm::Status st;

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
  input = new ConstBuffer(compressed->data(), compressed->size());
  st = tiledb::sm::RLE::decompress(sizeof(int), input, decompressed);
  CHECK(st.ok());
  CHECK_FALSE(memcmp(data, decompressed->data(), sizeof(int)));

  delete input;
  delete compressed;
  delete decompressed;
}

TEST_CASE(
    "Compression-RLE: Test when a run exceeds max run length",
    "[compression], [rle]") {
  // Initializations
  uint64_t run_size = 6;
  auto decompressed = new Buffer();
  tiledb::sm::Status st;

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
  input = new ConstBuffer(compressed->data(), compressed->size());
  st = tiledb::sm::RLE::decompress(sizeof(int), input, decompressed);
  CHECK(st.ok());
  CHECK_FALSE(memcmp(data, decompressed->data(), sizeof(data)));

  delete input;
  delete compressed;
  delete decompressed;
}

TEST_CASE(
    "Compression-RLE: Test compression/decompression with type double:2",
    "[compression], [rle]") {
  // Initializations
  tiledb::sm::Status st;
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
  input = new ConstBuffer(compressed->data(), compressed->size());
  st = tiledb::sm::RLE::decompress(value_size, input, decompressed);
  CHECK(st.ok());
  CHECK_FALSE(memcmp(data, decompressed->data(), sizeof(data)));

  delete input;
  delete compressed;
  delete decompressed;
}
