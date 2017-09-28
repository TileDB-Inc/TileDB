/**
 * @file   unit-compression-rle.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017 TileDB Inc.
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
#include "rle_compressor.h"

using namespace tiledb;

TEST_CASE("Compression-RLE: Test invalid format and compress bound", "[rle]") {
  // Initializations
  auto input = new Buffer();
  auto compressed = new Buffer();
  tiledb::Status st;

  // Test empty buffer
  st = tiledb::RLE::compress(sizeof(int), input, compressed);
  CHECK(!st.ok());

  // Test input buffer invalid format
  int int_v = 0;
  st = input->write(&int_v, sizeof(int));
  REQUIRE(st.ok());
  char char_v = 'a';
  st = input->write(&char_v, sizeof(char));
  REQUIRE(st.ok());
  CHECK(input->size() == 5);
  st = compressed->realloc(1000000);
  REQUIRE(st.ok());
  st = tiledb::RLE::compress(sizeof(int), input, compressed);
  CHECK(!st.ok());

  input->clear();
  compressed->clear();

  // Test output buffer overflow
  int vec[16];  // Arbirtrary vec
  st = input->write(vec, sizeof(vec));
  REQUIRE(st.ok());
  compressed->realloc(4);
  st = tiledb::RLE::compress(sizeof(int), input, compressed);
  CHECK(!st.ok());

  // Test compress bound
  uint64_t compress_bound =
      tiledb::RLE::compress_bound(input->size(), sizeof(int));
  CHECK(compress_bound == input->size() + ((input->size() / sizeof(int)) * 2));

  delete input;
  delete compressed;
}

TEST_CASE("Compression-RLE: Test all values unique", "[rle]") {
  // Initializations
  auto input = new Buffer();
  auto compressed = new Buffer();
  auto decompressed = new Buffer();
  tiledb::Status st;

  st = input->realloc(100 * sizeof(int));
  REQUIRE(st.ok());
  uint64_t compressed_size =
      tiledb::RLE::compress_bound(input->alloced_size(), sizeof(int));
  st = compressed->realloc(compressed_size);
  REQUIRE(st.ok());

  for (int i = 0; i < 100; ++i) {
    st = input->write(&i, sizeof(int));
    REQUIRE(st.ok());
  }
  input->reset_offset();
  st = tiledb::RLE::compress(sizeof(int), input, compressed);
  CHECK(st.ok());
  st = decompressed->realloc(100 * sizeof(int));
  REQUIRE(st.ok());
  compressed->reset_offset();
  st = tiledb::RLE::decompress(sizeof(int), compressed, decompressed);
  CHECK(st.ok());
  CHECK_FALSE(memcmp(input->data(), decompressed->data(), 100 * sizeof(int)));

  delete input;
  delete compressed;
  delete decompressed;
}

TEST_CASE("Compression-RLE: Test all values the same", "[rle]") {
  // Initializations
  uint64_t run_size = 6;
  auto input = new Buffer();
  auto compressed = new Buffer();
  auto decompressed = new Buffer();
  tiledb::Status st;

  st = input->realloc(100 * sizeof(int));
  REQUIRE(st.ok());
  uint64_t compressed_size =
      tiledb::RLE::compress_bound(input->alloced_size(), sizeof(int));
  st = compressed->realloc(compressed_size);
  REQUIRE(st.ok());

  int v = 111;
  for (int i = 0; i < 100; ++i) {
    st = input->write(&v, sizeof(int));
    REQUIRE(st.ok());
  }
  input->reset_offset();
  st = tiledb::RLE::compress(sizeof(int), input, compressed);
  CHECK(st.ok());
  CHECK(compressed->size() == run_size);
  compressed->reset_offset();
  st = decompressed->realloc(100 * sizeof(int));
  REQUIRE(st.ok());
  st = tiledb::RLE::decompress(sizeof(int), compressed, decompressed);
  CHECK(st.ok());
  CHECK_FALSE(memcmp(input->data(), decompressed->data(), input->size()));

  delete input;
  delete compressed;
  delete decompressed;
}

TEST_CASE("Compression-RLE: Test a mix of short and long runs", "[rle]") {
  // Initializations
  uint64_t run_size = 6;
  auto input = new Buffer();
  auto compressed = new Buffer();
  auto decompressed = new Buffer();
  tiledb::Status st;

  st = input->realloc(110 * sizeof(int));
  REQUIRE(st.ok());
  uint64_t compressed_size =
      tiledb::RLE::compress_bound(input->alloced_size(), sizeof(int));
  st = compressed->realloc(compressed_size);
  REQUIRE(st.ok());
  for (int i = 0; i < 10; ++i) {
    st = input->write(&i, sizeof(int));
    REQUIRE(st.ok());
  }
  int v = 110;
  for (int i = 10; i < 100; ++i) {
    st = input->write(&v, sizeof(int));
    REQUIRE(st.ok());
  }
  for (int i = 100; i < 110; ++i) {
    input->write(&i, sizeof(int));
    REQUIRE(st.ok());
  }
  input->reset_offset();
  st = tiledb::RLE::compress(sizeof(int), input, compressed);
  CHECK(st.ok());
  CHECK(compressed->size() == 21 * run_size);
  st = decompressed->realloc(input->size());
  REQUIRE(st.ok());
  compressed->reset_offset();
  st = tiledb::RLE::decompress(sizeof(int), compressed, decompressed);
  CHECK(st.ok());
  CHECK_FALSE(memcmp(input->data(), decompressed->data(), sizeof(int)));

  delete input;
  delete compressed;
  delete decompressed;
}

TEST_CASE("Compression-RLE: Test when a run exceeds max run length", "[rle]") {
  // Initializations
  uint64_t run_size = 6;
  auto input = new Buffer();
  auto compressed = new Buffer();
  auto decompressed = new Buffer();
  tiledb::Status st;

  st = input->realloc(70030 * sizeof(int));
  uint64_t compressed_size =
      tiledb::RLE::compress_bound(input->alloced_size(), sizeof(int));
  st = compressed->realloc(compressed_size);
  REQUIRE(st.ok());
  for (int i = 0; i < 10; ++i) {
    st = input->write(&i, sizeof(int));
    REQUIRE(st.ok());
  }
  int v = 20;
  for (int i = 10; i < 70010; ++i) {
    st = input->write(&v, sizeof(int));
    REQUIRE(st.ok());
  }
  for (int i = 70010; i < 70030; ++i) {
    input->write(&i, sizeof(int));
    REQUIRE(st.ok());
  }
  input->reset_offset();
  st = tiledb::RLE::compress(sizeof(int), input, compressed);
  CHECK(st.ok());
  CHECK(compressed->size() == 32 * run_size);
  st = decompressed->realloc(input->size());
  REQUIRE(st.ok());
  compressed->reset_offset();
  st = tiledb::RLE::decompress(sizeof(int), compressed, decompressed);
  CHECK(st.ok());
  CHECK_FALSE(memcmp(input->data(), decompressed->data(), input->size()));

  delete input;
  delete compressed;
  delete decompressed;
}

TEST_CASE(
    "Compression-RLE: Test compression/decompression with type double:2",
    "[rle]") {
  // Initializations
  auto input = new Buffer();
  auto compressed = new Buffer();
  auto decompressed = new Buffer();
  tiledb::Status st;
  uint64_t value_size = 2 * sizeof(double);
  uint64_t run_size = value_size + 2;

  st = input->realloc(110 * value_size);
  REQUIRE(st.ok());
  uint64_t compressed_size =
      tiledb::RLE::compress_bound(input->alloced_size(), value_size);
  st = compressed->realloc(compressed_size);
  REQUIRE(st.ok());

  // Test a mix of short and long runs
  double j = 0.1, k = 0.2;
  for (int i = 0; i < 10; ++i) {
    j += 10000.12;
    st = input->write(&j, sizeof(double));
    REQUIRE(st.ok());
    k += 1000.12;
    st = input->write(&k, sizeof(double));
  }
  j += 10000.12;
  k += 1000.12;
  for (int i = 10; i < 100; ++i) {
    st = input->write(&j, sizeof(double));
    REQUIRE(st.ok());
    st = input->write(&k, sizeof(double));
    REQUIRE(st.ok());
  }
  for (int i = 100; i < 110; ++i) {
    j += 10000.12;
    st = input->write(&j, sizeof(double));
    REQUIRE(st.ok());
    k += 1000.12;
    st = input->write(&k, sizeof(double));
    REQUIRE(st.ok());
  }
  input->reset_offset();
  st = tiledb::RLE::compress(value_size, input, compressed);
  CHECK(st.ok());
  CHECK(compressed->size() == 21 * run_size);
  st = decompressed->realloc(input->size());
  REQUIRE(st.ok());
  compressed->reset_offset();
  st = tiledb::RLE::decompress(value_size, compressed, decompressed);
  CHECK(st.ok());
  CHECK_FALSE(memcmp(input->data(), decompressed->data(), input->size()));

  delete input;
  delete compressed;
  delete decompressed;
}
