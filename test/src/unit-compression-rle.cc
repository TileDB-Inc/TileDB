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

TEST_CASE("Compression: Test RLE attribute compression") {
  // Initializations
  Buffer *input, *compressed, *decompressed;

  uint64_t value_size;
  uint64_t run_size = 6;
  uint64_t compress_bound;

  // === Attribute compression (value_size = sizeof(int)) === //
  value_size = sizeof(int);
  input = new Buffer();
  compressed = new Buffer;

  // Test empty bufffer
  tiledb::Status st;
  st = tiledb::RLE::compress(value_size, input, compressed);
  CHECK(!st.ok());

  delete input;
  delete compressed;

  // Test input buffer invalid format
  input = new Buffer(5);
  input->set_offset(5);
  compressed = new Buffer(1000000);

  st = tiledb::RLE::compress(value_size, input, compressed);
  CHECK(!st.ok());

  delete input;
  delete compressed;

  // Test output buffer overflow
  input = new Buffer(16);
  input->set_offset(16);
  compressed = new Buffer(4);

  st = tiledb::RLE::compress(value_size, input, compressed);
  CHECK(!st.ok());

  // Test compress bound
  compress_bound = tiledb::RLE::compress_bound(input->offset(), value_size);
  CHECK(
      compress_bound == input->offset() + ((input->offset() / value_size) * 2));

  delete input;
  delete compressed;

  // Test all values unique (many unitary runs)
  input = new Buffer(100 * value_size);
  input->set_offset(100 * value_size);
  uint64_t compressed_size =
      tiledb::RLE::compress_bound(input->offset(), value_size);
  compressed = new Buffer(compressed_size);

  for (int i = 0; i < 100; ++i)
    memcpy((char *)input->data() + i * value_size, &i, value_size);
  st = tiledb::RLE::compress(value_size, input, compressed);
  CHECK(st.ok());
  CHECK(compressed->offset() == compressed_size);
  decompressed = new Buffer(100 * value_size);
  st = tiledb::RLE::decompress(value_size, compressed, decompressed);
  CHECK(st.ok());
  CHECK_FALSE(memcmp(input->data(), decompressed->data(), 100 * value_size));

  delete input;
  delete compressed;
  delete decompressed;

  // Test all values the same (a single long run)
  input = new Buffer(100 * value_size);
  input->set_offset(100 * value_size);
  compressed_size = tiledb::RLE::compress_bound(input->offset(), value_size);
  compressed = new Buffer(compressed_size);

  int v = 111;
  for (int i = 0; i < 100; ++i)
    memcpy((char *)input->data() + i * value_size, &v, value_size);
  st = tiledb::RLE::compress(value_size, input, compressed);
  CHECK(st.ok());
  CHECK(compressed->offset() == run_size);
  decompressed = new Buffer(100 * value_size);
  compressed->set_size(compressed->offset());
  st = tiledb::RLE::decompress(value_size, compressed, decompressed);
  CHECK(st.ok());
  CHECK_FALSE(memcmp(input->data(), decompressed->data(), input->offset()));

  delete input;
  delete compressed;
  delete decompressed;

  // Test a mix of short and long runs
  uint64_t input_size = 110 * value_size;
  input = new Buffer(input_size);
  input->set_offset(input_size);
  compressed_size = tiledb::RLE::compress_bound(input_size, value_size);
  compressed = new Buffer(compressed_size);
  for (int i = 0; i < 10; ++i)
    memcpy((char *)input->data() + i * value_size, &i, value_size);
  for (int i = 10; i < 100; ++i)
    memcpy((char *)input->data() + i * value_size, &v, value_size);
  for (int i = 100; i < 110; ++i)
    memcpy((char *)input->data() + i * value_size, &i, value_size);
  st = tiledb::RLE::compress(value_size, input, compressed);
  CHECK(st.ok());
  CHECK(compressed->offset() == 21 * run_size);
  decompressed = new Buffer(input_size);
  compressed->set_size(compressed->offset());
  st = tiledb::RLE::decompress(value_size, compressed, decompressed);
  CHECK(st.ok());
  CHECK_FALSE(memcmp(input->data(), decompressed->data(), input_size));

  delete input;
  delete compressed;
  delete decompressed;

  // Test when a run exceeds max run length
  input_size = 70030 * value_size;
  input = new Buffer(input_size);
  input->set_offset(input_size);
  compressed_size = tiledb::RLE::compress_bound(input_size, value_size);
  compressed = new Buffer(compressed_size);
  for (int i = 0; i < 10; ++i)
    memcpy((char *)input->data() + i * value_size, &i, value_size);
  for (int i = 10; i < 70010; ++i)
    memcpy((char *)input->data() + i * value_size, &v, value_size);
  for (int i = 70010; i < 70030; ++i)
    memcpy((char *)input->data() + i * value_size, &i, value_size);
  st = tiledb::RLE::compress(value_size, input, compressed);
  CHECK(st.ok());
  CHECK(compressed->offset() == 32 * run_size);
  decompressed = new Buffer(input_size);
  compressed->set_size(compressed->offset());
  st = tiledb::RLE::decompress(value_size, compressed, decompressed);
  CHECK(st.ok());
  CHECK_FALSE(memcmp(input->data(), decompressed->data(), input_size));

  delete input;
  delete compressed;
  delete decompressed;

  // === Attribute compression (value_size = 2*sizeof(double)) === //
  value_size = 2 * sizeof(double);
  run_size = value_size + 2;
  input_size = 110 * value_size;
  input = new Buffer(input_size);
  input->set_offset(input_size);
  compressed_size = tiledb::RLE::compress_bound(input_size, value_size);
  compressed = new Buffer(compressed_size);

  // Test a mix of short and long runs
  double j = 0.1, k = 0.2;
  for (int i = 0; i < 10; ++i) {
    j += 10000.12;
    memcpy((char *)input->data() + 2 * i * sizeof(double), &j, value_size);
    k += 1000.12;
    memcpy(
        (char *)input->data() + (2 * i + 1) * sizeof(double), &k, value_size);
  }
  j += 10000.12;
  k += 1000.12;
  for (int i = 10; i < 100; ++i) {
    memcpy((char *)input->data() + 2 * i * sizeof(double), &j, value_size);
    memcpy(
        (char *)input->data() + (2 * i + 1) * sizeof(double), &k, value_size);
  }
  for (int i = 100; i < 110; ++i) {
    j += 10000.12;
    memcpy((char *)input->data() + 2 * i * sizeof(double), &j, value_size);
    k += 1000.12;
    memcpy(
        (char *)input->data() + (2 * i + 1) * sizeof(double), &k, value_size);
  }
  st = tiledb::RLE::compress(value_size, input, compressed);
  CHECK(st.ok());
  CHECK(compressed->offset() == 21 * run_size);
  decompressed = new Buffer(input_size);
  compressed->set_size(compressed->offset());
  st = tiledb::RLE::decompress(value_size, compressed, decompressed);
  CHECK(st.ok());
  CHECK_FALSE(memcmp(input->data(), decompressed->data(), input_size));

  delete input;
  delete compressed;
  delete decompressed;
}
