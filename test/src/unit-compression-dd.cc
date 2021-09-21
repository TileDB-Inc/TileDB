/**
 * @file   unit-compression-dd.cc
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
 * Tests the double delta compression.
 */

#include "catch.hpp"
#include "tiledb/sm/buffer/buffer.h"
#include "tiledb/sm/compressors/dd_compressor.h"
#include "tiledb/sm/enums/datatype.h"

#include <ctime>
#include <iostream>

TEST_CASE(
    "Compression-DoubleDelta: Test 1-element case",
    "[compression][double-delta]") {
  // Compress
  int data[] = {1};
  auto comp_in_buff = new tiledb::sm::ConstBuffer(data, sizeof(data));
  auto comp_out_buff = new tiledb::sm::Buffer();
  auto st = tiledb::sm::DoubleDelta::compress(
      tiledb::sm::Datatype::INT32, comp_in_buff, comp_out_buff);
  REQUIRE(st.ok());

  // Decompress
  auto decomp_in_buff =
      new tiledb::sm::ConstBuffer(comp_out_buff->data(), comp_out_buff->size());
  auto decomp_out_buff = new tiledb::sm::Buffer();
  st = decomp_out_buff->realloc(sizeof(data));
  REQUIRE(st.ok());
  tiledb::sm::PreallocatedBuffer prealloc_buf(
      decomp_out_buff->data(), decomp_out_buff->alloced_size());
  st = tiledb::sm::DoubleDelta::decompress(
      tiledb::sm::Datatype::INT32, decomp_in_buff, &prealloc_buf);
  REQUIRE(st.ok());

  // Check data
  auto decomp_data = (int*)decomp_out_buff->data();
  REQUIRE(decomp_data[0] == data[0]);

  // Clean up
  delete comp_in_buff;
  delete comp_out_buff;
  delete decomp_in_buff;
  delete decomp_out_buff;
}

TEST_CASE(
    "Compression-DoubleDelta: Test 2-element case",
    "[compression][double-delta]") {
  // Compress
  int data[] = {1, 2};
  auto comp_in_buff = new tiledb::sm::ConstBuffer(data, sizeof(data));
  auto comp_out_buff = new tiledb::sm::Buffer();
  auto st = tiledb::sm::DoubleDelta::compress(
      tiledb::sm::Datatype::INT32, comp_in_buff, comp_out_buff);
  REQUIRE(st.ok());

  // Decompress
  auto decomp_in_buff =
      new tiledb::sm::ConstBuffer(comp_out_buff->data(), comp_out_buff->size());
  auto decomp_out_buff = new tiledb::sm::Buffer();
  st = decomp_out_buff->realloc(sizeof(data));
  REQUIRE(st.ok());
  tiledb::sm::PreallocatedBuffer prealloc_buf(
      decomp_out_buff->data(), decomp_out_buff->alloced_size());
  st = tiledb::sm::DoubleDelta::decompress(
      tiledb::sm::Datatype::INT32, decomp_in_buff, &prealloc_buf);
  REQUIRE(st.ok());

  // Check data
  auto decomp_data = (int*)decomp_out_buff->data();
  REQUIRE(decomp_data[0] == data[0]);
  REQUIRE(decomp_data[1] == data[1]);

  // Clean up
  delete comp_in_buff;
  delete comp_out_buff;
  delete decomp_in_buff;
  delete decomp_out_buff;
}

TEST_CASE(
    "Compression-DoubleDelta: Test 3-element case",
    "[compression][double-delta]") {
  // Compress
  int data[] = {100, 300, 200};
  auto comp_in_buff = new tiledb::sm::ConstBuffer(data, sizeof(data));
  auto comp_out_buff = new tiledb::sm::Buffer();
  auto st = tiledb::sm::DoubleDelta::compress(
      tiledb::sm::Datatype::INT32, comp_in_buff, comp_out_buff);
  REQUIRE(st.ok());

  // Decompress
  auto decomp_in_buff =
      new tiledb::sm::ConstBuffer(comp_out_buff->data(), comp_out_buff->size());
  auto decomp_out_buff = new tiledb::sm::Buffer();
  st = decomp_out_buff->realloc(sizeof(data));
  REQUIRE(st.ok());
  tiledb::sm::PreallocatedBuffer prealloc_buf(
      decomp_out_buff->data(), decomp_out_buff->alloced_size());
  st = tiledb::sm::DoubleDelta::decompress(
      tiledb::sm::Datatype::INT32, decomp_in_buff, &prealloc_buf);
  REQUIRE(st.ok());

  // Check data
  auto decomp_data = (int*)decomp_out_buff->data();
  REQUIRE(decomp_data[0] == data[0]);
  REQUIRE(decomp_data[1] == data[1]);
  REQUIRE(decomp_data[2] == data[2]);

  // Clean up
  delete comp_in_buff;
  delete comp_out_buff;
  delete decomp_in_buff;
  delete decomp_out_buff;
}

TEST_CASE(
    "Compression-DoubleDelta: Test 4-element case",
    "[compression][double-delta]") {
  // Compress
  int data[] = {100, 300, 200, 600};
  auto comp_in_buff = new tiledb::sm::ConstBuffer(data, sizeof(data));
  auto comp_out_buff = new tiledb::sm::Buffer();
  auto st = tiledb::sm::DoubleDelta::compress(
      tiledb::sm::Datatype::INT32, comp_in_buff, comp_out_buff);
  REQUIRE(st.ok());

  // Decompress
  auto decomp_in_buff = new tiledb::sm::ConstBuffer(
      comp_out_buff->data(), comp_out_buff->offset());
  auto decomp_out_buff = new tiledb::sm::Buffer();
  st = decomp_out_buff->realloc(sizeof(data));
  REQUIRE(st.ok());
  tiledb::sm::PreallocatedBuffer prealloc_buf(
      decomp_out_buff->data(), decomp_out_buff->alloced_size());
  st = tiledb::sm::DoubleDelta::decompress(
      tiledb::sm::Datatype::INT32, decomp_in_buff, &prealloc_buf);
  REQUIRE(st.ok());

  // Check data
  auto decomp_data = (int*)decomp_out_buff->data();
  REQUIRE(decomp_data[0] == data[0]);
  REQUIRE(decomp_data[1] == data[1]);
  REQUIRE(decomp_data[2] == data[2]);
  REQUIRE(decomp_data[3] == data[3]);

  // Clean up
  delete comp_in_buff;
  delete comp_out_buff;
  delete decomp_in_buff;
  delete decomp_out_buff;
}

TEST_CASE(
    "Compression-DoubleDelta: Test n-element case",
    "[compression][double-delta]") {
  // Create random array
  std::srand(std::time(nullptr));
  int n = 1000000;
  int max_value = 1000;
  auto data = new int[n];
  for (int i = 0; i < n; ++i)
    data[i] = std::rand() % max_value;

  // Compress
  auto comp_in_buff = new tiledb::sm::ConstBuffer(data, n * sizeof(int));
  auto comp_out_buff = new tiledb::sm::Buffer();
  auto st = tiledb::sm::DoubleDelta::compress(
      tiledb::sm::Datatype::INT32, comp_in_buff, comp_out_buff);
  REQUIRE(st.ok());

  // Decompress
  auto decomp_in_buff = new tiledb::sm::ConstBuffer(
      comp_out_buff->data(), comp_out_buff->offset());
  auto decomp_out_buff = new tiledb::sm::Buffer();
  st = decomp_out_buff->realloc(sizeof(int) * n);
  REQUIRE(st.ok());
  tiledb::sm::PreallocatedBuffer prealloc_buf(
      decomp_out_buff->data(), decomp_out_buff->alloced_size());
  st = tiledb::sm::DoubleDelta::decompress(
      tiledb::sm::Datatype::INT32, decomp_in_buff, &prealloc_buf);
  REQUIRE(st.ok());

  // Check data
  auto decomp_data = (int*)decomp_out_buff->data();
  REQUIRE(std::memcmp(data, decomp_data, n * sizeof(int)) == 0);

  // Clean up
  delete comp_in_buff;
  delete comp_out_buff;
  delete decomp_in_buff;
  delete decomp_out_buff;
  delete[] data;
}

TEST_CASE(
    "Compression-DoubleDelta: Test uncompressible case",
    "[compression][double-delta]") {
  // Compress
  int8_t data[] = {-100, -101, 100, 101};
  auto comp_in_buff = new tiledb::sm::ConstBuffer(data, sizeof(data));
  auto comp_out_buff = new tiledb::sm::Buffer();
  auto st = tiledb::sm::DoubleDelta::compress(
      tiledb::sm::Datatype::INT8, comp_in_buff, comp_out_buff);
  REQUIRE(st.ok());

  // Decompress
  auto decomp_in_buff = new tiledb::sm::ConstBuffer(
      comp_out_buff->data(), comp_out_buff->offset());
  auto decomp_out_buff = new tiledb::sm::Buffer();
  st = decomp_out_buff->realloc(sizeof(data));
  REQUIRE(st.ok());
  tiledb::sm::PreallocatedBuffer prealloc_buf(
      decomp_out_buff->data(), decomp_out_buff->alloced_size());
  st = tiledb::sm::DoubleDelta::decompress(
      tiledb::sm::Datatype::INT8, decomp_in_buff, &prealloc_buf);
  REQUIRE(st.ok());

  // Check data
  auto decomp_data = (int8_t*)decomp_out_buff->data();
  REQUIRE(decomp_data[0] == data[0]);
  REQUIRE(decomp_data[1] == data[1]);
  REQUIRE(decomp_data[2] == data[2]);
  REQUIRE(decomp_data[3] == data[3]);

  // Clean up
  delete comp_in_buff;
  delete comp_out_buff;
  delete decomp_in_buff;
  delete decomp_out_buff;
}
