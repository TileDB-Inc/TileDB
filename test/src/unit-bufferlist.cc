/**
 * @file unit-buffer.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2023 TileDB, Inc.
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
 * Tests the `BufferList` class.
 */

#include "tiledb/sm/buffer/buffer_list.h"
#include "tiledb/sm/c_api/tiledb.h"
#include "tiledb/sm/c_api/tiledb_struct_def.h"

#include <catch2/catch_test_macros.hpp>
#include <iostream>

using namespace tiledb::sm;

TEST_CASE("BufferList: Test append", "[buffer][bufferlist]") {
  Buffer buff1, buff2;
  const char data1[3] = {1, 2, 3}, data2[4] = {4, 5, 6, 7};
  REQUIRE(buff1.write(data1, sizeof(data1)).ok());
  REQUIRE(buff2.write(data2, sizeof(data2)).ok());
  REQUIRE(buff1.data() != nullptr);
  REQUIRE(buff2.data() != nullptr);

  BufferList buffer_list;
  REQUIRE(buffer_list.num_buffers() == 0);
  REQUIRE(buffer_list.total_size() == 0);

  REQUIRE(buffer_list.add_buffer(std::move(buff1)).ok());
  REQUIRE(buffer_list.add_buffer(std::move(buff2)).ok());
  REQUIRE(buffer_list.num_buffers() == 2);
  REQUIRE(buffer_list.total_size() == sizeof(data1) + sizeof(data2));
  REQUIRE(buff1.data() == nullptr);
  REQUIRE(buff2.data() == nullptr);

  const Buffer *b1 = nullptr, *b2 = nullptr;
  REQUIRE(buffer_list.get_buffer(0, &b1).ok());
  REQUIRE(buffer_list.get_buffer(1, &b2).ok());
  REQUIRE(!buffer_list.get_buffer(2, nullptr).ok());
  REQUIRE(b1->size() == sizeof(data1));
  REQUIRE(b2->size() == sizeof(data2));
  REQUIRE(!std::memcmp(b1->data(), &data1[0], sizeof(data1)));
  REQUIRE(!std::memcmp(b2->data(), &data2[0], sizeof(data2)));
}

TEST_CASE("BufferList: Test read", "[buffer][bufferlist]") {
  BufferList buffer_list;
  auto data = static_cast<char*>(std::malloc(10));

  REQUIRE(!buffer_list.read(data, 1).ok());
  REQUIRE(buffer_list.read(data, 0).ok());
  REQUIRE(buffer_list.read(nullptr, 0).ok());

  Buffer buff1, buff2;
  const char data1[3] = {1, 2, 3}, data2[4] = {4, 5, 6, 7};
  REQUIRE(buff1.write(data1, sizeof(data1)).ok());
  REQUIRE(buff2.write(data2, sizeof(data2)).ok());
  REQUIRE(buffer_list.add_buffer(std::move(buff1)).ok());
  REQUIRE(buffer_list.add_buffer(std::move(buff2)).ok());

  REQUIRE(buffer_list.read(data, 2).ok());
  REQUIRE(data[0] == 1);
  REQUIRE(data[1] == 2);
  REQUIRE(buffer_list.read(data + 2, 2).ok());
  REQUIRE(data[2] == 3);
  REQUIRE(data[3] == 4);
  REQUIRE(buffer_list.read(data + 4, 3).ok());
  REQUIRE(data[4] == 5);
  REQUIRE(data[5] == 6);
  REQUIRE(data[6] == 7);
  REQUIRE(!buffer_list.read(data, 1).ok());

  std::memset(data, 0, 10);
  buffer_list.reset_offset();
  REQUIRE(buffer_list.read(data, 7).ok());
  REQUIRE(!std::memcmp(data, &data1[0], sizeof(data1)));
  REQUIRE(!std::memcmp(data + 3, &data2[0], sizeof(data2)));

  uint64_t num_read = 0;
  std::memset(data, 0, 10);
  buffer_list.reset_offset();
  REQUIRE(buffer_list.read_at_most(data, 2, &num_read).ok());
  REQUIRE(num_read == 2);
  REQUIRE(!std::memcmp(data, &data1[0], 2));

  num_read = 0;
  std::memset(data, 0, 10);
  buffer_list.reset_offset();
  REQUIRE(buffer_list.read_at_most(data, 100, &num_read).ok());
  REQUIRE(num_read == 7);
  REQUIRE(!std::memcmp(data, &data1[0], sizeof(data1)));
  REQUIRE(!std::memcmp(data + 3, &data2[0], sizeof(data2)));

  num_read = 0;
  std::memset(data, 0, 10);
  buffer_list.reset_offset();
  REQUIRE(buffer_list.read_at_most(data, 0, &num_read).ok());
  REQUIRE(num_read == 0);

  std::free(data);
}
