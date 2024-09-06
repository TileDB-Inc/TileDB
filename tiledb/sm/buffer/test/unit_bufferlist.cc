/**
 * @file unit_bufferlist.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2024 TileDB, Inc.
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

#include "test/support/src/mem_helpers.h"
#include "tiledb/sm/buffer/buffer.h"
#include "tiledb/sm/buffer/buffer_list.h"

#include <test/support/tdb_catch.h>
#include <iostream>

using namespace tiledb::sm;

TEST_CASE("BufferList: Test append", "[buffer][bufferlist]") {
  auto tracker = tiledb::test::get_test_memory_tracker();
  BufferList buffer_list{tracker};
  REQUIRE(buffer_list.num_buffers() == 0);
  REQUIRE(buffer_list.total_size() == 0);

  auto& buff1{buffer_list.emplace_buffer()};
  auto& buff2{buffer_list.emplace_buffer()};
  const char data1[3] = {1, 2, 3}, data2[4] = {4, 5, 6, 7};
  buff1.assign(span(data1, sizeof(data1)));
  buff2.assign(span(data2, sizeof(data2)));
  REQUIRE(static_cast<span<const char>>(buff1).data() != nullptr);
  REQUIRE(static_cast<span<const char>>(buff2).data() != nullptr);

  REQUIRE(buffer_list.num_buffers() == 2);
  REQUIRE(buffer_list.total_size() == sizeof(data1) + sizeof(data2));

  span<const char> b1 = buffer_list.get_buffer(0);
  span<const char> b2 = buffer_list.get_buffer(1);
  REQUIRE_THROWS(buffer_list.get_buffer(2));
  REQUIRE(b1.size() == sizeof(data1));
  REQUIRE(b2.size() == sizeof(data2));
  REQUIRE_FALSE(std::memcmp(b1.data(), &data1[0], sizeof(data1)));
  REQUIRE_FALSE(std::memcmp(b2.data(), &data2[0], sizeof(data2)));
}

TEST_CASE("BufferList: Test read", "[buffer][bufferlist]") {
  BufferList buffer_list{tiledb::test::get_test_memory_tracker()};
  char data[10];

  REQUIRE_THROWS(buffer_list.read(data, 1));
  REQUIRE_NOTHROW(buffer_list.read(data, 0));
  REQUIRE_NOTHROW(buffer_list.read(nullptr, 0));

  auto& buff1{buffer_list.emplace_buffer()};
  auto& buff2{buffer_list.emplace_buffer()};
  const char data1[3] = {1, 2, 3}, data2[4] = {4, 5, 6, 7};
  buff1.assign(span(data1, sizeof(data1)));
  buff2.assign(span(data2, sizeof(data2)));

  REQUIRE_NOTHROW(buffer_list.read(data, 2));
  REQUIRE(data[0] == 1);
  REQUIRE(data[1] == 2);
  REQUIRE_NOTHROW(buffer_list.read(data + 2, 2));
  REQUIRE(data[2] == 3);
  REQUIRE(data[3] == 4);
  REQUIRE_NOTHROW(buffer_list.read(data + 4, 3));
  REQUIRE(data[4] == 5);
  REQUIRE(data[5] == 6);
  REQUIRE(data[6] == 7);
  REQUIRE_THROWS(buffer_list.read(data, 1));

  std::memset(data, 0, 10);
  buffer_list.reset_offset();
  REQUIRE_NOTHROW(buffer_list.read(data, 7));
  REQUIRE(!std::memcmp(data, &data1[0], sizeof(data1)));
  REQUIRE(!std::memcmp(data + 3, &data2[0], sizeof(data2)));

  uint64_t num_read = 0;
  std::memset(data, 0, 10);
  buffer_list.reset_offset();
  num_read = buffer_list.read_at_most(data, 2);
  REQUIRE(num_read == 2);
  REQUIRE(!std::memcmp(data, &data1[0], 2));

  num_read = 0;
  std::memset(data, 0, 10);
  buffer_list.reset_offset();
  num_read = buffer_list.read_at_most(data, 100);
  REQUIRE(num_read == 7);
  REQUIRE(!std::memcmp(data, &data1[0], sizeof(data1)));
  REQUIRE(!std::memcmp(data + 3, &data2[0], sizeof(data2)));

  num_read = 0;
  std::memset(data, 0, 10);
  buffer_list.reset_offset();
  num_read = buffer_list.read_at_most(data, 0);
  REQUIRE(num_read == 0);
}
