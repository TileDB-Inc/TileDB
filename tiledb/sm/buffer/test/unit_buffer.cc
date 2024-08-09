/**
 * @file unit_buffer.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2021 TileDB, Inc.
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
 * Tests the `Buffer` class.
 */

#include "tiledb/sm/buffer/buffer.h"

#include <catch2/catch_test_macros.hpp>
#include <iostream>

using namespace tiledb::common;
using namespace tiledb::sm;

TEST_CASE("Buffer: Test default constructor with write void*", "[buffer]") {
  // Write a char array
  Status st;
  char data[3] = {1, 2, 3};
  auto buff = make_shared<Buffer>(HERE());
  CHECK(buff->size() == 0);

  st = buff->write(data, sizeof(data));
  REQUIRE(st.ok());
  CHECK(buff->offset() == 3);
  CHECK(buff->size() == sizeof(data));
  CHECK(buff->alloced_size() == 3);
  buff->reset_offset();
  CHECK(buff->offset() == 0);

  // Read a single char value
  char val = 0;
  st = buff->read(&val, sizeof(char));
  REQUIRE(st.ok());
  CHECK(val == 1);
  CHECK(buff->offset() == 1);

  // Read two values
  char readtwo[2] = {0, 0};
  st = buff->read(readtwo, 2);
  REQUIRE(st.ok());
  CHECK(readtwo[0] == 2);
  CHECK(readtwo[1] == 3);
  CHECK(buff->offset() == 3);

  // Reallocate
  st = buff->realloc(10);
  REQUIRE(st.ok());
  CHECK(buff->size() == 3);
  CHECK(buff->alloced_size() == 10);
  CHECK(buff->offset() == 3);

  // Test copy constructor
  Buffer buff2 = *buff;
  CHECK(buff->size() == buff2.size());
  CHECK(buff->alloced_size() == buff2.alloced_size());
  CHECK(buff->offset() == buff2.offset());
  CHECK(buff->owns_data() == buff2.owns_data());
  if (buff->data() != nullptr)
    CHECK(!std::memcmp(buff->data(), buff2.data(), buff->alloced_size()));

  // Test copy assignment
  Buffer buff3;
  buff3 = *buff;
  CHECK(buff->size() == buff3.size());
  CHECK(buff->alloced_size() == buff3.alloced_size());
  CHECK(buff->offset() == buff3.offset());
  CHECK(buff->owns_data() == buff3.owns_data());
  if (buff->data() != nullptr)
    CHECK(!std::memcmp(buff->data(), buff3.data(), buff->alloced_size()));
}

TEST_CASE("Buffer: Test swap", "[buffer]") {
  // Write a char array
  Status st;
  char data1[3] = {1, 2, 3};
  Buffer buff1;
  st = buff1.write(data1, sizeof(data1));
  REQUIRE(st.ok());
  CHECK(buff1.owns_data());
  CHECK(buff1.offset() == 3);
  CHECK(buff1.size() == sizeof(data1));
  CHECK(buff1.alloced_size() == 3);
  CHECK(std::memcmp(buff1.data(), &data1, sizeof(data1)) == 0);

  char data2[5] = {4, 5, 6, 7, 8};
  Buffer buff2;
  st = buff2.write(data2, sizeof(data2));
  CHECK(buff2.owns_data());
  CHECK(std::memcmp(buff2.data(), &data2, sizeof(data2)) == 0);

  buff1.swap(buff2);
  CHECK(buff1.owns_data());
  CHECK(buff1.offset() == 5);
  CHECK(buff1.size() == sizeof(data2));
  CHECK(buff1.alloced_size() == 5);
  CHECK(std::memcmp(buff1.data(), &data2, sizeof(data2)) == 0);
  CHECK(buff2.owns_data());
  CHECK(buff2.offset() == 3);
  CHECK(buff2.size() == sizeof(data1));
  CHECK(buff2.alloced_size() == 3);
  CHECK(std::memcmp(buff2.data(), &data1, sizeof(data1)) == 0);

  char data3[] = {9};
  Buffer buff3(data3, sizeof(data3));
  CHECK(!buff3.owns_data());
  buff1.swap(buff3);
  CHECK(!buff1.owns_data());
  CHECK(buff1.data() == &data3[0]);
  CHECK(buff1.offset() == 0);
  CHECK(buff1.size() == sizeof(data3));
  CHECK(buff1.alloced_size() == 0);
  CHECK(buff3.owns_data());
  CHECK(buff3.offset() == 5);
  CHECK(buff3.size() == sizeof(data2));
  CHECK(buff3.alloced_size() == 5);
  CHECK(std::memcmp(buff3.data(), &data2, sizeof(data2)) == 0);
}

TEST_CASE("Buffer: Test move", "[buffer]") {
  // Write a char array
  Status st;
  char data1[3] = {1, 2, 3};
  Buffer buff1;
  st = buff1.write(data1, sizeof(data1));
  REQUIRE(st.ok());
  CHECK(buff1.owns_data());
  CHECK(buff1.offset() == 3);
  CHECK(buff1.size() == sizeof(data1));
  CHECK(buff1.alloced_size() == 3);
  CHECK(std::memcmp(buff1.data(), &data1, sizeof(data1)) == 0);

  // Move constructor
  Buffer b = std::move(buff1);
  CHECK(b.owns_data());
  CHECK(b.offset() == 3);
  CHECK(b.size() == 3);
  CHECK(b.alloced_size() == 3);
  CHECK(std::memcmp(b.data(), &data1, sizeof(data1)) == 0);
  CHECK(buff1.owns_data());
  CHECK(buff1.offset() == 0);
  CHECK(buff1.size() == 0);
  CHECK(buff1.alloced_size() == 0);
  CHECK(buff1.data() == nullptr);

  // Move-assign
  Buffer b2;
  b2 = std::move(b);
  CHECK(b2.owns_data());
  CHECK(b2.offset() == 3);
  CHECK(b2.size() == 3);
  CHECK(b2.alloced_size() == 3);
  CHECK(std::memcmp(b2.data(), &data1, sizeof(data1)) == 0);
  CHECK(b.owns_data());
  CHECK(b.offset() == 0);
  CHECK(b.size() == 0);
  CHECK(b.alloced_size() == 0);
  CHECK(b.data() == nullptr);
}

TEST_CASE("ConstBuffer: Overflow on read", "[buffer][ConstBuffer]") {
  char data[3] = {1, 2, 3};
  auto buff = ConstBuffer(data, 3);

  CHECK(buff.size() == 3);
  buff.advance_offset(2);
  CHECK(buff.offset() == 2);
  uint64_t n = std::numeric_limits<uint64_t>::max() - 1;
  auto st = buff.read(data, n);
  REQUIRE(!st.ok());
}

TEST_CASE("Buffer: Overflow on read", "[buffer][Buffer]") {
  char data[3] = {1, 2, 3};
  auto buff = Buffer(data, 3);

  CHECK(buff.size() == 3);
  buff.advance_offset(2);
  CHECK(buff.offset() == 2);
  uint64_t n = std::numeric_limits<uint64_t>::max() - 1;
  auto st = buff.read(data, n);
  REQUIRE(!st.ok());
}

TEST_CASE("Buffer: Overflow on set_offset 1", "[buffer][Buffer]") {
  char data[3] = {1, 2, 3};
  auto buff = Buffer(data, 3);

  CHECK(buff.size() == 3);
  CHECK(buff.offset() == 0);
  REQUIRE_THROWS(buff.set_offset(4));
  CHECK(buff.offset() <= buff.size());
}

TEST_CASE("Buffer: Overflow on set_offset 2", "[buffer][Buffer]") {
  char data[3] = {1, 2, 3};
  auto buff = Buffer(data, 3);

  CHECK(buff.size() == 3);
  buff.advance_offset(2);
  CHECK(buff.offset() == 2);
  REQUIRE_THROWS(buff.set_offset(4));
  CHECK(buff.offset() <= buff.size());
}

/**
 * `advance_offset` did not have a legacy of documented behavior. Below we treat
 * it as if advancing past the end is the same as advancing to the end.
 */
TEST_CASE("Buffer: advance_offset past end", "[buffer][Buffer]") {
  char data[3] = {1, 2, 3};
  auto buff = Buffer(data, 3);
  CHECK(buff.size() == 3);
  buff.advance_offset(7);
  CHECK(buff.offset() == buff.size());
}

TEST_CASE("Buffer: Overflow on advance_offset", "[buffer][Buffer]") {
  char data[3] = {1, 2, 3};
  auto buff = Buffer(data, 3);
  REQUIRE(buff.size() == 3);
  buff.advance_offset(1);
  REQUIRE(buff.offset() == 1);
  // Max value acts as a signed value of -1 when naively added to the offset
  uint64_t n = std::numeric_limits<uint64_t>::max();
  buff.advance_offset(n);
  CHECK(buff.offset() == buff.size());
}
