/**
 * @file unit_pool_allocator.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022 TileDB, Inc.
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
 * Tests the data_block class.
 */

#include "unit_data_block.h"
#include <memory>
#include "experimental/tiledb/common/dag/data_block/data_block.h"

using namespace tiledb::common;

template <class DB>
void db_test_0(DB& db) {
  auto a = db.begin();
  auto b = db.cbegin();
  auto c = db.end();
  auto d = db.cend();

  REQUIRE(a == b);
  REQUIRE(++a == ++b);
  REQUIRE(a++ == b++);
  REQUIRE(a == b);
  REQUIRE(++a != b);
  REQUIRE(a == ++b);
  REQUIRE(c == d);
  auto e = c + 5;
  auto f = d + 5;
  REQUIRE(c == e - 5);
  REQUIRE(d == f - 5);
  REQUIRE(e == f);
  REQUIRE(e - 5 == f - 5);
  auto g = a + 1;
  REQUIRE(g > a);
  REQUIRE(g >= a);
  REQUIRE(a < g);
  REQUIRE(a <= g);
}

template <class DB>
void db_test_1(const DB& db) {
  auto a = db.begin();
  auto b = db.cbegin();
  auto c = db.end();
  auto d = db.cend();

  REQUIRE(a == b);
  REQUIRE(++a == ++b);
  REQUIRE(a++ == b++);
  REQUIRE(a == b);
  REQUIRE(++a != b);
  REQUIRE(a == ++b);
  REQUIRE(c == d);
  auto e = c + 5;
  auto f = d + 5;
  REQUIRE(c == e - 5);
  REQUIRE(d == f - 5);
  REQUIRE(e == f);
  REQUIRE(e - 5 == f - 5);
  auto g = a + 1;
  REQUIRE(g > a);
  REQUIRE(g >= a);
  REQUIRE(a < g);
  REQUIRE(a <= g);
}

TEST_CASE("DataBlock: Test create DataBlock", "[data_block]") {
  auto db = DataBlock();
  db_test_0(db);
  db_test_1(db);
}

TEST_CASE(
    "DataBlock: Test create DataBlock with std::allocator<std::byte>",
    "[data_block]") {
  auto db = DataBlockImpl<std::allocator<std::byte>>{};
  db_test_0(db);
  db_test_1(db);
}

TEST_CASE("DataBlock: Iterate through data_block", "[data_block]") {
  auto db = DataBlockImpl<std::allocator<std::byte>>{};
  for (auto& j : db) {
    j = std::byte{255};
  }
  auto e = std::find_if_not(
      db.begin(), db.end(), [](auto a) { return std::byte{255} == a; });
  CHECK(e == db.end());

  for (auto& j : db) {
    j = std::byte{13};
  }
  auto f = std::find_if_not(
      db.begin(), db.end(), [](auto a) { return std::byte{13} == a; });
  CHECK(f == db.end());
}

TEST_CASE("DataBlock: Iterate through 8 data_blocks", "[data_block]") {
  for (size_t i = 0; i < 8; ++i) {
    auto db = DataBlockImpl<std::allocator<std::byte>>{};
    for (auto& j : db) {
      j = std::byte{255};
    }
    auto e = std::find_if_not(
        db.begin(), db.end(), [](auto a) { return std::byte{255} == a; });
    CHECK(e == db.end());

    for (auto& j : db) {
      j = std::byte{13};
    }
    auto f = std::find_if_not(
        db.begin(), db.end(), [](auto a) { return std::byte{13} == a; });
    CHECK(f == db.end());
  }
}
