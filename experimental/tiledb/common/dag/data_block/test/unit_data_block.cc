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
#include "experimental/tiledb/common/dag/data_block/data_block.h"

using namespace tiledb::common;

void db_test_0(DataBlock& db) {
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

void db_test_1(const DataBlock& db) {
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

TEST_CASE("Ports: Test create DataBlock", "[ports]") {
  auto db = DataBlock();
  db_test_0(db);
  db_test_1(db);
}
