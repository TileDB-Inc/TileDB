/**
 * @file test-cppapi-ndrectangle.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2024 TileDB, Inc.
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
 * Tests the NDRectangle C++ API
 */

#include "test/support/src/helpers.h"
#include "test/support/src/vfs_helpers.h"
#include "tiledb/sm/cpp_api/tiledb"
#include "tiledb/sm/cpp_api/tiledb_experimental"

#include <catch2/catch_test_macros.hpp>

using namespace tiledb::test;

TEST_CASE("NDRectangle - Basic", "[cppapi][ArraySchema][NDRectangle]") {
  // Create the C++ context.
  tiledb::Context ctx_;
  tiledb::Domain domain(ctx_);
  auto d1 = tiledb::Dimension::create<int32_t>(ctx_, "x", {{0, 100}}, 10);
  auto d2 = tiledb::Dimension::create<int32_t>(ctx_, "y", {{0, 100}}, 10);
  domain.add_dimension(d1);
  domain.add_dimension(d2);

  tiledb::NDRectangle ndrect(ctx_, domain);

  int range_one[] = {10, 20};
  ndrect.set_range(0, range_one[0], range_one[1]);

  int range_two[] = {30, 40};
  ndrect.set_range(1, range_two[0], range_two[1]);

  // Get range
  auto range = ndrect.range<int>(0);
  CHECK(range[0] == 10);
  CHECK(range[1] == 20);
  range = ndrect.range<int>(1);
  CHECK(range[0] == 30);
  CHECK(range[1] == 40);

  range = ndrect.range<int>("x");
  CHECK(range[0] == 10);
  CHECK(range[1] == 20);
  range = ndrect.range<int>("y");
  CHECK(range[0] == 30);
  CHECK(range[1] == 40);
}

TEST_CASE("NDRectangle - Errors", "[cppapi][ArraySchema][NDRectangle]") {
  tiledb::Context ctx;

  // Create a domain
  tiledb::Domain domain(ctx);
  auto d1 = tiledb::Dimension::create<int32_t>(ctx, "d1", {{1, 10}}, 5);
  auto d2 = tiledb::Dimension::create(
      ctx, "d2", TILEDB_STRING_ASCII, nullptr, nullptr);
  domain.add_dimension(d1);
  domain.add_dimension(d2);

  // Create an NDRectangle
  tiledb::NDRectangle ndrect(ctx, domain);

  // Set range with non-existent dimension
  CHECK_THROWS(ndrect.set_range(2, 1, 2));
  CHECK_THROWS(ndrect.set_range("d3", 1, 2));

  // Set too small range type
  CHECK_THROWS(ndrect.set_range<uint8_t>(0, 1, 2));
  CHECK_THROWS(ndrect.set_range<uint8_t>("d1", 1, 2));

  // Set range out of order
  CHECK_THROWS(ndrect.set_range(0, 2, 1));
  CHECK_THROWS(ndrect.set_range("d2", "bbb", "aaa"));
}
