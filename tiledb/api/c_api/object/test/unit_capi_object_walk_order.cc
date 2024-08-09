/**
 * @file tiledb/api/c_api/object/test/unit_capi_object.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022 TileDB Inc.
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
 * Tests the object C API.
 */

#include <catch2/catch_test_macros.hpp>
#include <catch2/generators/catch_generators.hpp>

#include "tiledb/api/c_api/object/object_api_external.h"

#include <string>

struct TestCase {
  TestCase(tiledb_walk_order_t order, const char* name, int defined_as)
      : order_(order)
      , name_(name)
      , defined_as_(defined_as) {
  }

  tiledb_walk_order_t order_;
  const char* name_;
  int defined_as_;

  void run() {
    const char* c_str = nullptr;
    tiledb_walk_order_t from_str;

    REQUIRE(order_ == defined_as_);

    REQUIRE(tiledb_walk_order_to_str(order_, &c_str) == TILEDB_OK);
    REQUIRE(std::string(c_str) == name_);

    REQUIRE(tiledb_walk_order_from_str(name_, &from_str) == TILEDB_OK);
    REQUIRE(from_str == order_);
  }
};

TEST_CASE("C API: Test object enum", "[capi][enums][object]") {
  // clang-format off
  TestCase test = GENERATE(
    TestCase(TILEDB_PREORDER,   "PREORDER",   0),
    TestCase(TILEDB_POSTORDER,  "POSTORDER",  1));

  DYNAMIC_SECTION("[" << test.name_ << "]") {
    test.run();
  }
}
