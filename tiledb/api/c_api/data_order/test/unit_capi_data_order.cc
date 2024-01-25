/**
 * @file tiledb/api/c_api/data_order/test/unit_capi_data_order.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2023 TileDB Inc.
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
 * Tests the data order C API.
 */

#include <test/support/tdb_catch.h>
#include "tiledb/api/c_api/data_order/data_order_api_external.h"

#include <string>

struct TestCase {
  TestCase(tiledb_data_order_t data_order, const char* name, int defined_as)
      : data_order_(data_order)
      , name_(name)
      , defined_as_(defined_as) {
  }

  tiledb_data_order_t data_order_;
  const char* name_;
  int defined_as_;

  void run() {
    const char* c_str = nullptr;
    tiledb_data_order_t from_str;

    REQUIRE(data_order_ == defined_as_);

    REQUIRE(tiledb_data_order_to_str(data_order_, &c_str) == TILEDB_OK);
    REQUIRE(std::string(c_str) == name_);

    REQUIRE(tiledb_data_order_from_str(name_, &from_str) == TILEDB_OK);
    REQUIRE(from_str == data_order_);
  }
};

TEST_CASE(
    "C API: Test data order enum string conversion",
    "[capi][enums][data_order]") {
  // clang-format off
  TestCase test = GENERATE(
    TestCase(TILEDB_UNORDERED_DATA,     "unordered",        0),
    TestCase(TILEDB_INCREASING_DATA,    "increasing",       1),
    TestCase(TILEDB_DECREASING_DATA,    "decreasing",       2)); 
  DYNAMIC_SECTION("[" << test.name_ << "]") {
    test.run();
  }
}
