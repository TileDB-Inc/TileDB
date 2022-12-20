/**
 * @file tiledeb/api/c_api/filesystem/test/unit_capi_filesystem.cc
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
 * Tests the filesystem C API.
 */

#include <test/support/tdb_catch.h>
#include "tiledb/api/c_api/query/query_api_external.h"

#include <string>

struct TestCase {
  TestCase(tiledb_query_type_t query_type, const char* name, int defined_as)
      : query_type_(query_type)
      , name_(name)
      , defined_as_(defined_as) {
  }

  tiledb_query_type_t query_type_;
  const char* name_;
  int defined_as_;

  void run() {
    const char* c_str = nullptr;
    tiledb_query_type_t from_str;

    REQUIRE(query_type_ == defined_as_);

    REQUIRE(tiledb_query_type_to_str(query_type_, &c_str) == TILEDB_OK);
    REQUIRE(std::string(c_str) == name_);

    REQUIRE(tiledb_query_type_from_str(name_, &from_str) == TILEDB_OK);
    REQUIRE(from_str == query_type_);
  }
};

TEST_CASE("C API: Test object enum", "[capi][enums][object]") {
  // clang-format off
  TestCase test = GENERATE(
      TestCase(TILEDB_READ,             "READ",             0),
      TestCase(TILEDB_WRITE,            "WRITE",            1),
      TestCase(TILEDB_DELETE,           "DELETE",           2),
      TestCase(TILEDB_UPDATE,           "UPDATE",           3),
      TestCase(TILEDB_MODIFY_EXCLUSIVE, "MODIFY_EXCLUSIVE", 4));

  DYNAMIC_SECTION("[" << test.name_ << "]") {
    test.run();
  }
}
