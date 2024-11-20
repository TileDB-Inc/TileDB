/**
 * @file tiledb/api/cpp_api_support/test/unit_cppapi_string.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2023 TileDB, Inc.
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
 * This file defines tests for the CAPIString class of the TileDB C++ API.
 */

#include <test/support/tdb_catch.h>

#include "tiledb/api/c_api/string/string_api_internal.h"
#include "tiledb/sm/cpp_api/capi_string.h"

using namespace tiledb::api;
using namespace tiledb::impl;

TEST_CASE(
    "CAPIString: Test constructor with null parameter throws",
    "[capi_string][null-param]") {
  REQUIRE_THROWS_AS(CAPIString(nullptr), std::invalid_argument);
}

TEST_CASE(
    "CAPIString: Test constructor with non-null parameter pointing to null "
    "handle throws",
    "[capi_string][null-param-ptr]") {
  tiledb_string_t* string = nullptr;
  REQUIRE_THROWS_AS(CAPIString(&string), std::invalid_argument);
}

TEST_CASE(
    "CAPIString: Test creating string handle and getting its value",
    "[capi_string][get]") {
  const std::string test_string = "hello";
  tiledb_string_t* handle =
      make_handle<tiledb_string_t>(test_string);
  std::string result;

  SECTION("convert_to_string") {
    auto result_maybe = convert_to_string(&handle);
    REQUIRE(result_maybe);
    result = *result_maybe;
  }
  SECTION("CAPIString") {
    result = CAPIString(&handle).str();
  }

  REQUIRE(handle == nullptr);
  REQUIRE(result == test_string);
}

#ifndef HAVE_SANITIZER
TEST_CASE(
    "CAPIString: Test that accessing freed handle fails",
    "[capi_string][freed_handle]") {
  const std::string test_string = "hello";
  tiledb_string_t* handle =
      make_handle<tiledb_string_t>(test_string);
  tiledb_string_t* handle_copy = handle;
  std::ignore = convert_to_string(&handle);
  const char* chars = nullptr;
  size_t length = 0;
  REQUIRE(tiledb_string_view(handle_copy, &chars, &length) == TILEDB_ERR);
}
#endif

TEST_CASE(
    "CAPIString: Test convert_to_string with null handle", "[capi_string]") {
  tiledb_string_t* handle = nullptr;
  REQUIRE(!convert_to_string(&handle).has_value());
}
