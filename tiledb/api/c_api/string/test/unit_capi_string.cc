/**
 * @file tiledb/api/c_api/string/test/unit_capi_string.cc
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
 */

#include <test/support/tdb_catch.h>
#include "../string_api_external.h"
#include "../string_api_internal.h"

using namespace tiledb::api;

TEST_CASE("C API: tiledb_string_view argument validation", "[capi][string]") {
  auto s{make_handle<tiledb_string_handle_t>("foo")};
  const char* data{};
  size_t length{};
  SECTION("null string handle") {
    auto rc{tiledb_string_view(nullptr, &data, &length)};
    CHECK(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null data") {
    auto rc{tiledb_string_view(s, nullptr, &length)};
    CHECK(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null length") {
    auto rc{tiledb_string_view(s, &data, nullptr)};
    CHECK(tiledb_status(rc) == TILEDB_ERR);
  }
  break_handle(s);
}

TEST_CASE("C API: tiledb_string_view basic behavior", "[capi][string]") {
  auto s{make_handle<tiledb_string_handle_t>("foo")};
  const char* data{};
  size_t length{};
  auto rc{tiledb_string_view(s, &data, &length)};
  REQUIRE(tiledb_status(rc) == TILEDB_OK);
  REQUIRE(length == 3);
  CHECK(std::string(data, length) == "foo");
  break_handle(s);
}

TEST_CASE("C API: tiledb_string_free argument validation", "[capi][string]") {
  /*
   * `void` returns mean we have no return status to check
   */
  SECTION("null argument") {
    capi_return_t rc{tiledb_string_free(nullptr)};
    CHECK(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("non-null argument, null string handle") {
    tiledb_string_handle_t* string{nullptr};
    capi_return_t rc{tiledb_string_free(&string)};
    CHECK(tiledb_status(rc) == TILEDB_ERR);
  }
}
