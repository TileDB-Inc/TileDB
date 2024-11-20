/**
 * @file tiledb/api/c_api/error/test/unit_capi_error.cc
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
#include "../error_api_external.h"
#include "../error_api_internal.h"

using namespace tiledb::api;

TEST_CASE("C API: tiledb_error_message argument validation", "[capi][error]") {
  SECTION("null error") {
    const char* p;
    auto rc{tiledb_error_message(nullptr, &p)};
    CHECK(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null error message") {
    auto error{make_handle<tiledb_error_handle_t>("foo")};
    REQUIRE(error != nullptr);
    auto rc{tiledb_error_message(error, nullptr)};
    CHECK(tiledb_status(rc) == TILEDB_ERR);
    tiledb_error_free(&error);
  }
}

TEST_CASE("C API: tiledb_error_free argument validation", "[capi][error]") {
  /*
   * `void` returns mean we have no return status to check
   */
  SECTION("null error") {
    CHECK_NOTHROW(tiledb_error_free(nullptr));
  }
  SECTION("non-null bad error") {
    tiledb_error_handle_t* error{nullptr};
    REQUIRE_NOTHROW(tiledb_error_free(&error));
  }
}
