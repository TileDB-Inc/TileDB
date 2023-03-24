/**
 * @file tiledb/api/c_api/dictionary/test/unit_capi_dictionary.cc
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
 */

#define CATCH_CONFIG_MAIN
#include <test/support/tdb_catch.h>
#include "../../../c_api_test_support/testsupport_capi_context.h"
#include "../dictionary_api_internal.h"
using namespace tiledb::api::test_support;

TEST_CASE(
    "C API: tiledb_dictionary_alloc", "[capi][dictionary]") {
  ordinary_context ctx{};
  tiledb_dictionary_t* dict;

  SECTION("success") {
    auto rc = tiledb_dictionary_alloc(ctx.context, TILEDB_UINT32, &dict);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
    tiledb_dictionary_free(&dict);
  }

  SECTION("null context") {
    auto rc = tiledb_dictionary_alloc(nullptr, TILEDB_UINT32, &dict);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
}
