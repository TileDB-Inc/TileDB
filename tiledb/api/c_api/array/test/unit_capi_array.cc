/**
 * @file tiledb/api/c_api/array/test/unit_capi_array.cc
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
 * Validates the arguments for the Array C API.
 */

#define CATCH_CONFIG_MAIN
#include <test/support/tdb_catch.h>
#include "tiledb/api/c_api_test_support/testsupport_capi_context.h"
#include "tiledb/sm/c_api/tiledb.h"

#include "../array_api_external.h"
#include "../array_api_internal.h"

using namespace tiledb::api::test_support;

const char* TEST_URI = "unit_capi_array";

TEST_CASE(
    "C API: tiledb_array_delete_fragments_list argument validation",
    "[capi][array]") {
  ordinary_context x;
  const char* fragment_uris[1] = {"unit_capi_array/__fragments/fragment_uri"};
  /*
   * No "success" section here; too much overhead to set up.
   */
  SECTION("null context") {
    auto rc{tiledb_array_delete_fragments_list(
        nullptr, TEST_URI, fragment_uris, 1)};
    CHECK(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null uri") {
    auto rc{tiledb_array_delete_fragments_list(
        x.context, nullptr, fragment_uris, 1)};
    CHECK(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("empty uri") {
    const char* empty_uri = "";
    auto rc{tiledb_array_delete_fragments_list(
        x.context, empty_uri, fragment_uris, 1)};
    CHECK(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null fragment_uris") {
    const char* null_fragment_uris[1] = {nullptr};
    auto rc{tiledb_array_delete_fragments_list(
        x.context, TEST_URI, null_fragment_uris, 1)};
    CHECK(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("empty uri") {
    const char* empty_fragment_uris[1] = {""};
    auto rc{tiledb_array_delete_fragments_list(
        x.context, TEST_URI, empty_fragment_uris, 1)};
    CHECK(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("invalid num_fragments") {
    auto rc{tiledb_array_delete_fragments_list(
        x.context, TEST_URI, fragment_uris, 0)};
    CHECK(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE(
    "C API: tiledb_array_schema_load argument validation", "[capi][array]") {
  capi_return_t rc;
  ordinary_context ctx{};
  const char* array_uri = "array_uri";
  tiledb_array_schema_handle_t* schema{};
  /*
   * No "success" section here; too much overhead to set up.
   */
  SECTION("null context") {
    rc = tiledb_array_schema_load(nullptr, array_uri, &schema);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null array_uri") {
    rc = tiledb_array_schema_load(ctx.context, nullptr, &schema);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null schema") {
    rc = tiledb_array_schema_load(ctx.context, array_uri, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}
