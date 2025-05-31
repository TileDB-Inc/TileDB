/**
 * @file unit-capi-enumerations.cc
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
 * Tests for the various Enumerations C API errors.
 */

#include <test/support/tdb_catch.h>
#include "test/support/src/helpers.h"
#include "tiledb/sm/c_api/tiledb.h"
#include "tiledb/sm/c_api/tiledb_experimental.h"

#include <iostream>

using namespace tiledb::test;

TEST_CASE(
    "C API: Test invalid attribute for tiledb_attribute_set_enumeration_name",
    "[enumeration][capi][error]") {
  tiledb_ctx_t* const ctx = vanilla_context_c();

  auto rc = tiledb_attribute_set_enumeration_name(ctx, nullptr, "enmr_name");
  REQUIRE(rc == TILEDB_ERR);
}

TEST_CASE(
    "C API: Test invalid attribute for tiledb_attribute_get_enumeration_name",
    "[enumeration][capi][error]") {
  tiledb_ctx_t* const ctx = vanilla_context_c();

  tiledb_string_t* name;
  auto rc = tiledb_attribute_get_enumeration_name(ctx, nullptr, &name);
  REQUIRE(rc == TILEDB_ERR);
}

TEST_CASE(
    "C API: Test invalid array schema for tiledb_array_schema_add_enumeration",
    "[enumeration][capi][error]") {
  tiledb_ctx_t* const ctx = vanilla_context_c();

  tiledb_enumeration_t* enmr;
  uint32_t values[5] = {1, 2, 3, 4, 5};
  auto rc = tiledb_enumeration_alloc(
      ctx,
      "an_enumeration",
      TILEDB_UINT32,
      1,
      0,
      values,
      sizeof(uint32_t) * 5,
      nullptr,
      0,
      &enmr);
  REQUIRE(rc == TILEDB_OK);

  rc = tiledb_array_schema_add_enumeration(ctx, nullptr, enmr);
  REQUIRE(rc == TILEDB_ERR);
}

TEST_CASE(
    "C API: Test invalid array for tiledb_array_get_enumeration",
    "[enumeration][capi][error]") {
  tiledb_ctx_t* const ctx = vanilla_context_c();

  tiledb_enumeration_t* enmr;
  auto rc = tiledb_array_get_enumeration(ctx, nullptr, "an_enumeration", &enmr);
  REQUIRE(rc == TILEDB_ERR);
}

TEST_CASE(
    "C API: Test invalid enumeration name for tiledb_array_get_enumeration",
    "[enumeration][capi][error]") {
  tiledb_ctx_t* const ctx = vanilla_context_c();

  tiledb_array_t* array;
  auto rc = tiledb_array_alloc(ctx, "array_uri", &array);
  REQUIRE(rc == TILEDB_OK);

  tiledb_enumeration_t* enmr;
  rc = tiledb_array_get_enumeration(ctx, array, nullptr, &enmr);
  REQUIRE(rc == TILEDB_ERR);
}
