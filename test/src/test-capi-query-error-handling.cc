/**
 * @file test-capi-query-error-handling.cc
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
 *
 * Tests catching errors for queries with invalid options set.
 */

#include "test/support/src/helpers.h"
#include "test/support/src/vfs_helpers.h"
#include "tiledb/api/c_api/context/context_api_internal.h"
#include "tiledb/sm/c_api/tiledb.h"
#include "tiledb/sm/c_api/tiledb_experimental.h"
#include "tiledb/sm/c_api/tiledb_struct_def.h"
#include "tiledb/sm/enums/encryption_type.h"

#ifdef _WIN32
#include "tiledb/sm/filesystem/win.h"
#else
#include "tiledb/sm/filesystem/posix.h"
#endif

#include <test/support/tdb_catch.h>
#include <iostream>
#include <string>
#include <unordered_map>

using namespace tiledb::sm;
using namespace tiledb::test;

TEST_CASE_METHOD(
    TemporaryDirectoryFixture,
    "Error when setting invalid layout on sparse write",
    "[capi][query]") {
  auto ctx = get_ctx();

  // Create the array.
  uint64_t domain[2]{0, 3};
  uint64_t x_tile_extent{4};
  auto array_schema = create_array_schema(
      ctx,
      TILEDB_SPARSE,
      {"x"},
      {TILEDB_UINT64},
      {&domain[0]},
      {&x_tile_extent},
      {"a"},
      {TILEDB_FLOAT64},
      {1},
      {tiledb::test::Compressor(TILEDB_FILTER_NONE, -1)},
      TILEDB_ROW_MAJOR,
      TILEDB_ROW_MAJOR,
      4096,
      false);
  auto array_name = create_temporary_array("sparse_array1", array_schema);
  tiledb_array_schema_free(&array_schema);

  // Open array for writing.
  tiledb_array_t* array;
  require_tiledb_ok(tiledb_array_alloc(ctx, array_name.c_str(), &array));
  require_tiledb_ok(tiledb_array_open(ctx, array, TILEDB_WRITE));

  // Create write query.
  tiledb_query_t* query;
  require_tiledb_ok(tiledb_query_alloc(ctx, array, TILEDB_WRITE, &query));
  SECTION("ROW_MAJOR") {
    auto rc = tiledb_query_set_layout(ctx, query, TILEDB_ROW_MAJOR);
    REQUIRE(rc != TILEDB_OK);
  }
  SECTION("COL_MAJOR") {
    auto rc = tiledb_query_set_layout(ctx, query, TILEDB_ROW_MAJOR);
    REQUIRE(rc != TILEDB_OK);
  }

  // Clean-up.
  tiledb_query_free(&query);
  tiledb_array_free(&array);
}

TEST_CASE_METHOD(
    TemporaryDirectoryFixture,
    "Error setting invalid layout for dense array",
    "[capi][query]") {
  auto ctx = get_ctx();
  // Create the array.
  uint64_t x_tile_extent{4};
  uint64_t domain[2]{0, 3};
  auto array_schema = create_array_schema(
      ctx,
      TILEDB_DENSE,
      {"x"},
      {TILEDB_UINT64},
      {&domain[0]},
      {&x_tile_extent},
      {"a"},
      {TILEDB_FLOAT64},
      {1},
      {tiledb::test::Compressor(TILEDB_FILTER_NONE, -1)},
      TILEDB_ROW_MAJOR,
      TILEDB_ROW_MAJOR,
      4096,
      false);
  auto array_name = create_temporary_array("dense_array_1", array_schema);
  tiledb_array_schema_free(&array_schema);

  // Open array for writing.
  tiledb_array_t* array;
  require_tiledb_ok(tiledb_array_alloc(ctx, array_name.c_str(), &array));
  require_tiledb_ok(tiledb_array_open(ctx, array, TILEDB_WRITE));

  // Create write query.
  tiledb_query_t* query;
  require_tiledb_ok(tiledb_query_alloc(ctx, array, TILEDB_WRITE, &query));
  auto rc = tiledb_query_set_layout(ctx, query, TILEDB_UNORDERED);
  REQUIRE(rc != TILEDB_OK);

  // Clean-up.
  tiledb_query_free(&query);
  tiledb_array_free(&array);
}
