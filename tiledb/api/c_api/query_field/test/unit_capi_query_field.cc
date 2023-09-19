/**
 * @file tiledeb/api/c_api/query_field/test/unit_capi_query_field.cc
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
 * Tests the query field C API.
 */

#include <test/support/tdb_catch.h>
#include "test/support/src/vfs_helpers.h"
#include "tiledb/api/c_api/config/config_api_internal.h"
#include "tiledb/api/c_api/context/context_api_internal.h"
#include "tiledb/api/c_api/query_field/query_field_api_external_experimental.h"

using namespace tiledb::test;

struct QueryFieldFx : TemporaryDirectoryFixture {
  QueryFieldFx() {
  }
  void create_sparse_array(const std::string& path);
};

void QueryFieldFx::create_sparse_array(const std::string& array_name) {
  // Create dimensions
  uint64_t tile_extents[] = {2, 2};
  uint64_t dim_domain[] = {1, 10, 1, 10};

  tiledb_dimension_t* d1;
  int rc = tiledb_dimension_alloc(
      ctx, "d1", TILEDB_UINT64, &dim_domain[0], &tile_extents[0], &d1);
  CHECK(rc == TILEDB_OK);
  tiledb_dimension_t* d2;
  rc = tiledb_dimension_alloc(
      ctx, "d2", TILEDB_UINT64, &dim_domain[2], &tile_extents[1], &d2);
  CHECK(rc == TILEDB_OK);

  // Create domain
  tiledb_domain_t* domain;
  rc = tiledb_domain_alloc(ctx, &domain);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_domain_add_dimension(ctx, domain, d1);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_domain_add_dimension(ctx, domain, d2);
  CHECK(rc == TILEDB_OK);

  // Create attributes
  tiledb_attribute_t* a;
  rc = tiledb_attribute_alloc(ctx, "a", TILEDB_INT32, &a);
  CHECK(rc == TILEDB_OK);
  tiledb_attribute_t* b;
  rc = tiledb_attribute_alloc(ctx, "b", TILEDB_INT32, &b);
  CHECK(rc == TILEDB_OK);
  tiledb_attribute_t* c;
  rc = tiledb_attribute_alloc(ctx, "c", TILEDB_STRING_ASCII, &c);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_attribute_set_cell_val_num(ctx, c, TILEDB_VAR_NUM);
  CHECK(rc == TILEDB_OK);
  tiledb_attribute_t* d;
  rc = tiledb_attribute_alloc(ctx, "d", TILEDB_STRING_UTF8, &d);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_attribute_set_cell_val_num(ctx, d, TILEDB_VAR_NUM);
  CHECK(rc == TILEDB_OK);

  // Create array schema
  tiledb_array_schema_t* array_schema;
  rc = tiledb_array_schema_alloc(ctx, TILEDB_SPARSE, &array_schema);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_cell_order(ctx, array_schema, TILEDB_ROW_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_tile_order(ctx, array_schema, TILEDB_ROW_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_capacity(ctx, array_schema, 4);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_domain(ctx, array_schema, domain);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_schema_add_attribute(ctx, array_schema, a);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_schema_add_attribute(ctx, array_schema, b);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_schema_add_attribute(ctx, array_schema, c);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_schema_add_attribute(ctx, array_schema, d);
  CHECK(rc == TILEDB_OK);

  // Check array schema
  rc = tiledb_array_schema_check(ctx, array_schema);
  CHECK(rc == TILEDB_OK);

  // Create array
  rc = tiledb_array_create(ctx, array_name.c_str(), array_schema);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_attribute_free(&a);
  tiledb_attribute_free(&b);
  tiledb_dimension_free(&d1);
  tiledb_dimension_free(&d2);
  tiledb_domain_free(&domain);
  tiledb_array_schema_free(&array_schema);
}

TEST_CASE_METHOD(
    QueryFieldFx, "C API: argument validation", "[capi][query_field][args]") {
  std::string array_name = temp_dir_ + "queryfield_array";
  create_sparse_array(array_name);

  tiledb_array_t* array;
  REQUIRE(tiledb_array_alloc(ctx, array_name.c_str(), &array) == TILEDB_OK);
  REQUIRE(tiledb_array_open(ctx, array, TILEDB_READ) == TILEDB_OK);

  tiledb_query_t* query;
  REQUIRE(tiledb_query_alloc(ctx, array, TILEDB_READ, &query) == TILEDB_OK);

  REQUIRE(tiledb_query_set_layout(ctx, query, TILEDB_UNORDERED) == TILEDB_OK);

  int64_t dom[] = {1, 9, 1, 2};
  REQUIRE(tiledb_query_set_subarray(ctx, query, &dom) == TILEDB_OK);

  // Clean up
  CHECK(tiledb_array_close(ctx, array) == TILEDB_OK);
  tiledb_array_free(&array);
  tiledb_query_free(&query);
}
