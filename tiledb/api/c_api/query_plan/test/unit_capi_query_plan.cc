/**
 * @file tiledeb/api/c_api/query_plan/test/unit_capi_query_plan.cc
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
 * Tests the query_plan C API.
 */

#include <test/support/tdb_catch.h>
#include "external/include/nlohmann/json.hpp"
#include "test/support/src/vfs_helpers.h"
#include "tiledb/api/c_api/query_plan/query_plan_api_external_experimental.h"

using namespace tiledb::test;

struct QueryPlanFx : TemporaryDirectoryFixture {
  void create_dense_array(const std::string& path);
  void create_sparse_array(const std::string& path);
};

void QueryPlanFx::create_dense_array(const std::string& path) {
  // Create array schema
  tiledb_array_schema_t* array_schema;
  int rc = tiledb_array_schema_alloc(ctx, TILEDB_DENSE, &array_schema);
  REQUIRE(rc == TILEDB_OK);

  // Set schema members
  rc = tiledb_array_schema_set_capacity(ctx, array_schema, 10000);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_cell_order(ctx, array_schema, TILEDB_ROW_MAJOR);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_tile_order(ctx, array_schema, TILEDB_ROW_MAJOR);
  REQUIRE(rc == TILEDB_OK);

  // Create dimensions
  tiledb_dimension_t* d1;
  uint64_t dim_domain[] = {1, 10, 1, 10};
  uint64_t extents[] = {5, 5};
  rc = tiledb_dimension_alloc(
      ctx, "dim_1", TILEDB_INT64, &dim_domain[0], &extents[0], &d1);
  REQUIRE(rc == TILEDB_OK);
  tiledb_dimension_t* d2;
  rc = tiledb_dimension_alloc(
      ctx, "dim_2", TILEDB_INT64, &dim_domain[2], &extents[1], &d2);
  REQUIRE(rc == TILEDB_OK);

  // Set domain
  tiledb_domain_t* domain;
  rc = tiledb_domain_alloc(ctx, &domain);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_domain_add_dimension(ctx, domain, d1);
  REQUIRE(rc == TILEDB_OK);
  tiledb_datatype_t domain_type;
  rc = tiledb_domain_get_type(ctx, domain, &domain_type);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(domain_type == TILEDB_INT64);
  rc = tiledb_domain_add_dimension(ctx, domain, d2);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_domain(ctx, array_schema, domain);
  REQUIRE(rc == TILEDB_OK);

  // Add attributes
  tiledb_attribute_t* a1;
  rc = tiledb_attribute_alloc(ctx, "a1", TILEDB_INT32, &a1);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_schema_add_attribute(ctx, array_schema, a1);
  REQUIRE(rc == TILEDB_OK);
  tiledb_attribute_t* a2;
  rc = tiledb_attribute_alloc(ctx, "a2", TILEDB_INT32, &a2);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_schema_add_attribute(ctx, array_schema, a2);
  REQUIRE(rc == TILEDB_OK);

  // Create array
  rc = tiledb_array_create(ctx, path.c_str(), array_schema);
  REQUIRE(rc == TILEDB_OK);

  // Clean up
  tiledb_array_schema_free(&array_schema);
  tiledb_attribute_free(&a1);
  tiledb_attribute_free(&a2);
  tiledb_dimension_free(&d1);
  tiledb_dimension_free(&d2);
  tiledb_domain_free(&domain);
}

void QueryPlanFx::create_sparse_array(const std::string& array_name) {
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
    QueryPlanFx,
    "C API: tiledb_query_get_plan argument validation",
    "[capi][query_plan]") {
  std::string array_name = temp_dir_ + "queryplan_array";
  create_dense_array(array_name);

  tiledb_array_t* array;
  REQUIRE(tiledb_array_alloc(ctx, array_name.c_str(), &array) == TILEDB_OK);
  REQUIRE(tiledb_array_open(ctx, array, TILEDB_READ) == TILEDB_OK);

  tiledb_query_t* query;
  REQUIRE(tiledb_query_alloc(ctx, array, TILEDB_READ, &query) == TILEDB_OK);

  // nullptr context
  tiledb_string_handle_t* string_handle;
  CHECK(
      tiledb_query_get_plan(nullptr, query, &string_handle) ==
      TILEDB_INVALID_CONTEXT);

  // nullptr query
  CHECK(tiledb_query_get_plan(ctx, nullptr, &string_handle) == TILEDB_ERR);

  REQUIRE(tiledb_array_close(ctx, array) == TILEDB_OK);
  tiledb_query_free(&query);
  tiledb_array_free(&array);
}

TEST_CASE_METHOD(
    QueryPlanFx,
    "C API: tiledb_query_get_plan API lifecycle checks",
    "[capi][query_plan]") {
  std::string array_name = temp_dir_ + "queryplan_array";
  create_dense_array(array_name);

  tiledb_array_t* array;
  REQUIRE(tiledb_array_alloc(ctx, array_name.c_str(), &array) == TILEDB_OK);
  REQUIRE(tiledb_array_open(ctx, array, TILEDB_READ) == TILEDB_OK);

  tiledb_query_t* query;
  REQUIRE(tiledb_query_alloc(ctx, array, TILEDB_READ, &query) == TILEDB_OK);

  CHECK(tiledb_query_set_layout(ctx, query, TILEDB_ROW_MAJOR) == TILEDB_OK);

  int64_t dom[] = {1, 2, 1, 2};
  CHECK(tiledb_query_set_subarray(ctx, query, &dom) == TILEDB_OK);

  std::vector<int32_t> d(4);
  uint64_t size = 1;
  CHECK(
      tiledb_query_set_data_buffer(ctx, query, "a1", d.data(), &size) ==
      TILEDB_OK);

  tiledb_string_handle_t* string_handle;
  CHECK(tiledb_query_get_plan(ctx, query, &string_handle) == TILEDB_OK);

  // API lifecycle checks
  // It's not possible to set subarrays, layout or new buffers
  // once the query plan got generated.
  CHECK(tiledb_query_set_subarray(ctx, query, &dom) == TILEDB_ERR);
  CHECK(tiledb_query_set_layout(ctx, query, TILEDB_COL_MAJOR) == TILEDB_ERR);
  CHECK(
      tiledb_query_set_data_buffer(ctx, query, "a2", d.data(), &size) ==
      TILEDB_ERR);

  // But it's possible to set existing buffers and query conditions to
  // accomodate existing query INCOMPLETEs functionality
  CHECK(
      tiledb_query_set_data_buffer(ctx, query, "a1", d.data(), &size) ==
      TILEDB_OK);
  tiledb_query_condition_t* qc;
  CHECK(tiledb_query_condition_alloc(ctx, &qc) == TILEDB_OK);
  int32_t val = 10000;
  CHECK(
      tiledb_query_condition_init(
          ctx, qc, "a1", &val, sizeof(int32_t), TILEDB_LT) == TILEDB_OK);
  CHECK(tiledb_query_set_condition(ctx, query, qc) == TILEDB_OK);

  REQUIRE(tiledb_string_free(&string_handle) == TILEDB_OK);
  REQUIRE(tiledb_array_close(ctx, array) == TILEDB_OK);
  tiledb_query_free(&query);
  tiledb_array_free(&array);
}

TEST_CASE_METHOD(
    QueryPlanFx,
    "C API: Query plan basic bahaviour",
    "[capi][query_plan][basic1]") {
  std::string array_name = temp_dir_ + "queryplan_array";
  create_dense_array(array_name);

  tiledb_array_t* array;
  REQUIRE(tiledb_array_alloc(ctx, array_name.c_str(), &array) == TILEDB_OK);
  REQUIRE(tiledb_array_open(ctx, array, TILEDB_READ) == TILEDB_OK);

  tiledb_query_t* query;
  REQUIRE(tiledb_query_alloc(ctx, array, TILEDB_READ, &query) == TILEDB_OK);

  CHECK(tiledb_query_set_layout(ctx, query, TILEDB_ROW_MAJOR) == TILEDB_OK);

  int64_t dom[] = {1, 2, 1, 2};
  CHECK(tiledb_query_set_subarray(ctx, query, &dom) == TILEDB_OK);

  std::vector<int32_t> d(4);
  uint64_t size = 1;
  CHECK(
      tiledb_query_set_data_buffer(ctx, query, "a1", d.data(), &size) ==
      TILEDB_OK);
  CHECK(
      tiledb_query_set_data_buffer(ctx, query, "a2", d.data(), &size) ==
      TILEDB_OK);

  tiledb_string_handle_t* string_handle;
  const char* data;
  size_t len;
  CHECK(tiledb_query_get_plan(ctx, query, &string_handle) == TILEDB_OK);
  CHECK(tiledb_string_view(string_handle, &data, &len) == TILEDB_OK);

  // This throws if the query plan is not valid JSON
  std::string str_plan(data, len);
  nlohmann::json json_plan = nlohmann::json::parse(str_plan);

  CHECK(json_plan["TileDB Query Plan"]["Array.URI"] == array_name);
  CHECK(json_plan["TileDB Query Plan"]["Array.Type"] == "dense");
  CHECK(
      json_plan["TileDB Query Plan"]["VFS.Backend"] ==
      tiledb::sm::URI(array_name).backend_name());
  CHECK(json_plan["TileDB Query Plan"]["Query.Layout"] == "row-major");
  CHECK(json_plan["TileDB Query Plan"]["Query.Strategy.Name"] == "DenseReader");
  CHECK(
      json_plan["TileDB Query Plan"]["Query.Attributes"] ==
      std::vector({"a2", "a1"}));
  CHECK(
      json_plan["TileDB Query Plan"]["Query.Dimensions"] ==
      std::vector({"dim_1", "dim_2"}));

  REQUIRE(tiledb_string_free(&string_handle) == TILEDB_OK);
  REQUIRE(tiledb_array_close(ctx, array) == TILEDB_OK);
  tiledb_query_free(&query);
  tiledb_array_free(&array);
}

TEST_CASE_METHOD(
    QueryPlanFx,
    "C API: Query plan basic bahaviour 2",
    "[capi][query_plan][basic2]") {
  std::string array_name = temp_dir_ + "queryplan_array";
  create_sparse_array(array_name);

  tiledb_array_t* array;
  REQUIRE(tiledb_array_alloc(ctx, array_name.c_str(), &array) == TILEDB_OK);
  REQUIRE(tiledb_array_open(ctx, array, TILEDB_WRITE) == TILEDB_OK);

  tiledb_query_t* query;
  REQUIRE(tiledb_query_alloc(ctx, array, TILEDB_WRITE, &query) == TILEDB_OK);

  CHECK(tiledb_query_set_layout(ctx, query, TILEDB_GLOBAL_ORDER) == TILEDB_OK);

  std::vector<uint64_t> coords = {1, 2, 3};
  uint64_t coords_size = coords.size() * sizeof(uint64_t);
  std::vector<int> a = {1, 2, 3};
  uint64_t a_size = a.size() * sizeof(int);
  std::vector<int> b = {1, 2, 3};
  uint64_t b_size = b.size() * sizeof(int);

  CHECK(
      tiledb_query_set_data_buffer(ctx, query, "a", (void*)a.data(), &a_size) ==
      TILEDB_OK);
  CHECK(
      tiledb_query_set_data_buffer(ctx, query, "b", (void*)b.data(), &b_size) ==
      TILEDB_OK);
  CHECK(
      tiledb_query_set_data_buffer(
          ctx, query, TILEDB_COORDS, (void*)coords.data(), &coords_size) ==
      TILEDB_OK);

  tiledb_string_handle_t* string_handle;
  const char* data;
  size_t len;
  CHECK(tiledb_query_get_plan(ctx, query, &string_handle) == TILEDB_OK);
  CHECK(tiledb_string_view(string_handle, &data, &len) == TILEDB_OK);

  // This throws if the query plan is not valid JSON
  std::string str_plan(data, len);
  nlohmann::json json_plan = nlohmann::json::parse(str_plan);

  CHECK(json_plan["TileDB Query Plan"]["Array.URI"] == array_name);
  CHECK(json_plan["TileDB Query Plan"]["Array.Type"] == "sparse");
  CHECK(
      json_plan["TileDB Query Plan"]["VFS.Backend"] ==
      tiledb::sm::URI(array_name).backend_name());
  CHECK(json_plan["TileDB Query Plan"]["Query.Layout"] == "global-order");
  CHECK(
      json_plan["TileDB Query Plan"]["Query.Strategy.Name"] ==
      "GlobalOrderWriter");
  CHECK(
      json_plan["TileDB Query Plan"]["Query.Attributes"] ==
      std::vector({"b", "a", "__coords"}));
  CHECK(
      json_plan["TileDB Query Plan"]["Query.Dimensions"] ==
      std::vector<std::string>());

  REQUIRE(tiledb_string_free(&string_handle) == TILEDB_OK);
  REQUIRE(tiledb_array_close(ctx, array) == TILEDB_OK);
  tiledb_query_free(&query);
  tiledb_array_free(&array);
}
