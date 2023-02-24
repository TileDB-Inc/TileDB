/**
 * @file tiledb/sm/query_plan/test/unit_query_plan.cc
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
 * This file defines a test `main()`
 */

#include <test/support/tdb_catch.h>

#include "tiledb/api/c_api/query_plan/query_plan_api_external_experimental.h"

#include <tiledb/sm/c_api/tiledb_struct_def.h>
#include <string>
#include "test/support/src/helpers.h"
#include "test/support/src/vfs_helpers.h"
#ifdef _WIN32
#include "tiledb/sm/filesystem/win.h"
#else
#include "tiledb/sm/filesystem/posix.h"
#endif

#include "tiledb/sm/filesystem/uri.h"

#include "external/include/nlohmann/json.hpp"

using namespace tiledb::test;

struct QueryPlanFx {
  // TileDB context and vfs
  tiledb_ctx_t* ctx_;
  tiledb_vfs_t* vfs_;

  // Vector of supported filsystems
  const std::vector<tdb_unique_ptr<SupportedFs>> fs_vec_;

  // Functions
  QueryPlanFx();
  ~QueryPlanFx();
  void remove_temp_dir(const std::string& path);
  void create_temp_dir(const std::string& path);
  void create_dense_array(const std::string& path);
  void create_sparse_array(const std::string& path);
};

QueryPlanFx::QueryPlanFx()
    : fs_vec_(tdb_vfs_test_get_fs_vec()) {
  // Initialize vfs test
  REQUIRE(tdb_vfs_test_init(fs_vec_, &ctx_, &vfs_).ok());
}

QueryPlanFx::~QueryPlanFx() {
  // Close vfs test
  REQUIRE(tdb_vfs_test_close(fs_vec_, ctx_, vfs_).ok());
  tiledb_vfs_free(&vfs_);
  tiledb_ctx_free(&ctx_);
}

void QueryPlanFx::create_temp_dir(const std::string& path) {
  remove_temp_dir(path);
  REQUIRE(tiledb_vfs_create_dir(ctx_, vfs_, path.c_str()) == TILEDB_OK);
}

void QueryPlanFx::remove_temp_dir(const std::string& path) {
  int is_dir = 0;
  REQUIRE(tiledb_vfs_is_dir(ctx_, vfs_, path.c_str(), &is_dir) == TILEDB_OK);
  if (is_dir)
    REQUIRE(tiledb_vfs_remove_dir(ctx_, vfs_, path.c_str()) == TILEDB_OK);
}

void QueryPlanFx::create_dense_array(const std::string& path) {
  // Create array schema
  tiledb_array_schema_t* array_schema;
  int rc = tiledb_array_schema_alloc(ctx_, TILEDB_DENSE, &array_schema);
  REQUIRE(rc == TILEDB_OK);

  // Set schema members
  rc = tiledb_array_schema_set_capacity(ctx_, array_schema, 10000);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_cell_order(ctx_, array_schema, TILEDB_ROW_MAJOR);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_tile_order(ctx_, array_schema, TILEDB_ROW_MAJOR);
  REQUIRE(rc == TILEDB_OK);

  // Create dimensions
  tiledb_dimension_t* d1;
  uint64_t dim_domain[] = {1, 10, 1, 10};
  uint64_t extents[] = {5, 5};
  rc = tiledb_dimension_alloc(
      ctx_, "dim_1", TILEDB_INT64, &dim_domain[0], &extents[0], &d1);
  REQUIRE(rc == TILEDB_OK);
  tiledb_dimension_t* d2;
  rc = tiledb_dimension_alloc(
      ctx_, "dim_2", TILEDB_INT64, &dim_domain[2], &extents[1], &d2);
  REQUIRE(rc == TILEDB_OK);

  // Set domain
  tiledb_domain_t* domain;
  rc = tiledb_domain_alloc(ctx_, &domain);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_domain_add_dimension(ctx_, domain, d1);
  REQUIRE(rc == TILEDB_OK);
  tiledb_datatype_t domain_type;
  rc = tiledb_domain_get_type(ctx_, domain, &domain_type);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(domain_type == TILEDB_INT64);
  rc = tiledb_domain_add_dimension(ctx_, domain, d2);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_domain(ctx_, array_schema, domain);
  REQUIRE(rc == TILEDB_OK);

  // Add attributes
  tiledb_attribute_t* a1;
  rc = tiledb_attribute_alloc(ctx_, "a1", TILEDB_INT32, &a1);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_schema_add_attribute(ctx_, array_schema, a1);
  REQUIRE(rc == TILEDB_OK);
  tiledb_attribute_t* a2;
  rc = tiledb_attribute_alloc(ctx_, "a2", TILEDB_INT32, &a2);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_schema_add_attribute(ctx_, array_schema, a2);
  REQUIRE(rc == TILEDB_OK);

  // Create array
  rc = tiledb_array_create(ctx_, path.c_str(), array_schema);
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
      ctx_, "d1", TILEDB_UINT64, &dim_domain[0], &tile_extents[0], &d1);
  CHECK(rc == TILEDB_OK);
  tiledb_dimension_t* d2;
  rc = tiledb_dimension_alloc(
      ctx_, "d2", TILEDB_UINT64, &dim_domain[2], &tile_extents[1], &d2);
  CHECK(rc == TILEDB_OK);

  // Create domain
  tiledb_domain_t* domain;
  rc = tiledb_domain_alloc(ctx_, &domain);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_domain_add_dimension(ctx_, domain, d1);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_domain_add_dimension(ctx_, domain, d2);
  CHECK(rc == TILEDB_OK);

  // Create attributes
  tiledb_attribute_t* a;
  rc = tiledb_attribute_alloc(ctx_, "a", TILEDB_INT32, &a);
  CHECK(rc == TILEDB_OK);
  tiledb_attribute_t* b;
  rc = tiledb_attribute_alloc(ctx_, "b", TILEDB_INT32, &b);
  CHECK(rc == TILEDB_OK);

  // Create array schema
  tiledb_array_schema_t* array_schema;
  rc = tiledb_array_schema_alloc(ctx_, TILEDB_SPARSE, &array_schema);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_cell_order(ctx_, array_schema, TILEDB_ROW_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_tile_order(ctx_, array_schema, TILEDB_ROW_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_capacity(ctx_, array_schema, 4);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_domain(ctx_, array_schema, domain);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_schema_add_attribute(ctx_, array_schema, a);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_schema_add_attribute(ctx_, array_schema, b);
  CHECK(rc == TILEDB_OK);

  // Check array schema
  rc = tiledb_array_schema_check(ctx_, array_schema);
  CHECK(rc == TILEDB_OK);

  // Create array
  rc = tiledb_array_create(ctx_, array_name.c_str(), array_schema);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_attribute_free(&a);
  tiledb_attribute_free(&b);
  tiledb_dimension_free(&d1);
  tiledb_dimension_free(&d2);
  tiledb_domain_free(&domain);
  tiledb_array_schema_free(&array_schema);
}

TEST_CASE_METHOD(QueryPlanFx, "Query plan basic bahaviour", "[query_plan]") {
  SupportedFsLocal local_fs;
  std::string temp_dir = local_fs.file_prefix() + local_fs.temp_dir();
  std::string array_name = temp_dir + "queryplan_array";
  create_temp_dir(temp_dir);
  create_dense_array(array_name);

  tiledb_array_t* array;
  REQUIRE(tiledb_array_alloc(ctx_, array_name.c_str(), &array) == TILEDB_OK);
  REQUIRE(tiledb_array_open(ctx_, array, TILEDB_READ) == TILEDB_OK);

  tiledb_query_t* query;
  REQUIRE(tiledb_query_alloc(ctx_, array, TILEDB_READ, &query) == TILEDB_OK);

  CHECK(tiledb_query_set_layout(ctx_, query, TILEDB_ROW_MAJOR) == TILEDB_OK);

  int64_t dom[] = {1, 2, 1, 2};
  CHECK(tiledb_query_set_subarray(ctx_, query, &dom) == TILEDB_OK);

  std::vector<int32_t> d(4);
  uint64_t size = 1;
  CHECK(
      tiledb_query_set_data_buffer(ctx_, query, "a1", d.data(), &size) ==
      TILEDB_OK);
  CHECK(
      tiledb_query_set_data_buffer(ctx_, query, "a2", d.data(), &size) ==
      TILEDB_OK);

  tiledb_string_handle_t* string_handle;
  const char* data;
  size_t len;
  CHECK(tiledb_query_get_plan(ctx_, query, &string_handle) == TILEDB_OK);
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

  tiledb_string_free(&string_handle);
  REQUIRE(tiledb_array_close(ctx_, array) == TILEDB_OK);
  tiledb_query_free(&query);
  tiledb_array_free(&array);
  remove_temp_dir(temp_dir);
}

TEST_CASE_METHOD(QueryPlanFx, "Query plan basic bahaviour 2", "[query_plan]") {
  SupportedFsLocal local_fs;
  std::string temp_dir = local_fs.file_prefix() + local_fs.temp_dir();
  std::string array_name = temp_dir + "queryplan_array";
  create_temp_dir(temp_dir);
  create_sparse_array(array_name);

  tiledb_array_t* array;
  REQUIRE(tiledb_array_alloc(ctx_, array_name.c_str(), &array) == TILEDB_OK);
  REQUIRE(tiledb_array_open(ctx_, array, TILEDB_WRITE) == TILEDB_OK);

  tiledb_query_t* query;
  REQUIRE(tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query) == TILEDB_OK);

  CHECK(tiledb_query_set_layout(ctx_, query, TILEDB_GLOBAL_ORDER) == TILEDB_OK);

  std::vector<uint64_t> coords = {1, 2, 3};
  uint64_t coords_size = coords.size() * sizeof(uint64_t);
  std::vector<int> a = {1, 2, 3};
  uint64_t a_size = a.size() * sizeof(int);
  std::vector<int> b = {1, 2, 3};
  uint64_t b_size = b.size() * sizeof(int);

  CHECK(
      tiledb_query_set_data_buffer(
          ctx_, query, "a", (void*)a.data(), &a_size) == TILEDB_OK);
  CHECK(
      tiledb_query_set_data_buffer(
          ctx_, query, "b", (void*)b.data(), &b_size) == TILEDB_OK);
  CHECK(
      tiledb_query_set_data_buffer(
          ctx_, query, TILEDB_COORDS, (void*)coords.data(), &coords_size) ==
      TILEDB_OK);

  tiledb_string_handle_t* string_handle;
  const char* data;
  size_t len;
  CHECK(tiledb_query_get_plan(ctx_, query, &string_handle) == TILEDB_OK);
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

  tiledb_string_free(&string_handle);
  REQUIRE(tiledb_array_close(ctx_, array) == TILEDB_OK);
  tiledb_query_free(&query);
  tiledb_array_free(&array);
  remove_temp_dir(temp_dir);
}
