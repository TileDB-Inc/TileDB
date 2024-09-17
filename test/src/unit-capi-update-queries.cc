/**
 * @file   unit-capi-update-queries.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2021-2024 TileDB Inc.
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
 * Tests of C API for updates.
 */

#include "test/support/src/vfs_helpers.h"
#include "test/support/tdb_catch.h"
#include "tiledb/api/c_api/array/array_api_internal.h"
#include "tiledb/sm/c_api/tiledb.h"
#include "tiledb/sm/c_api/tiledb_experimental.h"
#include "tiledb/sm/c_api/tiledb_struct_def.h"
#ifdef _WIN32
#include "tiledb/sm/filesystem/path_win.h"
#include "tiledb/sm/filesystem/win.h"
#else
#include "tiledb/sm/filesystem/posix.h"
#endif

using namespace tiledb::test;

struct UpdateValuesfx {
  // TileDB context
  tiledb_ctx_t* ctx_;
  tiledb_vfs_t* vfs_;

  // Vector of supported filesystems
  const std::vector<std::unique_ptr<SupportedFs>> fs_vec_;
  std::string array_name_ = "array-updates";

  // Functions
  UpdateValuesfx();
  ~UpdateValuesfx();
  void create_temp_dir();
  void remove_temp_dir();
  void create_sparse_array(
      const std::string& attr_name, tiledb_datatype_t attr_type);
};

UpdateValuesfx::UpdateValuesfx()
    : fs_vec_(vfs_test_get_fs_vec()) {
  // Initialize vfs test
  tiledb_error_t* error = nullptr;
  tiledb_config_t* config = nullptr;
  REQUIRE(tiledb_config_alloc(&config, &error) == TILEDB_OK);
  REQUIRE(error == nullptr);

  tiledb_config_set(config, "sm.allow_updates_experimental", "true", &error);
  REQUIRE(error == nullptr);

  REQUIRE(vfs_test_init(fs_vec_, &ctx_, &vfs_, config).ok());
}

UpdateValuesfx::~UpdateValuesfx() {
  // Close vfs test
  REQUIRE(vfs_test_close(fs_vec_, ctx_, vfs_).ok());
  tiledb_vfs_free(&vfs_);
  tiledb_ctx_free(&ctx_);
}

void UpdateValuesfx::create_temp_dir() {
  remove_temp_dir();
  REQUIRE(tiledb_vfs_create_dir(ctx_, vfs_, array_name_.c_str()) == TILEDB_OK);
}

void UpdateValuesfx::remove_temp_dir() {
  int is_dir = 0;
  REQUIRE(
      tiledb_vfs_is_dir(ctx_, vfs_, array_name_.c_str(), &is_dir) == TILEDB_OK);
  if (is_dir) {
    REQUIRE(
        tiledb_vfs_remove_dir(ctx_, vfs_, array_name_.c_str()) == TILEDB_OK);
  }
}

void UpdateValuesfx::create_sparse_array(
    const std::string& attr_name, tiledb_datatype_t attr_type) {
  int rc;
  int64_t dim_domain[] = {1, 10};
  int64_t tile_extent = 2;

  // Create domain
  tiledb_domain_t* domain;
  rc = tiledb_domain_alloc(ctx_, &domain);
  REQUIRE(rc == TILEDB_OK);
  tiledb_dimension_t* dim;
  rc = tiledb_dimension_alloc(
      ctx_, "d1", TILEDB_INT64, dim_domain, &tile_extent, &dim);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_domain_add_dimension(ctx_, domain, dim);
  REQUIRE(rc == TILEDB_OK);

  tiledb_attribute_t* attr;
  rc = tiledb_attribute_alloc(ctx_, attr_name.c_str(), attr_type, &attr);
  REQUIRE(rc == TILEDB_OK);

  // Create array schema
  tiledb_array_schema_t* array_schema;
  rc = tiledb_array_schema_alloc(ctx_, TILEDB_SPARSE, &array_schema);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_cell_order(ctx_, array_schema, TILEDB_ROW_MAJOR);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_tile_order(ctx_, array_schema, TILEDB_ROW_MAJOR);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_domain(ctx_, array_schema, domain);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_schema_add_attribute(ctx_, array_schema, attr);
  REQUIRE(rc == TILEDB_OK);

  rc = tiledb_array_schema_check(ctx_, array_schema);
  REQUIRE(rc == TILEDB_OK);

  // Create array
  rc = tiledb_array_create(ctx_, array_name_.c_str(), array_schema);
  REQUIRE(rc == TILEDB_OK);

  tiledb_attribute_free(&attr);
  tiledb_dimension_free(&dim);
  tiledb_domain_free(&domain);
  tiledb_array_schema_free(&array_schema);
}

TEST_CASE_METHOD(
    UpdateValuesfx,
    "C API: Test update values with invalid query types",
    "[capi][updates][invalid-query-types]") {
  create_temp_dir();
  create_sparse_array("a", TILEDB_FLOAT32);

  tiledb_query_type_t type = GENERATE(TILEDB_READ, TILEDB_WRITE, TILEDB_DELETE);

  tiledb_array_t* array;
  auto rc = tiledb_array_alloc(ctx_, array_name_.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, type);
  CHECK(rc == TILEDB_OK);

  // Prepare query.
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, type, &query);
  REQUIRE(rc == TILEDB_OK);

  // Add the update value.
  float val = 1.0f;
  rc = tiledb_query_add_update_value(ctx_, query, "a", &val, sizeof(val));
  REQUIRE(rc == TILEDB_ERR);

  // Close array.
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up.
  tiledb_array_free(&array);
  tiledb_query_free(&query);

  remove_temp_dir();
}

TEST_CASE_METHOD(
    UpdateValuesfx,
    "C API: Test update values with invalid values",
    "[capi][updates][invalid-values]") {
  create_temp_dir();
  create_sparse_array("a", TILEDB_FLOAT32);

  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name_.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_UPDATE);
  CHECK(rc == TILEDB_OK);

  // Prepare query.
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_UPDATE, &query);
  REQUIRE(rc == TILEDB_OK);

  SECTION("Invalid field name") {
    // Add the update value.
    float val = 1.0f;
    rc = tiledb_query_add_update_value(ctx_, query, "g", &val, sizeof(val));
    CHECK(rc == TILEDB_OK);
  }

  SECTION("Invalid field size") {
    double val = 1.0;
    rc = tiledb_query_add_update_value(ctx_, query, "a", &val, sizeof(val));
    CHECK(rc == TILEDB_OK);
  }

  SECTION("Nullptr on non nullable attribute") {
    rc = tiledb_query_add_update_value(ctx_, query, "a", nullptr, 0);
    CHECK(rc == TILEDB_OK);
  }

  // Check the update value.
  CHECK_THROWS(
      query->query_->update_values()[0].check(array->array_schema_latest()));

  // Clean up.
  tiledb_query_free(&query);
  tiledb_array_free(&array);

  remove_temp_dir();
}

TEST_CASE_METHOD(
    UpdateValuesfx,
    "C API: Adding update value twice",
    "[capi][updates][adding-twice]") {
  create_temp_dir();
  create_sparse_array("a", TILEDB_FLOAT32);

  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name_.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_UPDATE);
  CHECK(rc == TILEDB_OK);

  // Prepare query.
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_UPDATE, &query);
  REQUIRE(rc == TILEDB_OK);

  // Add the update value.
  float val = 1.0f;
  rc = tiledb_query_add_update_value(ctx_, query, "a", &val, sizeof(val));
  CHECK(rc == TILEDB_OK);

  // Try to add again.
  rc = tiledb_query_add_update_value(ctx_, query, "a", &val, sizeof(val));
  CHECK(rc == TILEDB_ERR);

  // Clean up.
  tiledb_query_free(&query);
  tiledb_array_free(&array);

  remove_temp_dir();
}
