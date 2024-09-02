/**
 * @file   unit-capi-partial-attribute-write.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022 TileDB Inc.
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
 * Tests the C API for partial attribute write.
 */

#include <test/support/tdb_catch.h>
#include "test/support/src/helpers.h"
#include "tiledb/sm/array/array_directory.h"
#include "tiledb/sm/c_api/tiledb.h"
#include "tiledb/sm/c_api/tiledb_experimental.h"
#include "tiledb/sm/c_api/tiledb_struct_def.h"

using namespace tiledb;
using namespace tiledb::test;

/** Tests for CAPI partial attribute write. */
struct PartialAttrWriteFx {
  // Constants.
  const char* ARRAY_NAME = "test_partial_attr_write_array";

  // TileDB context.
  tiledb_ctx_t* ctx_;
  tiledb_vfs_t* vfs_;

  // Constructors/destructors.
  PartialAttrWriteFx();
  ~PartialAttrWriteFx();

  // Functions.
  void create_sparse_array();
  void write_sparse(
      tiledb_layout_t layout,
      std::vector<int> a1,
      std::vector<uint64_t> dim1,
      std::vector<uint64_t> dim2,
      uint64_t timestamp);
  void read_sparse(
      std::vector<int>& a1,
      std::vector<uint64_t>& dim1,
      std::vector<uint64_t>& dim2);

  void remove_array();
  void remove_array(const std::string& array_name);
  bool is_array(const std::string& array_name);
};

PartialAttrWriteFx::PartialAttrWriteFx() {
  tiledb_config_t* config;
  tiledb_error_t* error;
  tiledb_config_alloc(&config, &error);
  tiledb_config_set(
      config, "sm.allow_separate_attribute_writes", "true", &error);
  REQUIRE(tiledb_ctx_alloc(config, &ctx_) == TILEDB_OK);
  REQUIRE(tiledb_vfs_alloc(ctx_, nullptr, &vfs_) == TILEDB_OK);
}

PartialAttrWriteFx::~PartialAttrWriteFx() {
  tiledb_vfs_free(&vfs_);
  tiledb_ctx_free(&ctx_);
}

void PartialAttrWriteFx::create_sparse_array() {
  // Create dimensions
  uint64_t dim_domain[] = {1, 4, 1, 4};
  uint64_t tile_extents[] = {2, 2};
  tiledb_dimension_t* d1;
  CHECK(
      tiledb_dimension_alloc(
          ctx_, "d1", TILEDB_UINT64, &dim_domain[0], &tile_extents[0], &d1) ==
      TILEDB_OK);
  tiledb_dimension_t* d2;
  CHECK(
      tiledb_dimension_alloc(
          ctx_, "d2", TILEDB_UINT64, &dim_domain[2], &tile_extents[1], &d2) ==
      TILEDB_OK);

  // Create domain
  tiledb_domain_t* domain;
  CHECK(tiledb_domain_alloc(ctx_, &domain) == TILEDB_OK);
  CHECK(tiledb_domain_add_dimension(ctx_, domain, d1) == TILEDB_OK);
  CHECK(tiledb_domain_add_dimension(ctx_, domain, d2) == TILEDB_OK);

  // Create attributes
  tiledb_attribute_t* a1;
  CHECK(tiledb_attribute_alloc(ctx_, "a1", TILEDB_INT32, &a1) == TILEDB_OK);

  // Create array schema
  tiledb_array_schema_t* array_schema;
  CHECK(
      tiledb_array_schema_alloc(ctx_, TILEDB_SPARSE, &array_schema) ==
      TILEDB_OK);
  CHECK(
      tiledb_array_schema_set_cell_order(
          ctx_, array_schema, TILEDB_ROW_MAJOR) == TILEDB_OK);
  CHECK(
      tiledb_array_schema_set_tile_order(
          ctx_, array_schema, TILEDB_ROW_MAJOR) == TILEDB_OK);
  CHECK(tiledb_array_schema_set_capacity(ctx_, array_schema, 2) == TILEDB_OK);
  CHECK(
      tiledb_array_schema_set_domain(ctx_, array_schema, domain) == TILEDB_OK);
  CHECK(tiledb_array_schema_add_attribute(ctx_, array_schema, a1) == TILEDB_OK);

  // Create array
  CHECK(tiledb_array_create(ctx_, ARRAY_NAME, array_schema) == TILEDB_OK);

  // Clean up
  tiledb_attribute_free(&a1);
  tiledb_dimension_free(&d1);
  tiledb_dimension_free(&d2);
  tiledb_domain_free(&domain);
  tiledb_array_schema_free(&array_schema);
}

void PartialAttrWriteFx::write_sparse(
    tiledb_layout_t layout,
    std::vector<int> a1,
    std::vector<uint64_t> dim1,
    std::vector<uint64_t> dim2,
    uint64_t timestamp) {
  // Open array.
  tiledb_array_t* array;
  REQUIRE(tiledb_array_alloc(ctx_, ARRAY_NAME, &array) == TILEDB_OK);
  REQUIRE(
      tiledb_array_set_open_timestamp_end(ctx_, array, timestamp) == TILEDB_OK);
  REQUIRE(tiledb_array_open(ctx_, array, TILEDB_WRITE) == TILEDB_OK);

  // Create query.
  tiledb_query_t* query;
  REQUIRE(tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query) == TILEDB_OK);
  REQUIRE(tiledb_query_set_layout(ctx_, query, layout) == TILEDB_OK);

  uint64_t dim1_data_size = dim1.size() * sizeof(uint64_t);
  REQUIRE(
      tiledb_query_set_data_buffer(
          ctx_, query, "d1", dim1.data(), &dim1_data_size) == TILEDB_OK);
  uint64_t dim2_data_size = dim1.size() * sizeof(uint64_t);
  REQUIRE(
      tiledb_query_set_data_buffer(
          ctx_, query, "d2", dim2.data(), &dim2_data_size) == TILEDB_OK);
  tiledb_query_submit(ctx_, query);

  dim1.clear();
  dim2.clear();

  uint64_t a1_data_size = a1.size() * sizeof(int);
  REQUIRE(
      tiledb_query_set_data_buffer(
          ctx_, query, "a1", a1.data(), &a1_data_size) == TILEDB_OK);

  if (layout != TILEDB_UNORDERED) {
    REQUIRE(tiledb_query_submit(ctx_, query) == TILEDB_ERR);

    tiledb_error_t* err = NULL;
    tiledb_ctx_get_last_error(ctx_, &err);
    const char* msg;
    tiledb_error_message(err, &msg);

    std::string message(msg);
    CHECK(
        message ==
        "Query: Partial attribute write is only supported for unordered "
        "writes.");
  } else {
    REQUIRE(tiledb_query_submit(ctx_, query) == TILEDB_OK);
    REQUIRE(tiledb_query_finalize(ctx_, query) == TILEDB_OK);
  }

  // Close array.
  REQUIRE(tiledb_array_close(ctx_, array) == TILEDB_OK);

  tiledb_query_free(&query);
  tiledb_array_free(&array);
}

void PartialAttrWriteFx::read_sparse(
    std::vector<int>& a1,
    std::vector<uint64_t>& dim1,
    std::vector<uint64_t>& dim2) {
  // Open array.
  tiledb_array_t* array;
  REQUIRE(tiledb_array_alloc(ctx_, ARRAY_NAME, &array) == TILEDB_OK);
  REQUIRE(tiledb_array_open(ctx_, array, TILEDB_READ) == TILEDB_OK);

  // Create query.
  tiledb_query_t* query;
  REQUIRE(tiledb_query_alloc(ctx_, array, TILEDB_READ, &query) == TILEDB_OK);
  REQUIRE(
      tiledb_query_set_layout(ctx_, query, TILEDB_GLOBAL_ORDER) == TILEDB_OK);

  uint64_t dim1_data_size = dim1.size() * sizeof(uint64_t);
  REQUIRE(
      tiledb_query_set_data_buffer(
          ctx_, query, "d1", dim1.data(), &dim1_data_size) == TILEDB_OK);
  uint64_t dim2_data_size = dim1.size() * sizeof(uint64_t);
  REQUIRE(
      tiledb_query_set_data_buffer(
          ctx_, query, "d2", dim2.data(), &dim2_data_size) == TILEDB_OK);
  uint64_t a1_data_size = a1.size() * sizeof(int);
  REQUIRE(
      tiledb_query_set_data_buffer(
          ctx_, query, "a1", a1.data(), &a1_data_size) == TILEDB_OK);

  // Submit the query.
  tiledb_query_status_t status;
  REQUIRE(tiledb_query_submit(ctx_, query) == TILEDB_OK);
  REQUIRE(tiledb_query_get_status(ctx_, query, &status) == TILEDB_OK);
  CHECK(status == TILEDB_COMPLETED);

  // Close array.
  REQUIRE(tiledb_array_close(ctx_, array) == TILEDB_OK);

  tiledb_query_free(&query);
  tiledb_array_free(&array);
}

void PartialAttrWriteFx::remove_array(const std::string& array_name) {
  if (!is_array(array_name)) {
    return;
  }

  tiledb_vfs_remove_dir(ctx_, vfs_, array_name.c_str());
}

void PartialAttrWriteFx::remove_array() {
  remove_array(ARRAY_NAME);
}

bool PartialAttrWriteFx::is_array(const std::string& array_name) {
  int32_t is_dir = 0;
  tiledb_vfs_is_dir(ctx_, vfs_, array_name.c_str(), &is_dir);
  return is_dir;
}

TEST_CASE_METHOD(
    PartialAttrWriteFx,
    "CAPI: Test partial attribute write",
    "[capi][partial-attribute-write]") {
  remove_array();
  create_sparse_array();

  // Write fragment.
  write_sparse(
      TILEDB_UNORDERED,
      {0, 1, 2, 3, 4, 5, 6, 7},
      {1, 1, 1, 2, 3, 4, 3, 3},
      {1, 2, 4, 3, 1, 2, 3, 4},
      1);

  size_t buffer_size = 8;
  std::vector<int> a1(buffer_size);
  std::vector<uint64_t> dim1(buffer_size);
  std::vector<uint64_t> dim2(buffer_size);
  read_sparse(a1, dim1, dim2);

  CHECK(a1 == std::vector<int>({0, 1, 2, 3, 4, 5, 6, 7}));
  CHECK(dim1 == std::vector<uint64_t>({1, 1, 1, 2, 3, 4, 3, 3}));
  CHECK(dim2 == std::vector<uint64_t>({1, 2, 4, 3, 1, 2, 3, 4}));

  remove_array();
}
