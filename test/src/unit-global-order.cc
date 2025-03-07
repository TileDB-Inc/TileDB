/**
 * @file   unit-global-order.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2021 TileDB Inc.
 * @copyright Copyright (c) 2016 MIT and Intel Corporation
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
 * Tests for global order writes.
 */

#include <test/support/tdb_catch.h>
#include "test/support/src/helpers.h"
#include "tiledb/sm/c_api/tiledb.h"

#include <cstring>
#include <iostream>

struct GlobalOrderWriteFx {
  GlobalOrderWriteFx();
  ~GlobalOrderWriteFx();
  void create_array_1D(bool hilbert);
  void create_array_2D(bool hilbert);
  void write_array_1D(
      bool expect_fail,
      tiledb_array_t*& array,
      tiledb_query_t*& query,
      uint64_t coord_d1);
  void write_array_2D(
      bool expect_fail,
      tiledb_array_t*& array,
      tiledb_query_t*& query,
      uint64_t coord_d1,
      std::string coord_d2);
  void delete_array();

  tiledb_ctx_t* ctx_;
  const std::string array_name_ = "global_order_write";
};

GlobalOrderWriteFx::GlobalOrderWriteFx() {
  ctx_ = tiledb::test::vanilla_context_c();
}

GlobalOrderWriteFx::~GlobalOrderWriteFx() {
  tiledb_ctx_free(&ctx_);
}

// Create a simple dense 1D array
void GlobalOrderWriteFx::create_array_1D(bool hilbert) {
  // Create dimensions
  uint64_t dim_domain[] = {1, 4};
  uint64_t tile_extent = 2;
  tiledb_dimension_t* d1;
  auto rc = tiledb_dimension_alloc(
      ctx_, "d1", TILEDB_UINT64, &dim_domain[0], &tile_extent, &d1);
  REQUIRE(rc == TILEDB_OK);

  // Create domain
  tiledb_domain_t* domain;
  rc = tiledb_domain_alloc(ctx_, &domain);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_domain_add_dimension(ctx_, domain, d1);
  REQUIRE(rc == TILEDB_OK);

  // Create array schema
  tiledb_array_schema_t* array_schema;
  rc = tiledb_array_schema_alloc(ctx_, TILEDB_SPARSE, &array_schema);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_cell_order(
      ctx_, array_schema, hilbert ? TILEDB_HILBERT : TILEDB_ROW_MAJOR);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_tile_order(ctx_, array_schema, TILEDB_ROW_MAJOR);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_domain(ctx_, array_schema, domain);
  REQUIRE(rc == TILEDB_OK);

  // Check array schema
  rc = tiledb_array_schema_check(ctx_, array_schema);
  REQUIRE(rc == TILEDB_OK);

  // Create array
  rc = tiledb_array_create(ctx_, array_name_.c_str(), array_schema);
  REQUIRE(rc == TILEDB_OK);

  // Clean up
  tiledb_dimension_free(&d1);
  tiledb_domain_free(&domain);
  tiledb_array_schema_free(&array_schema);
}

// Create a simple dense 2D array
void GlobalOrderWriteFx::create_array_2D(bool hilbert) {
  // Create dimensions
  uint64_t dim_domain[] = {1, 4};
  uint64_t tile_extent = 2;
  tiledb_dimension_t* d1;
  auto rc = tiledb_dimension_alloc(
      ctx_, "d1", TILEDB_UINT64, &dim_domain[0], &tile_extent, &d1);
  REQUIRE(rc == TILEDB_OK);

  tiledb_dimension_t* d2;
  rc = tiledb_dimension_alloc(
      ctx_, "d2", TILEDB_STRING_ASCII, nullptr, nullptr, &d2);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_dimension_set_cell_val_num(ctx_, d2, TILEDB_VAR_NUM);
  REQUIRE(rc == TILEDB_OK);

  // Create domain
  tiledb_domain_t* domain;
  rc = tiledb_domain_alloc(ctx_, &domain);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_domain_add_dimension(ctx_, domain, d1);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_domain_add_dimension(ctx_, domain, d2);
  REQUIRE(rc == TILEDB_OK);

  // Create array schema
  tiledb_array_schema_t* array_schema;
  rc = tiledb_array_schema_alloc(ctx_, TILEDB_SPARSE, &array_schema);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_cell_order(
      ctx_, array_schema, hilbert ? TILEDB_HILBERT : TILEDB_ROW_MAJOR);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_tile_order(ctx_, array_schema, TILEDB_ROW_MAJOR);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_domain(ctx_, array_schema, domain);
  REQUIRE(rc == TILEDB_OK);

  // Check array schema
  rc = tiledb_array_schema_check(ctx_, array_schema);
  REQUIRE(rc == TILEDB_OK);

  // Create array
  rc = tiledb_array_create(ctx_, array_name_.c_str(), array_schema);
  REQUIRE(rc == TILEDB_OK);

  // Clean up
  tiledb_dimension_free(&d1);
  tiledb_dimension_free(&d2);
  tiledb_domain_free(&domain);
  tiledb_array_schema_free(&array_schema);
}

void GlobalOrderWriteFx::write_array_1D(
    bool expect_fail,
    tiledb_array_t*& array,
    tiledb_query_t*& query,
    uint64_t coord_d1) {
  // Create array if necessary
  if (array == nullptr) {
    auto rc = tiledb_array_alloc(ctx_, array_name_.c_str(), &array);
    CHECK(rc == TILEDB_OK);
    rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
    CHECK(rc == TILEDB_OK);
  }

  uint64_t buffer_size_d1 = sizeof(uint64_t);

  // Create query if necessary
  if (query == nullptr) {
    auto rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query);
    REQUIRE(rc == TILEDB_OK);
    rc = tiledb_query_set_layout(ctx_, query, TILEDB_GLOBAL_ORDER);
    REQUIRE(rc == TILEDB_OK);
  }

  auto rc = tiledb_query_set_data_buffer(
      ctx_, query, "d1", &coord_d1, &buffer_size_d1);
  REQUIRE(rc == TILEDB_OK);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == (expect_fail ? TILEDB_ERR : TILEDB_OK));

  if (expect_fail) {
    // Check we hit the correct error.
    tiledb_error_t* error = NULL;
    rc = tiledb_ctx_get_last_error(ctx_, &error);
    REQUIRE(rc == TILEDB_OK);

    const char* msg;
    rc = tiledb_error_message(error, &msg);
    REQUIRE(rc == TILEDB_OK);

    std::string error_str(msg);
    REQUIRE(
        error_str.find("comes before last written coordinate") !=
        std::string::npos);
  }
}

void GlobalOrderWriteFx::write_array_2D(
    bool expect_fail,
    tiledb_array_t*& array,
    tiledb_query_t*& query,
    uint64_t coord_d1,
    std::string coord_d2) {
  // Create array if necessary
  if (array == nullptr) {
    auto rc = tiledb_array_alloc(ctx_, array_name_.c_str(), &array);
    CHECK(rc == TILEDB_OK);
    rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
    CHECK(rc == TILEDB_OK);
  }

  uint64_t buffer_size_d1 = sizeof(uint64_t);
  uint64_t buffer_size_d2 = coord_d2.size();
  uint64_t offsets_d2 = 0;
  uint64_t offsets_buffer_size_d2 = sizeof(uint64_t);

  // Create query if necessary
  if (query == nullptr) {
    auto rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query);
    REQUIRE(rc == TILEDB_OK);
    rc = tiledb_query_set_layout(ctx_, query, TILEDB_GLOBAL_ORDER);
    REQUIRE(rc == TILEDB_OK);
  }

  auto rc = tiledb_query_set_data_buffer(
      ctx_, query, "d1", &coord_d1, &buffer_size_d1);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, "d2", coord_d2.data(), &buffer_size_d2);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_offsets_buffer(
      ctx_, query, "d2", &offsets_d2, &offsets_buffer_size_d2);
  REQUIRE(rc == TILEDB_OK);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == (expect_fail ? TILEDB_ERR : TILEDB_OK));

  if (expect_fail) {
    // Check we hit the correct error.
    tiledb_error_t* error = NULL;
    rc = tiledb_ctx_get_last_error(ctx_, &error);
    REQUIRE(rc == TILEDB_OK);

    const char* msg;
    rc = tiledb_error_message(error, &msg);
    REQUIRE(rc == TILEDB_OK);

    std::string error_str(msg);
    REQUIRE(
        error_str.find("comes before last written coordinate") !=
        std::string::npos);
  }
}

void GlobalOrderWriteFx::delete_array() {
  // Remove array
  tiledb_object_t type;
  auto rc = tiledb_object_type(ctx_, array_name_.c_str(), &type);
  REQUIRE(rc == TILEDB_OK);
  if (type == TILEDB_ARRAY) {
    rc = tiledb_object_remove(ctx_, array_name_.c_str());
    REQUIRE(rc == TILEDB_OK);
  }
}

TEST_CASE_METHOD(
    GlobalOrderWriteFx,
    "Global order continuation 1D out of order",
    "[global-order][continuation][out-of-order]") {
  bool hilbert = GENERATE(false, true);

  delete_array();
  create_array_1D(hilbert);

  tiledb_array_t* array = nullptr;
  tiledb_query_t* query = nullptr;

  write_array_1D(false, array, query, 3);
  write_array_1D(true, array, query, 2);

  // Close array
  auto rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_query_free(&query);
  tiledb_array_free(&array);

  delete_array();
}

TEST_CASE_METHOD(
    GlobalOrderWriteFx,
    "Global order continuation 2D out of order",
    "[global-order][continuation][out-of-order]") {
  bool hilbert = GENERATE(false, true);

  delete_array();
  create_array_2D(hilbert);

  tiledb_array_t* array = nullptr;
  tiledb_query_t* query = nullptr;

  write_array_2D(false, array, query, 2, "bb");
  write_array_2D(true, array, query, 2, "a");

  // Close array
  auto rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_query_free(&query);
  tiledb_array_free(&array);

  delete_array();
}
