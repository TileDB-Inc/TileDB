/**
 * @file   unit-capi-expr.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2018 TileDB Inc.
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
 * Tests the C API expr.
 */

#include <catch.hpp>

#include "tiledb/sm/c_api/tiledb.h"
#include "tiledb/sm/filesystem/posix.h"

struct ExprFx {
#ifdef _WIN32
  const std::string FILE_URI_PREFIX = "";
  const std::string FILE_TEMP_DIR =
      tiledb::sm::Win::current_dir() + "\\tiledb_test\\";
#else
  const std::string FILE_URI_PREFIX = "file://";
  const std::string FILE_TEMP_DIR =
      tiledb::sm::Posix::current_dir() + "/tiledb_test/";
#endif
  const std::string TMPDIR = FILE_URI_PREFIX + FILE_TEMP_DIR;

  // TileDB context
  tiledb_ctx_t* ctx_;
  tiledb_vfs_t* vfs_;

  ExprFx();

  ~ExprFx();

  void create_sparse_vector(const std::string& path);

  void create_temp_dir();
  void remove_temp_dir();
};

ExprFx::ExprFx() {
  // Create TileDB context
  tiledb_config_t* config = nullptr;
  tiledb_error_t* error = nullptr;
  REQUIRE(tiledb_config_alloc(&config, &error) == TILEDB_OK);
  REQUIRE(error == nullptr);
  REQUIRE(tiledb_ctx_alloc(config, &ctx_) == TILEDB_OK);
  REQUIRE(error == nullptr);
  vfs_ = nullptr;
  REQUIRE(tiledb_vfs_alloc(ctx_, config, &vfs_) == TILEDB_OK);
  tiledb_config_free(&config);

  create_temp_dir();
}

ExprFx::~ExprFx() {
  remove_temp_dir();

  tiledb_vfs_free(&vfs_);
  tiledb_ctx_free(&ctx_);
}

void ExprFx::create_temp_dir() {
  remove_temp_dir();
  REQUIRE(tiledb_vfs_create_dir(ctx_, vfs_, TMPDIR.c_str()) == TILEDB_OK);
}

void ExprFx::remove_temp_dir() {
  int is_dir = 0;
  REQUIRE(tiledb_vfs_is_dir(ctx_, vfs_, TMPDIR.c_str(), &is_dir) == TILEDB_OK);
  if (is_dir)
    REQUIRE(tiledb_vfs_remove_dir(ctx_, vfs_, TMPDIR.c_str()) == TILEDB_OK);
}

void ExprFx::create_sparse_vector(const std::string& path) {
  int rc;
  int64_t dim_domain[] = {0, 9};
  int64_t tile_extent = 10;
  tiledb_layout_t cell_order = TILEDB_ROW_MAJOR;
  tiledb_layout_t tile_order = TILEDB_ROW_MAJOR;

  // Create domain
  tiledb_domain_t* domain;
  rc = tiledb_domain_alloc(ctx_, &domain);
  REQUIRE(rc == TILEDB_OK);
  tiledb_dimension_t* dim;
  rc = tiledb_dimension_alloc(
      ctx_, "d0", TILEDB_INT64, dim_domain, &tile_extent, &dim);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_domain_add_dimension(ctx_, domain, dim);
  REQUIRE(rc == TILEDB_OK);

  // Create attributes
  tiledb_attribute_t* attr1;
  rc = tiledb_attribute_alloc(ctx_, "a1", TILEDB_INT32, &attr1);
  REQUIRE(rc == TILEDB_OK);
  tiledb_attribute_t* attr2;
  rc = tiledb_attribute_alloc(ctx_, "a2", TILEDB_INT32, &attr2);
  REQUIRE(rc == TILEDB_OK);

  // Create array schema
  tiledb_array_schema_t* array_schema;
  rc = tiledb_array_schema_alloc(ctx_, TILEDB_SPARSE, &array_schema);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_cell_order(ctx_, array_schema, cell_order);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_tile_order(ctx_, array_schema, tile_order);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_domain(ctx_, array_schema, domain);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_schema_add_attribute(ctx_, array_schema, attr1);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_schema_add_attribute(ctx_, array_schema, attr2);
  REQUIRE(rc == TILEDB_OK);

  rc = tiledb_array_schema_check(ctx_, array_schema);
  REQUIRE(rc == TILEDB_OK);

  // Create array
  rc = tiledb_array_create(ctx_, path.c_str(), array_schema);
  REQUIRE(rc == TILEDB_OK);
  tiledb_attribute_free(&attr1);
  tiledb_dimension_free(&dim);
  tiledb_array_schema_free(&array_schema);

  // Open array
  tiledb_array_t* array;
  rc = tiledb_array_alloc(ctx_, path.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  CHECK(rc == TILEDB_OK);

  int64_t buffer_coords[] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
  int32_t buffer_val_a1[] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
  int32_t buffer_val_a2[] = {10, 11, 12, 13, 14, 15, 16, 17, 18, 19};
  // Write array
  void* write_buffers[] = {buffer_coords, buffer_val_a1, buffer_val_a2};
  uint64_t write_buffer_sizes[] = {
      sizeof(buffer_coords), sizeof(buffer_val_a1), sizeof(buffer_val_a2)};
  const char* attributes[] = {TILEDB_COORDS, "a1", "a2"};
  tiledb_query_t* write_query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &write_query);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(
      ctx_,
      write_query,
      attributes[0],
      write_buffers[0],
      &write_buffer_sizes[0]);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(
      ctx_,
      write_query,
      attributes[1],
      write_buffers[1],
      &write_buffer_sizes[1]);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(
      ctx_,
      write_query,
      attributes[2],
      write_buffers[2],
      &write_buffer_sizes[2]);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, write_query, TILEDB_UNORDERED);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_submit(ctx_, write_query);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_finalize(ctx_, write_query);
  REQUIRE(rc == TILEDB_OK);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&write_query);
}

TEST_CASE("C API: Test expr", "[capi], [expr], [computation]") {
  tiledb_ctx_t* ctx;
  int rc = tiledb_ctx_alloc(nullptr, &ctx);
  REQUIRE(rc == TILEDB_OK);

  tiledb_expr_t* expr;
  rc = tiledb_expr_alloc(ctx, &expr);
  REQUIRE(rc == TILEDB_OK);

  rc = tiledb_expr_set(ctx, expr, "a + b");
  REQUIRE(rc == TILEDB_OK);

  // Clean up
  tiledb_expr_free(&expr);
  tiledb_ctx_free(&ctx);
}

TEST_CASE_METHOD(
    ExprFx, "C API: Test set expr on query", "[capi], [expr], [computation]") {
  std::string path = TMPDIR + "sparse_vec";
  create_sparse_vector(path);

  // Set up a query
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, path.c_str(), &array);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  REQUIRE(rc == TILEDB_OK);
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  REQUIRE(rc == TILEDB_OK);

  // Create a simple expression
  tiledb_expr_t* expr;
  rc = tiledb_expr_alloc(ctx_, &expr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_expr_set(ctx_, expr, "a1 * 2");
  REQUIRE(rc == TILEDB_OK);

  // Set on the query.
  int32_t buffer[100];
  uint64_t buffer_size = sizeof(buffer);
  rc = tiledb_query_set_expr(ctx_, query, expr, buffer, &buffer_size);
  REQUIRE(rc == TILEDB_OK);

  // Create expression referring to an invalid attribute.
  tiledb_expr_free(&expr);
  rc = tiledb_expr_alloc(ctx_, &expr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_expr_set(ctx_, expr, "a1 + a2");
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_expr(ctx_, query, expr, buffer, &buffer_size);
  REQUIRE(rc == TILEDB_ERR);

  tiledb_expr_free(&expr);
  rc = tiledb_expr_alloc(ctx_, &expr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_expr_set(ctx_, expr, "a1 + -(2 * (1 + a2))");
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_expr(ctx_, query, expr, buffer, &buffer_size);
  REQUIRE(rc == TILEDB_ERR);

  // Clean up
  rc = tiledb_array_close(ctx_, array);
  REQUIRE(rc == TILEDB_OK);
  tiledb_array_free(&array);
  tiledb_query_free(&query);
  tiledb_expr_free(&expr);
}

TEST_CASE_METHOD(
    ExprFx,
    "C API: Test expr materialize int",
    "[capi], [expr], [computation], [expr-materialize-int]") {
  std::string path = TMPDIR + "sparse_vec";
  create_sparse_vector(path);

  // Set up a query
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, path.c_str(), &array);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  REQUIRE(rc == TILEDB_OK);
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  REQUIRE(rc == TILEDB_OK);
  // Note currently you have to set at least one attribute on the query.
  int32_t attr_buffer[100];
  uint64_t attr_buffer_size = sizeof(attr_buffer);
  REQUIRE(
      tiledb_query_set_buffer(
          ctx_, query, "a1", attr_buffer, &attr_buffer_size) == TILEDB_OK);

  // Create a simple expression that just duplicates an integer value into
  // all cells in the output.
  tiledb_expr_t* expr;
  rc = tiledb_expr_alloc(ctx_, &expr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_expr_set(ctx_, expr, "123");
  REQUIRE(rc == TILEDB_OK);

  // Set on the query.
  int32_t expr_buffer[100];
  uint64_t expr_buffer_size = sizeof(expr_buffer);
  rc = tiledb_query_set_expr(ctx_, query, expr, expr_buffer, &expr_buffer_size);
  REQUIRE(rc == TILEDB_OK);

  // Full vector
  int64_t subarray[] = {0, 9};
  REQUIRE(tiledb_query_set_subarray(ctx_, query, subarray) == TILEDB_OK);
  REQUIRE(tiledb_query_submit(ctx_, query) == TILEDB_OK);
  for (unsigned i = 0; i < 10; i++) {
    REQUIRE(attr_buffer[i] == i);
    REQUIRE(expr_buffer[i] == 123);
  }

  // Partial vector
  std::memset(attr_buffer, 0, sizeof(attr_buffer));
  std::memset(expr_buffer, 0, sizeof(expr_buffer));
  subarray[0] = 3;
  subarray[1] = 7;
  tiledb_query_free(&query);
  REQUIRE(tiledb_query_alloc(ctx_, array, TILEDB_READ, &query) == TILEDB_OK);
  REQUIRE(
      tiledb_query_set_buffer(
          ctx_, query, "a1", attr_buffer, &attr_buffer_size) == TILEDB_OK);
  REQUIRE(
      tiledb_query_set_expr(
          ctx_, query, expr, expr_buffer, &expr_buffer_size) == TILEDB_OK);
  REQUIRE(tiledb_query_set_subarray(ctx_, query, subarray) == TILEDB_OK);
  REQUIRE(tiledb_query_submit(ctx_, query) == TILEDB_OK);
  for (unsigned i = 0; i < 5; i++) {
    REQUIRE(attr_buffer[i] == 3 + i);
    REQUIRE(expr_buffer[i] == 123);
  }

  // Clean up
  rc = tiledb_array_close(ctx_, array);
  REQUIRE(rc == TILEDB_OK);
  tiledb_array_free(&array);
  tiledb_query_free(&query);
  tiledb_expr_free(&expr);
}

TEST_CASE_METHOD(
    ExprFx, "C API: Test expr copy attribute", "[capi][expr][computation]") {
  std::string path = TMPDIR + "sparse_vec";
  create_sparse_vector(path);

  // Set up a query
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, path.c_str(), &array);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  REQUIRE(rc == TILEDB_OK);
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  REQUIRE(rc == TILEDB_OK);
  // TODO: Note currently you have to set at least one attribute on the query.
  int32_t attr_buffer[100];
  uint64_t attr_buffer_size = sizeof(attr_buffer);
  REQUIRE(
      tiledb_query_set_buffer(
          ctx_, query, "a1", attr_buffer, &attr_buffer_size) == TILEDB_OK);

  // Create a simple expression that just copies all cells of an attribute.
  tiledb_expr_t* expr;
  rc = tiledb_expr_alloc(ctx_, &expr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_expr_set(ctx_, expr, "a1");
  REQUIRE(rc == TILEDB_OK);

  // Set on the query.
  int32_t expr_buffer[100];
  uint64_t expr_buffer_size = sizeof(expr_buffer);
  rc = tiledb_query_set_expr(ctx_, query, expr, expr_buffer, &expr_buffer_size);
  REQUIRE(rc == TILEDB_OK);

  // Full vector
  int64_t subarray[] = {0, 9};
  REQUIRE(tiledb_query_set_subarray(ctx_, query, subarray) == TILEDB_OK);
  REQUIRE(tiledb_query_submit(ctx_, query) == TILEDB_OK);
  for (unsigned i = 0; i < 10; i++) {
    REQUIRE(attr_buffer[i] == i);
    REQUIRE(expr_buffer[i] == attr_buffer[i]);
  }

  // Partial vector
  std::memset(attr_buffer, 0, sizeof(attr_buffer));
  std::memset(expr_buffer, 0, sizeof(expr_buffer));
  subarray[0] = 3;
  subarray[1] = 7;
  tiledb_query_free(&query);
  REQUIRE(tiledb_query_alloc(ctx_, array, TILEDB_READ, &query) == TILEDB_OK);
  REQUIRE(
      tiledb_query_set_buffer(
          ctx_, query, "a1", attr_buffer, &attr_buffer_size) == TILEDB_OK);
  REQUIRE(
      tiledb_query_set_expr(
          ctx_, query, expr, expr_buffer, &expr_buffer_size) == TILEDB_OK);
  REQUIRE(tiledb_query_set_subarray(ctx_, query, subarray) == TILEDB_OK);
  REQUIRE(tiledb_query_submit(ctx_, query) == TILEDB_OK);
  for (unsigned i = 0; i < 5; i++) {
    REQUIRE(attr_buffer[i] == 3 + i);
    REQUIRE(expr_buffer[i] == attr_buffer[i]);
  }

  // Clean up
  rc = tiledb_array_close(ctx_, array);
  REQUIRE(rc == TILEDB_OK);
  tiledb_array_free(&array);
  tiledb_query_free(&query);
  tiledb_expr_free(&expr);
}

TEST_CASE_METHOD(
    ExprFx,
    "C API: Test expr attribute addition",
    "[capi][expr][computation]") {
  std::string path = TMPDIR + "sparse_vec";
  create_sparse_vector(path);

  // Set up a query
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, path.c_str(), &array);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  REQUIRE(rc == TILEDB_OK);
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  REQUIRE(rc == TILEDB_OK);
  // TODO: Note currently you have to set buffers for all attributes used by the
  // expr.
  int32_t a1_buffer[100];
  uint64_t a1_buffer_size = sizeof(a1_buffer);
  REQUIRE(
      tiledb_query_set_buffer(ctx_, query, "a1", a1_buffer, &a1_buffer_size) ==
      TILEDB_OK);
  int32_t a2_buffer[100];
  uint64_t a2_buffer_size = sizeof(a1_buffer);
  REQUIRE(
      tiledb_query_set_buffer(ctx_, query, "a2", a2_buffer, &a2_buffer_size) ==
      TILEDB_OK);

  // Create a simple expression that just adds two attributes.
  tiledb_expr_t* expr;
  rc = tiledb_expr_alloc(ctx_, &expr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_expr_set(ctx_, expr, "a1 + a2");
  REQUIRE(rc == TILEDB_OK);

  // Set on the query.
  int32_t expr_buffer[100];
  uint64_t expr_buffer_size = sizeof(expr_buffer);
  rc = tiledb_query_set_expr(ctx_, query, expr, expr_buffer, &expr_buffer_size);
  REQUIRE(rc == TILEDB_OK);

  // Full vector
  int64_t subarray[] = {0, 9};
  REQUIRE(tiledb_query_set_subarray(ctx_, query, subarray) == TILEDB_OK);
  REQUIRE(tiledb_query_submit(ctx_, query) == TILEDB_OK);
  for (unsigned i = 0; i < 10; i++) {
    REQUIRE(a1_buffer[i] == i);
    REQUIRE(expr_buffer[i] == (10 + i) + a1_buffer[i]);
  }

  // Partial vector
  std::memset(a1_buffer, 0, sizeof(a1_buffer));
  std::memset(a2_buffer, 0, sizeof(a2_buffer));
  std::memset(expr_buffer, 0, sizeof(expr_buffer));
  subarray[0] = 3;
  subarray[1] = 7;
  tiledb_query_free(&query);
  REQUIRE(tiledb_query_alloc(ctx_, array, TILEDB_READ, &query) == TILEDB_OK);
  REQUIRE(
      tiledb_query_set_buffer(ctx_, query, "a1", a1_buffer, &a1_buffer_size) ==
      TILEDB_OK);
  REQUIRE(
      tiledb_query_set_buffer(ctx_, query, "a2", a2_buffer, &a2_buffer_size) ==
      TILEDB_OK);
  REQUIRE(
      tiledb_query_set_expr(
          ctx_, query, expr, expr_buffer, &expr_buffer_size) == TILEDB_OK);
  REQUIRE(tiledb_query_set_subarray(ctx_, query, subarray) == TILEDB_OK);
  REQUIRE(tiledb_query_submit(ctx_, query) == TILEDB_OK);
  for (unsigned i = 0; i < 5; i++) {
    REQUIRE(a1_buffer[i] == 3 + i);
    REQUIRE(expr_buffer[i] == (10 + 3 + i) + a1_buffer[i]);
  }

  // Clean up
  rc = tiledb_array_close(ctx_, array);
  REQUIRE(rc == TILEDB_OK);
  tiledb_array_free(&array);
  tiledb_query_free(&query);
  tiledb_expr_free(&expr);
}

TEST_CASE_METHOD(
    ExprFx,
    "C API: Test expr multiple operations",
    "[capi][expr][computation]") {
  std::string path = TMPDIR + "sparse_vec";
  create_sparse_vector(path);

  // Set up a query
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, path.c_str(), &array);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  REQUIRE(rc == TILEDB_OK);
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  REQUIRE(rc == TILEDB_OK);
  // TODO: Note currently you have to set buffers for all attributes used by the
  // expr.
  int32_t a1_buffer[100];
  uint64_t a1_buffer_size = sizeof(a1_buffer);
  REQUIRE(
      tiledb_query_set_buffer(ctx_, query, "a1", a1_buffer, &a1_buffer_size) ==
      TILEDB_OK);
  int32_t a2_buffer[100];
  uint64_t a2_buffer_size = sizeof(a1_buffer);
  REQUIRE(
      tiledb_query_set_buffer(ctx_, query, "a2", a2_buffer, &a2_buffer_size) ==
      TILEDB_OK);

  // Create an expression.
  tiledb_expr_t* expr;
  rc = tiledb_expr_alloc(ctx_, &expr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_expr_set(ctx_, expr, "((2 * (a1 + a2)) + (a1 / a2)) - 1");
  REQUIRE(rc == TILEDB_OK);

  // Set on the query.
  int32_t expr_buffer[100];
  uint64_t expr_buffer_size = sizeof(expr_buffer);
  rc = tiledb_query_set_expr(ctx_, query, expr, expr_buffer, &expr_buffer_size);
  REQUIRE(rc == TILEDB_OK);

  // Full vector
  int64_t subarray[] = {0, 9};
  REQUIRE(tiledb_query_set_subarray(ctx_, query, subarray) == TILEDB_OK);
  REQUIRE(tiledb_query_submit(ctx_, query) == TILEDB_OK);
  for (unsigned i = 0; i < 10; i++) {
    REQUIRE(a1_buffer[i] == i);
    REQUIRE(
        expr_buffer[i] ==
        ((2 * (a1_buffer[i] + a2_buffer[i])) + (a1_buffer[i] / a2_buffer[i])) -
            1);
  }

  // Partial vector
  std::memset(a1_buffer, 0, sizeof(a1_buffer));
  std::memset(a2_buffer, 0, sizeof(a2_buffer));
  std::memset(expr_buffer, 0, sizeof(expr_buffer));
  subarray[0] = 3;
  subarray[1] = 7;
  tiledb_query_free(&query);
  REQUIRE(tiledb_query_alloc(ctx_, array, TILEDB_READ, &query) == TILEDB_OK);
  REQUIRE(
      tiledb_query_set_buffer(ctx_, query, "a1", a1_buffer, &a1_buffer_size) ==
      TILEDB_OK);
  REQUIRE(
      tiledb_query_set_buffer(ctx_, query, "a2", a2_buffer, &a2_buffer_size) ==
      TILEDB_OK);
  REQUIRE(
      tiledb_query_set_expr(
          ctx_, query, expr, expr_buffer, &expr_buffer_size) == TILEDB_OK);
  REQUIRE(tiledb_query_set_subarray(ctx_, query, subarray) == TILEDB_OK);
  REQUIRE(tiledb_query_submit(ctx_, query) == TILEDB_OK);
  for (unsigned i = 0; i < 5; i++) {
    REQUIRE(a1_buffer[i] == 3 + i);
    REQUIRE(
        expr_buffer[i] ==
        ((2 * (a1_buffer[i] + a2_buffer[i])) + (a1_buffer[i] / a2_buffer[i])) -
            1);
  }

  // Clean up
  rc = tiledb_array_close(ctx_, array);
  REQUIRE(rc == TILEDB_OK);
  tiledb_array_free(&array);
  tiledb_query_free(&query);
  tiledb_expr_free(&expr);
}