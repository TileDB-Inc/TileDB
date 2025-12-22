/**
 * @file   unit-capi-rest-dense_array.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2024 TileDB Inc.
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
 * Tests of C API for dense array operations via a REST server.
 *
 * This currently is a subset of the normal dense array tests in
 * `unit-capi-dense_array.cc` -- not everything is supported via the REST server
 * yet. Additionally global order queries have been removed.
 */

#include <test/support/tdb_catch.h>
#include "test/support/src/helpers.h"
#include "test/support/src/vfs_helpers.h"
#ifdef _WIN32
#include "tiledb/sm/filesystem/win.h"
#else
#include "tiledb/sm/filesystem/posix.h"
#endif
#include "test/support/src/helpers.h"
#include "tiledb/sm/c_api/tiledb.h"
#include "tiledb/sm/enums/encryption_type.h"
#include "tiledb/sm/rest/rest_client.h"

#include <array>
#include <cassert>
#include <climits>
#include <cstring>
#include <ctime>
#include <iostream>
#include <map>
#include <sstream>
#include <thread>

using namespace tiledb::test;

struct DenseArrayRESTFx {
  // Constant parameters
  const char* ATTR_NAME = "a";
  const tiledb_datatype_t ATTR_TYPE = TILEDB_INT32;
  const char* DIM1_NAME = "x";
  const char* DIM2_NAME = "y";
  const tiledb_datatype_t DIM_TYPE = TILEDB_INT64;
  const int ITER_NUM = 10;

  VFSTestSetup vfs_test_setup_;
  std::string array_uri_;

  // TileDB context and VFS
  tiledb_ctx_t* ctx_;
  tiledb_vfs_t* vfs_;

  // Functions
  DenseArrayRESTFx();
  void check_sorted_reads();
  void check_incomplete_reads();
  void check_sorted_writes();
  void check_simultaneous_writes();
  void create_dense_array(const std::string& array_name);
  void create_dense_array_1_attribute(const std::string& array_name);
  void write_dense_array(const std::string& array_name);
  void write_dense_array_missing_attributes(const std::string& array_name);

  /**
   * Creates a 2D dense array.
   *
   * @param array_name The array name.
   * @param tile_extent_0 The tile extent along the first dimension.
   * @param tile_extent_1 The tile extent along the second dimension.
   * @param domain_0_lo The smallest value of the first dimension domain.
   * @param domain_0_hi The largest value of the first dimension domain.
   * @param domain_1_lo The smallest value of the second dimension domain.
   * @param domain_1_hi The largest value of the second dimension domain.
   * @param capacity The tile capacity.
   * @param cell_order The cell order.
   * @param tile_order The tile order.
   */
  void create_dense_array_2D(
      const std::string& array_name,
      const int64_t tile_extent_0,
      const int64_t tile_extent_1,
      const int64_t domain_0_lo,
      const int64_t domain_0_hi,
      const int64_t domain_1_lo,
      const int64_t domain_1_hi,
      const uint64_t capacity,
      const tiledb_layout_t cell_order,
      const tiledb_layout_t tile_order);

  /**
   * Generates a 2D buffer containing the cell values of a 2D array.
   * Each cell value equals (row index * total number of columns + col index).
   * @param domain_size_0 The domain size of the first dimension.
   * @param domain_size_1 The domain size of the second dimension.
   * @return The created 2D buffer. Note that the function creates the buffer
   *     with 'new'. Make sure to delete the returned buffer in the caller
   *     function.
   */
  int** generate_2D_buffer(
      const int64_t domain_size_0, const int64_t domain_size_1);

  /**
   * Reads a subarray oriented by the input boundaries and outputs the buffer
   * containing the attribute values of the corresponding cells.
   *
   * @param array_name The array name.
   * @param domain_0_lo The smallest value of the first dimension domain to
   *     be read.
   * @param domain_0_hi The largest value of the first dimension domain to
   *     be read.
   * @param domain_1_lo The smallest value of the second dimension domain to
   *     be read.
   * @param domain_1_hi The largest value of the second dimension domain to
   *     be read.
   * @param read_mode The read mode.
   * @return The buffer with the read values. Note that this function is
   *     creating the buffer with 'new'. Therefore, make sure to properly delete
   *     it in the caller. On error, it returns NULL.
   */
  int* read_dense_array_2D(
      const std::string& array_name,
      const int64_t domain_0_lo,
      const int64_t domain_0_hi,
      const int64_t domain_1_lo,
      const int64_t domain_1_hi,
      const tiledb_query_type_t query_type,
      const tiledb_layout_t query_layout);

  /**
   * Write to a 2D dense array tile by tile. The buffer is initialized
   * with row_id*domain_size_1+col_id values.
   *
   * @param array_name The array name.
   * @param domain_size_0 The domain size of the first dimension.
   * @param domain_size_1 The domain size of the second dimension.
   * @param tile_extent_0 The tile extent along the first dimension.
   * @param tile_extent_1 The tile extent along the second dimension.
   */
  void write_dense_array_by_tiles(
      const std::string& array_name,
      const int64_t domain_size_0,
      const int64_t domain_size_1,
      const int64_t tile_extent_0,
      const int64_t tile_extent_1);

  /**
   * Writes a 2D dense subarray.
   *
   * @param array_name The array name.
   * @param subarray The subarray to focus on, given as a vector of low, high
   *     values.
   * @param write_mode The write mode.
   * @param buffer The attribute buffer to be populated and written.
   * @param buffer_sizes The buffer sizes to be dispatched to the write command.
   */
  void write_dense_subarray_2D(
      const std::string& array_name,
      int64_t* subarray,
      tiledb_query_type_t query_type,
      tiledb_layout_t query_layout,
      int* buffer,
      uint64_t* buffer_sizes);
};

DenseArrayRESTFx::DenseArrayRESTFx()
    : ctx_(vfs_test_setup_.ctx_c)
    , vfs_(vfs_test_setup_.vfs_c) {
  std::srand(0);
}

void DenseArrayRESTFx::create_dense_array_2D(
    const std::string& array_name,
    const int64_t tile_extent_0,
    const int64_t tile_extent_1,
    const int64_t domain_0_lo,
    const int64_t domain_0_hi,
    const int64_t domain_1_lo,
    const int64_t domain_1_hi,
    const uint64_t capacity,
    const tiledb_layout_t cell_order,
    const tiledb_layout_t tile_order) {
  // Create attribute
  tiledb_attribute_t* a;
  int rc = tiledb_attribute_alloc(ctx_, ATTR_NAME, ATTR_TYPE, &a);
  REQUIRE(rc == TILEDB_OK);

  // Create dimensions
  int64_t dim_domain[] = {domain_0_lo, domain_0_hi, domain_1_lo, domain_1_hi};
  tiledb_dimension_t* d1;
  rc = tiledb_dimension_alloc(
      ctx_, DIM1_NAME, TILEDB_INT64, &dim_domain[0], &tile_extent_0, &d1);
  REQUIRE(rc == TILEDB_OK);
  tiledb_dimension_t* d2;
  rc = tiledb_dimension_alloc(
      ctx_, DIM2_NAME, TILEDB_INT64, &dim_domain[2], &tile_extent_1, &d2);
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
  rc = tiledb_array_schema_alloc(ctx_, TILEDB_DENSE, &array_schema);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_capacity(ctx_, array_schema, capacity);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_cell_order(ctx_, array_schema, cell_order);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_tile_order(ctx_, array_schema, tile_order);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_schema_add_attribute(ctx_, array_schema, a);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_domain(ctx_, array_schema, domain);
  REQUIRE(rc == TILEDB_OK);

  // Create the array
  rc = tiledb_array_create(ctx_, array_name.c_str(), array_schema);
  REQUIRE(rc == TILEDB_OK);

  // Clean up
  tiledb_attribute_free(&a);
  tiledb_dimension_free(&d1);
  tiledb_dimension_free(&d2);
  tiledb_domain_free(&domain);
  tiledb_array_schema_free(&array_schema);
}

int** DenseArrayRESTFx::generate_2D_buffer(
    const int64_t domain_size_0, const int64_t domain_size_1) {
  // Create buffer
  auto buffer = new int*[domain_size_0];

  // Populate buffer
  for (int64_t i = 0; i < domain_size_0; ++i) {
    buffer[i] = new int[domain_size_1];
    for (int64_t j = 0; j < domain_size_1; ++j) {
      buffer[i][j] = (int)(i * domain_size_1 + j);
    }
  }

  // Return
  return buffer;
}

int* DenseArrayRESTFx::read_dense_array_2D(
    const std::string& array_name,
    const int64_t domain_0_lo,
    const int64_t domain_0_hi,
    const int64_t domain_1_lo,
    const int64_t domain_1_hi,
    const tiledb_query_type_t query_type,
    const tiledb_layout_t query_layout) {
  // Error code
  int rc;

  // Initialize a subarray
  const int64_t subarray[] = {
      domain_0_lo, domain_0_hi, domain_1_lo, domain_1_hi};

  const char* attributes[] = {ATTR_NAME};

  // Prepare the buffers that will store the result
  int64_t domain_size_0 = domain_0_hi - domain_0_lo + 1;
  int64_t domain_size_1 = domain_1_hi - domain_1_lo + 1;
  int64_t cell_num = domain_size_0 * domain_size_1;
  auto buffer_a1 = new int[cell_num];
  REQUIRE(buffer_a1 != nullptr);
  void* buffers[] = {buffer_a1};
  uint64_t buffer_size_a1 = cell_num * sizeof(int);
  uint64_t buffer_sizes[] = {buffer_size_a1};

  // Open array
  tiledb_array_t* array;
  rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, query_type);
  CHECK(rc == TILEDB_OK);

  // Create query
  tiledb_query_t* query;
  tiledb_subarray_t* sub;
  rc = tiledb_query_alloc(ctx_, array, query_type, &query);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, attributes[0], buffers[0], &buffer_sizes[0]);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_subarray_alloc(ctx_, array, &sub);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_set_subarray(ctx_, sub, subarray);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray_t(ctx_, query, sub);
  CHECK(rc == TILEDB_OK);
  tiledb_subarray_free(&sub);
  rc = tiledb_query_set_layout(ctx_, query, query_layout);
  REQUIRE(rc == TILEDB_OK);

  // Check that the query has no results yet
  int has_results;
  rc = tiledb_query_has_results(ctx_, query, &has_results);
  CHECK(rc == TILEDB_OK);
  CHECK(!has_results);

  // Read from array
  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  // Now the query must have results
  rc = tiledb_query_has_results(ctx_, query, &has_results);
  CHECK(rc == TILEDB_OK);
  CHECK(has_results);

  rc = tiledb_query_finalize(ctx_, query);
  REQUIRE(rc == TILEDB_OK);
  rc =
      tiledb_query_finalize(ctx_, query);  // Second time must create no problem
  REQUIRE(rc == TILEDB_OK);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  CHECK(array == nullptr);
  tiledb_query_free(&query);
  CHECK(query == nullptr);

  // Success - return the created buffer
  return buffer_a1;
}

void DenseArrayRESTFx::write_dense_array_by_tiles(
    const std::string& array_name,
    const int64_t domain_size_0,
    const int64_t domain_size_1,
    const int64_t tile_extent_0,
    const int64_t tile_extent_1) {
  // Error code
  int rc;

  // Other initializations
  auto buffer = generate_2D_buffer(domain_size_0, domain_size_1);
  int64_t cell_num_in_tile = tile_extent_0 * tile_extent_1;
  auto buffer_a1 = new int[cell_num_in_tile];
  for (int64_t i = 0; i < cell_num_in_tile; ++i)
    buffer_a1[i] = 0;
  void* buffers[2];
  buffers[0] = buffer_a1;
  uint64_t buffer_sizes[2];
  int64_t index = 0L;
  uint64_t buffer_size = 0L;

  const char* attributes[] = {ATTR_NAME};

  // Open array
  tiledb_array_t* array;
  rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  CHECK(rc == TILEDB_OK);

  // Populate and write tile by tile
  for (int64_t i = 0; i < domain_size_0; i += tile_extent_0) {
    for (int64_t j = 0; j < domain_size_1; j += tile_extent_1) {
      // Create query
      tiledb_query_t* query;
      tiledb_subarray_t* sub;
      rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query);
      REQUIRE(rc == TILEDB_OK);
      rc = tiledb_query_set_data_buffer(
          ctx_, query, attributes[0], buffers[0], &buffer_sizes[0]);
      REQUIRE(rc == TILEDB_OK);
      rc = tiledb_query_set_layout(ctx_, query, TILEDB_ROW_MAJOR);
      REQUIRE(rc == TILEDB_OK);

      int64_t tile_rows = ((i + tile_extent_0) < domain_size_0) ?
                              tile_extent_0 :
                              (domain_size_0 - i);
      int64_t tile_cols = ((j + tile_extent_1) < domain_size_1) ?
                              tile_extent_1 :
                              (domain_size_1 - j);

      int64_t subarray[4] = {i, i + tile_rows - 1, j, j + tile_cols - 1};
      rc = tiledb_subarray_alloc(ctx_, array, &sub);
      CHECK(rc == TILEDB_OK);
      rc = tiledb_subarray_set_subarray(ctx_, sub, subarray);
      CHECK(rc == TILEDB_OK);
      rc = tiledb_query_set_subarray_t(ctx_, query, sub);
      CHECK(rc == TILEDB_OK);
      tiledb_subarray_free(&sub);
      REQUIRE(rc == TILEDB_OK);

      int64_t k = 0, l = 0;
      for (k = 0; k < tile_rows; ++k) {
        for (l = 0; l < tile_cols; ++l) {
          index = uint64_t(k * tile_cols + l);
          buffer_a1[index] = buffer[uint64_t(i + k)][uint64_t(j + l)];
        }
      }
      buffer_size = k * l * sizeof(int);
      buffer_sizes[0] = {buffer_size};

      rc = tiledb_query_submit(ctx_, query);
      REQUIRE(rc == TILEDB_OK);

      tiledb_query_free(&query);
    }
  }

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  for (int64_t i = 0; i < domain_size_0; ++i)
    delete[] buffer[i];
  delete[] buffer;
  delete[] buffer_a1;
}

void DenseArrayRESTFx::write_dense_subarray_2D(
    const std::string& array_name,
    int64_t* subarray,
    tiledb_query_type_t query_type,
    tiledb_layout_t query_layout,
    int* buffer,
    uint64_t* buffer_sizes) {
  // Attribute to focus on and buffers
  const char* attributes[] = {ATTR_NAME};
  void* buffers[] = {buffer};

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK_SAFE(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, query_type);
  CHECK_SAFE(rc == TILEDB_OK);

  // Create query
  tiledb_query_t* query;
  tiledb_subarray_t* sub;
  rc = tiledb_query_alloc(ctx_, array, query_type, &query);
  REQUIRE_SAFE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, attributes[0], buffers[0], &buffer_sizes[0]);
  REQUIRE_SAFE(rc == TILEDB_OK);
  rc = tiledb_subarray_alloc(ctx_, array, &sub);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_set_subarray(ctx_, sub, subarray);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray_t(ctx_, query, sub);
  CHECK(rc == TILEDB_OK);
  tiledb_subarray_free(&sub);
  REQUIRE_SAFE(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, query_layout);
  REQUIRE_SAFE(rc == TILEDB_OK);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  REQUIRE_SAFE(rc == TILEDB_OK);
  rc = tiledb_query_finalize(ctx_, query);
  REQUIRE_SAFE(rc == TILEDB_OK);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK_SAFE(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
}

void DenseArrayRESTFx::check_sorted_reads() {
  // Parameters used in this test
  int64_t domain_size_0 = 5000;
  int64_t domain_size_1 = 10000;
  int64_t tile_extent_0 = 1000;
  int64_t tile_extent_1 = 1000;
  int64_t domain_0_lo = 0;
  int64_t domain_0_hi = domain_size_0 - 1;
  int64_t domain_1_lo = 0;
  int64_t domain_1_hi = domain_size_1 - 1;
  uint64_t capacity = 1000000;
  tiledb_layout_t cell_order = TILEDB_ROW_MAJOR;
  tiledb_layout_t tile_order = TILEDB_ROW_MAJOR;
  array_uri_ = vfs_test_setup_.array_uri("sorted_reads_array");

  // Create a dense integer array
  create_dense_array_2D(
      array_uri_,
      tile_extent_0,
      tile_extent_1,
      domain_0_lo,
      domain_0_hi,
      domain_1_lo,
      domain_1_hi,
      capacity,
      cell_order,
      tile_order);

  // Write array cells with value = row id * COLUMNS + col id
  // to disk tile by tile
  write_dense_array_by_tiles(
      array_uri_, domain_size_0, domain_size_1, tile_extent_0, tile_extent_1);

  // Test random subarrays and check with corresponding value set by
  // row_id*dim1+col_id. Top left corner is always 4,4.
  int64_t d0_lo = 4;
  int64_t d0_hi = 0;
  int64_t d1_lo = 4;
  int64_t d1_hi = 0;
  int64_t height = 0, width = 0;

  for (int iter = 0; iter < ITER_NUM; ++iter) {
    height = std::rand() % (domain_size_0 - d0_lo);
    width = std::rand() % (domain_size_1 - d1_lo);
    d0_hi = d0_lo + height;
    d1_hi = d1_lo + width;
    int64_t index = 0;

    // Read subarray
    int* buffer = read_dense_array_2D(
        array_uri_, d0_lo, d0_hi, d1_lo, d1_hi, TILEDB_READ, TILEDB_ROW_MAJOR);
    REQUIRE(buffer != nullptr);

    bool allok = true;
    // Check
    for (int64_t i = d0_lo; i <= d0_hi; ++i) {
      for (int64_t j = d1_lo; j <= d1_hi; ++j) {
        bool match = (buffer[index] == i * domain_size_1 + j);
        if (!match) {
          allok = false;
          std::cout << "mismatch: " << i << "," << j << "=" << buffer[index]
                    << "!=" << ((i * domain_size_1 + j)) << "\n";
          break;
        }
        ++index;
      }
      if (!allok)
        break;
    }
    REQUIRE(allok);

    // Clean up
    delete[] buffer;
  }

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_uri_.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  tiledb_config_t* cfg;
  tiledb_error_t* err = nullptr;
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Set config to throw on OOB subarray for reads.
  rc = tiledb_config_alloc(&cfg, &err);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(err == nullptr);
  rc = tiledb_config_set(cfg, "sm.read_range_oob", "error", &err);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(err == nullptr);

  // Check out of bounds subarray
  tiledb_query_t* query;
  tiledb_subarray_t* sub;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_config(ctx_, query, cfg);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_subarray_alloc(ctx_, array, &sub);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_set_config(ctx_, sub, cfg);
  REQUIRE(rc == TILEDB_OK);

  int64_t subarray_1[] = {-1, 5, 10, 10};
  rc = tiledb_subarray_set_subarray(ctx_, sub, subarray_1);
  CHECK(rc == TILEDB_ERR);
  int64_t subarray_2[] = {0, 5000000, 10, 10};
  rc = tiledb_subarray_set_subarray(ctx_, sub, subarray_2);
  CHECK(rc == TILEDB_ERR);
  int64_t subarray_3[] = {0, 5, -1, 10};
  rc = tiledb_subarray_set_subarray(ctx_, sub, subarray_3);
  CHECK(rc == TILEDB_ERR);
  int64_t subarray_4[] = {0, 5, 10, 100000000};
  rc = tiledb_subarray_set_subarray(ctx_, sub, subarray_4);
  CHECK(rc == TILEDB_ERR);
  int64_t subarray_5[] = {0, 5, 10, 10};
  rc = tiledb_subarray_set_subarray(ctx_, sub, subarray_5);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray_t(ctx_, query, sub);
  CHECK(rc == TILEDB_OK);
  tiledb_subarray_free(&sub);
  rc = tiledb_query_finalize(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_error_free(&err);
  tiledb_config_free(&cfg);
  tiledb_array_free(&array);
  tiledb_query_free(&query);
}

void DenseArrayRESTFx::check_incomplete_reads() {
  // Parameters used in this test
  int64_t domain_size_0 = 5000;
  int64_t domain_size_1 = 10000;
  int64_t tile_extent_0 = 1000;
  int64_t tile_extent_1 = 1000;
  int64_t domain_0_lo = 0;
  int64_t domain_0_hi = domain_size_0 - 1;
  int64_t domain_1_lo = 0;
  int64_t domain_1_hi = domain_size_1 - 1;
  uint64_t capacity = 1000000;
  tiledb_layout_t cell_order = TILEDB_ROW_MAJOR;
  tiledb_layout_t tile_order = TILEDB_ROW_MAJOR;
  array_uri_ = vfs_test_setup_.array_uri("incomplete_reads_array");

  // Create a dense integer array
  create_dense_array_2D(
      array_uri_,
      tile_extent_0,
      tile_extent_1,
      domain_0_lo,
      domain_0_hi,
      domain_1_lo,
      domain_1_hi,
      capacity,
      cell_order,
      tile_order);

  // Write array cells with value = row id * COLUMNS + col id
  // to disk tile by tile
  write_dense_array_by_tiles(
      array_uri_, domain_size_0, domain_size_1, tile_extent_0, tile_extent_1);

  // Open array
  tiledb_array_t* array;
  REQUIRE(tiledb_array_alloc(ctx_, array_uri_.c_str(), &array) == TILEDB_OK);
  REQUIRE(tiledb_array_open(ctx_, array, TILEDB_READ) == TILEDB_OK);

  // Create query
  tiledb_query_t* query;
  REQUIRE(tiledb_query_alloc(ctx_, array, TILEDB_READ, &query) == TILEDB_OK);
  int64_t subarray[] = {0, 50, 0, 50};
  int32_t attr_buffer[100];
  uint64_t attr_buffer_size = sizeof(attr_buffer);

  tiledb_subarray_t* sub;
  REQUIRE(tiledb_subarray_alloc(ctx_, array, &sub) == TILEDB_OK);
  REQUIRE(tiledb_subarray_set_subarray(ctx_, sub, subarray) == TILEDB_OK);
  REQUIRE(tiledb_query_set_subarray_t(ctx_, query, sub) == TILEDB_OK);
  tiledb_subarray_free(&sub);

  unsigned num_incompletes = 0;
  std::vector<int32_t> all_attr_values;
  while (true) {
    // Reset buffers and resubmit
    REQUIRE(
        tiledb_query_set_data_buffer(
            ctx_, query, ATTR_NAME, attr_buffer, &attr_buffer_size) ==
        TILEDB_OK);
    REQUIRE(tiledb_query_submit(ctx_, query) == TILEDB_OK);
    tiledb_query_status_t status;
    REQUIRE(tiledb_query_get_status(ctx_, query, &status) == TILEDB_OK);
    REQUIRE(attr_buffer_size > 0);
    for (uint64_t i = 0; i < (attr_buffer_size / sizeof(int32_t)); i++)
      all_attr_values.push_back(attr_buffer[i]);

    if (status == TILEDB_INCOMPLETE)
      num_incompletes++;
    else
      break;
  }

  // Check size of results
  REQUIRE(num_incompletes > 1);
  REQUIRE(
      all_attr_values.size() ==
      static_cast<size_t>(
          (subarray[1] - subarray[0] + 1) * (subarray[3] - subarray[2] + 1)));

  // Check all attribute values from all queries.
  bool allok = true;
  uint64_t index = 0;
  for (int64_t i = subarray[0]; i <= subarray[1]; ++i) {
    for (int64_t j = subarray[2]; j <= subarray[3]; ++j) {
      bool match = (all_attr_values[index] == i * domain_size_1 + j);
      if (!match) {
        allok = false;
        std::cout << "mismatch: " << i << "," << j << "="
                  << all_attr_values[index] << "!=" << ((i * domain_size_1 + j))
                  << "\n";
        break;
      }
      ++index;
    }
    if (!allok)
      break;
  }
  REQUIRE(allok);

  // Close array
  REQUIRE(tiledb_array_close(ctx_, array) == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
}

void DenseArrayRESTFx::check_sorted_writes() {
  // Parameters used in this test
  int64_t domain_size_0 = 100;
  int64_t domain_size_1 = 100;
  int64_t tile_extent_0 = 10;
  int64_t tile_extent_1 = 10;
  int64_t domain_0_lo = 0;
  int64_t domain_0_hi = domain_size_0 - 1;
  int64_t domain_1_lo = 0;
  int64_t domain_1_hi = domain_size_1 - 1;
  uint64_t capacity = 1000;
  tiledb_layout_t cell_order = TILEDB_ROW_MAJOR;
  tiledb_layout_t tile_order = TILEDB_ROW_MAJOR;
  array_uri_ = vfs_test_setup_.array_uri("sorted_writes_array");

  // Create a dense integer array
  create_dense_array_2D(
      array_uri_,
      tile_extent_0,
      tile_extent_1,
      domain_0_lo,
      domain_0_hi,
      domain_1_lo,
      domain_1_hi,
      capacity,
      cell_order,
      tile_order);

  // Write random subarray, then read it back and check
  int64_t d0[2], d1[2];
  for (int i = 0; i < ITER_NUM; ++i) {
    // Create subarray
    d0[0] = std::rand() % domain_size_0;
    d1[0] = std::rand() % domain_size_1;
    d0[1] = d0[0] + std::rand() % (domain_size_0 - d0[0]);
    d1[1] = d1[0] + std::rand() % (domain_size_1 - d1[0]);
    int64_t subarray[] = {d0[0], d0[1], d1[0], d1[1]};

    // Prepare buffers
    int64_t subarray_length[2] = {d0[1] - d0[0] + 1, d1[1] - d1[0] + 1};
    int64_t cell_num_in_subarray = subarray_length[0] * subarray_length[1];
    auto buffer = new int[cell_num_in_subarray];
    int64_t index = 0;
    uint64_t buffer_size = cell_num_in_subarray * sizeof(int);
    uint64_t buffer_sizes[] = {buffer_size};
    for (int64_t r = 0; r < subarray_length[0]; ++r)
      for (int64_t c = 0; c < subarray_length[1]; ++c)
        buffer[index++] = -(std::rand() % 999999);

    // Write 2D subarray
    write_dense_subarray_2D(
        array_uri_,
        subarray,
        TILEDB_WRITE,
        TILEDB_ROW_MAJOR,
        buffer,
        buffer_sizes);

    // Read back the same subarray
    int* read_buffer = read_dense_array_2D(
        array_uri_,
        subarray[0],
        subarray[1],
        subarray[2],
        subarray[3],
        TILEDB_READ,
        TILEDB_ROW_MAJOR);
    REQUIRE(read_buffer != nullptr);

    // Check the two buffers
    bool allok = true;
    for (index = 0; index < cell_num_in_subarray; ++index) {
      if (buffer[index] != read_buffer[index]) {
        allok = false;
        break;
      }
    }
    REQUIRE(allok);

    // Clean up
    delete[] buffer;
    delete[] read_buffer;
  }
}

void DenseArrayRESTFx::check_simultaneous_writes() {
  // Parameters used in this test
  int64_t domain_size_0 = 100;
  int64_t domain_size_1 = 100;
  int64_t tile_extent_0 = 10;
  int64_t tile_extent_1 = 10;
  int64_t domain_0_lo = 0;
  int64_t domain_0_hi = domain_size_0 - 1;
  int64_t domain_1_lo = 0;
  int64_t domain_1_hi = domain_size_1 - 1;
  uint64_t capacity = 1000;
  tiledb_layout_t cell_order = TILEDB_ROW_MAJOR;
  tiledb_layout_t tile_order = TILEDB_ROW_MAJOR;
  array_uri_ = vfs_test_setup_.array_uri("simultaneous_writes_array");

  // Create a dense integer array
  create_dense_array_2D(
      array_uri_,
      tile_extent_0,
      tile_extent_1,
      domain_0_lo,
      domain_0_hi,
      domain_1_lo,
      domain_1_hi,
      capacity,
      cell_order,
      tile_order);

  int nthreads = std::thread::hardware_concurrency();
  std::vector<int*> buffers;
  std::vector<std::array<uint64_t, 1>> buffer_sizes;
  std::vector<std::array<int64_t, 4>> subarrays;
  std::vector<std::thread> threads;

  // Pre-generate buffers to write
  for (int i = 0; i < nthreads; i++) {
    subarrays.push_back(
        {{domain_0_lo,
          domain_0_lo + tile_extent_0 - 1,
          domain_1_lo,
          domain_1_lo + tile_extent_1 - 1}});
    buffer_sizes.push_back({{tile_extent_0 * tile_extent_1 * sizeof(int)}});
    buffers.push_back(new int[buffer_sizes.back()[0] / sizeof(int)]);
  }

  // Write multiple subarrays in parallel with a shared context.
  for (int i = 0; i < nthreads; i++) {
    threads.emplace_back([&, i]() {
      const int writes_per_thread = 5;
      for (int j = 0; j < writes_per_thread; j++) {
        write_dense_subarray_2D(
            array_uri_,
            subarrays[i].data(),
            TILEDB_WRITE,
            TILEDB_ROW_MAJOR,
            buffers[i],
            buffer_sizes[i].data());
      }
    });
  }

  for (auto& t : threads) {
    t.join();
  }

  for (int* buffer : buffers) {
    delete[] buffer;
  }
}

void DenseArrayRESTFx::create_dense_array(const std::string& array_name) {
  // Create dimensions
  uint64_t dim_domain[] = {1, 4, 1, 4};
  uint64_t tile_extents[] = {2, 2};
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
  tiledb_attribute_t* a1;
  rc = tiledb_attribute_alloc(ctx_, "a1", TILEDB_INT32, &a1);
  CHECK(rc == TILEDB_OK);
  rc = set_attribute_compression_filter(ctx_, a1, TILEDB_FILTER_LZ4, -1);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_attribute_set_cell_val_num(ctx_, a1, 1);
  CHECK(rc == TILEDB_OK);
  tiledb_attribute_t* a2;
  CHECK(rc == TILEDB_OK);
  rc = tiledb_attribute_alloc(ctx_, "a2", TILEDB_CHAR, &a2);
  CHECK(rc == TILEDB_OK);
  rc = set_attribute_compression_filter(ctx_, a2, TILEDB_FILTER_GZIP, -1);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_attribute_set_cell_val_num(ctx_, a2, TILEDB_VAR_NUM);
  CHECK(rc == TILEDB_OK);
  tiledb_attribute_t* a3;
  CHECK(rc == TILEDB_OK);
  rc = tiledb_attribute_alloc(ctx_, "a3", TILEDB_FLOAT32, &a3);
  CHECK(rc == TILEDB_OK);
  rc = set_attribute_compression_filter(ctx_, a3, TILEDB_FILTER_ZSTD, -1);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_attribute_set_cell_val_num(ctx_, a3, 2);
  CHECK(rc == TILEDB_OK);

  // Create array schema
  tiledb_array_schema_t* array_schema;
  rc = tiledb_array_schema_alloc(ctx_, TILEDB_DENSE, &array_schema);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_cell_order(ctx_, array_schema, TILEDB_ROW_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_tile_order(ctx_, array_schema, TILEDB_ROW_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_domain(ctx_, array_schema, domain);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_schema_add_attribute(ctx_, array_schema, a1);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_schema_add_attribute(ctx_, array_schema, a2);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_schema_add_attribute(ctx_, array_schema, a3);
  CHECK(rc == TILEDB_OK);

  // Check array schema
  rc = tiledb_array_schema_check(ctx_, array_schema);
  CHECK(rc == TILEDB_OK);

  // Create array
  rc = tiledb_array_create(ctx_, array_name.c_str(), array_schema);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_attribute_free(&a1);
  tiledb_attribute_free(&a2);
  tiledb_attribute_free(&a3);
  tiledb_dimension_free(&d1);
  tiledb_dimension_free(&d2);
  tiledb_domain_free(&domain);
  tiledb_array_schema_free(&array_schema);
}

void DenseArrayRESTFx::create_dense_array_1_attribute(
    const std::string& array_name) {
  // Create dimensions
  uint64_t dim_domain[] = {1, 4, 1, 4};
  uint64_t tile_extents[] = {2, 2};
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
  tiledb_attribute_t* a1;
  rc = tiledb_attribute_alloc(ctx_, "a1", TILEDB_INT32, &a1);
  CHECK(rc == TILEDB_OK);

  // Create array schema
  tiledb_array_schema_t* array_schema;
  rc = tiledb_array_schema_alloc(ctx_, TILEDB_DENSE, &array_schema);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_cell_order(ctx_, array_schema, TILEDB_ROW_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_tile_order(ctx_, array_schema, TILEDB_ROW_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_domain(ctx_, array_schema, domain);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_schema_add_attribute(ctx_, array_schema, a1);
  CHECK(rc == TILEDB_OK);

  // Check array schema
  rc = tiledb_array_schema_check(ctx_, array_schema);
  CHECK(rc == TILEDB_OK);

  // Create array
  rc = tiledb_array_create(ctx_, array_name.c_str(), array_schema);
  CHECK(rc == TILEDB_OK);
  // this is needed for test cleanup to know the array needs to be deleted
  array_uri_ = array_name;

  // Clean up
  tiledb_attribute_free(&a1);
  tiledb_dimension_free(&d1);
  tiledb_dimension_free(&d2);
  tiledb_domain_free(&domain);
  tiledb_array_schema_free(&array_schema);
}

void DenseArrayRESTFx::write_dense_array(const std::string& array_name) {
  // Set attributes
  const char* attributes[] = {"a1", "a2", "a3"};

  // Prepare cell buffers
  // clang-format off
  int buffer_a1[] = {
      0,  1,  2,  3, 4,  5,  6,  7,
      8,  9,  10, 11, 12, 13, 14, 15
  };
  uint64_t buffer_a2[] = {
      0,  1,  3,  6, 10, 11, 13, 16,
      20, 21, 23, 26, 30, 31, 33, 36
  };
  char buffer_var_a2[] =
      "abbcccdddd"
      "effggghhhh"
      "ijjkkkllll"
      "mnnooopppp";
  float buffer_a3[] = {
      0.1f,  0.2f,  1.1f,  1.2f,  2.1f,  2.2f,  3.1f,  3.2f,
      4.1f,  4.2f,  5.1f,  5.2f,  6.1f,  6.2f,  7.1f,  7.2f,
      8.1f,  8.2f,  9.1f,  9.2f,  10.1f, 10.2f, 11.1f, 11.2f,
      12.1f, 12.2f, 13.1f, 13.2f, 14.1f, 14.2f, 15.1f, 15.2f,
  };
  void* buffers[] = { buffer_a1, buffer_a2, buffer_var_a2, buffer_a3 };
  uint64_t buffer_sizes[] =
      {
          sizeof(buffer_a1),
          sizeof(buffer_a2),
          sizeof(buffer_var_a2)-1,  // No need to store the last '\0' character
          sizeof(buffer_a3)
      };
  // clang-format on

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  CHECK(rc == TILEDB_OK);

  // Create query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_ROW_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, attributes[0], buffers[0], &buffer_sizes[0]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, attributes[1], buffers[2], &buffer_sizes[2]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_offsets_buffer(
      ctx_, query, attributes[1], (uint64_t*)buffers[1], &buffer_sizes[1]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, attributes[2], buffers[3], &buffer_sizes[3]);
  CHECK(rc == TILEDB_OK);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_OK);

  // Finalize query
  rc = tiledb_query_finalize(ctx_, query);
  CHECK(rc == TILEDB_OK);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
}

void DenseArrayRESTFx::write_dense_array_missing_attributes(
    const std::string& array_name) {
  // Set attributes
  const char* attributes[] = {"a1", "a2", "a3"};

  // Prepare cell buffers
  // clang-format off
  int buffer_a1[] = {
      0,  1,  2,  3, 4,  5,  6,  7,
      8,  9,  10, 11, 12, 13, 14, 15
  };
  uint64_t buffer_a2[] = {
      0,  1,  3,  6, 10, 11, 13, 16,
      20, 21, 23, 26, 30, 31, 33, 36
  };
  char buffer_var_a2[] =
      "abbcccdddd"
      "effggghhhh"
      "ijjkkkllll"
      "mnnooopppp";
  float buffer_a3[] = {
      0.1f,  0.2f,  1.1f,  1.2f,  2.1f,  2.2f,  3.1f,  3.2f,
      4.1f,  4.2f,  5.1f,  5.2f,  6.1f,  6.2f,  7.1f,  7.2f,
      8.1f,  8.2f,  9.1f,  9.2f,  10.1f, 10.2f, 11.1f, 11.2f,
      12.1f, 12.2f, 13.1f, 13.2f, 14.1f, 14.2f, 15.1f, 15.2f,
  };
  void* buffers[] = { buffer_a1, buffer_a2, buffer_var_a2, buffer_a3 };
  uint64_t buffer_sizes[] =
      {
          sizeof(buffer_a1),
          sizeof(buffer_a2),
          sizeof(buffer_var_a2)-1,  // No need to store the last '\0' character
          sizeof(buffer_a3)
      };
  // clang-format on

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  CHECK(rc == TILEDB_OK);

  // Create query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_GLOBAL_ORDER);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, attributes[0], buffers[0], &buffer_sizes[0]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, attributes[1], buffers[2], &buffer_sizes[2]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_offsets_buffer(
      ctx_, query, attributes[1], (uint64_t*)buffers[1], &buffer_sizes[1]);
  CHECK(rc == TILEDB_OK);

  // Observe we omit setting buffer for one of the attributes (a3)

  // Submit query - this should fail
  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_ERR);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
}

TEST_CASE_METHOD(
    DenseArrayRESTFx,
    "C API: REST Test dense array, sorted reads",
    "[capi][rest][dense]") {
  check_sorted_reads();
}

TEST_CASE_METHOD(
    DenseArrayRESTFx,
    "C API: REST Test dense array, sorted writes",
    "[capi][rest][dense][interference]") {
  check_sorted_writes();
}

TEST_CASE_METHOD(
    DenseArrayRESTFx,
    "C API: REST Test dense array, simultaneous writes",
    "[capi][non-rest][dense][dense-simultaneous-writes][.]") {
  /* Catch2's assertion macros are not thread safe. In this test we are
   * asserting inside threads using REQUIRE macros, which are not thread safe so
   * it might fail. Opened SC-66006 to adapt this test to avoid the issue.
   * https://github.com/catchorg/Catch2/blob/devel/docs/limitations.md#thread-safe-assertions
   * for more. */

  // Parallel array open requests on the same array fail on
  // remote arrays, as locking cannot work on Cloud REST
  // server to synchronize them, so we exclude this test
  // from REST-CI
  check_simultaneous_writes();
}

TEST_CASE_METHOD(
    DenseArrayRESTFx,
    "C API: REST Test dense array, global order reads",
    "[capi][rest][dense]") {
  array_uri_ = vfs_test_setup_.array_uri("global_order_reads");
  create_dense_array(array_uri_);
  write_dense_array(array_uri_);

  tiledb_array_t* array;
  REQUIRE(tiledb_array_alloc(ctx_, array_uri_.c_str(), &array) == TILEDB_OK);
  REQUIRE(tiledb_array_open(ctx_, array, TILEDB_READ) == TILEDB_OK);

  uint64_t subarray[] = {1, 4, 1, 4};
  uint64_t buffer_a1_size = 1024;
  uint64_t buffer_a2_off_size = 1024;
  uint64_t buffer_a2_val_size = 1024;
  uint64_t buffer_a3_size = 1024;
  uint64_t buffer_coords_size = 1024;

  auto buffer_a1 = (int*)malloc(buffer_a1_size);
  auto buffer_a2_off = (uint64_t*)malloc(buffer_a2_off_size);
  auto buffer_a2_val = (char*)malloc(buffer_a2_val_size);
  auto buffer_a3 = (float*)malloc(buffer_a3_size);
  auto buffer_coords = (uint64_t*)malloc(buffer_coords_size);

  tiledb_query_t* query;
  tiledb_subarray_t* sub;
  REQUIRE(tiledb_query_alloc(ctx_, array, TILEDB_READ, &query) == TILEDB_OK);
  REQUIRE(
      tiledb_query_set_layout(ctx_, query, TILEDB_GLOBAL_ORDER) == TILEDB_OK);
  REQUIRE(tiledb_subarray_alloc(ctx_, array, &sub) == TILEDB_OK);
  REQUIRE(tiledb_subarray_set_subarray(ctx_, sub, subarray) == TILEDB_OK);
  REQUIRE(tiledb_query_set_subarray_t(ctx_, query, sub) == TILEDB_OK);
  tiledb_subarray_free(&sub);
  REQUIRE(
      tiledb_query_set_data_buffer(
          ctx_, query, "a1", buffer_a1, &buffer_a1_size) == TILEDB_OK);
  REQUIRE(
      tiledb_query_set_data_buffer(
          ctx_, query, "a2", buffer_a2_val, &buffer_a2_val_size) == TILEDB_OK);
  REQUIRE(
      tiledb_query_set_offsets_buffer(
          ctx_, query, "a2", buffer_a2_off, &buffer_a2_off_size) == TILEDB_OK);
  REQUIRE(
      tiledb_query_set_data_buffer(
          ctx_, query, "a3", buffer_a3, &buffer_a3_size) == TILEDB_OK);
  REQUIRE(
      tiledb_query_set_data_buffer(
          ctx_, query, "d1", buffer_coords, &buffer_coords_size) == TILEDB_OK);
  REQUIRE(tiledb_query_submit(ctx_, query) == TILEDB_OK);

  tiledb_query_status_t status;
  REQUIRE(tiledb_query_get_status(ctx_, query, &status) == TILEDB_OK);
  REQUIRE(status == TILEDB_COMPLETED);

  REQUIRE(tiledb_array_close(ctx_, array) == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
  free(buffer_a1);
  free(buffer_a2_off);
  free(buffer_a2_val);
  free(buffer_a3);
  free(buffer_coords);
}

TEST_CASE_METHOD(
    DenseArrayRESTFx,
    "C API: REST Test dense array, missing attributes in writes",
    "[capi][rest][dense][dense-write-missing-attributes]") {
  array_uri_ = vfs_test_setup_.array_uri("dense_write_missing_attributes");
  create_dense_array(array_uri_);
  write_dense_array_missing_attributes(array_uri_);
}

TEST_CASE_METHOD(
    DenseArrayRESTFx,
    "C API: REST Test dense array, read subarrays with empty cells",
    "[capi][rest][dense][dense-read-empty]") {
  array_uri_ = vfs_test_setup_.array_uri("dense_read_empty");
  create_dense_array_1_attribute(array_uri_);

  // Write a slice
  const char* attributes[] = {"a1"};
  int write_a1[] = {1, 2, 3, 4};
  uint64_t write_a1_size = sizeof(write_a1);
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_uri_.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  CHECK(rc == TILEDB_OK);
  tiledb_query_t* query;
  tiledb_subarray_t* sub;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_ROW_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, attributes[0], write_a1, &write_a1_size);
  CHECK(rc == TILEDB_OK);
  uint64_t subarray[] = {2, 3, 1, 2};
  rc = tiledb_subarray_alloc(ctx_, array, &sub);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_set_subarray(ctx_, sub, subarray);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray_t(ctx_, query, sub);
  CHECK(rc == TILEDB_OK);
  tiledb_subarray_free(&sub);
  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
  tiledb_query_free(&query);

  // Read whole array
  uint64_t subarray_read[] = {1, 4, 1, 4};
  int c_a1[] = {
      INT_MIN,
      INT_MIN,
      INT_MIN,
      INT_MIN,
      1,
      2,
      INT_MIN,
      INT_MIN,
      3,
      4,
      INT_MIN,
      INT_MIN,
      INT_MIN,
      INT_MIN,
      INT_MIN,
      INT_MIN};
  int read_a1[16];
  uint64_t read_a1_size = sizeof(read_a1);
  rc = tiledb_array_alloc(ctx_, array_uri_.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_ROW_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_alloc(ctx_, array, &sub);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_set_subarray(ctx_, sub, subarray_read);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray_t(ctx_, query, sub);
  CHECK(rc == TILEDB_OK);
  tiledb_subarray_free(&sub);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, attributes[0], read_a1, &read_a1_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
  tiledb_query_free(&query);

  CHECK(!memcmp(c_a1, read_a1, sizeof(c_a1)));
}

TEST_CASE_METHOD(
    DenseArrayRESTFx,
    "C API: REST Test dense array, read subarrays with empty areas, merging "
    "adjacent cell ranges",
    "[capi][rest][dense][dense-read-empty][dense-read-empty-merge]") {
  array_uri_ = vfs_test_setup_.array_uri("dense_read_empty_merge");
  create_dense_array_1_attribute(array_uri_);

  // Write a slice
  const char* attributes[] = {"a1"};
  int write_a1[] = {1, 2, 3, 4};
  uint64_t write_a1_size = sizeof(write_a1);
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_uri_.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  CHECK(rc == TILEDB_OK);
  tiledb_query_t* query;
  tiledb_subarray_t* sub;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_ROW_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, attributes[0], write_a1, &write_a1_size);
  CHECK(rc == TILEDB_OK);
  uint64_t subarray[] = {2, 3, 2, 3};
  rc = tiledb_subarray_alloc(ctx_, array, &sub);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_set_subarray(ctx_, sub, subarray);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray_t(ctx_, query, sub);
  CHECK(rc == TILEDB_OK);
  tiledb_subarray_free(&sub);
  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
  tiledb_query_free(&query);

  // Read whole array
  uint64_t subarray_read[] = {1, 4, 1, 4};
  int c_a1[] = {
      INT_MIN,
      INT_MIN,
      INT_MIN,
      INT_MIN,
      INT_MIN,
      1,
      2,
      INT_MIN,
      INT_MIN,
      3,
      4,
      INT_MIN,
      INT_MIN,
      INT_MIN,
      INT_MIN,
      INT_MIN};
  int read_a1[16];
  uint64_t read_a1_size = sizeof(read_a1);
  rc = tiledb_array_alloc(ctx_, array_uri_.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_ROW_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_alloc(ctx_, array, &sub);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_set_subarray(ctx_, sub, subarray_read);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray_t(ctx_, query, sub);
  CHECK(rc == TILEDB_OK);
  tiledb_subarray_free(&sub);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, attributes[0], read_a1, &read_a1_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
  tiledb_query_free(&query);

  CHECK(!memcmp(c_a1, read_a1, sizeof(c_a1)));
}

TEST_CASE_METHOD(
    DenseArrayRESTFx,
    "C API: REST Test dense array, multi-fragment reads",
    "[capi][rest][dense][dense-multi-fragment]") {
  array_uri_ = vfs_test_setup_.array_uri("dense_multi_fragment");
  create_dense_array_1_attribute(array_uri_);

  // Write slice [1,2], [3,4]
  int write_a1[] = {1, 2, 3, 4, 5, 6, 7, 8};
  uint64_t write_a1_size = sizeof(write_a1);
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_uri_.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  CHECK(rc == TILEDB_OK);
  tiledb_query_t* query;
  tiledb_subarray_t* sub;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_ROW_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc =
      tiledb_query_set_data_buffer(ctx_, query, "a1", write_a1, &write_a1_size);
  CHECK(rc == TILEDB_OK);
  uint64_t subarray_1[] = {1, 2, 1, 4};
  rc = tiledb_subarray_alloc(ctx_, array, &sub);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_set_subarray(ctx_, sub, subarray_1);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray_t(ctx_, query, sub);
  CHECK(rc == TILEDB_OK);
  tiledb_subarray_free(&sub);
  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
  tiledb_query_free(&query);

  // Write slice [2,3], [2,3]
  int write_a2[] = {101, 102, 103, 104};
  uint64_t write_a2_size = sizeof(write_a2);
  rc = tiledb_array_alloc(ctx_, array_uri_.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_ROW_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc =
      tiledb_query_set_data_buffer(ctx_, query, "a1", write_a2, &write_a2_size);
  CHECK(rc == TILEDB_OK);
  uint64_t subarray_2[] = {2, 3, 2, 3};
  rc = tiledb_subarray_alloc(ctx_, array, &sub);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_set_subarray(ctx_, sub, subarray_2);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray_t(ctx_, query, sub);
  CHECK(rc == TILEDB_OK);
  tiledb_subarray_free(&sub);
  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
  tiledb_query_free(&query);

  // Read whole array
  uint64_t subarray[] = {1, 4, 1, 4};
  int c_a[] = {
      1,
      2,
      3,
      4,
      5,
      101,
      102,
      8,
      INT_MIN,
      103,
      104,
      INT_MIN,
      INT_MIN,
      INT_MIN,
      INT_MIN,
      INT_MIN};
  int read_a[16];
  uint64_t read_a_size = sizeof(read_a);
  rc = tiledb_array_alloc(ctx_, array_uri_.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_ROW_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_alloc(ctx_, array, &sub);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_set_subarray(ctx_, sub, subarray);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray_t(ctx_, query, sub);
  CHECK(rc == TILEDB_OK);
  tiledb_subarray_free(&sub);
  rc = tiledb_query_set_data_buffer(ctx_, query, "a1", read_a, &read_a_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
  tiledb_query_free(&query);

  CHECK(!memcmp(c_a, read_a, sizeof(c_a)));
}

TEST_CASE_METHOD(
    DenseArrayRESTFx,
    "C API: REST Test dense array, check if open",
    "[capi][rest][dense][dense-is-open]") {
  array_uri_ = vfs_test_setup_.array_uri("dense_is_open");
  create_dense_array(array_uri_);

  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_uri_.c_str(), &array);
  CHECK(rc == TILEDB_OK);

  int is_open;
  rc = tiledb_array_is_open(ctx_, array, &is_open);
  CHECK(rc == TILEDB_OK);
  CHECK(is_open == 0);

  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_is_open(ctx_, array, &is_open);
  CHECK(rc == TILEDB_OK);
  CHECK(is_open == 1);

  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_is_open(ctx_, array, &is_open);
  CHECK(rc == TILEDB_OK);
  CHECK(is_open == 0);

  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
}

TEST_CASE_METHOD(
    DenseArrayRESTFx,
    "C API: REST Test dense array, get schema from opened array",
    "[capi][rest][dense][dense-get-schema]") {
  array_uri_ = vfs_test_setup_.array_uri("dense_get_schema");
  create_dense_array(array_uri_);

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_uri_.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Get schema
  tiledb_array_schema_t* schema;
  rc = tiledb_array_get_schema(ctx_, array, &schema);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_schema_check(ctx_, schema);
  CHECK(rc == TILEDB_OK);

  // Clean up
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
  tiledb_array_schema_free(&schema);
}

TEST_CASE_METHOD(
    DenseArrayRESTFx,
    "C API: REST Test dense array, set subarray in sparse writes should error",
    "[capi][rest][dense][dense-set-subarray-sparse]") {
  array_uri_ = vfs_test_setup_.array_uri("dense_set_subarray_sparse");
  create_dense_array(array_uri_);

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_uri_.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  CHECK(rc == TILEDB_OK);

  // Create WRITE query
  tiledb_query_t* query;
  tiledb_subarray_t* sub;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query);
  CHECK(rc == TILEDB_OK);

  uint64_t subarray[] = {1, 1, 1, 1};

  // Set some subarray BEFORE setting the layout to UNORDERED
  rc = tiledb_subarray_alloc(ctx_, array, &sub);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_set_subarray(ctx_, sub, subarray);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray_t(ctx_, query, sub);
  CHECK(rc == TILEDB_OK);
  tiledb_subarray_free(&sub);

  // Unordered writes are only possible for sparse arrays.
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_UNORDERED);
  CHECK(rc == TILEDB_ERR);

  // Close array
  CHECK(tiledb_array_close(ctx_, array) == TILEDB_OK);

  // Clean up
  tiledb_query_free(&query);
  tiledb_array_free(&array);
}

TEST_CASE_METHOD(
    DenseArrayRESTFx,
    "C API: REST Test dense array, incomplete reads",
    "[capi][rest][dense][incomplete]") {
  tiledb_error_t* error;
  tiledb_config_t* config;
  tiledb_config_alloc(&config, &error);
  const char* value;
  REQUIRE(
      tiledb_config_get(config, "rest.resubmit_incomplete", &value, &error) ==
      TILEDB_OK);
  REQUIRE(strcmp(value, "true") == 0);

  // Disable rest.resubmit_incomplete and verify manual resubmits client side.
  REQUIRE(
      tiledb_config_set(config, "rest.resubmit_incomplete", "false", &error) ==
      TILEDB_OK);
  check_incomplete_reads();
}

TEST_CASE_METHOD(
    DenseArrayRESTFx,
    "C API: REST Test dense array, get nonempty domain",
    "[capi][rest][dense]") {
  // Parameters used in this test
  int64_t domain_size_0 = 100;
  int64_t domain_size_1 = 100;
  int64_t tile_extent_0 = 10;
  int64_t tile_extent_1 = 10;
  int64_t domain_0_lo = 0;
  int64_t domain_0_hi = domain_size_0 - 1;
  int64_t domain_1_lo = 0;
  int64_t domain_1_hi = domain_size_1 - 1;
  uint64_t capacity = 1000;
  tiledb_layout_t cell_order = TILEDB_ROW_MAJOR;
  tiledb_layout_t tile_order = TILEDB_ROW_MAJOR;
  array_uri_ = vfs_test_setup_.array_uri("nonempty_domain_array");

  // Create a dense integer array
  create_dense_array_2D(
      array_uri_,
      tile_extent_0,
      tile_extent_1,
      domain_0_lo,
      domain_0_hi,
      domain_1_lo,
      domain_1_hi,
      capacity,
      cell_order,
      tile_order);

  // Check nonempty domain before writing
  tiledb_array_t* array;
  REQUIRE(tiledb_array_alloc(ctx_, array_uri_.c_str(), &array) == TILEDB_OK);
  REQUIRE(tiledb_array_open(ctx_, array, TILEDB_READ) == TILEDB_OK);
  int64_t nonempty_domain[4];
  int32_t is_empty;
  REQUIRE(
      tiledb_array_get_non_empty_domain(
          ctx_, array, &nonempty_domain, &is_empty) == TILEDB_OK);
  REQUIRE(is_empty == 1);
  REQUIRE(tiledb_array_close(ctx_, array) == TILEDB_OK);
  tiledb_array_free(&array);

  // Create a subarray of data
  int64_t subarray[] = {10, 50, 20, 60};
  int64_t subarray_length[2] = {
      subarray[1] - subarray[0] + 1, subarray[3] - subarray[2] + 1};
  int64_t cell_num_in_subarray = subarray_length[0] * subarray_length[1];
  auto buffer = new int[cell_num_in_subarray];
  int64_t index = 0;
  uint64_t buffer_size = cell_num_in_subarray * sizeof(int);
  uint64_t buffer_sizes[] = {buffer_size};
  for (int64_t r = 0; r < subarray_length[0]; ++r)
    for (int64_t c = 0; c < subarray_length[1]; ++c)
      buffer[index++] = -(std::rand() % 999999);

  // Write 2D subarray
  write_dense_subarray_2D(
      array_uri_,
      subarray,
      TILEDB_WRITE,
      TILEDB_ROW_MAJOR,
      buffer,
      buffer_sizes);

  // Clean up
  delete[] buffer;

  // Open array
  int rc = tiledb_array_alloc(ctx_, array_uri_.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Check nonempty domain after writing
  REQUIRE(
      tiledb_array_get_non_empty_domain(
          ctx_, array, &nonempty_domain, &is_empty) == TILEDB_OK);
  REQUIRE(is_empty == 0);
  for (unsigned i = 0; i < 4; i++)
    REQUIRE(nonempty_domain[i] == subarray[i]);

  // Clean up
  REQUIRE(tiledb_array_close(ctx_, array) == TILEDB_OK);
  tiledb_array_free(&array);
}

TEST_CASE_METHOD(
    DenseArrayRESTFx,
    "C API: REST Test dense array, error without rest server configured",
    "[capi][rest][dense][rest-no-config]") {
  // test only applies to REST
  array_uri_ = vfs_test_setup_.array_uri("dense_set_subarray_sparse");
  create_dense_array(array_uri_);

  if (vfs_test_setup_.is_rest()) {
    // Set config to use a non-default environment prefix.
    // Prevents test from picking up on REST CI environment configuration.
    tiledb_config_t* cfg;
    tiledb_error_t* err;
    int rc = tiledb_config_alloc(&cfg, &err);
    CHECK(rc == TILEDB_OK);
    CHECK(err == nullptr);
    rc = tiledb_config_set(cfg, "config.env_var_prefix", "UNIT_", &err);
    CHECK(rc == TILEDB_OK);
    CHECK(err == nullptr);

    // Create context without a REST config
    tiledb_ctx_t* ctx2;
    REQUIRE(tiledb_ctx_alloc(cfg, &ctx2) == TILEDB_OK);

    tiledb_array_t* array;
    rc = tiledb_array_alloc(ctx2, array_uri_.c_str(), &array);
    CHECK(rc == TILEDB_OK);
    rc = tiledb_array_open(ctx2, array, TILEDB_WRITE);
    REQUIRE(rc == TILEDB_ERR);

    // Clean up
    CHECK(tiledb_array_close(ctx2, array) == TILEDB_OK);
    tiledb_config_free(&cfg);
    tiledb_error_free(&err);
    tiledb_array_free(&array);
    tiledb_ctx_free(&ctx2);
  }
}

TEST_CASE_METHOD(
    DenseArrayRESTFx,
    "C API: REST Test dense array, datetimes",
    "[capi][rest][dense][datetime]") {
  array_uri_ = vfs_test_setup_.array_uri("datetime_array");
  int64_t dim_domain[] = {1, 10};
  int64_t tile_extents[] = {2};
  tiledb_dimension_t* d1;
  REQUIRE(
      tiledb_dimension_alloc(
          ctx_,
          "d1",
          TILEDB_DATETIME_DAY,
          &dim_domain[0],
          &tile_extents[0],
          &d1) == TILEDB_OK);

  tiledb_domain_t* domain;
  REQUIRE(tiledb_domain_alloc(ctx_, &domain) == TILEDB_OK);
  REQUIRE(tiledb_domain_add_dimension(ctx_, domain, d1) == TILEDB_OK);

  tiledb_attribute_t* a1;
  REQUIRE(
      tiledb_attribute_alloc(ctx_, "a1", TILEDB_DATETIME_HR, &a1) == TILEDB_OK);

  tiledb_array_schema_t* array_schema;
  REQUIRE(
      tiledb_array_schema_alloc(ctx_, TILEDB_DENSE, &array_schema) ==
      TILEDB_OK);
  REQUIRE(
      tiledb_array_schema_set_domain(ctx_, array_schema, domain) == TILEDB_OK);
  REQUIRE(
      tiledb_array_schema_add_attribute(ctx_, array_schema, a1) == TILEDB_OK);
  REQUIRE(tiledb_array_schema_check(ctx_, array_schema) == TILEDB_OK);

  // Create array
  REQUIRE(
      tiledb_array_create(ctx_, array_uri_.c_str(), array_schema) == TILEDB_OK);

  // Clean up
  tiledb_attribute_free(&a1);
  tiledb_dimension_free(&d1);
  tiledb_domain_free(&domain);
  tiledb_array_schema_free(&array_schema);

  // Write some values
  const char* attributes[] = {"a1"};
  int64_t buffer_a1[] = {-3, -2, -1, 0, 1, 2, 3, 4, 5, 6};
  void* buffers[] = {buffer_a1};
  uint64_t buffer_sizes[] = {sizeof(buffer_a1)};
  tiledb_array_t* array;
  REQUIRE(tiledb_array_alloc(ctx_, array_uri_.c_str(), &array) == TILEDB_OK);
  REQUIRE(tiledb_array_open(ctx_, array, TILEDB_WRITE) == TILEDB_OK);
  tiledb_query_t* query;
  REQUIRE(tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query) == TILEDB_OK);
  REQUIRE(tiledb_query_set_layout(ctx_, query, TILEDB_ROW_MAJOR) == TILEDB_OK);
  REQUIRE(
      tiledb_query_set_data_buffer(
          ctx_, query, attributes[0], buffers[0], &buffer_sizes[0]) ==
      TILEDB_OK);
  REQUIRE(tiledb_query_submit(ctx_, query) == TILEDB_OK);
  REQUIRE(tiledb_array_close(ctx_, array) == TILEDB_OK);
  tiledb_array_free(&array);
  tiledb_query_free(&query);

  // Read a section back
  for (int i = 0; i < 10; i++)
    buffer_a1[i] = 0;
  int64_t subarray[] = {2, 5};
  buffer_sizes[0] = sizeof(buffer_a1);
  REQUIRE(tiledb_array_alloc(ctx_, array_uri_.c_str(), &array) == TILEDB_OK);
  REQUIRE(tiledb_array_open(ctx_, array, TILEDB_READ) == TILEDB_OK);
  REQUIRE(tiledb_query_alloc(ctx_, array, TILEDB_READ, &query) == TILEDB_OK);
  REQUIRE(tiledb_query_set_layout(ctx_, query, TILEDB_ROW_MAJOR) == TILEDB_OK);
  REQUIRE(
      tiledb_query_set_data_buffer(
          ctx_, query, attributes[0], buffers[0], &buffer_sizes[0]) ==
      TILEDB_OK);
  tiledb_subarray_t* sub;
  REQUIRE(tiledb_subarray_alloc(ctx_, array, &sub) == TILEDB_OK);
  REQUIRE(tiledb_subarray_set_subarray(ctx_, sub, subarray) == TILEDB_OK);
  REQUIRE(tiledb_query_set_subarray_t(ctx_, query, sub) == TILEDB_OK);
  tiledb_subarray_free(&sub);
  REQUIRE(tiledb_query_submit(ctx_, query) == TILEDB_OK);
  REQUIRE(tiledb_array_close(ctx_, array) == TILEDB_OK);

  REQUIRE(buffer_a1[0] == -2);
  REQUIRE(buffer_a1[1] == -1);
  REQUIRE(buffer_a1[2] == 0);
  REQUIRE(buffer_a1[3] == 1);

  tiledb_array_free(&array);
  tiledb_query_free(&query);
}

TEST_CASE_METHOD(
    DenseArrayRESTFx,
    "C API: REST Test dense array, array metadata",
    "[capi][rest][dense][metadata]") {
  array_uri_ = vfs_test_setup_.array_uri("metadata_array");
  int64_t dim_domain[] = {1, 10};
  int64_t tile_extents[] = {2};
  tiledb_dimension_t* d1;
  REQUIRE(
      tiledb_dimension_alloc(
          ctx_, "d1", TILEDB_INT64, &dim_domain[0], &tile_extents[0], &d1) ==
      TILEDB_OK);

  tiledb_domain_t* domain;
  REQUIRE(tiledb_domain_alloc(ctx_, &domain) == TILEDB_OK);
  REQUIRE(tiledb_domain_add_dimension(ctx_, domain, d1) == TILEDB_OK);

  tiledb_attribute_t* a1;
  REQUIRE(tiledb_attribute_alloc(ctx_, "a1", TILEDB_INT64, &a1) == TILEDB_OK);

  tiledb_array_schema_t* array_schema;
  REQUIRE(
      tiledb_array_schema_alloc(ctx_, TILEDB_DENSE, &array_schema) ==
      TILEDB_OK);
  REQUIRE(
      tiledb_array_schema_set_domain(ctx_, array_schema, domain) == TILEDB_OK);
  REQUIRE(
      tiledb_array_schema_add_attribute(ctx_, array_schema, a1) == TILEDB_OK);
  REQUIRE(tiledb_array_schema_check(ctx_, array_schema) == TILEDB_OK);

  // Create array
  REQUIRE(
      tiledb_array_create(ctx_, array_uri_.c_str(), array_schema) == TILEDB_OK);

  // Clean up
  tiledb_attribute_free(&a1);
  tiledb_dimension_free(&d1);
  tiledb_domain_free(&domain);
  tiledb_array_schema_free(&array_schema);

  // Write some metadata values
  tiledb_array_t* array;
  REQUIRE(tiledb_array_alloc(ctx_, array_uri_.c_str(), &array) == TILEDB_OK);
  REQUIRE(tiledb_array_open(ctx_, array, TILEDB_WRITE) == TILEDB_OK);
  int32_t v = 5;
  float f[] = {1.1f, 1.2f};
  REQUIRE(
      tiledb_array_put_metadata(ctx_, array, "aaa", TILEDB_INT32, 1, &v) ==
      TILEDB_OK);
  REQUIRE(
      tiledb_array_put_metadata(ctx_, array, "bb", TILEDB_FLOAT32, 2, f) ==
      TILEDB_OK);
  REQUIRE(tiledb_array_close(ctx_, array) == TILEDB_OK);
  tiledb_array_free(&array);

  // Read metadata and check values.
  REQUIRE(tiledb_array_alloc(ctx_, array_uri_.c_str(), &array) == TILEDB_OK);
  REQUIRE(tiledb_array_open(ctx_, array, TILEDB_READ) == TILEDB_OK);
  uint64_t num_metadata = 0;
  REQUIRE(
      tiledb_array_get_metadata_num(ctx_, array, &num_metadata) == TILEDB_OK);
  REQUIRE(num_metadata == 2);
  tiledb_datatype_t datatype = TILEDB_UINT8;
  uint32_t value_num = 0;
  const void* value = nullptr;
  REQUIRE(
      tiledb_array_get_metadata(
          ctx_, array, "aaa", &datatype, &value_num, &value) == TILEDB_OK);
  REQUIRE(datatype == TILEDB_INT32);
  REQUIRE(value_num == 1);
  REQUIRE(*static_cast<const int32_t*>(value) == 5);
  REQUIRE(
      tiledb_array_get_metadata(
          ctx_, array, "bb", &datatype, &value_num, &value) == TILEDB_OK);
  REQUIRE(datatype == TILEDB_FLOAT32);
  REQUIRE(value_num == 2);
  REQUIRE(static_cast<const float*>(value)[0] == 1.1f);
  REQUIRE(static_cast<const float*>(value)[1] == 1.2f);
  REQUIRE(tiledb_array_close(ctx_, array) == TILEDB_OK);
  tiledb_array_free(&array);

  // Prevent array metadata filename/timestamp conflicts
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  // Open for writing and delete a key.
  REQUIRE(tiledb_array_alloc(ctx_, array_uri_.c_str(), &array) == TILEDB_OK);
  REQUIRE(tiledb_array_open(ctx_, array, TILEDB_WRITE) == TILEDB_OK);
  REQUIRE(tiledb_array_delete_metadata(ctx_, array, "aaa") == TILEDB_OK);
  REQUIRE(tiledb_array_close(ctx_, array) == TILEDB_OK);
  tiledb_array_free(&array);

  // Read metadata and check values again.
  REQUIRE(tiledb_array_alloc(ctx_, array_uri_.c_str(), &array) == TILEDB_OK);
  REQUIRE(tiledb_array_open(ctx_, array, TILEDB_READ) == TILEDB_OK);
  REQUIRE(
      tiledb_array_get_metadata_num(ctx_, array, &num_metadata) == TILEDB_OK);
  REQUIRE(num_metadata == 1);
  REQUIRE(
      tiledb_array_get_metadata(
          ctx_, array, "aaa", &datatype, &value_num, &value) == TILEDB_OK);
  REQUIRE(value == nullptr);
  REQUIRE(
      tiledb_array_get_metadata(
          ctx_, array, "bb", &datatype, &value_num, &value) == TILEDB_OK);
  REQUIRE(datatype == TILEDB_FLOAT32);
  REQUIRE(value_num == 2);
  REQUIRE(static_cast<const float*>(value)[0] == 1.1f);
  REQUIRE(static_cast<const float*>(value)[1] == 1.2f);
  REQUIRE(tiledb_array_close(ctx_, array) == TILEDB_OK);
  tiledb_array_free(&array);
}
