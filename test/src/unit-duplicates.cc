/**
 * @file unit-duplicates.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2021 TileDB, Inc.
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
 * Tests enabling coordinate duplicates for sparse arrays.
 */

#include "test/src/helpers.h"
#include "tiledb/sm/c_api/tiledb.h"

#ifdef _WIN32
#include "tiledb/sm/filesystem/win.h"
#else
#include "tiledb/sm/filesystem/posix.h"
#endif

#include <catch.hpp>
#include <chrono>
#include <iostream>
#include <thread>

using namespace tiledb::sm;
using namespace tiledb::test;

/* ********************************* */
/*         STRUCT DEFINITION         */
/* ********************************* */

struct CDuplicatesFx {
  tiledb_ctx_t* ctx_ = nullptr;
  tiledb_vfs_t* vfs_ = nullptr;
  std::string temp_dir_;
  std::string array_name_;
  const char* ARRAY_NAME = "test_duplicates";
  tiledb_array_t* array_ = nullptr;

  void create_default_array_1d();

  CDuplicatesFx();
  ~CDuplicatesFx();
};

CDuplicatesFx::CDuplicatesFx() {
  tiledb_config_t* config = nullptr;
  tiledb_error_t* error = nullptr;
  REQUIRE(tiledb_ctx_alloc(config, &ctx_) == TILEDB_OK);
  REQUIRE(error == nullptr);
  REQUIRE(tiledb_vfs_alloc(ctx_, config, &vfs_) == TILEDB_OK);
  tiledb_config_free(&config);

// Create temporary directory based on the supported filesystem
#ifdef _WIN32
  temp_dir_ = tiledb::sm::Win::current_dir() + "\\tiledb_test\\";
#else
  temp_dir_ = "file://" + tiledb::sm::Posix::current_dir() + "/tiledb_test/";
#endif
  create_dir(temp_dir_, ctx_, vfs_);
  array_name_ = temp_dir_ + ARRAY_NAME;
}

CDuplicatesFx::~CDuplicatesFx() {
  tiledb_array_free(&array_);
  remove_dir(temp_dir_, ctx_, vfs_);
  tiledb_ctx_free(&ctx_);
  tiledb_vfs_free(&vfs_);
}

void CDuplicatesFx::create_default_array_1d() {
  int domain[] = {1, 10};
  int tile_extent = 5;
  create_array(
      ctx_,
      array_name_,
      TILEDB_SPARSE,
      {"d"},
      {TILEDB_INT32},
      {domain},
      {&tile_extent},
      {"a"},
      {TILEDB_INT32},
      {1},
      {tiledb::test::Compressor(TILEDB_FILTER_NONE, -1)},
      TILEDB_ROW_MAJOR,
      TILEDB_ROW_MAJOR,
      2,
      true);  // allows dups
}

/* ********************************* */
/*                TESTS              */
/* ********************************* */

TEST_CASE_METHOD(
    CDuplicatesFx,
    "C API: Duplicates, single fragment",
    "[duplicates][single-fragment]") {
  // Create default array
  create_default_array_1d();

  // Open array for writing
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name_.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  CHECK(rc == TILEDB_OK);

  // Write sparse cells
  int coords[] = {1, 2, 1, 4, 5};
  uint64_t coords_size = sizeof(coords);
  int data[] = {1, 2, 3, 4, 5};
  uint64_t data_size = sizeof(data);

  // Create the query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_UNORDERED);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, query, "a", data, &data_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, query, "d", coords, &coords_size);
  CHECK(rc == TILEDB_OK);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_OK);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);

  // Open array for reading
  rc = tiledb_array_alloc(ctx_, array_name_.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Set subarray
  int subarray[] = {1, 10};

  // Prepare the vector that will hold the result
  int coords_r[5];
  int data_r[5];
  uint64_t coords_r_size = sizeof(coords_r);
  uint64_t data_r_size = sizeof(data_r);

  // Create query
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray(ctx_, query, subarray);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_ROW_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, query, "a", data_r, &data_r_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, TILEDB_COORDS, coords_r, &coords_r_size);
  CHECK(rc == TILEDB_OK);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_OK);

  // Clean up
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
  tiledb_query_free(&query);

  CHECK(data_size == data_r_size);
  CHECK(coords_size == coords_r_size);

  int coords_c[] = {1, 1, 2, 4, 5};
  // The ordering for duplicates is undefined, check all variations.
  int data_c_1[] = {1, 3, 2, 4, 5};
  int data_c_2[] = {3, 1, 2, 4, 5};
  CHECK(!std::memcmp(coords_c, coords_r, coords_r_size));
  const bool data_c_matches = !std::memcmp(data_c_1, data_r, data_r_size) ||
                              !std::memcmp(data_c_2, data_r, data_r_size);
  CHECK(data_c_matches);
}

TEST_CASE_METHOD(
    CDuplicatesFx,
    "C API: Duplicates, multiple fragment",
    "[duplicates][multiple-fragments]") {
  // Create default array
  create_default_array_1d();

  // Open array for writing - #1
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name_.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  CHECK(rc == TILEDB_OK);

  // Write sparse cells
  int coords_1[] = {1, 2};
  uint64_t coords_1_size = sizeof(coords_1);
  int data_1[] = {1, 2};
  uint64_t data_1_size = sizeof(data_1);

  // Create the query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_UNORDERED);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, query, "a", data_1, &data_1_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, query, "d", coords_1, &coords_1_size);
  CHECK(rc == TILEDB_OK);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_OK);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);

  // Open array for writing - #2
  rc = tiledb_array_alloc(ctx_, array_name_.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  CHECK(rc == TILEDB_OK);

  // Write sparse cells
  int coords_2[] = {1, 4, 5};
  uint64_t coords_2_size = sizeof(coords_2);
  int data_2[] = {3, 4, 5};
  uint64_t data_2_size = sizeof(data_2);

  // Create the query
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_UNORDERED);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, query, "a", data_2, &data_2_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, query, "d", coords_2, &coords_2_size);
  CHECK(rc == TILEDB_OK);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_OK);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);

  // Open array for reading
  rc = tiledb_array_alloc(ctx_, array_name_.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Set subarray
  int subarray[] = {1, 10};

  // Prepare the vector that will hold the result
  int coords_r[5];
  int data_r[5];
  uint64_t coords_r_size = sizeof(coords_r);
  uint64_t data_r_size = sizeof(data_r);

  // Create query
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray(ctx_, query, subarray);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_ROW_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, query, "a", data_r, &data_r_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, TILEDB_COORDS, coords_r, &coords_r_size);
  CHECK(rc == TILEDB_OK);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_OK);

  // Clean up
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
  tiledb_query_free(&query);

  CHECK(data_1_size + data_2_size == data_r_size);
  CHECK(coords_1_size + coords_2_size == coords_r_size);

  int coords_c[] = {1, 1, 2, 4, 5};
  // The ordering for duplicates is undefined, check all variations.
  int data_c_1[] = {1, 3, 2, 4, 5};
  int data_c_2[] = {3, 1, 2, 4, 5};
  CHECK(!std::memcmp(coords_c, coords_r, coords_r_size));
  bool data_c_matches = !std::memcmp(data_c_1, data_r, data_r_size) ||
                        !std::memcmp(data_c_2, data_r, data_r_size);
  CHECK(data_c_matches);

  // Consolidate
  rc = tiledb_array_consolidate(ctx_, array_name_.c_str(), nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_vacuum(ctx_, array_name_.c_str(), nullptr);
  REQUIRE(rc == TILEDB_OK);

  // Open array for reading - #2
  rc = tiledb_array_alloc(ctx_, array_name_.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Prepare the vector that will hold the result
  coords_r_size = sizeof(coords_r);
  data_r_size = sizeof(data_r);

  // Create query
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray(ctx_, query, subarray);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_ROW_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, query, "a", data_r, &data_r_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, TILEDB_COORDS, coords_r, &coords_r_size);
  CHECK(rc == TILEDB_OK);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_OK);

  // Clean up
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
  tiledb_query_free(&query);

  CHECK(data_1_size + data_2_size == data_r_size);
  CHECK(coords_1_size + coords_2_size == coords_r_size);

  CHECK(!std::memcmp(coords_c, coords_r, coords_r_size));
  // The ordering for duplicates is undefined, check all variations.
  data_c_matches = !std::memcmp(data_c_1, data_r, data_r_size) ||
                   !std::memcmp(data_c_2, data_r, data_r_size);
  CHECK(data_c_matches);
}

TEST_CASE_METHOD(
    CDuplicatesFx,
    "C API: Duplicates, multiple fragment, read multiple ranges, unordered",
    "[duplicates][multiple-fragments][multiple-ranges][unordered]") {
  // Create default array
  create_default_array_1d();

  // Open array for writing - #1
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name_.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  CHECK(rc == TILEDB_OK);

  // Write sparse cells
  int coords_1[] = {1, 2};
  uint64_t coords_1_size = sizeof(coords_1);
  int data_1[] = {1, 2};
  uint64_t data_1_size = sizeof(data_1);

  // Create the query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_UNORDERED);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, query, "a", data_1, &data_1_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, query, "d", coords_1, &coords_1_size);
  CHECK(rc == TILEDB_OK);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_OK);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);

  // Open array for writing - #2
  rc = tiledb_array_alloc(ctx_, array_name_.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  CHECK(rc == TILEDB_OK);

  // Write sparse cells
  int coords_2[] = {1, 4, 5};
  uint64_t coords_2_size = sizeof(coords_2);
  int data_2[] = {3, 4, 5};
  uint64_t data_2_size = sizeof(data_2);

  // Create the query
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_UNORDERED);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, query, "a", data_2, &data_2_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, query, "d", coords_2, &coords_2_size);
  CHECK(rc == TILEDB_OK);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_OK);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);

  // Open array for reading
  rc = tiledb_array_alloc(ctx_, array_name_.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Prepare the vector that will hold the result
  int coords_r[5];
  int data_r[5];
  uint64_t coords_r_size = sizeof(coords_r);
  uint64_t data_r_size = sizeof(data_r);

  // Create query
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_UNORDERED);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, query, "a", data_r, &data_r_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, TILEDB_COORDS, coords_r, &coords_r_size);
  CHECK(rc == TILEDB_OK);

  // Set multi-range subarray to query
  int start_1 = 1, end_1 = 2;
  int start_2 = 4, end_2 = 10;
  rc = tiledb_query_add_range(ctx_, query, 0, &start_1, &end_1, NULL);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_add_range(ctx_, query, 0, &start_2, &end_2, NULL);
  CHECK(rc == TILEDB_OK);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_OK);

  // Keep track of found data
  uint64_t num_found_coords[10] = {0};
  uint64_t num_found_data[10] = {0};

  tiledb_query_status_t status;
  rc = tiledb_query_get_status(ctx_, query, &status);
  CHECK(rc == TILEDB_OK);

  CHECK(data_r_size == 20);
  CHECK(coords_r_size == 20);

  // Save found data
  for (uint64_t i = 0; i < coords_r_size / sizeof(int); i++) {
    if (coords_r[i] < 10) {
      num_found_coords[coords_r[i]]++;
    }
  }

  for (uint64_t i = 0; i < data_r_size / sizeof(int); i++) {
    if (data_r[i] < 10) {
      num_found_data[data_r[i]]++;
    }
  }

  // Clean up
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
  tiledb_query_free(&query);

  // Validate results
  CHECK(num_found_coords[1] == 2);
  CHECK(num_found_coords[2] == 1);
  CHECK(num_found_coords[4] == 1);
  CHECK(num_found_coords[5] == 1);

  CHECK(num_found_data[1] == 1);
  CHECK(num_found_data[2] == 1);
  CHECK(num_found_data[3] == 1);
  CHECK(num_found_data[4] == 1);
  CHECK(num_found_data[5] == 1);
}

TEST_CASE_METHOD(
    CDuplicatesFx,
    "C API: Duplicates, empty range, unordered",
    "[duplicates][empty-range][unordered]") {
  // Create default array
  create_default_array_1d();

  // Open array for writing - #1
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name_.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  CHECK(rc == TILEDB_OK);

  // Write sparse cells
  int coords_1[] = {1, 2};
  uint64_t coords_1_size = sizeof(coords_1);
  int data_1[] = {1, 2};
  uint64_t data_1_size = sizeof(data_1);

  // Create the query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_UNORDERED);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, query, "a", data_1, &data_1_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, query, "d", coords_1, &coords_1_size);
  CHECK(rc == TILEDB_OK);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_OK);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);

  // Open array for reading
  rc = tiledb_array_alloc(ctx_, array_name_.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Prepare the vector that will hold the result
  int coords_r[5];
  int data_r[5];
  uint64_t coords_r_size = sizeof(coords_r);
  uint64_t data_r_size = sizeof(data_r);

  // Create query
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_UNORDERED);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, query, "a", data_r, &data_r_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, TILEDB_COORDS, coords_r, &coords_r_size);
  CHECK(rc == TILEDB_OK);

  // Set empty subarray to query
  int start_1 = 9, end_1 = 10;
  rc = tiledb_query_add_range(ctx_, query, 0, &start_1, &end_1, NULL);
  CHECK(rc == TILEDB_OK);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_OK);

  tiledb_query_status_t status;
  rc = tiledb_query_get_status(ctx_, query, &status);
  CHECK(rc == TILEDB_OK);
  CHECK(status == TILEDB_COMPLETED);

  CHECK(data_r_size == 0);
  CHECK(coords_r_size == 0);

  // Clean up
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
  tiledb_query_free(&query);
}

TEST_CASE_METHOD(
    CDuplicatesFx,
    "C API: Duplicates, empty range then non empty range, unordered",
    "[duplicates][empty-range-then-non-empty][unordered]") {
  // Create default array
  create_default_array_1d();

  // Open array for writing - #1
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name_.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  CHECK(rc == TILEDB_OK);

  // Write sparse cells
  int coords_1[] = {1, 2};
  uint64_t coords_1_size = sizeof(coords_1);
  int data_1[] = {1, 2};
  uint64_t data_1_size = sizeof(data_1);

  // Create the query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_UNORDERED);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, query, "a", data_1, &data_1_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, query, "d", coords_1, &coords_1_size);
  CHECK(rc == TILEDB_OK);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_OK);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);

  // Open array for reading
  rc = tiledb_array_alloc(ctx_, array_name_.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Prepare the vector that will hold the result
  int coords_r[5];
  int data_r[5];
  uint64_t coords_r_size = sizeof(coords_r);
  uint64_t data_r_size = sizeof(data_r);

  // Create query
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_UNORDERED);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, query, "a", data_r, &data_r_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, TILEDB_COORDS, coords_r, &coords_r_size);
  CHECK(rc == TILEDB_OK);

  // Set multi-range subarray to query
  int start_1 = 9, end_1 = 10;
  int start_2 = 1, end_2 = 2;
  rc = tiledb_query_add_range(ctx_, query, 0, &start_1, &end_1, NULL);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_add_range(ctx_, query, 0, &start_2, &end_2, NULL);
  CHECK(rc == TILEDB_OK);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_OK);

  tiledb_query_status_t status;
  rc = tiledb_query_get_status(ctx_, query, &status);
  CHECK(rc == TILEDB_OK);
  CHECK(status == TILEDB_COMPLETED);

  CHECK(data_r_size == 8);
  CHECK(coords_r_size == 8);

  CHECK(data_r[0] == 1);
  CHECK(data_r[1] == 2);

  // Clean up
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
  tiledb_query_free(&query);
}