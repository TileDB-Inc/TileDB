/**
 * @file unit-sparse-global-order-reader.cc
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
 * Tests for the sparse global order reader.
 */

#include "test/support/src/helpers.h"
#include "test/support/src/vfs_helpers.h"
#include "tiledb/sm/c_api/tiledb.h"
#include "tiledb/sm/c_api/tiledb_struct_def.h"
#include "tiledb/sm/cpp_api/tiledb"
#include "tiledb/sm/query/readers/sparse_global_order_reader.h"

#ifdef _WIN32
#include "tiledb/sm/filesystem/win.h"
#else
#include "tiledb/sm/filesystem/posix.h"
#endif

#include <test/support/tdb_catch.h>
#include <numeric>

using namespace tiledb;
using namespace tiledb::test;

/* ********************************* */
/*         STRUCT DEFINITION         */
/* ********************************* */

struct CSparseGlobalOrderFx {
  tiledb_ctx_t* ctx_ = nullptr;
  tiledb_vfs_t* vfs_ = nullptr;
  std::string temp_dir_;
  std::string array_name_;
  const char* ARRAY_NAME = "test_sparse_global_order";
  tiledb_array_t* array_ = nullptr;
  std::string total_budget_;
  std::string ratio_tile_ranges_;
  std::string ratio_array_data_;
  std::string ratio_coords_;

  void create_default_array_1d(bool allow_dups = false);
  void create_default_array_1d_strings(bool allow_dups = false);
  void write_1d_fragment(
      int* coords, uint64_t* coords_size, int* data, uint64_t* data_size);
  void write_1d_fragment_strings(
      int* coords,
      uint64_t* coords_size,
      char* data,
      uint64_t* data_size,
      uint64_t* offsets,
      uint64_t* offsets_size);
  void write_delete_condition(char* value_to_delete, uint64_t value_size);
  int32_t read(
      bool set_subarray,
      int qc_idx,
      int* coords,
      uint64_t* coords_size,
      int* data,
      uint64_t* data_size,
      tiledb_query_t** query = nullptr,
      tiledb_array_t** array_ret = nullptr,
      std::vector<int> subarray = {1, 10});
  int32_t read_strings(
      bool set_subarray,
      int* coords,
      uint64_t* coords_size,
      char* data,
      uint64_t* data_size,
      uint64_t* offsets,
      uint64_t* offsets_size,
      tiledb_query_t** query = nullptr,
      tiledb_array_t** array_ret = nullptr,
      std::vector<int> subarray = {1, 10});
  void reset_config();
  void update_config();

  CSparseGlobalOrderFx();
  ~CSparseGlobalOrderFx();
};

CSparseGlobalOrderFx::CSparseGlobalOrderFx() {
  reset_config();

  // Create temporary directory based on the supported filesystem.
#ifdef _WIN32
  temp_dir_ = tiledb::sm::Win::current_dir() + "\\tiledb_test\\";
#else
  temp_dir_ = "file://" + tiledb::sm::Posix::current_dir() + "/tiledb_test/";
#endif
  create_dir(temp_dir_, ctx_, vfs_);
  array_name_ = temp_dir_ + ARRAY_NAME;
}

CSparseGlobalOrderFx::~CSparseGlobalOrderFx() {
  tiledb_array_free(&array_);
  remove_dir(temp_dir_, ctx_, vfs_);
  tiledb_ctx_free(&ctx_);
  tiledb_vfs_free(&vfs_);
}

void CSparseGlobalOrderFx::reset_config() {
  total_budget_ = "1048576";
  ratio_tile_ranges_ = "0.1";
  ratio_array_data_ = "0.1";
  ratio_coords_ = "0.5";
  update_config();
}

void CSparseGlobalOrderFx::update_config() {
  if (ctx_ != nullptr)
    tiledb_ctx_free(&ctx_);

  if (vfs_ != nullptr)
    tiledb_vfs_free(&vfs_);

  tiledb_config_t* config;
  tiledb_error_t* error = nullptr;
  REQUIRE(tiledb_config_alloc(&config, &error) == TILEDB_OK);
  REQUIRE(error == nullptr);

  REQUIRE(
      tiledb_config_set(
          config,
          "sm.query.sparse_global_order.reader",
          "refactored",
          &error) == TILEDB_OK);
  REQUIRE(error == nullptr);

  REQUIRE(
      tiledb_config_set(
          config, "sm.mem.total_budget", total_budget_.c_str(), &error) ==
      TILEDB_OK);
  REQUIRE(error == nullptr);

  REQUIRE(
      tiledb_config_set(
          config,
          "sm.mem.reader.sparse_global_order.ratio_tile_ranges",
          ratio_tile_ranges_.c_str(),
          &error) == TILEDB_OK);
  REQUIRE(error == nullptr);

  REQUIRE(
      tiledb_config_set(
          config,
          "sm.mem.reader.sparse_global_order.ratio_array_data",
          ratio_array_data_.c_str(),
          &error) == TILEDB_OK);
  REQUIRE(error == nullptr);

  REQUIRE(
      tiledb_config_set(
          config,
          "sm.mem.reader.sparse_global_order.ratio_coords",
          ratio_coords_.c_str(),
          &error) == TILEDB_OK);
  REQUIRE(error == nullptr);

  REQUIRE(tiledb_ctx_alloc(config, &ctx_) == TILEDB_OK);
  REQUIRE(error == nullptr);
  REQUIRE(tiledb_vfs_alloc(ctx_, config, &vfs_) == TILEDB_OK);
  tiledb_config_free(&config);
}

void CSparseGlobalOrderFx::create_default_array_1d(bool allow_dups) {
  int domain[] = {1, 200};
  int tile_extent = 2;
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
      allow_dups);
}

void CSparseGlobalOrderFx::create_default_array_1d_strings(bool allow_dups) {
  int domain[] = {1, 200};
  int tile_extent = 2;
  create_array(
      ctx_,
      array_name_,
      TILEDB_SPARSE,
      {"d"},
      {TILEDB_INT32},
      {domain},
      {&tile_extent},
      {"a"},
      {TILEDB_STRING_ASCII},
      {TILEDB_VAR_NUM},
      {tiledb::test::Compressor(TILEDB_FILTER_NONE, -1)},
      TILEDB_ROW_MAJOR,
      TILEDB_ROW_MAJOR,
      2,
      allow_dups);
}

void CSparseGlobalOrderFx::write_1d_fragment(
    int* coords, uint64_t* coords_size, int* data, uint64_t* data_size) {
  // Open array for writing.
  tiledb_array_t* array;
  auto rc = tiledb_array_alloc(ctx_, array_name_.c_str(), &array);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  REQUIRE(rc == TILEDB_OK);

  // Create the query.
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_UNORDERED);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, query, "a", data, data_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, query, "d", coords, coords_size);
  REQUIRE(rc == TILEDB_OK);

  // Submit query.
  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  // Close array.
  rc = tiledb_array_close(ctx_, array);
  REQUIRE(rc == TILEDB_OK);

  // Clean up.
  tiledb_array_free(&array);
  tiledb_query_free(&query);
}

void CSparseGlobalOrderFx::write_1d_fragment_strings(
    int* coords,
    uint64_t* coords_size,
    char* data,
    uint64_t* data_size,
    uint64_t* offsets,
    uint64_t* offsets_size) {
  // Open array for writing.
  tiledb_array_t* array;
  auto rc = tiledb_array_alloc(ctx_, array_name_.c_str(), &array);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  REQUIRE(rc == TILEDB_OK);

  // Create the query.
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_UNORDERED);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, query, "a", data, data_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_offsets_buffer(ctx_, query, "a", offsets, offsets_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, query, "d", coords, coords_size);
  REQUIRE(rc == TILEDB_OK);

  // Submit query.
  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  // Close array.
  rc = tiledb_array_close(ctx_, array);
  REQUIRE(rc == TILEDB_OK);

  // Clean up.
  tiledb_array_free(&array);
  tiledb_query_free(&query);
}

void CSparseGlobalOrderFx::write_delete_condition(
    char* value_to_delete, uint64_t value_size) {
  // Open array for delete.
  tiledb_array_t* array;
  auto rc = tiledb_array_alloc(ctx_, array_name_.c_str(), &array);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_DELETE);
  REQUIRE(rc == TILEDB_OK);

  // Create the query.
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_DELETE, &query);
  REQUIRE(rc == TILEDB_OK);

  // Add condition.
  tiledb_query_condition_t* qc;
  rc = tiledb_query_condition_alloc(ctx_, &qc);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_condition_init(
      ctx_, qc, "a", value_to_delete, value_size, TILEDB_EQ);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_condition(ctx_, query, qc);
  CHECK(rc == TILEDB_OK);

  // Submit query.
  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  // Close array.
  rc = tiledb_array_close(ctx_, array);
  REQUIRE(rc == TILEDB_OK);

  // Clean up.
  tiledb_query_condition_free(&qc);
  tiledb_array_free(&array);
  tiledb_query_free(&query);
}

int32_t CSparseGlobalOrderFx::read(
    bool set_subarray,
    int qc_idx,
    int* coords,
    uint64_t* coords_size,
    int* data,
    uint64_t* data_size,
    tiledb_query_t** query_ret,
    tiledb_array_t** array_ret,
    std::vector<int> subarray) {
  // Open array for reading.
  tiledb_array_t* array;
  auto rc = tiledb_array_alloc(ctx_, array_name_.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Create query.
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);

  if (set_subarray) {
    // Set subarray.
    tiledb_subarray_t* sub;
    rc = tiledb_subarray_alloc(ctx_, array, &sub);
    CHECK(rc == TILEDB_OK);
    rc = tiledb_subarray_set_subarray(ctx_, sub, subarray.data());
    CHECK(rc == TILEDB_OK);
    rc = tiledb_query_set_subarray_t(ctx_, query, sub);
    CHECK(rc == TILEDB_OK);
    tiledb_subarray_free(&sub);
  }

  if (qc_idx != 0) {
    tiledb_query_condition_t* query_condition = nullptr;
    rc = tiledb_query_condition_alloc(ctx_, &query_condition);
    CHECK(rc == TILEDB_OK);

    if (qc_idx == 1) {
      int32_t val = 11;
      rc = tiledb_query_condition_init(
          ctx_, query_condition, "a", &val, sizeof(int32_t), TILEDB_LT);
      CHECK(rc == TILEDB_OK);
    } else if (qc_idx == 2) {
      // Negated query condition should produce the same results.
      int32_t val = 11;
      tiledb_query_condition_t* qc;
      rc = tiledb_query_condition_alloc(ctx_, &qc);
      CHECK(rc == TILEDB_OK);
      rc = tiledb_query_condition_init(
          ctx_, qc, "a", &val, sizeof(int32_t), TILEDB_GE);
      CHECK(rc == TILEDB_OK);
      rc = tiledb_query_condition_negate(ctx_, qc, &query_condition);
      CHECK(rc == TILEDB_OK);

      tiledb_query_condition_free(&qc);
    }

    rc = tiledb_query_set_condition(ctx_, query, query_condition);
    CHECK(rc == TILEDB_OK);

    tiledb_query_condition_free(&query_condition);
  }

  rc = tiledb_query_set_layout(ctx_, query, TILEDB_GLOBAL_ORDER);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, query, "a", data, data_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, query, "d", coords, coords_size);
  CHECK(rc == TILEDB_OK);

  // Submit query.
  auto ret = tiledb_query_submit(ctx_, query);
  if (query_ret == nullptr || array_ret == nullptr) {
    // Clean up.
    rc = tiledb_array_close(ctx_, array);
    CHECK(rc == TILEDB_OK);
    tiledb_array_free(&array);
    tiledb_query_free(&query);
  } else {
    *query_ret = query;
    *array_ret = array;
  }

  return ret;
}

int32_t CSparseGlobalOrderFx::read_strings(
    bool set_subarray,
    int* coords,
    uint64_t* coords_size,
    char* data,
    uint64_t* data_size,
    uint64_t* offsets,
    uint64_t* offsets_size,
    tiledb_query_t** query_ret,
    tiledb_array_t** array_ret,
    std::vector<int> subarray) {
  // Open array for reading.
  tiledb_array_t* array;
  auto rc = tiledb_array_alloc(ctx_, array_name_.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Create query.
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);

  if (set_subarray) {
    // Set subarray.
    tiledb_subarray_t* sub;
    rc = tiledb_subarray_alloc(ctx_, array, &sub);
    CHECK(rc == TILEDB_OK);
    rc = tiledb_subarray_set_subarray(ctx_, sub, subarray.data());
    CHECK(rc == TILEDB_OK);
    rc = tiledb_query_set_subarray_t(ctx_, query, sub);
    CHECK(rc == TILEDB_OK);
    tiledb_subarray_free(&sub);
  }

  rc = tiledb_query_set_layout(ctx_, query, TILEDB_GLOBAL_ORDER);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, query, "a", data, data_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_offsets_buffer(ctx_, query, "a", offsets, offsets_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, query, "d", coords, coords_size);
  CHECK(rc == TILEDB_OK);

  // Submit query.
  auto ret = tiledb_query_submit(ctx_, query);
  if (query_ret == nullptr || array_ret == nullptr) {
    // Clean up.
    rc = tiledb_array_close(ctx_, array);
    CHECK(rc == TILEDB_OK);
    tiledb_array_free(&array);
    tiledb_query_free(&query);
  } else {
    *query_ret = query;
    *array_ret = array;
  }

  return ret;
}

/* ********************************* */
/*                TESTS              */
/* ********************************* */

TEST_CASE_METHOD(
    CSparseGlobalOrderFx,
    "Sparse global order reader: Tile ranges budget exceeded",
    "[sparse-global-order][tile-ranges][budget-exceeded]") {
  // Create default array.
  reset_config();
  create_default_array_1d();

  // Write a fragment.
  int coords[] = {1, 2, 3, 4, 5};
  uint64_t coords_size = sizeof(coords);
  int data[] = {1, 2, 3, 4, 5};
  uint64_t data_size = sizeof(data);
  write_1d_fragment(coords, &coords_size, data, &data_size);

  // We should have one tile range (size 16) which will be bigger than budget
  // (10).
  total_budget_ = "1000";
  ratio_tile_ranges_ = "0.01";
  update_config();

  // Try to read.
  int coords_r[5];
  int data_r[5];
  uint64_t coords_r_size = sizeof(coords_r);
  uint64_t data_r_size = sizeof(data_r);
  auto rc = read(true, 0, coords_r, &coords_r_size, data_r, &data_r_size);
  CHECK(rc == TILEDB_ERR);

  // Check we hit the correct error.
  tiledb_error_t* error = NULL;
  rc = tiledb_ctx_get_last_error(ctx_, &error);
  CHECK(rc == TILEDB_OK);

  const char* msg;
  rc = tiledb_error_message(error, &msg);
  CHECK(rc == TILEDB_OK);

  std::string error_str(msg);
  CHECK(
      error_str.find("Exceeded memory budget for result tile ranges") !=
      std::string::npos);
}

TEST_CASE_METHOD(
    CSparseGlobalOrderFx,
    "Sparse global order reader: Tile ranges budget exceeded sc-23660 A",
    "[sparse-global-order][tile-ranges][budget-exceeded][.regression]") {
  // Observed to produce the observed_bad_data in v2.12.2.

  // Create default array.
  reset_config();
  create_default_array_1d();

  // Write a fragment.
  int coords1[] = {1, 3, 5, 7, 9};
  uint64_t coords1_size = sizeof(coords1);
  int data1[] = {11, 33, 55, 77, 99};
  uint64_t data1_size = sizeof(data1);
  write_1d_fragment(coords1, &coords1_size, data1, &data1_size);

  // Write another fragment with coords(/values) interleaved with prev.
  int coords2[] = {2, 4, 6, 8, 10};
  uint64_t coords2_size = sizeof(coords2);
  int data2[] = {22, 44, 66, 88, 1010};
  uint64_t data2_size = sizeof(data2);
  write_1d_fragment(coords2, &coords2_size, data2, &data2_size);

  // Specific relationship for failure not known, but these values
  // will result in failure with data being written.
  total_budget_ = "10000";
  // Failure here occurs with the value of 0.1 for ratio_tile_ranges_.
  update_config();

  tiledb_array_t* array = nullptr;
  tiledb_query_t* query = nullptr;

  // Try to read.
  int coords_r[2];
  int data_r[2];
  uint64_t coords_r_size = sizeof(coords_r);
  uint64_t data_r_size = sizeof(data_r);
  std::vector<int> subarray{4, 10};
  int rc;
  tiledb_query_status_t status;
  rc = read(
      true,
      0,
      coords_r,
      &coords_r_size,
      data_r,
      &data_r_size,
      &query,
      &array,
      subarray);
  CHECK(rc == TILEDB_OK);

  std::vector<int> retrieved_data;
  retrieved_data.reserve(10);

  do {
    auto nitems = data_r_size / sizeof(int);
    for (auto ui = 0u; ui < nitems; ++ui) {
      retrieved_data.emplace_back(data_r[ui]);
    }

    // Check incomplete query status.
    tiledb_query_get_status(ctx_, query, &status);
    if (status == TILEDB_INCOMPLETE) {
      rc = tiledb_query_submit(ctx_, query);
      CHECK(rc == TILEDB_OK);
    }
  } while (status == TILEDB_INCOMPLETE);

  // The correct data should be...
  std::vector<int> expected_correct_data{44, 55, 66, 77, 88, 99, 1010};
  // But the error situation in v2.12.2 instead produced...
  std::vector<int> observed_bad_data{44, 66, 88, 1010};
  CHECK_FALSE(retrieved_data == observed_bad_data);
  CHECK(retrieved_data == expected_correct_data);
}

TEST_CASE_METHOD(
    CSparseGlobalOrderFx,
    "Sparse global order reader: Tile ranges budget exceeded sc-23660 B",
    "[sparse-global-order][tile-ranges][budget-exceeded][.regression]") {
  // Observed to produce the observed_bad_data in v2.12.2.

  // Similar in nature to the "... A" version, but using some differently
  // written data.

  // Create default array.
  reset_config();
  create_default_array_1d();

  // Write a fragment.
  int coords1[] = {1, 4, 7, 10};
  uint64_t coords1_size = sizeof(coords1);
  int data1[] = {11, 44, 77, 1010};
  uint64_t data1_size = sizeof(data1);

  // Write another fragment with coords(/values) interleaved with prev.
  int coords2[] = {2, 5, 8, 11};
  uint64_t coords2_size = sizeof(coords2);
  int data2[] = {22, 55, 88, 1111};
  uint64_t data2_size = sizeof(data2);

  // Write another fragment with coords(/values) interleaved with prev.
  int coords3[] = {3, 6, 9, 12};
  uint64_t coords3_size = sizeof(coords3);
  int data3[] = {33, 66, 99, 1212};
  uint64_t data3_size = sizeof(data3);

  write_1d_fragment(coords1, &coords1_size, data1, &data1_size);
  write_1d_fragment(coords3, &coords3_size, data3, &data3_size);
  write_1d_fragment(coords2, &coords2_size, data2, &data2_size);

  // specific relationship for failure not known, but these values
  // will result in failure with data being written.
  total_budget_ = "15000";
  // Failure here occurs with the value of 0.1 for ratio_tile_ranges_.
  update_config();

  tiledb_array_t* array = nullptr;
  tiledb_query_t* query = nullptr;

  // Try to read.
  int coords_r[1];
  int data_r[1];
  uint64_t coords_r_size = sizeof(coords_r);
  uint64_t data_r_size = sizeof(data_r);
  std::vector<int> subarray{5, 11};
  int rc;
  tiledb_query_status_t status;
  rc = read(
      true,
      0,
      coords_r,
      &coords_r_size,
      data_r,
      &data_r_size,
      &query,
      &array,
      subarray);
  CHECK(rc == TILEDB_OK);

  std::vector<int> retrieved_data;
  retrieved_data.reserve(10);

  do {
    auto nitems = data_r_size / sizeof(int);
    for (auto ui = 0u; ui < nitems; ++ui) {
      retrieved_data.emplace_back(data_r[ui]);
    }

    // Check incomplete query status.
    tiledb_query_get_status(ctx_, query, &status);
    if (status == TILEDB_INCOMPLETE) {
      rc = tiledb_query_submit(ctx_, query);
      CHECK(rc == TILEDB_OK);
    }
  } while (status == TILEDB_INCOMPLETE);

  // The correct data should be...
  std::vector<int> expected_correct_data{55, 66, 77, 88, 99, 1010, 1111};
  // But the error situation in v2.12.2 instead produced...
  std::vector<int> observed_bad_data{55, 66, 88, 99, 1111};
  CHECK_FALSE(retrieved_data == observed_bad_data);
  CHECK(retrieved_data == expected_correct_data);
}

TEST_CASE_METHOD(
    CSparseGlobalOrderFx,
    "Sparse global order reader: tile offsets budget exceeded",
    "[sparse-global-order][tile-offsets][budget-exceeded]") {
  // Create default array.
  reset_config();
  create_default_array_1d();

  // Write a fragment.
  std::vector<int> coords(200);
  std::iota(coords.begin(), coords.end(), 1);
  uint64_t coords_size = coords.size() * sizeof(int);

  std::vector<int> data(200);
  std::iota(data.begin(), data.end(), 1);
  uint64_t data_size = data.size() * sizeof(int);

  write_1d_fragment(coords.data(), &coords_size, data.data(), &data_size);

  // We should have 100 tiles (tile offset size 800) which will be bigger than
  // leftover budget.
  total_budget_ = "3000";
  ratio_array_data_ = "0.5";
  update_config();

  // Try to read.
  int coords_r[5];
  int data_r[5];
  uint64_t coords_r_size = sizeof(coords_r);
  uint64_t data_r_size = sizeof(data_r);
  auto rc = read(true, 0, coords_r, &coords_r_size, data_r, &data_r_size);
  CHECK(rc == TILEDB_ERR);

  // Check we hit the correct error.
  tiledb_error_t* error = NULL;
  rc = tiledb_ctx_get_last_error(ctx_, &error);
  CHECK(rc == TILEDB_OK);

  const char* msg;
  rc = tiledb_error_message(error, &msg);
  CHECK(rc == TILEDB_OK);

  std::string error_str(msg);
  CHECK(
      error_str.find("SparseGlobalOrderReader: Cannot load tile offsets, "
                     "computed size") != std::string::npos);
}

TEST_CASE_METHOD(
    CSparseGlobalOrderFx,
    "Sparse global order reader: coords budget forcing one tile at a time",
    "[sparse-global-order][small-coords-budget]") {
  // Create default array.
  reset_config();
  create_default_array_1d();

  bool use_subarray = false;
  SECTION("- No subarray") {
    use_subarray = false;
  }
  SECTION("- Subarray") {
    use_subarray = true;
  }

  int num_frags = 2;
  for (int i = 1; i < num_frags + 1; i++) {
    // Write a fragment.
    int coords[] = {
        i,
        num_frags + i,
        2 * num_frags + i,
        3 * num_frags + i,
        4 * num_frags + i};
    uint64_t coords_size = sizeof(coords);
    int data[] = {
        i,
        num_frags + i,
        2 * num_frags + i,
        3 * num_frags + i,
        4 * num_frags + i};
    uint64_t data_size = sizeof(data);
    write_1d_fragment(coords, &coords_size, data, &data_size);
  }

  // Two result tile (2 * (~3000 + 8) will be bigger than the per fragment
  // budget (1000).
  total_budget_ = "35000";
  ratio_coords_ = "0.11";
  update_config();

  tiledb_array_t* array = nullptr;
  tiledb_query_t* query = nullptr;

  uint32_t rc;

  // Try to read.
  int coords_r[10];
  int data_r[10];
  uint64_t coords_r_size = sizeof(coords_r);
  uint64_t data_r_size = sizeof(data_r);
  rc = read(
      use_subarray,
      0,
      coords_r,
      &coords_r_size,
      data_r,
      &data_r_size,
      &query,
      &array);
  CHECK(rc == TILEDB_OK);

  // Check the internal loop count against expected value.
  auto stats =
      ((sm::SparseGlobalOrderReader<uint8_t>*)query->query_->strategy())
          ->stats();
  REQUIRE(stats != nullptr);
  auto counters = stats->counters();
  REQUIRE(counters != nullptr);
  auto loop_num =
      counters->find("Context.StorageManager.Query.Reader.internal_loop_num");
  CHECK(5 == loop_num->second);

  // Check incomplete query status.
  tiledb_query_status_t status;
  tiledb_query_get_status(ctx_, query, &status);
  CHECK(status == TILEDB_COMPLETED);

  CHECK(40 == data_r_size);
  CHECK(40 == coords_r_size);

  int coords_c[] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
  int data_c[] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
  CHECK(!std::memcmp(coords_c, coords_r, coords_r_size));
  CHECK(!std::memcmp(data_c, data_r, data_r_size));

  // Clean up.
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
  tiledb_query_free(&query);
}

TEST_CASE_METHOD(
    CSparseGlobalOrderFx,
    "Sparse global order reader: coords budget too small",
    "[sparse-global-order][coords-budget][too-small]") {
  // Create default array.
  reset_config();
  create_default_array_1d();

  bool use_subarray = false;
  SECTION("- No subarray") {
    use_subarray = false;
  }
  SECTION("- Subarray") {
    use_subarray = true;
  }

  // Write a fragment.
  int coords[] = {1, 2, 3, 4, 5};
  uint64_t coords_size = sizeof(coords);
  int data[] = {1, 2, 3, 4, 5};
  uint64_t data_size = sizeof(data);
  write_1d_fragment(coords, &coords_size, data, &data_size);

  // One result tile (8 + ~440) will be bigger than the budget (400).
  total_budget_ = "19000";
  ratio_coords_ = "0.04";
  update_config();

  // Try to read.
  int coords_r[5];
  int data_r[5];
  uint64_t coords_r_size = sizeof(coords_r);
  uint64_t data_r_size = sizeof(data_r);
  auto rc =
      read(use_subarray, 0, coords_r, &coords_r_size, data_r, &data_r_size);
  CHECK(rc == TILEDB_ERR);

  // Check we hit the correct error.
  tiledb_error_t* error = NULL;
  rc = tiledb_ctx_get_last_error(ctx_, &error);
  CHECK(rc == TILEDB_OK);

  const char* msg;
  rc = tiledb_error_message(error, &msg);
  CHECK(rc == TILEDB_OK);

  std::string error_str(msg);
  CHECK(error_str.find("Cannot load a single tile") != std::string::npos);
}

TEST_CASE_METHOD(
    CSparseGlobalOrderFx,
    "Sparse global order reader: qc removes full tile",
    "[sparse-global-order][qc-removes-tile]") {
  // Create default array.
  reset_config();
  create_default_array_1d();

  bool use_subarray = false;
  int tile_idx = 0;
  int qc_idx = GENERATE(1, 2);
  SECTION("- No subarray") {
    use_subarray = false;
    SECTION("- First tile") {
      tile_idx = 0;
    }
    SECTION("- Second tile") {
      tile_idx = 1;
    }
    SECTION("- Last tile") {
      tile_idx = 2;
    }
  }
  SECTION("- Subarray") {
    use_subarray = true;
    SECTION("- First tile") {
      tile_idx = 0;
    }
    SECTION("- Second tile") {
      tile_idx = 1;
    }
    SECTION("- Last tile") {
      tile_idx = 2;
    }
  }

  int coords_1[] = {1, 2, 3};
  int data_1[] = {1, 2, 3};

  int coords_2[] = {4, 5, 6};
  int data_2[] = {4, 5, 6};

  int coords_3[] = {12, 13, 14};
  int data_3[] = {12, 13, 14};

  uint64_t coords_size = sizeof(coords_1);
  uint64_t data_size = sizeof(data_1);

  // Create the aray so the removed tile is at the correct index.
  switch (tile_idx) {
    case 0:
      write_1d_fragment(coords_3, &coords_size, data_3, &data_size);
      write_1d_fragment(coords_1, &coords_size, data_1, &data_size);
      write_1d_fragment(coords_2, &coords_size, data_2, &data_size);
      break;

    case 1:
      write_1d_fragment(coords_1, &coords_size, data_1, &data_size);
      write_1d_fragment(coords_3, &coords_size, data_3, &data_size);
      write_1d_fragment(coords_2, &coords_size, data_2, &data_size);
      break;

    case 2:
      write_1d_fragment(coords_1, &coords_size, data_1, &data_size);
      write_1d_fragment(coords_2, &coords_size, data_2, &data_size);
      write_1d_fragment(coords_3, &coords_size, data_3, &data_size);
      break;
  }

  // Read.
  int coords_r[6];
  int data_r[6];
  uint64_t coords_r_size = sizeof(coords_r);
  uint64_t data_r_size = sizeof(data_r);

  auto rc = read(
      use_subarray, qc_idx, coords_r, &coords_r_size, data_r, &data_r_size);
  CHECK(rc == TILEDB_OK);

  // Should read two tile (6 values).
  CHECK(24 == data_r_size);
  CHECK(24 == coords_r_size);

  int coords_c[] = {1, 2, 3, 4, 5, 6};
  int data_c[] = {1, 2, 3, 4, 5, 6};
  CHECK(!std::memcmp(coords_c, coords_r, coords_r_size));
  CHECK(!std::memcmp(data_c, data_r, data_r_size));
}

TEST_CASE_METHOD(
    CSparseGlobalOrderFx,
    "Sparse global order reader: qc removes tile from second fragment "
    "replacing data from first fragment",
    "[sparse-global-order][qc-removes-replacement-data]") {
  // Create default array.
  reset_config();

  bool use_subarray = GENERATE(true, false);
  bool dups = GENERATE(false, true);
  bool extra_fragment = GENERATE(true, false);
  int qc_idx = GENERATE(1, 2);

  create_default_array_1d(dups);

  int coords_1[] = {1, 2, 3};
  int data_1[] = {2, 2, 2};

  int coords_2[] = {1, 2, 3};
  int data_2[] = {12, 12, 12};

  uint64_t coords_size = sizeof(coords_1);
  uint64_t data_size = sizeof(data_1);
  write_1d_fragment(coords_1, &coords_size, data_1, &data_size);
  write_1d_fragment(coords_2, &coords_size, data_2, &data_size);

  if (extra_fragment) {
    write_1d_fragment(coords_2, &coords_size, data_2, &data_size);
  }

  // Read.
  int coords_r[9];
  int data_r[9];
  uint64_t coords_r_size = sizeof(coords_r);
  uint64_t data_r_size = sizeof(data_r);

  auto rc = read(
      use_subarray, qc_idx, coords_r, &coords_r_size, data_r, &data_r_size);
  CHECK(rc == TILEDB_OK);

  if (dups) {
    CHECK(3 * sizeof(int) == coords_r_size);
    CHECK(3 * sizeof(int) == data_r_size);

    int coords_c[] = {1, 2, 3};
    int data_c[] = {2, 2, 2};
    CHECK(!std::memcmp(coords_c, coords_r, coords_r_size));
    CHECK(!std::memcmp(data_c, data_r, data_r_size));
  } else {
    // Should read nothing.
    CHECK(0 == data_r_size);
    CHECK(0 == coords_r_size);
  }
}

TEST_CASE_METHOD(
    CSparseGlobalOrderFx,
    "Sparse global order reader: qc removes tile from second fragment "
    "replacing data from first fragment, 2",
    "[sparse-global-order][qc-removes-replacement-data]") {
  // Create default array.
  reset_config();
  create_default_array_1d();

  int qc_idx = GENERATE(1, 2);
  bool use_subarray = false;
  SECTION("- No subarray") {
    use_subarray = false;
  }
  SECTION("- Subarray") {
    use_subarray = true;
  }

  int coords_1[] = {1, 2, 3};
  int data_1[] = {2, 2, 2};

  int coords_2[] = {1, 2, 3};
  int data_2[] = {12, 4, 12};

  uint64_t coords_size = sizeof(coords_1);
  uint64_t data_size = sizeof(data_1);
  write_1d_fragment(coords_1, &coords_size, data_1, &data_size);
  write_1d_fragment(coords_2, &coords_size, data_2, &data_size);

  // Read.
  int coords_r[6];
  int data_r[6];
  uint64_t coords_r_size = sizeof(coords_r);
  uint64_t data_r_size = sizeof(data_r);

  auto rc = read(
      use_subarray, qc_idx, coords_r, &coords_r_size, data_r, &data_r_size);
  CHECK(rc == TILEDB_OK);

  // One value.
  CHECK(sizeof(int) == data_r_size);
  CHECK(sizeof(int) == coords_r_size);

  int coords_c[] = {2};
  int data_c[] = {4};
  CHECK(!std::memcmp(coords_c, coords_r, coords_r_size));
  CHECK(!std::memcmp(data_c, data_r, data_r_size));
}

TEST_CASE_METHOD(
    CSparseGlobalOrderFx,
    "Sparse global order reader: merge with subarray and dups",
    "[sparse-global-order][merge][subarray][dups]") {
  // Create default array.
  reset_config();
  create_default_array_1d(true);

  bool use_subarray = false;
  int qc_idx = GENERATE(1, 2);

  int coords_1[] = {8, 9, 10, 11, 12, 13};
  int data_1[] = {8, 9, 10, 11, 12, 13};

  int coords_2[] = {8, 9, 10, 11, 12, 13};
  int data_2[] = {8, 9, 10, 11, 12, 13};

  uint64_t coords_size = sizeof(coords_1);
  uint64_t data_size = sizeof(data_1);

  // Create the aray.
  write_1d_fragment(coords_1, &coords_size, data_1, &data_size);
  write_1d_fragment(coords_2, &coords_size, data_2, &data_size);

  // Read.
  int coords_r[6];
  int data_r[6];
  uint64_t coords_r_size = sizeof(coords_r);
  uint64_t data_r_size = sizeof(data_r);

  auto rc = read(
      use_subarray, qc_idx, coords_r, &coords_r_size, data_r, &data_r_size);
  CHECK(rc == TILEDB_OK);

  // Should read (6 values).
  CHECK(24 == data_r_size);
  CHECK(24 == coords_r_size);

  int coords_c[] = {8, 8, 9, 9, 10, 10};
  int data_c[] = {8, 8, 9, 9, 10, 10};
  CHECK(!std::memcmp(coords_c, coords_r, coords_r_size));
  CHECK(!std::memcmp(data_c, data_r, data_r_size));
}

TEST_CASE(
    "Sparse global order reader: user buffer cannot fit single cell",
    "[sparse-global-order][user-buffer][too-small][rest]") {
  VFSTestSetup vfs_test_setup;
  std::string array_name = vfs_test_setup.array_uri("test_sparse_global_order");
  auto ctx = vfs_test_setup.ctx();

  // Create array with var-sized attribute.
  Domain dom(ctx);
  dom.add_dimension(Dimension::create<int64_t>(ctx, "d1", {{1, 4}}, 2));

  Attribute attr(ctx, "a", TILEDB_STRING_ASCII);
  attr.set_cell_val_num(TILEDB_VAR_NUM);

  ArraySchema schema(ctx, TILEDB_SPARSE);
  schema.add_attribute(attr);
  schema.set_tile_order(TILEDB_ROW_MAJOR);
  schema.set_cell_order(TILEDB_ROW_MAJOR);
  schema.set_domain(dom);
  schema.set_allows_dups(true);

  Array::create(array_name, schema);

  // Write a fragment.
  Array array(ctx, array_name, TILEDB_WRITE);
  Query query(ctx, array, TILEDB_WRITE);
  query.set_layout(TILEDB_GLOBAL_ORDER);
  std::vector<int64_t> d1 = {1, 2, 3};
  std::string a1_data{
      "astringofsize15"
      "foo"
      "bar"};
  std::vector<uint64_t> a1_offsets{0, 15, 18};

  query.set_data_buffer("d1", d1);
  query.set_data_buffer("a", a1_data);
  query.set_offsets_buffer("a", a1_offsets);

  // Submit query
  query.submit_and_finalize();

  // Read using a buffer that can't fit a single result
  Array array2(ctx, array_name, TILEDB_READ);
  Query query2(ctx, array2, TILEDB_READ);

  std::string attr_val;
  // The first result is 15 bytes long so it won't fit in 10 byte user buffer
  attr_val.resize(10);
  std::vector<uint64_t> attr_off(sizeof(uint64_t));

  auto layout = GENERATE(TILEDB_GLOBAL_ORDER, TILEDB_UNORDERED);

  query2.set_layout(layout);
  query2.set_data_buffer("a", (char*)attr_val.data(), attr_val.size());
  query2.set_offsets_buffer("a", attr_off);

  // The user buffer cannot fit a single result so it should return Incomplete
  // with the right reason
  query2.submit();
  REQUIRE(query2.query_status() == Query::Status::INCOMPLETE);

  tiledb_query_status_details_t details;
  auto rc = tiledb_query_get_status_details(
      ctx.ptr().get(), query2.ptr().get(), &details);
  CHECK(rc == TILEDB_OK);
  CHECK(details.incomplete_reason == TILEDB_REASON_USER_BUFFER_SIZE);

  array2.close();
}

TEST_CASE(
    "Sparse global order reader: attribute copy memory limit",
    "[sparse-global-order][attribute-copy][memory-limit][rest]") {
  Config config;
  config["sm.mem.total_budget"] = "20000";
  VFSTestSetup vfs_test_setup(config.ptr().get());
  std::string array_name = vfs_test_setup.array_uri("test_sparse_global_order");
  auto ctx = vfs_test_setup.ctx();

  // Create array with var-sized attribute.
  Domain dom(ctx);
  dom.add_dimension(Dimension::create<int64_t>(ctx, "d1", {{1, 4}}, 2));

  Attribute attr(ctx, "a", TILEDB_STRING_ASCII);
  attr.set_cell_val_num(TILEDB_VAR_NUM);

  ArraySchema schema(ctx, TILEDB_SPARSE);
  schema.add_attribute(attr);
  schema.set_tile_order(TILEDB_ROW_MAJOR);
  schema.set_cell_order(TILEDB_ROW_MAJOR);
  schema.set_domain(dom);
  schema.set_allows_dups(true);
  schema.set_capacity(4);

  Array::create(array_name, schema);

  // Write a fragment with two tiles of 4 cells each. The var size tiles will
  // have a size of 5000.
  Array array(ctx, array_name, TILEDB_WRITE);
  Query query(ctx, array, TILEDB_WRITE);
  query.set_layout(TILEDB_GLOBAL_ORDER);
  std::vector<int64_t> d1 = {1, 1, 2, 2, 3, 3, 4, 4};
  std::string a1_data;
  a1_data.resize(10000);
  for (uint64_t i = 0; i < 10000; i++) {
    a1_data[i] = '0' + i % 10;
  }
  std::vector<uint64_t> a1_offsets{0, 1250, 2500, 3750, 5000, 6250, 7500, 8750};

  query.set_data_buffer("d1", d1);
  query.set_data_buffer("a", a1_data);
  query.set_offsets_buffer("a", a1_offsets);
  CHECK_NOTHROW(query.submit_and_finalize());

  // Read using a budget that can only fit one of the var size tiles.
  Array array2(ctx, array_name, TILEDB_READ);
  Query query2(ctx, array2, TILEDB_READ);

  std::string attr_val;
  attr_val.resize(5000);
  std::vector<uint64_t> attr_off(sizeof(uint64_t));

  query2.set_layout(TILEDB_GLOBAL_ORDER);
  query2.set_data_buffer("a", (char*)attr_val.data(), attr_val.size());
  query2.set_offsets_buffer("a", attr_off);

  // Submit and validate we only get 4 cells back (one tile).
  auto st = query2.submit();
  CHECK(st == Query::Status::INCOMPLETE);

  auto result_num = (int)query2.result_buffer_elements()["a"].first;
  CHECK(result_num == 4);

  // Submit and validate we get the last 4 cells back.
  st = query2.submit();
  CHECK(st == Query::Status::COMPLETE);

  result_num = (int)query2.result_buffer_elements()["a"].first;
  CHECK(result_num == 4);

  array2.close();
}

TEST_CASE_METHOD(
    CSparseGlobalOrderFx,
    "Sparse global order reader: no new coords tile",
    "[sparse-global-order][no-new-coords-tile]") {
  // Create default array.
  reset_config();
  create_default_array_1d();

  bool use_subarray = false;
  SECTION("- No subarray") {
    use_subarray = false;
  }
  SECTION("- Subarray") {
    use_subarray = true;
  }

  int num_frags = 2;
  for (int i = 1; i < num_frags + 1; i++) {
    // Write a fragment.
    int coords[] = {
        i,
        num_frags + i,
        2 * num_frags + i,
        3 * num_frags + i,
        4 * num_frags + i};
    uint64_t coords_size = sizeof(coords);
    int data[] = {
        i,
        num_frags + i,
        2 * num_frags + i,
        3 * num_frags + i,
        4 * num_frags + i};
    uint64_t data_size = sizeof(data);
    write_1d_fragment(coords, &coords_size, data, &data_size);
  }

  // Two result tile (2 * (~4000 + 8) will be bigger than the per fragment
  // budget (1000).
  total_budget_ = "40000";
  ratio_coords_ = "0.22";
  update_config();

  tiledb_array_t* array = nullptr;
  tiledb_query_t* query = nullptr;

  // Try to read.
  int coords_r[1];
  int data_r[1];
  uint64_t coords_r_size = sizeof(coords_r);
  uint64_t data_r_size = sizeof(data_r);
  tiledb_query_status_t status;
  uint32_t rc = read(
      use_subarray,
      0,
      coords_r,
      &coords_r_size,
      data_r,
      &data_r_size,
      &query,
      &array);
  CHECK(rc == TILEDB_OK);
  tiledb_query_get_status(ctx_, query, &status);
  CHECK(status == TILEDB_INCOMPLETE);

  uint64_t loop_idx = 1;
  CHECK(4 == data_r_size);
  CHECK(4 == coords_r_size);
  CHECK(!std::memcmp(&loop_idx, coords_r, coords_r_size));
  CHECK(!std::memcmp(&loop_idx, data_r, data_r_size));
  loop_idx++;

  while (status == TILEDB_INCOMPLETE && rc == TILEDB_OK) {
    rc = tiledb_query_submit(ctx_, query);
    tiledb_query_get_status(ctx_, query, &status);
    CHECK(4 == data_r_size);
    CHECK(4 == coords_r_size);
    CHECK(!std::memcmp(&loop_idx, coords_r, coords_r_size));
    CHECK(!std::memcmp(&loop_idx, data_r, data_r_size));
    loop_idx++;
  }
  CHECK(loop_idx == 11);
  CHECK(rc == TILEDB_OK);

  // Clean up.
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
  tiledb_query_free(&query);
}

TEST_CASE_METHOD(
    CSparseGlobalOrderFx,
    "Sparse global order reader: correct read state on duplicates",
    "[sparse-global-order][no-dups][read-state]") {
  bool dups = GENERATE(false, true);
  create_default_array_1d(dups);

  // Write one fragment in coordinates 1-10 with data 1-10.
  int coords[] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
  uint64_t coords_size = sizeof(coords);
  int data1[] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
  uint64_t data1_size = sizeof(data1);
  write_1d_fragment(coords, &coords_size, data1, &data1_size);

  // Write another fragment with the same coordinates but data 11-20.
  int data2[] = {11, 12, 13, 14, 15, 16, 17, 18, 19, 20};
  uint64_t data2_size = sizeof(data2);
  write_1d_fragment(coords, &coords_size, data2, &data2_size);

  tiledb_array_t* array = nullptr;
  tiledb_query_t* query = nullptr;

  // Read with buffers that can only fit one cell.
  int coords_r[1];
  int data_r[1];
  uint64_t coords_r_size = sizeof(coords_r);
  uint64_t data_r_size = sizeof(data_r);

  // Read the first cell.
  int rc;
  tiledb_query_status_t status;
  rc = read(
      true, 0, coords_r, &coords_r_size, data_r, &data_r_size, &query, &array);
  CHECK(rc == TILEDB_OK);

  CHECK(coords_r[0] == 1);

  if (dups) {
    CHECK((data_r[0] == 1 || data_r[0] == 11));

    for (int i = 3; i <= 21; i++) {
      // Check incomplete query status.
      tiledb_query_get_status(ctx_, query, &status);
      CHECK(status == TILEDB_INCOMPLETE);

      rc = tiledb_query_submit(ctx_, query);
      CHECK(rc == TILEDB_OK);

      CHECK(coords_r[0] == i / 2);
      CHECK((data_r[0] == i / 2 + 10 || data_r[0] == i / 2));
    }
  } else {
    CHECK(data_r[0] == 11);

    for (int i = 2; i <= 10; i++) {
      // Check incomplete query status.
      tiledb_query_get_status(ctx_, query, &status);
      CHECK(status == TILEDB_INCOMPLETE);

      rc = tiledb_query_submit(ctx_, query);
      CHECK(rc == TILEDB_OK);

      CHECK(coords_r[0] == i);
      CHECK(data_r[0] == i + 10);
    }
  }

  // Check completed query status.
  tiledb_query_get_status(ctx_, query, &status);
  CHECK(status == TILEDB_COMPLETED);

  // Clean up.
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
  tiledb_query_free(&query);
}

TEST_CASE_METHOD(
    CSparseGlobalOrderFx,
    "Sparse global order reader: revert deleted duplicate",
    "[sparse-global-order][revert][deleted-duplicate]") {
  bool deleted_dup = GENERATE(true, false);
  create_default_array_1d_strings(false);

  // Write the first fragment.
  int coords1[] = {3, 4, 5, 6};
  uint64_t coords_size1 = sizeof(coords1);
  char data1[] = "333444555666";

  if (deleted_dup) {
    data1[8] = '2';
  }

  uint64_t data1_size = sizeof(data1) - 1;
  uint64_t offsets1[] = {0, 3, 6, 9};
  uint64_t offsets1_size = sizeof(offsets1);
  write_1d_fragment_strings(
      coords1, &coords_size1, data1, &data1_size, offsets1, &offsets1_size);

  // Write the second fragment.
  int coords2[] = {1, 2, 5, 7};
  uint64_t coords_size2 = sizeof(coords2);
  char data2[] = "111222552777";
  uint64_t data2_size = sizeof(data2) - 1;
  uint64_t offsets2[] = {0, 3, 6, 9};
  uint64_t offsets2_size = sizeof(offsets2);
  write_1d_fragment_strings(
      coords2, &coords_size2, data2, &data2_size, offsets2, &offsets2_size);

  // Delete the cell at coordinate 5, but only for the second fragment.
  char to_delete[] = "552";
  write_delete_condition(to_delete, 3);

  tiledb_array_t* array = nullptr;
  tiledb_query_t* query = nullptr;

  // Read with fixed size buffers that can fit the whole dataset but var sized
  // buffer can only fit 3 cells. This will revert progress for the second
  // fragment before cell at coord 5 for sure because of the cell at coordinate
  // 4 but might not regress progress for the second fragment before cell at
  // coord 5 as cell at coordinate 5 was deleted.
  int coords_r[100];
  char data_r[9];
  uint64_t offsets_r[100];
  uint64_t coords_r_size = sizeof(coords_r);
  uint64_t data_r_size = sizeof(data_r);
  uint64_t offsets_r_size = sizeof(offsets_r);

  // Read the first cell.
  int rc;
  tiledb_query_status_t status;
  rc = read_strings(
      false,
      coords_r,
      &coords_r_size,
      data_r,
      &data_r_size,
      offsets_r,
      &offsets_r_size,
      &query,
      &array);
  CHECK(rc == TILEDB_OK);

  // Validate the first read.
  int coords_c[] = {1, 2, 3};
  char data_c[] = "111222333";
  uint64_t offsets_c[] = {0, 3, 6};
  CHECK(coords_r_size == 12);
  CHECK(!std::memcmp(coords_c, coords_r, coords_r_size));
  CHECK(data_r_size == 9);
  CHECK(!std::memcmp(data_c, data_r, data_r_size));
  CHECK(offsets_r_size == 24);
  CHECK(!std::memcmp(offsets_c, offsets_r, offsets_r_size));

  // Check completed query status.
  tiledb_query_get_status(ctx_, query, &status);
  CHECK(status == TILEDB_INCOMPLETE);

  // Reset buffer sizes.
  coords_r_size = sizeof(coords_r);
  data_r_size = sizeof(data_r);
  offsets_r_size = sizeof(offsets_r);

  // Submit query.
  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  // Validate the second read.
  int coords_c2[] = {4, 6, 7};
  char data_c2[] = "4446667777";
  uint64_t offsets_c2[] = {0, 3, 6};
  CHECK(coords_r_size == 12);
  CHECK(!std::memcmp(coords_c2, coords_r, coords_r_size));
  CHECK(data_r_size == 9);
  CHECK(!std::memcmp(data_c2, data_r, data_r_size));
  CHECK(offsets_r_size == 24);
  CHECK(!std::memcmp(offsets_c2, offsets_r, offsets_r_size));

  // Check completed query status.
  tiledb_query_get_status(ctx_, query, &status);
  CHECK(status == TILEDB_COMPLETED);

  // Clean up.
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
  tiledb_query_free(&query);
}
