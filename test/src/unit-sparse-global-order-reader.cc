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

#include "test/src/helpers.h"
#include "tiledb/sm/c_api/tiledb.h"
#include "tiledb/sm/c_api/tiledb_struct_def.h"
#include "tiledb/sm/query/sparse_global_order_reader.h"

#ifdef _WIN32
#include "tiledb/sm/filesystem/win.h"
#else
#include "tiledb/sm/filesystem/posix.h"
#endif

#include <catch.hpp>

using namespace tiledb::sm;
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
  std::string ratio_query_condition_;

  void create_default_array_1d();
  void write_1d_fragment(
      int* coords, uint64_t* coords_size, int* data, uint64_t* data_size);
  int32_t read(
      bool set_subarray,
      bool set_qc,
      int* coords,
      uint64_t* coords_size,
      int* data,
      uint64_t* data_size,
      tiledb_query_t** query = nullptr,
      tiledb_array_t** array_ret = nullptr);
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
  ratio_query_condition_ = "0.25";
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

  REQUIRE(
      tiledb_config_set(
          config,
          "sm.mem.reader.sparse_global_order.ratio_query_condition",
          ratio_query_condition_.c_str(),
          &error) == TILEDB_OK);
  REQUIRE(error == nullptr);

  REQUIRE(tiledb_ctx_alloc(config, &ctx_) == TILEDB_OK);
  REQUIRE(error == nullptr);
  REQUIRE(tiledb_vfs_alloc(ctx_, config, &vfs_) == TILEDB_OK);
  tiledb_config_free(&config);
}

void CSparseGlobalOrderFx::create_default_array_1d() {
  int domain[] = {1, 20};
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
      false);
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

int32_t CSparseGlobalOrderFx::read(
    bool set_subarray,
    bool set_qc,
    int* coords,
    uint64_t* coords_size,
    int* data,
    uint64_t* data_size,
    tiledb_query_t** query_ret,
    tiledb_array_t** array_ret) {
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
    int subarray[] = {1, 10};
    rc = tiledb_query_set_subarray(ctx_, query, subarray);
    CHECK(rc == TILEDB_OK);
  }

  if (set_qc) {
    tiledb_query_condition_t* query_condition = nullptr;
    rc = tiledb_query_condition_alloc(ctx_, &query_condition);
    CHECK(rc == TILEDB_OK);
    int32_t val = 11;
    rc = tiledb_query_condition_init(
        ctx_, query_condition, "a", &val, sizeof(int32_t), TILEDB_LT);
    CHECK(rc == TILEDB_OK);

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
  auto rc = read(true, false, coords_r, &coords_r_size, data_r, &data_r_size);
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
    "Sparse global order reader: tile offsets budget exceeded",
    "[sparse-global-order][tile-offsets][budget-exceeded]") {
  // Create default array.
  reset_config();
  create_default_array_1d();

  // Write a fragment.
  int coords[] = {1, 2, 3, 4, 5};
  uint64_t coords_size = sizeof(coords);
  int data[] = {1, 2, 3, 4, 5};
  uint64_t data_size = sizeof(data);
  write_1d_fragment(coords, &coords_size, data, &data_size);

  // We should have 3 tiles (tile offset size 24) which will be bigger than
  // budget (10).
  total_budget_ = "1000";
  ratio_array_data_ = "0.01";
  update_config();

  // Try to read.
  int coords_r[5];
  int data_r[5];
  uint64_t coords_r_size = sizeof(coords_r);
  uint64_t data_r_size = sizeof(data_r);
  auto rc = read(true, false, coords_r, &coords_r_size, data_r, &data_r_size);
  CHECK(rc == TILEDB_ERR);

  // Check we hit the correct error.
  tiledb_error_t* error = NULL;
  rc = tiledb_ctx_get_last_error(ctx_, &error);
  CHECK(rc == TILEDB_OK);

  const char* msg;
  rc = tiledb_error_message(error, &msg);
  CHECK(rc == TILEDB_OK);

  std::string error_str(msg);
  CHECK(error_str.find("Cannot load tile offsets") != std::string::npos);
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
    int coords[] = {i,
                    num_frags + i,
                    2 * num_frags + i,
                    3 * num_frags + i,
                    4 * num_frags + i};
    uint64_t coords_size = sizeof(coords);
    int data[] = {i,
                  num_frags + i,
                  2 * num_frags + i,
                  3 * num_frags + i,
                  4 * num_frags + i};
    uint64_t data_size = sizeof(data);
    write_1d_fragment(coords, &coords_size, data, &data_size);
  }

  // Two result tile (2 * (~504 + 8) will be bigger than the per fragment budget
  // (1000).
  total_budget_ = "10000";
  ratio_coords_ = "0.2";
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
      false,
      coords_r,
      &coords_r_size,
      data_r,
      &data_r_size,
      &query,
      &array);
  CHECK(rc == TILEDB_OK);

  // Check the internal loop count against expected value.
  auto stats = ((SparseGlobalOrderReader*)query->query_->strategy())->stats();
  REQUIRE(stats != nullptr);
  auto counters = stats->counters();
  REQUIRE(counters != nullptr);
  auto loop_num =
      counters->find("Context.StorageManager.Query.Reader.loop_num");
  CHECK(5 == loop_num->second);

  // Check incomplete query status.
  tiledb_query_status_t status;
  tiledb_query_get_status(ctx_, query, &status);
  CHECK(status == TILEDB_COMPLETED);

  // Should only read one tile (2 values).
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
  total_budget_ = "10000";
  ratio_coords_ = "0.04";
  update_config();

  // Try to read.
  int coords_r[5];
  int data_r[5];
  uint64_t coords_r_size = sizeof(coords_r);
  uint64_t data_r_size = sizeof(data_r);
  auto rc =
      read(use_subarray, false, coords_r, &coords_r_size, data_r, &data_r_size);
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
    "Sparse global order reader: qc budget too small",
    "[sparse-global-order][qc-budget][too-small]") {
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

  // One qc tile (8) will be bigger than the budget (5).
  total_budget_ = "10000";
  ratio_query_condition_ = "0.0005";
  update_config();

  // Try to read.
  int coords_r[5];
  int data_r[5];
  uint64_t coords_r_size = sizeof(coords_r);
  uint64_t data_r_size = sizeof(data_r);
  auto rc =
      read(use_subarray, true, coords_r, &coords_r_size, data_r, &data_r_size);
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
    "Sparse global order reader: qc budget forcing one tile at a time",
    "[sparse-global-order][small-qc-budget]") {
  // Create default array.
  reset_config();
  create_default_array_1d();

  bool use_subarray = false;
  int num_frags = 0;
  SECTION("- No subarray") {
    use_subarray = false;
    SECTION("- Two fragments") {
      num_frags = 2;
    }
  }
  SECTION("- Subarray") {
    use_subarray = true;
    SECTION("- One fragment") {
      num_frags = 1;
    }
    SECTION("- Two fragments") {
      num_frags = 2;
    }
  }

  for (int i = 0; i < num_frags; i++) {
    // Write a fragment.
    int coords[] = {1 + i * 5, 2 + i * 5, 3 + i * 5, 4 + i * 5, 5 + i * 5};
    uint64_t coords_size = sizeof(coords);
    int data[] = {1 + i * 5, 2 + i * 5, 3 + i * 5, 4 + i * 5, 5 + i * 5};
    uint64_t data_size = sizeof(data);
    write_1d_fragment(coords, &coords_size, data, &data_size);
  }

  // Two qc tile (16) will be bigger than the budget (10).
  total_budget_ = "10000";
  ratio_query_condition_ = num_frags == 1 ? "0.001" : "0.002";
  update_config();

  tiledb_array_t* array = nullptr;
  tiledb_query_t* query = nullptr;

  uint32_t rc;
  uint64_t coords_r_size;
  uint64_t data_r_size;

  // Try to read.
  int coords_r[10];
  int data_r[10];
  coords_r_size = sizeof(coords_r);
  data_r_size = sizeof(data_r);

  rc = read(
      use_subarray,
      true,
      coords_r,
      &coords_r_size,
      data_r,
      &data_r_size,
      &query,
      &array);
  CHECK(rc == TILEDB_OK);

  // Check the internal loop count against expected value.
  auto stats = ((SparseGlobalOrderReader*)query->query_->strategy())->stats();
  REQUIRE(stats != nullptr);
  auto counters = stats->counters();
  REQUIRE(counters != nullptr);
  auto loop_num =
      counters->find("Context.StorageManager.Query.Reader.loop_num");
  CHECK(2 + (uint64_t)num_frags == loop_num->second);

  // Check incomplete query status.
  tiledb_query_status_t status;
  tiledb_query_get_status(ctx_, query, &status);
  CHECK(status == TILEDB_COMPLETED);

  // Should only read one tile (2 values).
  CHECK(uint64_t(num_frags * 20) == data_r_size);
  CHECK(uint64_t(num_frags * 20) == coords_r_size);

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
    "Sparse global order reader: qc removes full tile",
    "[sparse-global-order][qc-removes-tile]") {
  // Create default array.
  reset_config();
  create_default_array_1d();

  bool use_subarray = false;
  int tile_idx = 0;
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

  auto rc =
      read(use_subarray, true, coords_r, &coords_r_size, data_r, &data_r_size);
  CHECK(rc == TILEDB_OK);

  // Should read two tile (6 values).
  CHECK(24 == data_r_size);
  CHECK(24 == coords_r_size);

  int coords_c[] = {1, 2, 3, 4, 5, 6};
  int data_c[] = {1, 2, 3, 4, 5, 6};
  CHECK(!std::memcmp(coords_c, coords_r, coords_r_size));
  CHECK(!std::memcmp(data_c, data_r, data_r_size));
}