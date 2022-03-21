/**
 * @file unit-sparse-unordered-with-dups-reader.cc
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
 * Tests for the sparse unordered with duplicates reader.
 */

#include "test/src/helpers.h"
#include "tiledb/sm/c_api/tiledb.h"
#include "tiledb/sm/c_api/tiledb_struct_def.h"
#include "tiledb/sm/query/query_buffer.h"
#include "tiledb/sm/query/sparse_index_reader_base.h"
#include "tiledb/sm/query/sparse_unordered_with_dups_reader.h"

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

struct CSparseUnorderedWithDupsFx {
  tiledb_ctx_t* ctx_ = nullptr;
  tiledb_vfs_t* vfs_ = nullptr;
  std::string temp_dir_;
  std::string array_name_;
  const char* ARRAY_NAME = "test_sparse_unordered_with_dups";
  std::string total_budget_;
  std::string ratio_tile_ranges_;
  std::string ratio_array_data_;
  std::string ratio_coords_;
  std::string ratio_query_condition_;

  void create_default_array_1d();
  void create_default_array_1d_string();
  void write_1d_fragment(
      int* coords, uint64_t* coords_size, int* data, uint64_t* data_size);
  void write_1d_fragment_empty_strings(int* coords, uint64_t* coords_size);
  int32_t read(
      bool set_subarray,
      bool set_qc,
      int* coords,
      uint64_t* coords_size,
      int* data,
      uint64_t* data_size,
      tiledb_query_t** query = nullptr,
      tiledb_array_t** array_ret = nullptr);
  int32_t read_strings(
      int* coords,
      uint64_t* coords_size,
      char* data,
      uint64_t* data_size,
      uint64_t* data_offsets,
      uint64_t* data_offsets_size);
  void reset_config();
  void update_config();

  CSparseUnorderedWithDupsFx();
  ~CSparseUnorderedWithDupsFx();
};

CSparseUnorderedWithDupsFx::CSparseUnorderedWithDupsFx() {
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

CSparseUnorderedWithDupsFx::~CSparseUnorderedWithDupsFx() {
  remove_dir(temp_dir_, ctx_, vfs_);
  tiledb_ctx_free(&ctx_);
  tiledb_vfs_free(&vfs_);
}

void CSparseUnorderedWithDupsFx::reset_config() {
  total_budget_ = "1048576";
  ratio_tile_ranges_ = "0.1";
  ratio_array_data_ = "0.1";
  ratio_coords_ = "0.5";
  ratio_query_condition_ = "0.25";
  update_config();
}

void CSparseUnorderedWithDupsFx::update_config() {
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
          "sm.query.sparse_unordered_with_dups.reader",
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
          "sm.mem.reader.sparse_unordered_with_dups.ratio_tile_ranges",
          ratio_tile_ranges_.c_str(),
          &error) == TILEDB_OK);
  REQUIRE(error == nullptr);

  REQUIRE(
      tiledb_config_set(
          config,
          "sm.mem.reader.sparse_unordered_with_dups.ratio_array_data",
          ratio_array_data_.c_str(),
          &error) == TILEDB_OK);
  REQUIRE(error == nullptr);

  REQUIRE(
      tiledb_config_set(
          config,
          "sm.mem.reader.sparse_unordered_with_dups.ratio_coords",
          ratio_coords_.c_str(),
          &error) == TILEDB_OK);
  REQUIRE(error == nullptr);

  REQUIRE(
      tiledb_config_set(
          config,
          "sm.mem.reader.sparse_unordered_with_dups.ratio_query_condition",
          ratio_query_condition_.c_str(),
          &error) == TILEDB_OK);
  REQUIRE(error == nullptr);

  REQUIRE(tiledb_ctx_alloc(config, &ctx_) == TILEDB_OK);
  REQUIRE(error == nullptr);
  REQUIRE(tiledb_vfs_alloc(ctx_, config, &vfs_) == TILEDB_OK);
  tiledb_config_free(&config);
}

void CSparseUnorderedWithDupsFx::create_default_array_1d() {
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
      true);  // allows dups.
}

void CSparseUnorderedWithDupsFx::create_default_array_1d_string() {
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
      {TILEDB_STRING_ASCII},
      {TILEDB_VAR_NUM},
      {tiledb::test::Compressor(TILEDB_FILTER_NONE, -1)},
      TILEDB_ROW_MAJOR,
      TILEDB_ROW_MAJOR,
      2,
      true);  // allows dups.
}

void CSparseUnorderedWithDupsFx::write_1d_fragment(
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

void CSparseUnorderedWithDupsFx::write_1d_fragment_empty_strings(
    int* coords, uint64_t* coords_size) {
  const uint64_t num_cells = *coords_size / sizeof(TILEDB_INT32);

  // Open array for writing.
  tiledb_array_t* array;
  auto rc = tiledb_array_alloc(ctx_, array_name_.c_str(), &array);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  REQUIRE(rc == TILEDB_OK);

  char data[1];
  uint64_t data_size = 0;
  std::vector<uint64_t> data_offsets(num_cells, 0);
  uint64_t data_offsets_size = num_cells * sizeof(uint64_t);

  // Create the query.
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_UNORDERED);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, query, "a", data, &data_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_offsets_buffer(
      ctx_, query, "a", data_offsets.data(), &data_offsets_size);
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

int32_t CSparseUnorderedWithDupsFx::read(
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

  rc = tiledb_query_set_layout(ctx_, query, TILEDB_UNORDERED);
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

int32_t CSparseUnorderedWithDupsFx::read_strings(
    int* coords,
    uint64_t* coords_size,
    char* data,
    uint64_t* data_size,
    uint64_t* data_offsets,
    uint64_t* data_offsets_size) {
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

  rc = tiledb_query_set_layout(ctx_, query, TILEDB_UNORDERED);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, query, "a", data, data_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_offsets_buffer(
      ctx_, query, "a", data_offsets, data_offsets_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, query, "d", coords, coords_size);
  CHECK(rc == TILEDB_OK);

  // Submit query.
  auto ret = tiledb_query_submit(ctx_, query);

  // Clean up.
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
  tiledb_query_free(&query);
  return ret;
}

struct CSparseUnorderedWithDupsVarDataFx {
  tiledb_ctx_t* ctx_ = nullptr;
  tiledb_vfs_t* vfs_ = nullptr;
  std::string temp_dir_;
  std::string array_name_;
  const char* ARRAY_NAME = "test_sparse_unordered_with_dups_var_data";

  void create_default_array_2d();
  void write_2d_fragment();
  void read_and_check_data(bool set_subarray);

  tuple<tiledb_array_t*, std::vector<tdb_shared_ptr<FragmentMetadata>>>
  open_default_array_1d_with_fragments();

  CSparseUnorderedWithDupsVarDataFx();
  ~CSparseUnorderedWithDupsVarDataFx();
};

CSparseUnorderedWithDupsVarDataFx::CSparseUnorderedWithDupsVarDataFx() {
  tiledb_config_t* config;
  tiledb_error_t* error = nullptr;
  REQUIRE(tiledb_config_alloc(&config, &error) == TILEDB_OK);
  REQUIRE(error == nullptr);
  REQUIRE(tiledb_ctx_alloc(config, &ctx_) == TILEDB_OK);
  REQUIRE(error == nullptr);
  REQUIRE(tiledb_vfs_alloc(ctx_, config, &vfs_) == TILEDB_OK);
  tiledb_config_free(&config);

  // Create temporary directory based on the supported filesystem.
#ifdef _WIN32
  temp_dir_ = tiledb::sm::Win::current_dir() + "\\tiledb_test\\";
#else
  temp_dir_ = "file://" + tiledb::sm::Posix::current_dir() + "/tiledb_test/";
#endif
  create_dir(temp_dir_, ctx_, vfs_);
  array_name_ = temp_dir_ + ARRAY_NAME;
}

CSparseUnorderedWithDupsVarDataFx::~CSparseUnorderedWithDupsVarDataFx() {
  remove_dir(temp_dir_, ctx_, vfs_);
  tiledb_ctx_free(&ctx_);
  tiledb_vfs_free(&vfs_);
}

void CSparseUnorderedWithDupsVarDataFx::create_default_array_2d() {
  int64_t domain[] = {1, 4};
  int64_t tile_extent = 2;
  create_array(
      ctx_,
      array_name_,
      TILEDB_SPARSE,
      {"d1", "d2"},
      {TILEDB_INT64, TILEDB_INT64},
      {domain, domain},
      {&tile_extent, &tile_extent},
      {"attr"},
      {TILEDB_INT32},
      {TILEDB_VAR_NUM},
      {tiledb::test::Compressor(TILEDB_FILTER_NONE, -1)},
      TILEDB_ROW_MAJOR,
      TILEDB_ROW_MAJOR,
      4,
      true);  // allows dups.
}

void CSparseUnorderedWithDupsVarDataFx::write_2d_fragment() {
  // Open array for writing.
  tiledb_array_t* array;
  auto rc = tiledb_array_alloc(ctx_, array_name_.c_str(), &array);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  REQUIRE(rc == TILEDB_OK);

  std::vector<int64_t> d1 = {1, 2, 3, 4};
  std::vector<int64_t> d2 = {2, 1, 3, 4};
  uint64_t d1_size = d1.size() * sizeof(int64_t);
  uint64_t d2_size = d2.size() * sizeof(int64_t);

  std::vector<int32_t> data = {1, 2, 3, 4, 5, 6};
  uint64_t data_size = data.size() * sizeof(int32_t);
  std::vector<uint64_t> offsets = {0, 4, 12, 20};
  uint64_t offsets_size = offsets.size() * sizeof(uint64_t);

  // Create the query.
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_UNORDERED);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, "attr", data.data(), &data_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_offsets_buffer(
      ctx_, query, "attr", offsets.data(), &offsets_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, query, "d1", d1.data(), &d1_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, query, "d2", d2.data(), &d2_size);
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

void CSparseUnorderedWithDupsVarDataFx::read_and_check_data(bool set_subarray) {
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
    int64_t subarray[] = {1, 4, 1, 4};
    rc = tiledb_query_set_subarray(ctx_, query, subarray);
    CHECK(rc == TILEDB_OK);
  }

  std::vector<int32_t> data(3);
  uint64_t data_size = data.size() * sizeof(int32_t);
  std::vector<uint64_t> offsets(4);
  uint64_t offsets_size = offsets.size() * sizeof(uint64_t);

  rc = tiledb_query_set_layout(ctx_, query, TILEDB_UNORDERED);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, "attr", data.data(), &data_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_offsets_buffer(
      ctx_, query, "attr", offsets.data(), &offsets_size);
  CHECK(rc == TILEDB_OK);

  // Submit query.
  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_OK);

  // Check incomplete query status.
  tiledb_query_status_t status;
  tiledb_query_get_status(ctx_, query, &status);
  CHECK(status == TILEDB_INCOMPLETE);

  // Should only read 2 values.
  CHECK(12 == data_size);
  CHECK(16 == offsets_size);

  int32_t data_c[] = {1, 2, 3};
  uint64_t offsets_c[] = {0, 4};
  CHECK(!std::memcmp(data.data(), data_c, data_size));
  CHECK(!std::memcmp(offsets.data(), offsets_c, offsets_size));

  // Submit query.
  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_OK);

  // Check completed query status.
  tiledb_query_get_status(ctx_, query, &status);
  CHECK(status == TILEDB_COMPLETED);

  // Should only read 2 values.
  CHECK(12 == data_size);
  CHECK(16 == offsets_size);

  int32_t data_c_2[] = {4, 5, 6};
  uint64_t offsets_c_2[] = {0, 8};
  CHECK(!std::memcmp(data.data(), data_c_2, data_size));
  CHECK(!std::memcmp(offsets.data(), offsets_c_2, offsets_size));

  // Clean up.
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
  tiledb_query_free(&query);
}

tuple<tiledb_array_t*, std::vector<tdb_shared_ptr<FragmentMetadata>>>
CSparseUnorderedWithDupsVarDataFx::open_default_array_1d_with_fragments() {
  int64_t domain[] = {1, 10};
  int64_t tile_extent = 5;
  // Create array
  create_array(
      ctx_,
      array_name_,
      TILEDB_SPARSE,
      {"d"},
      {TILEDB_INT64},
      {domain},
      {&tile_extent},
      {"a"},
      {TILEDB_STRING_ASCII},
      {1},
      {tiledb::test::Compressor(TILEDB_FILTER_NONE, -1)},
      TILEDB_ROW_MAJOR,
      TILEDB_ROW_MAJOR,
      5);

  // Open array for writing.
  tiledb_array_t* array;
  auto rc = tiledb_array_alloc(ctx_, array_name_.c_str(), &array);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  REQUIRE(rc == TILEDB_OK);

  std::vector<tdb_shared_ptr<FragmentMetadata>> fragments;
  tdb_shared_ptr<FragmentMetadata> fragment =
      tdb::make_shared<FragmentMetadata>(
          HERE(),
          nullptr,
          nullptr,
          array->array_->array_schema_latest_ptr(),
          URI(),
          std::make_pair<uint64_t, uint64_t>(0, 0),
          true);
  fragments.emplace_back(std::move(fragment));

  return {array, std::move(fragments)};
}

/* ********************************* */
/*                TESTS              */
/* ********************************* */

TEST_CASE_METHOD(
    CSparseUnorderedWithDupsFx,
    "Sparse unordered with dups reader: Tile ranges budget exceeded",
    "[sparse-unordered-with-dups][tile-ranges][budget-exceeded]") {
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
    CSparseUnorderedWithDupsFx,
    "Sparse unordered with dups reader: tile offsets budget exceeded",
    "[sparse-unordered-with-dups][tile-offsets][budget-exceeded]") {
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
    CSparseUnorderedWithDupsFx,
    "Sparse unordered with dups reader: coords budget forcing one tile at a "
    "time",
    "[sparse-unordered-with-dups][small-coords-budget]") {
  // Create default array.
  reset_config();
  create_default_array_1d();

  bool use_subarray = false;
  int num_frags = 0;
  SECTION("- No subarray") {
    use_subarray = false;
    SECTION("- One fragment") {
      num_frags = 1;
    }
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

  // Two result tile (2 * ~505) will be bigger than the budget (800).
  total_budget_ = "10000";
  ratio_coords_ = "0.08";
  update_config();

  tiledb_array_t* array = nullptr;
  tiledb_query_t* query = nullptr;

  // Try to read.
  int coords_r[10];
  int data_r[10];
  uint64_t coords_r_size = sizeof(coords_r);
  uint64_t data_r_size = sizeof(data_r);

  auto rc = read(
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
  auto stats =
      ((SparseUnorderedWithDupsReader<uint8_t>*)query->query_->strategy())
          ->stats();
  REQUIRE(stats != nullptr);
  auto counters = stats->counters();
  REQUIRE(counters != nullptr);
  auto loop_num =
      counters->find("Context.StorageManager.Query.Reader.loop_num");
  CHECK(uint64_t(num_frags * 3) == loop_num->second);

  // Check query status.
  tiledb_query_status_t status;
  tiledb_query_get_status(ctx_, query, &status);
  CHECK(status == TILEDB_COMPLETED);

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
    CSparseUnorderedWithDupsFx,
    "Sparse unordered with dups reader: coords budget too small",
    "[sparse-unordered-with-dups][coords-budget][too-small]") {
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

  // One result tile (~505) will be bigger than the budget (5).
  total_budget_ = "10000";
  ratio_coords_ = "0.0005";
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
    CSparseUnorderedWithDupsFx,
    "Sparse unordered with dups reader: qc budget too small",
    "[sparse-unordered-with-dups][qc-budget][too-small]") {
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
    CSparseUnorderedWithDupsFx,
    "Sparse unordered with dups reader: qc budget forcing one tile at a time",
    "[sparse-unordered-with-dups][small-qc-budget]") {
  // Create default array.
  reset_config();
  create_default_array_1d();

  bool use_subarray = false;
  int num_frags = 0;
  SECTION("- No subarray") {
    use_subarray = false;
    SECTION("- One fragment") {
      num_frags = 1;
    }
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
  ratio_query_condition_ = "0.001";
  update_config();

  tiledb_array_t* array = nullptr;
  tiledb_query_t* query = nullptr;

  // Try to read.
  int coords_r[10];
  int data_r[10];
  uint64_t coords_r_size = sizeof(coords_r);
  uint64_t data_r_size = sizeof(data_r);

  auto rc = read(
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
  auto stats =
      ((SparseUnorderedWithDupsReader<uint8_t>*)query->query_->strategy())
          ->stats();
  REQUIRE(stats != nullptr);
  auto counters = stats->counters();
  REQUIRE(counters != nullptr);
  auto loop_num =
      counters->find("Context.StorageManager.Query.Reader.loop_num");
  CHECK(uint64_t(num_frags * 3) == loop_num->second);

  // Check query status.
  tiledb_query_status_t status;
  tiledb_query_get_status(ctx_, query, &status);
  CHECK(status == TILEDB_COMPLETED);

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
    CSparseUnorderedWithDupsFx,
    "Sparse unordered with dups reader: fixed user buffer too small",
    "[sparse-unordered-with-dups][small-fixed-buffer]") {
  // Create default array.
  reset_config();
  create_default_array_1d();

  // Write a fragment.
  int coords[] = {1, 2, 3, 4, 5};
  uint64_t coords_size = sizeof(coords);
  int data[] = {1, 2, 3, 4, 5};
  uint64_t data_size = sizeof(data);
  write_1d_fragment(coords, &coords_size, data, &data_size);

  tiledb_array_t* array = nullptr;
  tiledb_query_t* query = nullptr;

  // Try to read.
  int coords_r[2];  // only room for one tile.
  int data_r[2];
  uint64_t coords_r_size = sizeof(coords_r);
  uint64_t data_r_size = sizeof(data_r);
  auto rc = read(
      false,
      false,
      coords_r,
      &coords_r_size,
      data_r,
      &data_r_size,
      &query,
      &array);
  CHECK(rc == TILEDB_OK);

  // Check incomplete query status.
  tiledb_query_status_t status;
  tiledb_query_get_status(ctx_, query, &status);
  CHECK(status == TILEDB_INCOMPLETE);

  // Should only read one tile (2 values).
  CHECK(8 == data_r_size);
  CHECK(8 == coords_r_size);

  int coords_c_1[] = {1, 2};
  int data_c_1[] = {1, 2};
  CHECK(!std::memcmp(coords_c_1, coords_r, coords_r_size));
  CHECK(!std::memcmp(data_c_1, data_r, data_r_size));

  // Read again.
  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_OK);

  // Check incomplete query status.
  tiledb_query_get_status(ctx_, query, &status);
  CHECK(status == TILEDB_INCOMPLETE);

  // Should only read one more tile (2 values).
  CHECK(8 == data_r_size);
  CHECK(8 == coords_r_size);

  int coords_c_2[] = {3, 4};
  int data_c_2[] = {3, 4};
  CHECK(!std::memcmp(coords_c_2, coords_r, coords_r_size));
  CHECK(!std::memcmp(data_c_2, data_r, data_r_size));

  // Read again.
  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_OK);

  // Check completed query status.
  tiledb_query_get_status(ctx_, query, &status);
  CHECK(status == TILEDB_COMPLETED);

  // Should read last tile (1 value).
  CHECK(4 == data_r_size);
  CHECK(4 == coords_r_size);

  int coords_c_3[] = {5};
  int data_c_3[] = {5};
  CHECK(!std::memcmp(coords_c_3, coords_r, coords_r_size));
  CHECK(!std::memcmp(data_c_3, data_r, data_r_size));

  // Clean up.
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
  tiledb_query_free(&query);
}

TEST_CASE_METHOD(
    CSparseUnorderedWithDupsFx,
    "Sparse unordered with dups reader: qc removes full tile",
    "[sparse-unordered-with-dups][qc-removes-tile]") {
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

TEST_CASE_METHOD(
    CSparseUnorderedWithDupsFx,
    "Sparse unordered with dups reader: single tile query continuation",
    "[sparse-unordered-with-dups][single-tile][continuation]") {
  bool use_subarray = false;
  SECTION("- No subarray") {
    use_subarray = false;
  }
  SECTION("- Subarray") {
    use_subarray = true;
  }

  // Create default array.
  reset_config();
  create_default_array_1d();

  // Write a fragment.
  int coords[] = {1, 2};
  uint64_t coords_size = sizeof(coords);
  int data[] = {1, 2};
  uint64_t data_size = sizeof(data);
  write_1d_fragment(coords, &coords_size, data, &data_size);

  tiledb_array_t* array = nullptr;
  tiledb_query_t* query = nullptr;

  // Try to read.
  int coords_r[1];  // only room for one cell.
  int data_r[1];
  uint64_t coords_r_size = sizeof(coords_r);
  uint64_t data_r_size = sizeof(data_r);
  auto rc = read(
      use_subarray,
      false,
      coords_r,
      &coords_r_size,
      data_r,
      &data_r_size,
      &query,
      &array);
  CHECK(rc == TILEDB_OK);

  // Check incomplete query status.
  tiledb_query_status_t status;
  tiledb_query_get_status(ctx_, query, &status);
  CHECK(status == TILEDB_INCOMPLETE);

  // Should only read one cell (1 values).
  CHECK(4 == data_r_size);
  CHECK(4 == coords_r_size);

  int coords_c_1[] = {1};
  int data_c_1[] = {1};
  CHECK(!std::memcmp(coords_c_1, coords_r, coords_r_size));
  CHECK(!std::memcmp(data_c_1, data_r, data_r_size));

  // Read again.
  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_OK);

  // Check incomplete query status.
  tiledb_query_get_status(ctx_, query, &status);
  CHECK(status == TILEDB_COMPLETED);

  // Should read last cell (1 values).
  CHECK(4 == data_r_size);
  CHECK(4 == coords_r_size);

  int coords_c_2[] = {2};
  int data_c_2[] = {2};
  CHECK(!std::memcmp(coords_c_2, coords_r, coords_r_size));
  CHECK(!std::memcmp(data_c_2, data_r, data_r_size));

  // Clean up.
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
  tiledb_query_free(&query);
}

TEST_CASE_METHOD(
    CSparseUnorderedWithDupsVarDataFx,
    "Sparse unordered with dups reader: results shrinked due to data buffer",
    "[sparse-unordered-with-dups][data-buffer-overflow]") {
  // Create default array.
  create_default_array_2d();
  write_2d_fragment();

  bool use_subarray = false;
  SECTION("- No subarray") {
    use_subarray = false;
  }
  SECTION("- Subarray") {
    use_subarray = true;
  }

  read_and_check_data(use_subarray);
}

TEST_CASE_METHOD(
    CSparseUnorderedWithDupsVarDataFx,
    "Sparse unordered with dups reader: test compute_var_size_offsets",
    "[sparse-unordered-with-dups][compute_var_size_offsets]") {
  auto&& [array, fragments] = open_default_array_1d_with_fragments();

  // Make a vector of tiles.
  ResultTileWithBitmap<uint64_t> result_tile(
      0, 0, array->array_->array_schema_latest());
  std::vector<ResultTileWithBitmap<uint64_t>> rt;
  rt.push_back(std::move(result_tile));

  SECTION("- No bitmap") {
  }

  SECTION("- With bitmap") {
    rt[0].bitmap_.resize(5, 1);
  }

  // Create the result_tiles pointer vector.
  std::vector<ResultTile*> result_tiles(rt.size());
  for (uint64_t i = 0; i < rt.size(); i++) {
    result_tiles[i] = &rt[i];
  }

  // Create the cell_offsets vector.
  std::vector<uint64_t> cell_offsets(rt.size() + 1);
  uint64_t offset = 0;
  for (uint64_t i = 0; i < rt.size(); i++) {
    cell_offsets[i] = offset;
    offset += 5;
  }
  cell_offsets[rt.size()] = offset;

  // Create a Query buffer.
  tiledb::sm::QueryBuffer query_buffer;
  uint64_t offsets[] = {2, 2, 2, 2, 2};
  uint64_t offsets_size = sizeof(offsets);
  query_buffer.buffer_ = offsets;
  query_buffer.buffer_size_ = &offsets_size;
  query_buffer.original_buffer_size_ = offsets_size;
  uint64_t buffer_var_size = 0;
  query_buffer.buffer_var_size_ = &buffer_var_size;
  query_buffer.original_buffer_var_size_ = 6;

  // Call the function.
  auto&& [buffers_full, var_buffer_size, result_tiles_size] =
      SparseUnorderedWithDupsReader<uint64_t>::compute_var_size_offsets<
          uint64_t>(
          &tiledb::test::g_helper_stats,
          fragments,
          result_tiles,
          0,
          cell_offsets,
          query_buffer);

  // Validate results.
  CHECK(buffers_full == true);
  CHECK(cell_offsets[1] == 3);
  CHECK(result_tiles_size == 1);
  CHECK(var_buffer_size == 6);

  // Clean up.
  REQUIRE(tiledb_array_close(ctx_, array) == TILEDB_OK);
  tiledb_array_free(&array);
}

TEST_CASE_METHOD(
    CSparseUnorderedWithDupsVarDataFx,
    "Sparse unordered with dups reader: test compute_var_size_offsets count "
    "bitmap",
    "[sparse-unordered-with-dups][compute_var_size_offsets][count-bitmap]") {
  auto&& [array, fragments] = open_default_array_1d_with_fragments();

  // Make a vector of tiles.
  ResultTileWithBitmap<uint64_t> result_tile(
      0, 0, array->array_->array_schema_latest());
  std::vector<ResultTileWithBitmap<uint64_t>> rt;
  rt.push_back(std::move(result_tile));
  rt[0].bitmap_.resize(5);
  rt[0].bitmap_ = {0, 1, 2, 0, 2};

  // Create the result_tiles pointer vector.
  std::vector<ResultTile*> result_tiles(rt.size());
  for (uint64_t i = 0; i < rt.size(); i++) {
    result_tiles[i] = &rt[i];
  }

  // Create the cell_offsets vector.
  std::vector<uint64_t> cell_offsets(rt.size() + 1);
  uint64_t offset = 0;
  for (uint64_t i = 0; i < rt.size(); i++) {
    cell_offsets[i] = offset;
    offset += 5;
  }
  cell_offsets[rt.size()] = offset;

  // Create a Query buffer.
  tiledb::sm::QueryBuffer query_buffer;
  uint64_t offsets[] = {2, 2, 2, 2, 2};
  uint64_t offsets_size = sizeof(offsets);
  query_buffer.buffer_ = offsets;
  query_buffer.buffer_size_ = &offsets_size;
  uint64_t buffer_var_size = 0;
  query_buffer.buffer_var_size_ = &buffer_var_size;
  query_buffer.original_buffer_var_size_ = 6;

  // Call the function.
  auto&& [buffers_full, var_buffer_size, result_tiles_size] =
      SparseUnorderedWithDupsReader<uint64_t>::compute_var_size_offsets<
          uint64_t>(
          &tiledb::test::g_helper_stats,
          fragments,
          result_tiles,
          0,
          cell_offsets,
          query_buffer);

  // Validate results.
  CHECK(buffers_full == true);
  CHECK(cell_offsets[1] == 3);
  CHECK(result_tiles_size == 1);
  CHECK(var_buffer_size == 6);

  // Clean up.
  REQUIRE(tiledb_array_close(ctx_, array) == TILEDB_OK);
  tiledb_array_free(&array);
}

TEST_CASE_METHOD(
    CSparseUnorderedWithDupsVarDataFx,
    "Sparse unordered with dups reader: test compute_var_size_offsets "
    "continuation",
    "[sparse-unordered-with-dups][compute_var_size_offsets][continuation]") {
  auto&& [array, fragments] = open_default_array_1d_with_fragments();

  // Make a vector of tiles.
  ResultTileWithBitmap<uint64_t> result_tile(
      0, 0, array->array_->array_schema_latest());
  std::vector<ResultTileWithBitmap<uint64_t>> rt;
  rt.push_back(std::move(result_tile));

  SECTION("- No bitmap") {
  }

  SECTION("- With bitmap") {
    rt[0].bitmap_.resize(5, 1);
  }

  // Create the result_tiles pointer vector.
  std::vector<ResultTile*> result_tiles(rt.size());
  for (uint64_t i = 0; i < rt.size(); i++) {
    result_tiles[i] = &rt[i];
  }

  // Create the cell_offsets vector.
  std::vector<uint64_t> cell_offsets(rt.size() + 1);
  uint64_t offset = 0;
  for (uint64_t i = 0; i < rt.size(); i++) {
    cell_offsets[i] = offset;
    offset += 3;
  }
  cell_offsets[rt.size()] = offset;

  // Create a Query buffer.
  tiledb::sm::QueryBuffer query_buffer;
  uint64_t offsets[] = {2, 2, 2, 0, 0};
  uint64_t offsets_size = sizeof(offsets);
  query_buffer.buffer_ = offsets;
  query_buffer.buffer_size_ = &offsets_size;
  uint64_t buffer_var_size = 0;
  query_buffer.buffer_var_size_ = &buffer_var_size;
  query_buffer.original_buffer_var_size_ = 5;

  // Call the function.
  auto&& [buffers_full, var_buffer_size, result_tiles_size] =
      SparseUnorderedWithDupsReader<uint64_t>::compute_var_size_offsets<
          uint64_t>(
          &tiledb::test::g_helper_stats,
          fragments,
          result_tiles,
          2,
          cell_offsets,
          query_buffer);

  // Validate results.
  CHECK(buffers_full == true);
  CHECK(cell_offsets[1] == 2);
  CHECK(result_tiles_size == 1);
  CHECK(var_buffer_size == 4);

  // Clean up.
  REQUIRE(tiledb_array_close(ctx_, array) == TILEDB_OK);
  tiledb_array_free(&array);
}

TEST_CASE_METHOD(
    CSparseUnorderedWithDupsFx,
    "Sparse unordered with dups reader: empty strings",
    "[sparse-unordered-with-dups][empty-strings]") {
  // Create default array.
  reset_config();
  create_default_array_1d_string();

  // Write a fragment.
  int coords[] = {1, 2};
  uint64_t coords_size = sizeof(coords);
  write_1d_fragment_empty_strings(coords, &coords_size);

  // Try to read.
  int coords_r[5];
  char data_r[5];
  uint64_t data_offsets_r[5];
  uint64_t coords_r_size = sizeof(coords_r);
  uint64_t data_r_size = sizeof(data_r);
  uint64_t data_offsets_r_size = sizeof(data_offsets_r);
  auto rc = read_strings(
      coords_r,
      &coords_r_size,
      data_r,
      &data_r_size,
      data_offsets_r,
      &data_offsets_r_size);
  CHECK(rc == TILEDB_OK);
}