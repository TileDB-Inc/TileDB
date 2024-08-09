/**
 * @file unit-sparse-unordered-with-dups-reader.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2023 TileDB, Inc.
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

#include "test/support/src/helpers.h"
#include "tiledb/common/common.h"
#include "tiledb/common/memory_tracker.h"
#include "tiledb/sm/c_api/tiledb.h"
#include "tiledb/sm/c_api/tiledb_struct_def.h"
#include "tiledb/sm/query/query_buffer.h"
#include "tiledb/sm/query/readers/sparse_index_reader_base.h"
#include "tiledb/sm/query/readers/sparse_unordered_with_dups_reader.h"

#ifdef _WIN32
#include "tiledb/sm/filesystem/win.h"
#else
#include "tiledb/sm/filesystem/posix.h"
#endif

#include <catch2/catch_test_macros.hpp>
#include <catch2/generators/catch_generators.hpp>

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
  std::string tile_upper_memory_limit_;
  std::string ratio_tile_ranges_;
  std::string ratio_array_data_;
  std::string ratio_coords_;
  std::string partial_tile_offsets_loading_;

  void create_default_array_1d();
  void create_large_domain_array_1d();
  void create_default_array_1d_string(int tile_extent = 2, int capacity = 2);
  void write_1d_fragment(
      int* coords, uint64_t* coords_size, int* data, uint64_t* data_size);
  void write_1d_fragment_empty_strings(int* coords, uint64_t* coords_size);
  void write_1d_fragment_string(
      int* coords,
      uint64_t* coords_size,
      uint64_t* a1_offsets,
      uint64_t* a1_offsets_size,
      char* a1_data,
      uint64_t* a1_data_size,
      int64_t* a2_data,
      uint64_t* a2_data_size,
      uint8_t* a2_validity,
      uint64_t* a2_validity_size);
  int32_t read(
      bool set_subarray,
      int qc_idx,
      int* coords,
      uint64_t* coords_size,
      int* data,
      uint64_t* data_size,
      tiledb_query_t** query = nullptr,
      tiledb_array_t** array_ret = nullptr);
  int32_t read_strings(
      int* coords,
      uint64_t* coords_size,
      char* a1_data,
      uint64_t* a1_data_size,
      uint64_t* a1_offsets,
      uint64_t* a1_offsets_size,
      int64_t* a2_data,
      uint64_t* a2_data_size,
      uint8_t* a2_validity,
      uint64_t* a2_validity_size,
      uint64_t num_subarrays = 0,
      tiledb_config_t* config = nullptr,
      tiledb_query_t** query_ret = nullptr,
      tiledb_array_t** array_ret = nullptr);
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
  partial_tile_offsets_loading_ = "false";
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

  if (tile_upper_memory_limit_ != "") {
    REQUIRE(
        tiledb_config_set(
            config,
            "sm.mem.tile_upper_memory_limit",
            tile_upper_memory_limit_.c_str(),
            &error) == TILEDB_OK);
    REQUIRE(error == nullptr);
  }

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
          "sm.partial_tile_offsets_loading",
          partial_tile_offsets_loading_.c_str(),
          &error) == TILEDB_OK);
  REQUIRE(error == nullptr);

  REQUIRE(tiledb_ctx_alloc(config, &ctx_) == TILEDB_OK);
  REQUIRE(error == nullptr);
  REQUIRE(tiledb_vfs_alloc(ctx_, config, &vfs_) == TILEDB_OK);
  tiledb_config_free(&config);
}

void CSparseUnorderedWithDupsFx::create_default_array_1d() {
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
      true);  // allows dups.
}

void CSparseUnorderedWithDupsFx::create_large_domain_array_1d() {
  int domain[] = {1, 20000};
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

void CSparseUnorderedWithDupsFx::create_default_array_1d_string(
    int tile_extent, int capacity) {
  int domain[] = {1, 20};
  create_array(
      ctx_,
      array_name_,
      TILEDB_SPARSE,
      {"d"},
      {TILEDB_INT32},
      {domain},
      {&tile_extent},
      {"a1", "a2"},
      {TILEDB_STRING_ASCII, TILEDB_INT64},
      {TILEDB_VAR_NUM, 1},
      {tiledb::test::Compressor(TILEDB_FILTER_NONE, -1),
       tiledb::test::Compressor(TILEDB_FILTER_NONE, -1)},
      TILEDB_ROW_MAJOR,
      TILEDB_ROW_MAJOR,
      capacity,
      true,
      false,
      optional<std::vector<bool>>({false, true}));  // allows dups.
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

  char a1_data[1];
  uint64_t a1_data_size = 0;
  std::vector<uint64_t> a1_offsets(num_cells, 0);
  uint64_t a1_offsets_size = num_cells * sizeof(uint64_t);

  std::vector<int64_t> a2_data(num_cells, 0);
  uint64_t a2_data_size = num_cells * sizeof(int64_t);
  std::vector<uint8_t> a2_validity(num_cells, 0);
  uint64_t a2_validity_size = num_cells;

  // Create the query.
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_UNORDERED);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, query, "a1", a1_data, &a1_data_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_offsets_buffer(
      ctx_, query, "a1", a1_offsets.data(), &a1_offsets_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, "a2", a2_data.data(), &a2_data_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_validity_buffer(
      ctx_, query, "a2", a2_validity.data(), &a2_validity_size);
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

void CSparseUnorderedWithDupsFx::write_1d_fragment_string(
    int* coords,
    uint64_t* coords_size,
    uint64_t* a1_offsets,
    uint64_t* a1_offsets_size,
    char* a1_data,
    uint64_t* a1_data_size,
    int64_t* a2_data,
    uint64_t* a2_data_size,
    uint8_t* a2_validity,
    uint64_t* a2_validity_size) {
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
  rc = tiledb_query_set_data_buffer(ctx_, query, "a1", a1_data, a1_data_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_offsets_buffer(
      ctx_, query, "a1", a1_offsets, a1_offsets_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, query, "a2", a2_data, a2_data_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_validity_buffer(
      ctx_, query, "a2", a2_validity, a2_validity_size);
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
    int qc_idx,
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
    int subarray[] = {1, 200};
    tiledb_subarray_t* sub;
    rc = tiledb_subarray_alloc(ctx_, array, &sub);
    CHECK(rc == TILEDB_OK);
    rc = tiledb_subarray_set_subarray(ctx_, sub, subarray);
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
    char* a1_data,
    uint64_t* a1_data_size,
    uint64_t* a1_offsets,
    uint64_t* a1_offsets_size,
    int64_t* a2_data,
    uint64_t* a2_data_size,
    uint8_t* a2_validity,
    uint64_t* a2_validity_size,
    uint64_t num_subarrays,
    tiledb_config_t* config,
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

  tiledb_subarray_t* subarray;
  rc = tiledb_subarray_alloc(ctx_, array, &subarray);
  CHECK(rc == TILEDB_OK);
  if (config != nullptr) {
    rc = tiledb_subarray_set_config(ctx_, subarray, config);
    CHECK(rc == TILEDB_OK);
  }

  for (uint64_t i = 0; i < num_subarrays; i++) {
    // Create subarray for reading data.
    int range[2] = {1, 20};
    rc = tiledb_subarray_add_range(
        ctx_, subarray, 0, &range[0], &range[1], NULL);
    CHECK(rc == TILEDB_OK);
  }

  if (num_subarrays > 0) {
    rc = tiledb_query_set_subarray_t(ctx_, query, subarray);
    CHECK(rc == TILEDB_OK);
  }

  rc = tiledb_query_set_layout(ctx_, query, TILEDB_UNORDERED);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, query, "a1", a1_data, a1_data_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_offsets_buffer(
      ctx_, query, "a1", a1_offsets, a1_offsets_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, query, "a2", a2_data, a2_data_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_validity_buffer(
      ctx_, query, "a2", a2_validity, a2_validity_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, query, "d", coords, coords_size);
  CHECK(rc == TILEDB_OK);

  // Submit query.
  auto ret = tiledb_query_submit(ctx_, query);

  tiledb_subarray_free(&subarray);

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

struct CSparseUnorderedWithDupsVarDataFx {
  tiledb_ctx_t* ctx_ = nullptr;
  tiledb_vfs_t* vfs_ = nullptr;
  std::string temp_dir_;
  std::string array_name_;
  const char* ARRAY_NAME = "test_sparse_unordered_with_dups_var_data";

  void create_default_array_2d();
  void write_2d_fragment();
  void read_and_check_data(bool set_subarray);

  tuple<tiledb_array_t*, std::vector<shared_ptr<FragmentMetadata>>>
  open_default_array_1d_with_fragments(uint64_t capacity = 5);

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
    tiledb_subarray_t* sub;
    rc = tiledb_subarray_alloc(ctx_, array, &sub);
    CHECK(rc == TILEDB_OK);
    rc = tiledb_subarray_set_subarray(ctx_, sub, subarray);
    CHECK(rc == TILEDB_OK);
    rc = tiledb_query_set_subarray_t(ctx_, query, sub);
    CHECK(rc == TILEDB_OK);
    tiledb_subarray_free(&sub);
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

tuple<tiledb_array_t*, std::vector<shared_ptr<FragmentMetadata>>>
CSparseUnorderedWithDupsVarDataFx::open_default_array_1d_with_fragments(
    uint64_t capacity) {
  int64_t domain[] = {1, 10};
  int64_t tile_extent = capacity;
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
      capacity);

  // Open array for reading.
  tiledb_array_t* array;
  auto rc = tiledb_array_alloc(ctx_, array_name_.c_str(), &array);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  REQUIRE(rc == TILEDB_OK);

  std::vector<shared_ptr<FragmentMetadata>> fragments;
  shared_ptr<FragmentMetadata> fragment = make_shared<FragmentMetadata>(
      HERE(),
      nullptr,
      array->array_->array_schema_latest_ptr(),
      generate_fragment_uri(array->array_.get()),
      std::make_pair<uint64_t, uint64_t>(0, 0),
      tiledb::test::create_test_memory_tracker(),
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
    CSparseUnorderedWithDupsFx,
    "Sparse unordered with dups reader: tile offsets budget exceeded",
    "[sparse-unordered-with-dups][tile-offsets][budget-exceeded]") {
  bool partial_tile_offsets_loading = GENERATE(true, false);
  bool set_subarray = GENERATE(true, false);

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
  partial_tile_offsets_loading_ =
      partial_tile_offsets_loading ? "true" : "false";
  update_config();

  // Try to read.
  int coords_r[200];
  int data_r[200];
  uint64_t coords_r_size = sizeof(coords_r);
  uint64_t data_r_size = sizeof(data_r);
  auto rc =
      read(set_subarray, 0, coords_r, &coords_r_size, data_r, &data_r_size);
  CHECK(rc == TILEDB_ERR);

  // Check we hit the correct error.
  tiledb_error_t* error = NULL;
  rc = tiledb_ctx_get_last_error(ctx_, &error);
  CHECK(rc == TILEDB_OK);

  const char* msg;
  rc = tiledb_error_message(error, &msg);
  CHECK(rc == TILEDB_OK);

  std::string error_str(msg);
  if (partial_tile_offsets_loading) {
    CHECK(
        error_str.find(
            "SparseUnorderedWithDupsReader: Cannot load tile offsets for only "
            "one fragment. Offsets size for the fragment") !=
        std::string::npos);
  } else {
    CHECK(
        error_str.find(
            "SparseUnorderedWithDupsReader: Cannot load tile offsets, "
            "computed size") != std::string::npos);
  }
}

TEST_CASE_METHOD(
    CSparseUnorderedWithDupsFx,
    "Sparse unordered with dups reader: tile offsets partial loading",
    "[sparse-unordered-with-dups][tile-offsets][multiple-iterations]") {
  bool enable_partial_tile_offsets_loading = GENERATE(true, false);

  // Create default array.
  reset_config();
  create_large_domain_array_1d();
  bool one_frag = false;

  SECTION("- One fragment") {
    one_frag = true;
  }
  SECTION("- Multiple fragments") {
    one_frag = false;
  }

  // Write fragments.
  if (one_frag) {
    std::vector<int> coords(100);
    std::iota(coords.begin(), coords.end(), 1);
    uint64_t coords_size = coords.size() * sizeof(int);

    std::vector<int> data(100);
    std::iota(data.begin(), data.end(), 1);
    uint64_t data_size = data.size() * sizeof(int);

    write_1d_fragment(coords.data(), &coords_size, data.data(), &data_size);
  } else {
    std::vector<int> coords(100);
    std::iota(coords.begin(), coords.end(), 1);
    uint64_t coords_size = coords.size() * sizeof(int);

    std::vector<int> data(100);
    std::iota(data.begin(), data.end(), 1);
    uint64_t data_size = data.size() * sizeof(int);

    write_1d_fragment(coords.data(), &coords_size, data.data(), &data_size);

    std::vector<int> coords2(1000);
    std::iota(coords2.begin(), coords2.end(), 101);
    uint64_t coords2_size = coords2.size() * sizeof(int);

    std::vector<int> data2(1000);
    std::iota(data2.begin(), data2.end(), 101);
    uint64_t data2_size = data2.size() * sizeof(int);
    write_1d_fragment(coords2.data(), &coords2_size, data2.data(), &data2_size);

    std::vector<int> coords3(5000);
    std::iota(coords3.begin(), coords3.end(), 1101);
    uint64_t coords3_size = coords3.size() * sizeof(int);

    std::vector<int> data3(5000);
    std::iota(data3.begin(), data3.end(), 1101);
    uint64_t data3_size = data3.size() * sizeof(int);
    write_1d_fragment(coords3.data(), &coords3_size, data3.data(), &data3_size);

    std::vector<int> coords4(10000);
    std::iota(coords4.begin(), coords4.end(), 6101);
    uint64_t coords4_size = coords4.size() * sizeof(int);

    std::vector<int> data4(10000);
    std::iota(data4.begin(), data4.end(), 6101);
    uint64_t data4_size = data4.size() * sizeof(int);
    write_1d_fragment(coords4.data(), &coords4_size, data4.data(), &data4_size);
  }

  total_budget_ = "3300000";
  ratio_array_data_ = "0.002";
  partial_tile_offsets_loading_ =
      enable_partial_tile_offsets_loading ? "true" : "false";
  update_config();

  tiledb_array_t* array = nullptr;
  tiledb_query_t* query = nullptr;

  int coords_r[16100];
  int data_r[16100];
  uint64_t coords_r_size = sizeof(coords_r);
  uint64_t data_r_size = sizeof(data_r);
  auto rc = 0;

  // Case 1: Read only one frag. Should be ok for both cases of partial tile
  // loading Case 2: Read multiple fragments with partial tile offset loading.
  // Should be ok
  if (enable_partial_tile_offsets_loading || one_frag) {
    rc = read(
        false,
        0,
        coords_r,
        &coords_r_size,
        data_r,
        &data_r_size,
        &query,
        &array);
    CHECK(rc == TILEDB_OK);

    // Validate the results.
    int elements_to_check;
    if (one_frag) {
      elements_to_check = 100;
    } else {
      elements_to_check = 16100;
    }

    for (int i = 0; i < elements_to_check; i++) {
      CHECK(coords_r[i] == i + 1);
      CHECK(data_r[i] == i + 1);
    }

    // Check the internal loop count against expected value.
    auto stats =
        ((SparseUnorderedWithDupsReader<uint8_t>*)query->query_->strategy())
            ->stats();
    REQUIRE(stats != nullptr);
    auto counters = stats->counters();
    REQUIRE(counters != nullptr);
    auto loop_num =
        counters->find("Context.StorageManager.Query.Reader.internal_loop_num");

    if (one_frag) {
      CHECK(1 == loop_num->second);
    } else {
      CHECK(9 == loop_num->second);
    }

    // Try to read multiple frags without partial tile offset reading. Should
    // fail
  } else {
    rc = read(
        false,
        0,
        coords_r,
        &coords_r_size,
        data_r,
        &data_r_size,
        &query,
        &array);
    CHECK(rc == TILEDB_ERR);

    tiledb_error_t* error = NULL;
    rc = tiledb_ctx_get_last_error(ctx_, &error);
    CHECK(rc == TILEDB_OK);

    const char* msg;
    rc = tiledb_error_message(error, &msg);
    CHECK(rc == TILEDB_OK);

    std::string error_str(msg);
    CHECK(
        error_str.find("Cannot load tile offsets, computed size") !=
        std::string::npos);
  }

  // Clean up.
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
  tiledb_query_free(&query);
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

  // Two result tile (2 * ~1208) will be bigger than the budget (1500).
  total_budget_ = "40000";
  ratio_coords_ = "0.04";
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
      ((SparseUnorderedWithDupsReader<uint8_t>*)query->query_->strategy())
          ->stats();
  REQUIRE(stats != nullptr);
  auto counters = stats->counters();
  REQUIRE(counters != nullptr);
  auto loop_num =
      counters->find("Context.StorageManager.Query.Reader.internal_loop_num");
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

  // One result tile (~505) will be larger than leftover memory.
  total_budget_ = "1800";
  ratio_array_data_ = "0.99";
  ratio_coords_ = "0.0005";
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
      false, 0, coords_r, &coords_r_size, data_r, &data_r_size, &query, &array);
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
  CHECK(sizeof(int) == data_r_size);
  CHECK(sizeof(int) == coords_r_size);

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
  int qc_index = GENERATE(1, 2);
  bool use_budget = GENERATE(true, false);
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

  if (use_budget) {
    // Two result tile (2 * ~1208) will be bigger than the budget (1500).
    total_budget_ = "100000";
    ratio_coords_ = "0.015";
    update_config();
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
      use_subarray, qc_index, coords_r, &coords_r_size, data_r, &data_r_size);
  CHECK(rc == TILEDB_OK);

  // Should read two tile (6 values).
  CHECK(6 * sizeof(int) == data_r_size);
  CHECK(6 * sizeof(int) == coords_r_size);

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
      0,
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
  CHECK(sizeof(int) == data_r_size);
  CHECK(sizeof(int) == coords_r_size);

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
  CHECK(sizeof(int) == data_r_size);
  CHECK(sizeof(int) == coords_r_size);

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
  uint64_t var_buffer_size = 0;
  std::vector<std::vector<uint64_t>> bitmaps;
  uint64_t capacity = 0;
  uint64_t num_tiles = 0;
  uint64_t first_tile_min_pos = 0;
  std::vector<uint64_t> offsets_buffer;
  std::vector<uint64_t> cell_offsets;
  bool expected_buffers_full = false;
  std::vector<uint64_t> expected_cell_offsets;
  uint64_t expected_result_tiles_size = 0;
  uint64_t expected_var_buffer_size = 0;

  SECTION("Basic") {
    var_buffer_size = 6;

    SECTION("- No bitmap") {
      bitmaps = {{}};
    }

    SECTION("- With bitmap") {
      bitmaps = {{1, 1, 1, 1, 1}};
    }

    capacity = 5;
    num_tiles = 1;
    first_tile_min_pos = 0;
    offsets_buffer = {2, 2, 2, 2, 2};
    cell_offsets = {0, 5};
    expected_buffers_full = true;
    expected_cell_offsets = {0, 3};
    expected_result_tiles_size = 1;
    expected_var_buffer_size = 6;
  }

  SECTION("Count Bitmap") {
    var_buffer_size = 6;
    bitmaps = {{0, 1, 2, 2, 0}};
    capacity = 5;
    num_tiles = 1;
    first_tile_min_pos = 0;
    offsets_buffer = {2, 2, 2, 2, 2};
    cell_offsets = {0, 5};
    expected_buffers_full = true;
    expected_cell_offsets = {0, 3};
    expected_result_tiles_size = 1;
    expected_var_buffer_size = 6;
  }

  SECTION("Continuation") {
    var_buffer_size = 5;

    SECTION("- No bitmap") {
      bitmaps = {{}};
    }

    SECTION("- With bitmap") {
      bitmaps = {{1, 1, 1, 1, 1}};
    }

    capacity = 5;
    num_tiles = 1;
    first_tile_min_pos = 2;
    offsets_buffer = {2, 2, 2, 0, 0};
    cell_offsets = {0, 3};
    expected_buffers_full = true;
    expected_cell_offsets = {0, 2};
    expected_result_tiles_size = 1;
    expected_var_buffer_size = 4;
  }

  SECTION("Last cell") {
    var_buffer_size = 5;

    SECTION("- No bitmap") {
      bitmaps = {{}};
    }

    SECTION("- With bitmap") {
      bitmaps = {{1, 1, 1, 1, 1}};
    }

    capacity = 5;
    num_tiles = 1;
    first_tile_min_pos = 0;
    offsets_buffer = {2, 2, 2, 0, 0};
    cell_offsets = {0, 3};
    expected_buffers_full = true;
    expected_cell_offsets = {0, 2};
    expected_result_tiles_size = 1;
    expected_var_buffer_size = 4;
  }

  SECTION("No empty tile") {
    var_buffer_size = 11;

    SECTION("- No bitmap") {
      bitmaps = {{}, {}};
    }

    SECTION("- With bitmap") {
      bitmaps = {{1, 1, 1, 1, 1}, {1, 1, 1, 1, 1}};
    }

    capacity = 5;
    num_tiles = 2;
    first_tile_min_pos = 0;
    offsets_buffer = {2, 2, 2, 2, 2, 2, 2, 2, 2, 2};
    cell_offsets = {0, 5, 10};
    expected_buffers_full = true;
    expected_cell_offsets = {0, 5, 5};
    expected_result_tiles_size = 1;
    expected_var_buffer_size = 10;
  }

  SECTION("Complex") {
    var_buffer_size = 15;
    bitmaps = {{1, 0, 1, 0, 1, 0, 1, 0, 1, 0}, {0, 1, 0, 1, 0, 1, 0, 1, 0, 1}};
    capacity = 10;
    num_tiles = 2;
    first_tile_min_pos = 2;
    offsets_buffer = {2, 2, 2, 2, 2, 2, 2, 2, 2};
    cell_offsets = {0, 4, 9};
    expected_buffers_full = true;
    expected_cell_offsets = {0, 4, 7};
    expected_result_tiles_size = 2;
    expected_var_buffer_size = 14;
  }

  auto&& [array, fragments] = open_default_array_1d_with_fragments(capacity);

  // Make a vector of tiles.
  std::list<UnorderedWithDupsResultTile<uint64_t>> rt;
  for (uint64_t t = 0; t < num_tiles; t++) {
    rt.emplace_back(
        0, t, *fragments[0], tiledb::test::get_test_memory_tracker());

    // Allocate and set the bitmap if required.
    if (bitmaps[t].size() > 0) {
      rt.back().bitmap().assign(bitmaps[t].begin(), bitmaps[t].end());
      rt.back().count_cells();
    }
  }

  // Create the result_tiles pointer vector.
  std::vector<ResultTile*> result_tiles(rt.size());
  uint64_t i = 0;
  for (auto& t : rt) {
    result_tiles[i++] = &t;
  }

  // Create a Query buffer.
  tiledb::sm::QueryBuffer query_buffer;
  uint64_t offsets_size = offsets_buffer.size() * sizeof(uint64_t);
  query_buffer.buffer_ = offsets_buffer.data();
  query_buffer.buffer_size_ = &offsets_size;
  query_buffer.original_buffer_size_ = offsets_size;
  uint64_t buffer_var_size = 0;
  query_buffer.buffer_var_size_ = &buffer_var_size;
  query_buffer.original_buffer_var_size_ = var_buffer_size;

  // Call the function.
  auto&& [buffers_full, var_buffer_size_ret, result_tiles_size] =
      SparseUnorderedWithDupsReader<uint64_t>::compute_var_size_offsets<
          uint64_t>(
          &tiledb::test::g_helper_stats,
          result_tiles,
          first_tile_min_pos,
          cell_offsets,
          query_buffer);

  // Validate results.
  CHECK(expected_buffers_full == buffers_full);
  CHECK(expected_cell_offsets == cell_offsets);
  CHECK(expected_result_tiles_size == result_tiles_size);
  CHECK(expected_var_buffer_size == var_buffer_size_ret);

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
  char a1_data_r[5];
  uint64_t a1_offsets_r[5];
  int64_t a2_data_r[5];
  uint8_t a2_validity_r[5];
  uint64_t coords_r_size = sizeof(coords_r);
  uint64_t a1_data_r_size = sizeof(a1_data_r);
  uint64_t a1_offsets_r_size = sizeof(a1_offsets_r);
  uint64_t a2_data_r_size = sizeof(a2_data_r);
  uint64_t a2_validity_r_size = sizeof(a2_validity_r);
  auto rc = read_strings(
      coords_r,
      &coords_r_size,
      a1_data_r,
      &a1_data_r_size,
      a1_offsets_r,
      &a1_offsets_r_size,
      a2_data_r,
      &a2_data_r_size,
      a2_validity_r,
      &a2_validity_r_size);
  CHECK(rc == TILEDB_OK);
}

TEST_CASE_METHOD(
    CSparseUnorderedWithDupsVarDataFx,
    "Sparse unordered with dups reader: test "
    "resize_fixed_result_tiles_to_copy",
    "[sparse-unordered-with-dups][resize_fixed_result_tiles_to_copy]") {
  std::vector<std::vector<uint64_t>> bitmaps;
  uint64_t capacity = 0;
  uint64_t num_tiles = 0;
  uint64_t max_num_cells = 0;
  uint64_t initial_cell_offset = 0;
  uint64_t first_tile_min_pos = 0;
  bool expected_buffers_full = false;
  std::vector<uint64_t> expected_cell_offsets;
  uint64_t expected_result_tiles_size = 0;

  SECTION("Basic all") {
    SECTION("- No bitmap") {
      bitmaps = {{}};
    }

    SECTION("- With bitmap") {
      bitmaps = {{1, 1, 1, 1, 1}};
    }

    capacity = 5;
    num_tiles = 1;
    max_num_cells = 10;
    initial_cell_offset = 0;
    first_tile_min_pos = 0;
    expected_buffers_full = false;
    expected_cell_offsets = {0, 5};
    expected_result_tiles_size = 1;
  }

  SECTION("Basic full") {
    SECTION("- No bitmap") {
      bitmaps = {{}};
    }

    SECTION("- With bitmap") {
      bitmaps = {{1, 1, 1, 1, 1}};
    }

    capacity = 5;
    num_tiles = 1;
    max_num_cells = 3;
    initial_cell_offset = 0;
    first_tile_min_pos = 0;
    expected_buffers_full = true;
    expected_cell_offsets = {0, 3};
    expected_result_tiles_size = 1;
  }

  SECTION("Basic last cell") {
    SECTION("- No bitmap") {
      bitmaps = {{}};
    }

    SECTION("- With bitmap") {
      bitmaps = {{1, 1, 1, 1, 1}};
    }

    capacity = 5;
    num_tiles = 1;
    max_num_cells = 5;
    initial_cell_offset = 0;
    first_tile_min_pos = 0;
    expected_buffers_full = true;
    expected_cell_offsets = {0, 5};
    expected_result_tiles_size = 1;
  }

  SECTION("Basic last cell, initial cell offset") {
    SECTION("- No bitmap") {
      bitmaps = {{}};
    }

    SECTION("- With bitmap") {
      bitmaps = {{1, 1, 1, 1, 1}};
    }

    capacity = 5;
    num_tiles = 1;
    max_num_cells = 7;
    initial_cell_offset = 2;
    first_tile_min_pos = 0;
    expected_buffers_full = true;
    expected_cell_offsets = {2, 7};
    expected_result_tiles_size = 1;
  }

  SECTION("Last cell with count doesn't fit") {
    bitmaps = {{1, 1, 1, 1, 2}};
    capacity = 5;
    num_tiles = 1;
    max_num_cells = 5;
    initial_cell_offset = 0;
    first_tile_min_pos = 0;
    expected_buffers_full = true;
    expected_cell_offsets = {0, 4};
    expected_result_tiles_size = 1;
  }

  SECTION("First cell with count doesn't fit") {
    bitmaps = {{1, 1, 1, 1, 1}, {2, 1, 1, 1, 1}};
    capacity = 5;
    num_tiles = 2;
    max_num_cells = 6;
    initial_cell_offset = 0;
    first_tile_min_pos = 0;
    expected_buffers_full = true;
    expected_cell_offsets = {0, 5};
    expected_result_tiles_size = 1;
  }

  SECTION("Resume, last cell with count doesn't fit") {
    bitmaps = {{1, 1, 1, 1, 2}};
    capacity = 5;
    num_tiles = 1;
    max_num_cells = 3;
    initial_cell_offset = 0;
    first_tile_min_pos = 2;
    expected_buffers_full = true;
    expected_cell_offsets = {0, 2};
    expected_result_tiles_size = 1;
  }

  auto&& [array, fragments] = open_default_array_1d_with_fragments(capacity);

  // Make a vector of tiles.
  std::list<UnorderedWithDupsResultTile<uint64_t>> rt;
  for (uint64_t t = 0; t < num_tiles; t++) {
    rt.emplace_back(
        0, t, *fragments[0], tiledb::test::get_test_memory_tracker());

    // Allocate and set the bitmap if required.
    if (bitmaps[t].size() > 0) {
      rt.back().bitmap().assign(bitmaps[t].begin(), bitmaps[t].end());
      rt.back().count_cells();
    }
  }

  // Create the result_tiles pointer vector.
  std::vector<ResultTile*> result_tiles(rt.size());
  uint64_t i = 0;
  for (auto& t : rt) {
    result_tiles[i++] = &t;
  }

  // Call the function.
  auto&& [buffers_full, cell_offsets] =
      SparseUnorderedWithDupsReader<uint64_t>::
          resize_fixed_result_tiles_to_copy(
              max_num_cells,
              initial_cell_offset,
              first_tile_min_pos,
              result_tiles);

  // Validate results.
  CHECK(expected_buffers_full == buffers_full);
  CHECK(expected_cell_offsets == cell_offsets);
  CHECK(expected_result_tiles_size == result_tiles.size());

  // Clean up.
  REQUIRE(tiledb_array_close(ctx_, array) == TILEDB_OK);
  tiledb_array_free(&array);
}

TEST_CASE_METHOD(
    CSparseUnorderedWithDupsFx,
    "Sparse unordered with dups reader: Cell offsets test",
    "[sparse-unordered-with-dups][cell-offsets-test]") {
  // Either vary the fixed buffer or the var buffer. Varying the fixed buffer
  // will trigger the first overflow protection in
  // resize_fixed_result_tiles_to_copy and varying the var buffer will trigger
  // the second overflow protection in compute_var_size_offsets.
  bool vary_fixed_buffer = GENERATE(true, false);

  // Create default array.
  reset_config();
  create_default_array_1d_string(5);

  // Write a fragment.
  std::vector<int32_t> coords = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14};
  uint64_t coords_size = coords.size() * sizeof(int32_t);
  std::vector<uint64_t> a1_offsets = {
      0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13};
  uint64_t a1_offsets_size = a1_offsets.size() * sizeof(uint64_t);
  std::string a1_data = "123456789abcde";
  uint64_t a1_data_size = a1_data.size();
  std::vector<int64_t> a2_data = {
      10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23};
  uint64_t a2_data_size = a2_data.size() * sizeof(uint64_t);
  std::vector<uint8_t> a2_validity = {1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1};
  uint64_t a2_validity_size = a2_validity.size();
  write_1d_fragment_string(
      coords.data(),
      &coords_size,
      a1_offsets.data(),
      &a1_offsets_size,
      a1_data.data(),
      &a1_data_size,
      a2_data.data(),
      &a2_data_size,
      a2_validity.data(),
      &a2_validity_size);

  tiledb_array_t* array = nullptr;
  tiledb_query_t* query = nullptr;

  // Disable merge overlapping sparse ranges.
  // Support for returning multiplicities for overlapping ranges will be
  // deprecated in a few releases. Turning off this setting allows to still
  // test that the feature functions properly until we do so. Once support is
  // fully removed for overlapping ranges, this section can be deleted.
  tiledb_config_t* config;
  tiledb_error_t* error = nullptr;
  REQUIRE(tiledb_config_alloc(&config, &error) == TILEDB_OK);
  REQUIRE(error == nullptr);

  REQUIRE(
      tiledb_config_set(
          config,
          "sm.merge_overlapping_ranges_experimental",
          "false",
          &error) == TILEDB_OK);
  REQUIRE(error == nullptr);

  REQUIRE(tiledb_ctx_alloc(config, &ctx_) == TILEDB_OK);
  REQUIRE(error == nullptr);
  REQUIRE(tiledb_vfs_alloc(ctx_, config, &vfs_) == TILEDB_OK);

  // Try to read with every possible buffer sizes. When varying
  // buffer, the minimum should fit the number of dups at a minimum.
  // For fixed size data, that will use the size of int and for var
  // size data 1 as we have one char per cell. Max will be num dups
  // times the size of either the full coordinate data or the var size
  // data depending.
  uint64_t num_dups = GENERATE(1, 2);
  uint64_t min_buffer_size =
      vary_fixed_buffer ? num_dups * sizeof(int) : num_dups;
  uint64_t max_buffer_size =
      vary_fixed_buffer ? coords_size * num_dups : a1_data_size * num_dups;
  for (uint64_t buffer_size = min_buffer_size; buffer_size <= max_buffer_size;
       buffer_size++) {
    // Only make the coordinate buffer change, the rest of the buffers
    // are big enough for everything.
    std::vector<int> coords_r(vary_fixed_buffer ? buffer_size : 1000, 0);
    std::string a1_data_r(vary_fixed_buffer ? 1000 : buffer_size, 0);
    std::vector<uint64_t> a1_offsets_r(1000, 0);
    std::vector<int64_t> a2_data_r(1000);
    std::vector<uint8_t> a2_validity_r(1000);
    uint64_t coords_r_size = coords_r.size() * sizeof(int32_t);
    uint64_t a1_data_r_size = a1_data_r.size();
    uint64_t a1_offsets_r_size = a1_offsets_r.size() * sizeof(uint64_t);
    uint64_t a2_data_r_size = a2_data_r.size() * sizeof(int64_t);
    uint64_t a2_validity_r_size = a2_validity_r.size();
    auto rc = read_strings(
        coords_r.data(),
        &coords_r_size,
        a1_data_r.data(),
        &a1_data_r_size,
        a1_offsets_r.data(),
        &a1_offsets_r_size,
        a2_data_r.data(),
        &a2_data_r_size,
        a2_validity_r.data(),
        &a2_validity_r_size,
        num_dups,
        config,
        &query,
        &array);
    CHECK(rc == TILEDB_OK);

    // Get result count.
    std::vector<uint64_t> counts(20, 0);

    // Keep running the query until complete.
    tiledb_query_status_t status;
    tiledb_query_get_status(ctx_, query, &status);
    for (uint64_t iter = 0; iter < 100; iter++) {
      // Aggregate results.
      uint64_t num_read = coords_r_size / sizeof(int);
      CHECK(a1_offsets_r_size == num_read * sizeof(uint64_t));
      CHECK(a2_data_r_size == num_read * sizeof(int64_t));
      CHECK(a2_validity_r_size == num_read);
      for (uint64_t i = 0; i < num_read; i++) {
        counts[coords_r[i]]++;
      }

      if (status == TILEDB_COMPLETED) {
        break;
      }

      rc = tiledb_query_submit(ctx_, query);
      REQUIRE(rc == TILEDB_OK);

      tiledb_query_get_status(ctx_, query, &status);
    }

    // Validate we got every cell back.
    for (uint64_t c = 0; c < counts.size(); c++) {
      if (c > 0 && c < 15) {
        CHECK(counts[c] == num_dups);
      } else {
        CHECK(counts[c] == 0);
      }
    }

    // Clean up.
    rc = tiledb_array_close(ctx_, array);
    CHECK(rc == TILEDB_OK);
    tiledb_array_free(&array);
    tiledb_query_free(&query);
  }
  tiledb_config_free(&config);
}

TEST_CASE_METHOD(
    CSparseUnorderedWithDupsFx,
    "Sparse unordered with dups reader: Increasing dups with "
    "overlapping range",
    "[sparse-unordered-with-dups][dup-data-test][overlapping-"
    "ranges]") {
  // Create default array.
  reset_config();
  int32_t extent = GENERATE(2, 7, 5, 10, 11);
  create_default_array_1d_string(extent, extent * 2);

  std::vector<int32_t> coords = {1,  2,  3,  4,  5,  6,  7,  8,  9,  10,
                                 11, 12, 13, 14, 15, 16, 17, 18, 19, 20};
  uint64_t coords_size = coords.size() * sizeof(int32_t);
  std::vector<uint64_t> a1_offsets = {0,  1,  2,  3,  4,  5,  6,  7,  8,  9,
                                      10, 11, 12, 13, 14, 15, 16, 17, 18, 19};
  uint64_t a1_offsets_size = a1_offsets.size() * sizeof(uint64_t);
  std::string a1_data = "123456789abcdefghijk";
  uint64_t a1_data_size = a1_data.size();
  std::vector<int64_t> a2_data = {10, 11, 12, 13, 14, 15, 16, 17, 18, 19,
                                  20, 21, 22, 23, 24, 25, 26, 27, 28, 29};
  uint64_t a2_data_size = a2_data.size() * sizeof(uint64_t);
  std::vector<uint8_t> a2_validity = {1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
                                      1, 1, 1, 1, 1, 1, 1, 1, 1, 1};
  uint64_t a2_validity_size = a2_validity.size();
  // Write dups for all cells up to capacity
  uint64_t total_cells = 0;
  for (int32_t i = 0; i < extent * 2; i++) {
    write_1d_fragment_string(
        coords.data(),
        &coords_size,
        a1_offsets.data(),
        &a1_offsets_size,
        a1_data.data(),
        &a1_data_size,
        a2_data.data(),
        &a2_data_size,
        a2_validity.data(),
        &a2_validity_size);
    total_cells += coords_size / sizeof(int32_t);
  }

  // Disable merge overlapping sparse ranges.
  // Support for returning multiplicities for overlapping ranges will be
  // deprecated in a few releases. Turning off this setting allows to still
  // test that the feature functions properly until we do so. Once support is
  // fully removed for overlapping ranges, this section can be deleted.
  tiledb_config_t* config;
  tiledb_error_t* error = nullptr;
  REQUIRE(tiledb_config_alloc(&config, &error) == TILEDB_OK);
  REQUIRE(error == nullptr);

  REQUIRE(
      tiledb_config_set(
          config,
          "sm.merge_overlapping_ranges_experimental",
          "false",
          &error) == TILEDB_OK);
  REQUIRE(error == nullptr);

  REQUIRE(tiledb_ctx_alloc(config, &ctx_) == TILEDB_OK);
  REQUIRE(error == nullptr);
  REQUIRE(tiledb_vfs_alloc(ctx_, config, &vfs_) == TILEDB_OK);

  tiledb_array_t* array;
  auto st = tiledb_array_alloc(ctx_, array_name_.c_str(), &array);
  CHECK(st == TILEDB_OK);
  st = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(st == TILEDB_OK);
  tiledb_query_t* query;
  st = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  CHECK(st == TILEDB_OK);

  tiledb_subarray_t* subarray;
  st = tiledb_subarray_alloc(ctx_, array, &subarray);
  CHECK(st == TILEDB_OK);
  int64_t start = 1;
  int64_t end = 20;
  int64_t end_2 = 10;
  // The first half of the array will be read twice, including dups.
  total_cells += total_cells / 2;
  st = tiledb_subarray_add_range(ctx_, subarray, 0, &start, &end, nullptr);
  CHECK(st == TILEDB_OK);
  st = tiledb_subarray_add_range(ctx_, subarray, 0, &start, &end_2, nullptr);
  CHECK(st == TILEDB_OK);
  st = tiledb_subarray_set_config(ctx_, subarray, config);
  CHECK(st == TILEDB_OK);
  st = tiledb_query_set_subarray_t(ctx_, query, subarray);
  CHECK(st == TILEDB_OK);
  st = tiledb_query_set_layout(ctx_, query, TILEDB_UNORDERED);
  CHECK(st == TILEDB_OK);

  // Submit incomplete reads with varying number of duplicates.
  tiledb_query_status_t status;
  uint64_t total_cells_r = 0;
  // Minimum buffer size should hold number of dups in a single cell.
  uint64_t buffer_size = 2;
  do {
    std::vector<int32_t> coords_r(buffer_size, 0);
    uint64_t coords_r_size = coords_r.size() * sizeof(int32_t);
    std::vector<uint64_t> offsets_r(buffer_size, 0);
    uint64_t offsets_r_size = offsets_r.size() * sizeof(uint64_t);
    std::string data_r(buffer_size++, 0);
    uint64_t data_r_size = data_r.size();

    st = tiledb_query_set_data_buffer(
        ctx_, query, "d", coords_r.data(), &coords_r_size);
    CHECK(st == TILEDB_OK);
    st = tiledb_query_set_offsets_buffer(
        ctx_, query, "a1", offsets_r.data(), &offsets_r_size);
    CHECK(st == TILEDB_OK);
    st = tiledb_query_set_data_buffer(
        ctx_, query, "a1", data_r.data(), &data_r_size);
    CHECK(st == TILEDB_OK);
    st = tiledb_query_submit(ctx_, query);
    CHECK(st == TILEDB_OK);

    st = tiledb_query_get_status(ctx_, query, &status);
    CHECK(st == TILEDB_OK);
    uint64_t cells_r = offsets_r_size / sizeof(uint64_t);
    total_cells_r += cells_r;
  } while (status == TILEDB_INCOMPLETE);
  CHECK(total_cells_r == total_cells);
  CHECK(status == TILEDB_COMPLETED);

  tiledb_array_free(&array);
  tiledb_query_free(&query);
  tiledb_subarray_free(&subarray);
  tiledb_config_free(&config);
}
