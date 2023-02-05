/**
 * @file unit-dense-reader.cc
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
 * Tests for the dense reader.
 */

#include "test/support/src/helpers.h"
#include "tiledb/common/common.h"
#include "tiledb/sm/c_api/tiledb.h"
#include "tiledb/sm/c_api/tiledb_struct_def.h"
#include "tiledb/sm/query/query_buffer.h"
#include "tiledb/sm/query/readers/dense_reader.h"

#ifdef _WIN32
#include "tiledb/sm/filesystem/win.h"
#else
#include "tiledb/sm/filesystem/posix.h"
#endif

#include <test/support/tdb_catch.h>

using namespace tiledb::sm;
using namespace tiledb::test;

/* ********************************* */
/*         STRUCT DEFINITION         */
/* ********************************* */

struct CDenseFx {
  tiledb_ctx_t* ctx_ = nullptr;
  tiledb_vfs_t* vfs_ = nullptr;
  std::string temp_dir_;
  std::string array_name_;
  const char* ARRAY_NAME = "test_dense_reader";
  std::string total_budget_;
  std::string tile_memory_budget_;

  void create_default_array_1d();
  void create_default_array_1d_string();
  void write_1d_fragment(int* subarray, int* data, uint64_t* data_size);
  void write_1d_fragment_strings(
      int* subarray,
      char* data,
      uint64_t* data_size,
      uint64_t* offsets,
      uint64_t* offsets_size);
  void read(int* subarray, int* data, uint64_t* data_size);
  void read_strings(
      int* subarray,
      char* data,
      uint64_t* data_size,
      uint64_t* offsets,
      uint64_t* offsets_size);
  void reset_config();
  void update_config();

  CDenseFx();
  ~CDenseFx();
};

CDenseFx::CDenseFx() {
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

CDenseFx::~CDenseFx() {
  remove_dir(temp_dir_, ctx_, vfs_);
  tiledb_ctx_free(&ctx_);
  tiledb_vfs_free(&vfs_);
}

void CDenseFx::reset_config() {
  total_budget_ = "1048576";
  tile_memory_budget_ = "1024";
  update_config();
}

void CDenseFx::update_config() {
  if (ctx_ != nullptr) {
    tiledb_ctx_free(&ctx_);
  }

  if (vfs_ != nullptr) {
    tiledb_vfs_free(&vfs_);
  }

  tiledb_config_t* config;
  tiledb_error_t* error = nullptr;
  REQUIRE(tiledb_config_alloc(&config, &error) == TILEDB_OK);
  REQUIRE(error == nullptr);

  REQUIRE(
      tiledb_config_set(
          config, "sm.mem.total_budget", total_budget_.c_str(), &error) ==
      TILEDB_OK);
  REQUIRE(error == nullptr);

  REQUIRE(
      tiledb_config_set(
          config,
          "sm.mem.tile_memory_budget",
          tile_memory_budget_.c_str(),
          &error) == TILEDB_OK);
  REQUIRE(error == nullptr);

  REQUIRE(tiledb_ctx_alloc(config, &ctx_) == TILEDB_OK);
  REQUIRE(error == nullptr);
  REQUIRE(tiledb_vfs_alloc(ctx_, config, &vfs_) == TILEDB_OK);
  tiledb_config_free(&config);
}

void CDenseFx::create_default_array_1d() {
  int domain[] = {1, 200};
  int tile_extent = 10;
  create_array(
      ctx_,
      array_name_,
      TILEDB_DENSE,
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
      10);  // allows dups.
}

void CDenseFx::create_default_array_1d_string() {
  int domain[] = {1, 200};
  int tile_extent = 10;
  create_array(
      ctx_,
      array_name_,
      TILEDB_DENSE,
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
      10);  // allows dups.
}

void CDenseFx::write_1d_fragment(
    int* subarray, int* data, uint64_t* data_size) {
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
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_ROW_MAJOR);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, query, "a", data, data_size);
  REQUIRE(rc == TILEDB_OK);

  // Set subarray.
  rc = tiledb_query_set_subarray(ctx_, query, subarray);
  CHECK(rc == TILEDB_OK);

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

void CDenseFx::write_1d_fragment_strings(
    int* subarray,
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
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_ROW_MAJOR);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, query, "a", data, data_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_offsets_buffer(ctx_, query, "a", offsets, offsets_size);
  REQUIRE(rc == TILEDB_OK);

  // Set subarray.
  rc = tiledb_query_set_subarray(ctx_, query, subarray);
  CHECK(rc == TILEDB_OK);

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

void CDenseFx::read(int* subarray, int* data, uint64_t* data_size) {
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

  // Set subarray.
  rc = tiledb_query_set_subarray(ctx_, query, subarray);
  CHECK(rc == TILEDB_OK);

  rc = tiledb_query_set_layout(ctx_, query, TILEDB_ROW_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, query, "a", data, data_size);
  CHECK(rc == TILEDB_OK);

  // Submit query.
  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_OK);

  // Check the internal loop count against expected value.
  auto stats = ((DenseReader*)query->query_->strategy())->stats();
  REQUIRE(stats != nullptr);
  auto counters = stats->counters();
  REQUIRE(counters != nullptr);
  auto loop_num =
      counters->find("Context.StorageManager.Query.Reader.internal_loop_num");
  CHECK(2 == loop_num->second);

  // Clean up.
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
  tiledb_query_free(&query);
}

void CDenseFx::read_strings(
    int* subarray,
    char* data,
    uint64_t* data_size,
    uint64_t* offsets,
    uint64_t* offsets_size) {
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

  // Set subarray.
  rc = tiledb_query_set_subarray(ctx_, query, subarray);
  CHECK(rc == TILEDB_OK);

  rc = tiledb_query_set_layout(ctx_, query, TILEDB_ROW_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, query, "a", data, data_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_offsets_buffer(ctx_, query, "a", offsets, offsets_size);
  CHECK(rc == TILEDB_OK);

  // Submit query.
  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_OK);

  // Clean up.
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
  tiledb_query_free(&query);
}

/* ********************************* */
/*                TESTS              */
/* ********************************* */

TEST_CASE_METHOD(
    CDenseFx,
    "Dense reader: tile budget exceeded, fixed attribute",
    "[dense-reader][tile-budget-exceeded][fixed]") {
  // Create default array.
  reset_config();
  create_default_array_1d();

  // Write a fragment.
  int subarray[] = {1, 20};
  int data[] = {1,  2,  3,  4,  5,  6,  7,  8,  9,  10,
                11, 12, 13, 14, 15, 16, 17, 18, 19, 20};
  uint64_t data_size = sizeof(data);
  write_1d_fragment(subarray, data, &data_size);

  // Each tile is 40 bytes, this will only allow to load one.
  tile_memory_budget_ = "50";
  update_config();

  // Try to read.
  int data_r[20] = {0};
  uint64_t data_r_size = sizeof(data_r);
  read(subarray, data_r, &data_r_size);

  CHECK(data_r_size == data_size);
  CHECK(!std::memcmp(data, data_r, data_size));
}

TEST_CASE_METHOD(
    CDenseFx,
    "Dense reader: tile budget exceeded, var attribute",
    "[dense-reader][tile-budget-exceeded][var]") {
  // Create default array.
  reset_config();
  create_default_array_1d_string();

  // Write a fragment.
  int subarray[] = {1, 20};
  char data[] = "1234567891011121314151617181920";
  uint64_t data_size = sizeof(data) - 1;
  uint64_t offsets[] = {0,  1,  2,  3,  4,  5,  6,  7,  8,  9,
                        11, 13, 15, 17, 19, 21, 23, 25, 27, 29};
  uint64_t offsets_size = sizeof(offsets);
  write_1d_fragment_strings(subarray, data, &data_size, offsets, &offsets_size);

  // Each tile is 40 bytes, this will only allow to load one.
  tile_memory_budget_ = "50";
  update_config();

  // Try to read.
  char data_r[100] = {0};
  uint64_t data_r_size = sizeof(data_r);
  uint64_t offsets_r[20] = {0};
  uint64_t offsets_r_size = sizeof(offsets_r);
  read_strings(subarray, data_r, &data_r_size, offsets_r, &offsets_r_size);

  CHECK(data_r_size == data_size);
  CHECK(!std::memcmp(data, data_r, data_size));
  CHECK(offsets_r_size == offsets_size);
  CHECK(!std::memcmp(offsets, offsets_r, offsets_size));
}

TEST_CASE_METHOD(
    CDenseFx,
    "Dense reader: tile budget exceeded, var attribute, unaligned read",
    "[dense-reader][tile-budget-exceeded][var-unaligned]") {
  // Create default array.
  reset_config();
  create_default_array_1d_string();

  // Write a fragment.
  int subarray[] = {1, 20};
  char data[] = "1234567891011121314151617181920";
  uint64_t data_size = sizeof(data) - 1;
  uint64_t offsets[] = {0,  1,  2,  3,  4,  5,  6,  7,  8,  9,
                        11, 13, 15, 17, 19, 21, 23, 25, 27, 29};
  uint64_t offsets_size = sizeof(offsets);
  write_1d_fragment_strings(subarray, data, &data_size, offsets, &offsets_size);

  // Each tile is 40 bytes, this will only allow to load one.
  tile_memory_budget_ = "50";
  update_config();

  // Try to read.
  int subarray_r[] = {6, 15};
  char data_r[100] = {0};
  uint64_t data_r_size = sizeof(data_r);
  uint64_t offsets_r[10] = {0};
  uint64_t offsets_r_size = sizeof(offsets_r);
  read_strings(subarray_r, data_r, &data_r_size, offsets_r, &offsets_r_size);

  char c_data[] = "6789101112131415";
  uint64_t c_data_size = sizeof(c_data) - 1;
  uint64_t c_offsets[] = {0, 1, 2, 3, 4, 6, 8, 10, 12, 14};
  uint64_t c_offsets_size = sizeof(c_offsets);
  CHECK(data_r_size == c_data_size);
  CHECK(!std::memcmp(c_data, data_r, c_data_size));
  CHECK(offsets_r_size == c_offsets_size);
  CHECK(!std::memcmp(c_offsets, offsets_r, c_offsets_size));
}