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
  std::string tile_upper_memory_limit_;

  void create_default_array_1d();
  void evolve_default_array_1d();
  void create_default_array_1d_string();
  void evolve_default_array_1d_string();
  void create_default_array_1d_fixed_string();
  void write_1d_fragment(int* subarray, int* data, uint64_t* data_size);
  void write_1d_fragment_strings(
      int* subarray,
      char* data,
      uint64_t* data_size,
      uint64_t* offsets,
      uint64_t* offsets_size);
  void write_1d_fragment_fixed_strings(
      int* subarray,
      int* a1_data,
      uint64_t* a1_data_size,
      char* a2_data,
      uint64_t* a2_data_size,
      uint64_t* a2_offsets,
      uint64_t* a2_offsets_size);
  void read(
      int* subarray,
      int* data,
      uint64_t* data_size,
      int qc_index = 0,
      std::string expected_error = std::string());
  void read_evolved(
      int* subarray,
      int* data,
      uint64_t* data_size,
      int* data_b,
      uint64_t* data_b_size);
  void read_strings(
      int* subarray,
      char* data,
      uint64_t* data_size,
      uint64_t* offsets,
      uint64_t* offsets_size,
      bool add_qc = false,
      std::string expected_error = std::string());
  void read_strings_evolved(
      int* subarray,
      char* data,
      uint64_t* data_size,
      uint64_t* offsets,
      uint64_t* offsets_size,
      char* data_b,
      uint64_t* data_b_size,
      uint64_t* offsets_b,
      uint64_t* offsets_b_size);
  void read_fixed_strings(
      int* subarray,
      int* a1_data,
      uint64_t* a1_data_size,
      char* a2_data,
      uint64_t* a2_data_size,
      uint64_t* a2_offsets,
      uint64_t* a2_offsets_size,
      uint64_t expected_num_loops,
      bool add_a1_qc = false,
      bool add_a2_qc = false,
      std::string expected_error = std::string());
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
  tile_upper_memory_limit_ = "1024";
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
          "sm.mem.tile_upper_memory_limit",
          tile_upper_memory_limit_.c_str(),
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
      10);
}

void CDenseFx::evolve_default_array_1d() {
  tiledb_array_schema_evolution_t* schemaEvolution;
  auto rc = tiledb_array_schema_evolution_alloc(ctx_, &schemaEvolution);
  REQUIRE(rc == TILEDB_OK);

  // Create attribute
  tiledb_attribute_t* b;
  rc = tiledb_attribute_alloc(ctx_, "b", TILEDB_INT32, &b);
  REQUIRE(rc == TILEDB_OK);

  int fill_value = 7;
  rc = tiledb_attribute_set_fill_value(ctx_, b, &fill_value, sizeof(int));
  REQUIRE(rc == TILEDB_OK);

  // Add attribute to schema evolution.
  rc = tiledb_array_schema_evolution_add_attribute(ctx_, schemaEvolution, b);
  REQUIRE(rc == TILEDB_OK);

  // Evolve array.
  rc = tiledb_array_evolve(ctx_, array_name_.c_str(), schemaEvolution);
  REQUIRE(rc == TILEDB_OK);

  // Cleanup.
  tiledb_attribute_free(&b);
  tiledb_array_schema_evolution_free(&schemaEvolution);
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
      10);
}

void CDenseFx::evolve_default_array_1d_string() {
  tiledb_array_schema_evolution_t* schemaEvolution;
  auto rc = tiledb_array_schema_evolution_alloc(ctx_, &schemaEvolution);
  REQUIRE(rc == TILEDB_OK);

  // Create attribute
  tiledb_attribute_t* b;
  rc = tiledb_attribute_alloc(ctx_, "b", TILEDB_STRING_ASCII, &b);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_attribute_set_cell_val_num(ctx_, b, TILEDB_VAR_NUM);
  REQUIRE(rc == TILEDB_OK);

  char fill_value = '7';
  rc = tiledb_attribute_set_fill_value(ctx_, b, &fill_value, 1);
  REQUIRE(rc == TILEDB_OK);

  // Add attribute to schema evolution.
  rc = tiledb_array_schema_evolution_add_attribute(ctx_, schemaEvolution, b);
  REQUIRE(rc == TILEDB_OK);

  // Evolve array.
  rc = tiledb_array_evolve(ctx_, array_name_.c_str(), schemaEvolution);
  REQUIRE(rc == TILEDB_OK);

  // Cleanup.
  tiledb_attribute_free(&b);
  tiledb_array_schema_evolution_free(&schemaEvolution);
}

void CDenseFx::create_default_array_1d_fixed_string() {
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
      {"a1", "a2"},
      {TILEDB_INT32, TILEDB_STRING_ASCII},
      {1, TILEDB_VAR_NUM},
      {tiledb::test::Compressor(TILEDB_FILTER_NONE, -1),
       tiledb::test::Compressor(TILEDB_FILTER_NONE, -1)},
      TILEDB_ROW_MAJOR,
      TILEDB_ROW_MAJOR,
      10);
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
  tiledb_subarray_t* sub;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_ROW_MAJOR);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, query, "a", data, data_size);
  REQUIRE(rc == TILEDB_OK);

  // Set subarray.
  rc = tiledb_subarray_alloc(ctx_, array, &sub);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_subarray_set_subarray(ctx_, sub, subarray);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray_t(ctx_, query, sub);
  REQUIRE(rc == TILEDB_OK);
  tiledb_subarray_free(&sub);

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
  tiledb_subarray_t* sub;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_ROW_MAJOR);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, query, "a", data, data_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_offsets_buffer(ctx_, query, "a", offsets, offsets_size);
  REQUIRE(rc == TILEDB_OK);

  // Set subarray.
  rc = tiledb_subarray_alloc(ctx_, array, &sub);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_subarray_set_subarray(ctx_, sub, subarray);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray_t(ctx_, query, sub);
  REQUIRE(rc == TILEDB_OK);
  tiledb_subarray_free(&sub);

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

void CDenseFx::write_1d_fragment_fixed_strings(
    int* subarray,
    int* a1_data,
    uint64_t* a1_data_size,
    char* a2_data,
    uint64_t* a2_data_size,
    uint64_t* a2_offsets,
    uint64_t* a2_offsets_size) {
  // Open array for writing.
  tiledb_array_t* array;
  auto rc = tiledb_array_alloc(ctx_, array_name_.c_str(), &array);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  REQUIRE(rc == TILEDB_OK);

  // Create the query.
  tiledb_query_t* query;
  tiledb_subarray_t* sub;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_ROW_MAJOR);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, query, "a1", a1_data, a1_data_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, query, "a2", a2_data, a2_data_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_offsets_buffer(
      ctx_, query, "a2", a2_offsets, a2_offsets_size);
  REQUIRE(rc == TILEDB_OK);

  // Set subarray.
  rc = tiledb_subarray_alloc(ctx_, array, &sub);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_subarray_set_subarray(ctx_, sub, subarray);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray_t(ctx_, query, sub);
  REQUIRE(rc == TILEDB_OK);
  tiledb_subarray_free(&sub);

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

void CDenseFx::read(
    int* subarray,
    int* data,
    uint64_t* data_size,
    int qc_index,
    std::string expected_error) {
  // Open array for reading.
  tiledb_array_t* array;
  auto rc = tiledb_array_alloc(ctx_, array_name_.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Create query.
  tiledb_query_t* query;
  tiledb_subarray_t* sub;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);

  // Set subarray.
  rc = tiledb_subarray_alloc(ctx_, array, &sub);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_set_subarray(ctx_, sub, subarray);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray_t(ctx_, query, sub);
  CHECK(rc == TILEDB_OK);
  tiledb_subarray_free(&sub);

  rc = tiledb_query_set_layout(ctx_, query, TILEDB_ROW_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, query, "a", data, data_size);
  CHECK(rc == TILEDB_OK);

  if (qc_index != 0) {
    tiledb_query_condition_t* qc;
    rc = tiledb_query_condition_alloc(ctx_, &qc);
    CHECK(rc == TILEDB_OK);

    if (qc_index == 1) {
      // Test with TILEDB_LT query condition.
      int32_t val = 10000;
      rc = tiledb_query_condition_init(
          ctx_, qc, "a", &val, sizeof(int32_t), TILEDB_LT);
      CHECK(rc == TILEDB_OK);
      rc = tiledb_query_set_condition(ctx_, query, qc);
      CHECK(rc == TILEDB_OK);
    } else if (qc_index == 2) {
      // Test with TILEDB_NOT query condition.
      int32_t val = 10;
      rc = tiledb_query_condition_init(
          ctx_, qc, "a", &val, sizeof(int32_t), TILEDB_GT);
      CHECK(rc == TILEDB_OK);
      tiledb_query_condition_t* qc_not;
      rc = tiledb_query_condition_alloc(ctx_, &qc_not);
      rc = tiledb_query_condition_negate(ctx_, qc, &qc_not);
      rc = tiledb_query_set_condition(ctx_, query, qc_not);
      CHECK(rc == TILEDB_OK);
      tiledb_query_condition_free(&qc_not);
    }

    tiledb_query_condition_free(&qc);
  }

  // Submit query.
  rc = tiledb_query_submit(ctx_, query);

  if (expected_error.length() == 0) {
    CHECK(rc == TILEDB_OK);
  } else {
    CHECK(rc == TILEDB_ERR);
    tiledb_error_t* err = NULL;
    tiledb_ctx_get_last_error(ctx_, &err);

    // Retrieve the error message by invoking `tiledb_error_message`.
    const char* msg;
    tiledb_error_message(err, &msg);
    CHECK(expected_error == std::string(msg));
  }

  // Check the internal loop count against expected value.
  if (rc == TILEDB_OK) {
    auto stats = ((DenseReader*)query->query_->strategy())->stats();
    REQUIRE(stats != nullptr);
    auto counters = stats->counters();
    REQUIRE(counters != nullptr);
    auto loop_num =
        counters->find("Context.StorageManager.Query.Reader.internal_loop_num");
    CHECK(2 == loop_num->second);
  }

  // Clean up.
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
  tiledb_query_free(&query);
}

void CDenseFx::read_evolved(
    int* subarray,
    int* data,
    uint64_t* data_size,
    int* data_b,
    uint64_t* data_b_size) {
  // Open array for reading.
  tiledb_array_t* array;
  auto rc = tiledb_array_alloc(ctx_, array_name_.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Create query.
  tiledb_query_t* query;
  tiledb_subarray_t* sub;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);

  // Set subarray.
  rc = tiledb_subarray_alloc(ctx_, array, &sub);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_set_subarray(ctx_, sub, subarray);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray_t(ctx_, query, sub);
  CHECK(rc == TILEDB_OK);
  tiledb_subarray_free(&sub);

  rc = tiledb_query_set_layout(ctx_, query, TILEDB_ROW_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, query, "a", data, data_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, query, "b", data_b, data_b_size);
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

void CDenseFx::read_strings(
    int* subarray,
    char* data,
    uint64_t* data_size,
    uint64_t* offsets,
    uint64_t* offsets_size,
    bool add_qc,
    std::string expected_error) {
  // Open array for reading.
  tiledb_array_t* array;
  auto rc = tiledb_array_alloc(ctx_, array_name_.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Create query.
  tiledb_query_t* query;
  tiledb_subarray_t* sub;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);

  // Set subarray.
  rc = tiledb_subarray_alloc(ctx_, array, &sub);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_set_subarray(ctx_, sub, subarray);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray_t(ctx_, query, sub);
  CHECK(rc == TILEDB_OK);
  tiledb_subarray_free(&sub);

  rc = tiledb_query_set_layout(ctx_, query, TILEDB_ROW_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, query, "a", data, data_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_offsets_buffer(ctx_, query, "a", offsets, offsets_size);
  CHECK(rc == TILEDB_OK);

  if (add_qc) {
    tiledb_query_condition_t* qc;
    rc = tiledb_query_condition_alloc(ctx_, &qc);
    CHECK(rc == TILEDB_OK);
    std::string val("ZZZZ");
    rc = tiledb_query_condition_init(
        ctx_, qc, "a", val.data(), val.size(), TILEDB_LT);
    CHECK(rc == TILEDB_OK);
    rc = tiledb_query_set_condition(ctx_, query, qc);
    CHECK(rc == TILEDB_OK);
    tiledb_query_condition_free(&qc);
  }

  // Submit query.
  rc = tiledb_query_submit(ctx_, query);
  if (expected_error.length() == 0) {
    CHECK(rc == TILEDB_OK);
  } else {
    CHECK(rc == TILEDB_ERR);
    tiledb_error_t* err = NULL;
    tiledb_ctx_get_last_error(ctx_, &err);

    // Retrieve the error message by invoking `tiledb_error_message`.
    const char* msg;
    tiledb_error_message(err, &msg);
    CHECK(expected_error == std::string(msg));
  }

  if (rc == TILEDB_OK) {
    // Check the internal loop count against expected value.
    auto stats = ((DenseReader*)query->query_->strategy())->stats();
    REQUIRE(stats != nullptr);
    auto counters = stats->counters();
    REQUIRE(counters != nullptr);
    auto loop_num =
        counters->find("Context.StorageManager.Query.Reader.internal_loop_num");
    CHECK(2 == loop_num->second);
  }

  // Clean up.
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
  tiledb_query_free(&query);
}

void CDenseFx::read_strings_evolved(
    int* subarray,
    char* data,
    uint64_t* data_size,
    uint64_t* offsets,
    uint64_t* offsets_size,
    char* data_b,
    uint64_t* data_b_size,
    uint64_t* offsets_b,
    uint64_t* offsets_b_size) {
  // Open array for reading.
  tiledb_array_t* array;
  auto rc = tiledb_array_alloc(ctx_, array_name_.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Create query.
  tiledb_query_t* query;
  tiledb_subarray_t* sub;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);

  // Set subarray.
  rc = tiledb_subarray_alloc(ctx_, array, &sub);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_set_subarray(ctx_, sub, subarray);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray_t(ctx_, query, sub);
  CHECK(rc == TILEDB_OK);
  tiledb_subarray_free(&sub);

  rc = tiledb_query_set_layout(ctx_, query, TILEDB_ROW_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, query, "a", data, data_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_offsets_buffer(ctx_, query, "a", offsets, offsets_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, query, "b", data_b, data_b_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_offsets_buffer(
      ctx_, query, "b", offsets_b, offsets_b_size);
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

void CDenseFx::read_fixed_strings(
    int* subarray,
    int* a1_data,
    uint64_t* a1_data_size,
    char* a2_data,
    uint64_t* a2_data_size,
    uint64_t* a2_offsets,
    uint64_t* a2_offsets_size,
    uint64_t expected_num_loops,
    bool add_a1_qc,
    bool add_a2_qc,
    std::string expected_error) {
  // Open array for reading.
  tiledb_array_t* array;
  auto rc = tiledb_array_alloc(ctx_, array_name_.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Create query.
  tiledb_query_t* query;
  tiledb_subarray_t* sub;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);

  // Set subarray.
  rc = tiledb_subarray_alloc(ctx_, array, &sub);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_set_subarray(ctx_, sub, subarray);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray_t(ctx_, query, sub);
  CHECK(rc == TILEDB_OK);
  tiledb_subarray_free(&sub);

  rc = tiledb_query_set_layout(ctx_, query, TILEDB_ROW_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, query, "a1", a1_data, a1_data_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, query, "a2", a2_data, a2_data_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_offsets_buffer(
      ctx_, query, "a2", a2_offsets, a2_offsets_size);
  CHECK(rc == TILEDB_OK);

  tiledb_query_condition_t* qc1 = nullptr;
  if (add_a1_qc) {
    rc = tiledb_query_condition_alloc(ctx_, &qc1);
    CHECK(rc == TILEDB_OK);
    int32_t val = 10000;
    rc = tiledb_query_condition_init(
        ctx_, qc1, "a1", &val, sizeof(int32_t), TILEDB_LT);
    CHECK(rc == TILEDB_OK);
  }

  tiledb_query_condition_t* qc2 = nullptr;
  if (add_a2_qc) {
    rc = tiledb_query_condition_alloc(ctx_, &qc2);
    CHECK(rc == TILEDB_OK);
    std::string val("ZZZZ");
    rc = tiledb_query_condition_init(
        ctx_, qc2, "a2", val.data(), val.size(), TILEDB_LT);
    CHECK(rc == TILEDB_OK);
    rc = tiledb_query_set_condition(ctx_, query, qc2);
    CHECK(rc == TILEDB_OK);
  }

  if (add_a1_qc && add_a2_qc) {
    tiledb_query_condition_t* qc;
    tiledb_query_condition_alloc(ctx_, &qc);
    tiledb_query_condition_combine(ctx_, qc1, qc2, TILEDB_AND, &qc);
    rc = tiledb_query_set_condition(ctx_, query, qc);
    CHECK(rc == TILEDB_OK);
    tiledb_query_condition_free(&qc1);
  } else if (add_a1_qc) {
    rc = tiledb_query_set_condition(ctx_, query, qc1);
    CHECK(rc == TILEDB_OK);
  } else if (add_a2_qc) {
    rc = tiledb_query_set_condition(ctx_, query, qc2);
    CHECK(rc == TILEDB_OK);
  }

  if (qc1 != nullptr) {
    tiledb_query_condition_free(&qc1);
  }

  if (qc2 != nullptr) {
    tiledb_query_condition_free(&qc2);
  }

  // Submit query.
  rc = tiledb_query_submit(ctx_, query);
  if (expected_error.length() == 0) {
    CHECK(rc == TILEDB_OK);
  } else {
    CHECK(rc == TILEDB_ERR);

    if (rc == TILEDB_ERR) {
      tiledb_error_t* err = NULL;
      tiledb_ctx_get_last_error(ctx_, &err);

      // Retrieve the error message by invoking `tiledb_error_message`.
      const char* msg;
      tiledb_error_message(err, &msg);
      CHECK(expected_error == std::string(msg));
    }
  }

  if (rc == TILEDB_OK) {
    // Check the internal loop count against expected value.
    auto stats = ((DenseReader*)query->query_->strategy())->stats();
    REQUIRE(stats != nullptr);
    auto counters = stats->counters();
    REQUIRE(counters != nullptr);
    auto loop_num =
        counters->find("Context.StorageManager.Query.Reader.internal_loop_num");
    CHECK(expected_num_loops == loop_num->second);
  }

  // Clean up.
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
  tiledb_query_free(&query);
}

/* ********************************* */
/*                TESTS              */
/* ********************************* */

#define NUM_CELLS 20

TEST_CASE_METHOD(
    CDenseFx,
    "Dense reader: budget too small",
    "[dense-reader][budget-too-small]") {
  // Create default array.
  reset_config();
  create_default_array_1d();

  // Write a fragment.
  int subarray[] = {1, NUM_CELLS};
  std::vector<int> data(NUM_CELLS);
  std::iota(data.begin(), data.end(), 1);
  uint64_t data_size = data.size() * sizeof(int);
  write_1d_fragment(subarray, data.data(), &data_size);

  // Each tile is 40 bytes, this will only allow to load one.
  total_budget_ = "50";
  tile_upper_memory_limit_ = "50";
  update_config();

  // Try to read.
  int data_r[NUM_CELLS] = {0};
  uint64_t data_r_size = sizeof(data_r);
  read(
      subarray,
      data_r,
      &data_r_size,
      0,
      "DenseReader: Memory budget is too small to open array");
}

TEST_CASE_METHOD(
    CDenseFx,
    "Dense reader: tile budget exceeded, fixed attribute",
    "[dense-reader][tile-budget-exceeded][fixed]") {
  int qc_index = GENERATE(0, 1, 2);

  // Create default array.
  reset_config();
  create_default_array_1d();

  // Write a fragment.
  int subarray[] = {1, NUM_CELLS};
  std::vector<int> data(NUM_CELLS);
  std::iota(data.begin(), data.end(), 1);
  uint64_t data_size = data.size() * sizeof(int);
  write_1d_fragment(subarray, data.data(), &data_size);

  // Each tile is 40 bytes, this will only allow to load one.
  tile_upper_memory_limit_ = "50";
  update_config();

  // Try to read.
  int data_r[NUM_CELLS] = {0};
  uint64_t data_r_size = sizeof(data_r);
  read(subarray, data_r, &data_r_size, qc_index);

  CHECK(data_r_size == data_size);
  if (qc_index == 2) {
    // TILEDB_NOT query condition returns first half of the data set.
    CHECK(!std::memcmp(data.data(), data_r, data_size / 2));
  } else {
    // TILEDB_LT or no query condition returns the full data set.
    CHECK(!std::memcmp(data.data(), data_r, data_size));
  }
}

TEST_CASE_METHOD(
    CDenseFx,
    "Dense reader: budget exceeded, fixed attribute",
    "[dense-reader][budget-too-small][fixed]") {
  int use_qc = GENERATE(0, 1, 2);

  // Create default array.
  reset_config();
  create_default_array_1d();

  // Write a fragment.
  int subarray[] = {1, NUM_CELLS};
  std::vector<int> data(NUM_CELLS);
  std::iota(data.begin(), data.end(), 1);
  uint64_t data_size = data.size() * sizeof(int);
  write_1d_fragment(subarray, data.data(), &data_size);

  total_budget_ = "420";
  tile_upper_memory_limit_ = "50";
  update_config();

  std::string error_expected =
      use_qc ?
          "DenseReader: Cannot process a single tile because of query "
          "condition, increase memory budget" :
          "DenseReader: Cannot process a single tile, increase memory budget";

  // Try to read.
  int data_r[20] = {0};
  uint64_t data_r_size = sizeof(data_r);
  read(subarray, data_r, &data_r_size, use_qc, error_expected);
}

TEST_CASE_METHOD(
    CDenseFx,
    "Dense reader: tile budget exceeded, var attribute",
    "[dense-reader][tile-budget-exceeded][var]") {
  bool use_qc = GENERATE(true, false);

  // Create default array.
  reset_config();
  create_default_array_1d_string();

  // Write a fragment.
  int subarray[] = {1, NUM_CELLS};
  std::string data;
  std::vector<uint64_t> offsets(NUM_CELLS);
  for (uint64_t i = 0; i < NUM_CELLS; i++) {
    offsets[i] = data.size();
    data += std::to_string(i + 1);
  }
  uint64_t data_size = data.size();
  uint64_t offsets_size = offsets.size() * sizeof(uint64_t);
  write_1d_fragment_strings(
      subarray, data.data(), &data_size, offsets.data(), &offsets_size);

  // Each tiles are 91 and 100 bytes respectively, this will only allow to
  // load one as the budget is split across two potential reads.
  tile_upper_memory_limit_ = "210";
  update_config();

  // Try to read.
  char data_r[NUM_CELLS * 2] = {0};
  uint64_t data_r_size = sizeof(data_r);
  uint64_t offsets_r[NUM_CELLS] = {0};
  uint64_t offsets_r_size = sizeof(offsets_r);
  read_strings(
      subarray, data_r, &data_r_size, offsets_r, &offsets_r_size, use_qc);

  CHECK(data_r_size == data_size);
  CHECK(!std::memcmp(data.data(), data_r, data_size));
  CHECK(offsets_r_size == offsets_size);
  CHECK(!std::memcmp(offsets.data(), offsets_r, offsets_size));
}

TEST_CASE_METHOD(
    CDenseFx,
    "Dense reader: budget exceeded, var attribute",
    "[dense-reader][budget-too-small][var]") {
  bool use_qc = GENERATE(true, false);

  // Create default array.
  reset_config();
  create_default_array_1d_string();

  // Write a fragment.
  int subarray[] = {1, NUM_CELLS};
  std::string data;
  std::vector<uint64_t> offsets(NUM_CELLS);
  for (uint64_t i = 0; i < NUM_CELLS; i++) {
    offsets[i] = data.size();
    data += std::to_string(i + 1);
  }
  uint64_t data_size = data.size();
  uint64_t offsets_size = offsets.size() * sizeof(uint64_t);
  write_1d_fragment_strings(
      subarray, data.data(), &data_size, offsets.data(), &offsets_size);

  // Each tiles are 91 and 100 bytes respectively, this will only allow to
  // load one as the budget is split across two potential reads.
  total_budget_ = "460";
  tile_upper_memory_limit_ = "210";
  update_config();

  std::string error_expected =
      use_qc ?
          "DenseReader: Cannot process a single tile because of query "
          "condition, increase memory budget" :
          "DenseReader: Cannot process a single tile, increase memory budget";

  // Try to read.
  char data_r[NUM_CELLS * 2] = {0};
  uint64_t data_r_size = sizeof(data_r);
  uint64_t offsets_r[10] = {0};
  uint64_t offsets_r_size = sizeof(offsets_r);
  read_strings(
      subarray,
      data_r,
      &data_r_size,
      offsets_r,
      &offsets_r_size,
      use_qc,
      error_expected);
}

TEST_CASE_METHOD(
    CDenseFx,
    "Dense reader: tile budget exceeded, var attribute, unaligned read",
    "[dense-reader][tile-budget-exceeded][var-unaligned]") {
  // Create default array.
  reset_config();
  create_default_array_1d_string();

  // Write a fragment.
  int subarray[] = {1, NUM_CELLS};
  std::string data;
  std::vector<uint64_t> offsets(NUM_CELLS);
  for (uint64_t i = 0; i < NUM_CELLS; i++) {
    offsets[i] = data.size();
    data += std::to_string(i + 1);
  }
  uint64_t data_size = data.size();
  uint64_t offsets_size = offsets.size() * sizeof(uint64_t);
  write_1d_fragment_strings(
      subarray, data.data(), &data_size, offsets.data(), &offsets_size);

  // Each tile is 40 bytes, this will only allow to load one.
  tile_upper_memory_limit_ = "100";
  update_config();

  // Try to read.
  int subarray_r[] = {6, 15};
  char data_r[NUM_CELLS * 2] = {0};
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

TEST_CASE_METHOD(
    CDenseFx,
    "Dense reader: tile budget exceeded, fixed/var attribute",
    "[dense-reader][tile-budget-exceeded][fixed-var]") {
  // Create default array.
  reset_config();
  create_default_array_1d_fixed_string();

  // Write a fragment.
  int subarray[] = {1, NUM_CELLS};
  std::vector<int> a1_data(NUM_CELLS);
  std::iota(a1_data.begin(), a1_data.end(), 1);
  uint64_t a1_data_size = a1_data.size() * sizeof(int);
  std::string a2_data;
  std::vector<uint64_t> a2_offsets(NUM_CELLS);
  for (uint64_t i = 0; i < NUM_CELLS; i++) {
    a2_offsets[i] = a2_data.size();
    a2_data += std::to_string(i + 1);
  }
  uint64_t a2_data_size = a2_data.size();
  uint64_t a2_offsets_size = a2_offsets.size() * sizeof(uint64_t);
  write_1d_fragment_fixed_strings(
      subarray,
      a1_data.data(),
      &a1_data_size,
      a2_data.data(),
      &a2_data_size,
      a2_offsets.data(),
      &a2_offsets_size);

  // Each var tiles are 91 and 100 bytes respectively, this will only allow to
  // load one as the budget is split across two potential reads. Fixed tiles are
  // both 40 so they both fit in the budget.
  total_budget_ = "2500";
  tile_upper_memory_limit_ = "200";
  update_config();

  // Try to read.
  int a1_data_r[NUM_CELLS] = {0};
  uint64_t a1_data_r_size = sizeof(a1_data_r);
  char a2_data_r[NUM_CELLS * 2] = {0};
  uint64_t a2_data_r_size = sizeof(a2_data_r);
  uint64_t a2_offsets_r[NUM_CELLS] = {0};
  uint64_t a2_offsets_r_size = sizeof(a2_offsets_r);
  read_fixed_strings(
      subarray,
      a1_data_r,
      &a1_data_r_size,
      a2_data_r,
      &a2_data_r_size,
      a2_offsets_r,
      &a2_offsets_r_size,
      2);

  CHECK(a1_data_r_size == a1_data_size);
  CHECK(!std::memcmp(a1_data.data(), a1_data_r, a1_data_size));
  CHECK(a2_data_r_size == a2_data_size);
  CHECK(!std::memcmp(a2_data.data(), a2_data_r, a2_data_size));
  CHECK(a2_offsets_r_size == a2_offsets_size);
  CHECK(!std::memcmp(a2_offsets.data(), a2_offsets_r, a2_offsets_size));

  // Now read with QC set for a1 only.
  read_fixed_strings(
      subarray,
      a1_data_r,
      &a1_data_r_size,
      a2_data_r,
      &a2_data_r_size,
      a2_offsets_r,
      &a2_offsets_r_size,
      2,
      true,
      false);

  CHECK(a1_data_r_size == a1_data_size);
  CHECK(!std::memcmp(a1_data.data(), a1_data_r, a1_data_size));
  CHECK(a2_data_r_size == a2_data_size);
  CHECK(!std::memcmp(a2_data.data(), a2_data_r, a2_data_size));
  CHECK(a2_offsets_r_size == a2_offsets_size);
  CHECK(!std::memcmp(a2_offsets.data(), a2_offsets_r, a2_offsets_size));

  // Now read with QC set for a2 only.
  read_fixed_strings(
      subarray,
      a1_data_r,
      &a1_data_r_size,
      a2_data_r,
      &a2_data_r_size,
      a2_offsets_r,
      &a2_offsets_r_size,
      2,
      false,
      true);

  CHECK(a1_data_r_size == a1_data_size);
  CHECK(!std::memcmp(a1_data.data(), a1_data_r, a1_data_size));
  CHECK(a2_data_r_size == a2_data_size);
  CHECK(!std::memcmp(a2_data.data(), a2_data_r, a2_data_size));
  CHECK(a2_offsets_r_size == a2_offsets_size);
  CHECK(!std::memcmp(a2_offsets.data(), a2_offsets_r, a2_offsets_size));

  total_budget_ = "1100";
  update_config();

  // Now read with QC set for a1 and a2, should fail.
  read_fixed_strings(
      subarray,
      a1_data_r,
      &a1_data_r_size,
      a2_data_r,
      &a2_data_r_size,
      a2_offsets_r,
      &a2_offsets_r_size,
      0,
      true,
      true,
      "DenseReader: Cannot process a single tile because of query "
      "condition, increase memory budget");
}

TEST_CASE_METHOD(
    CDenseFx,
    "Dense reader: budget exceeded, fixed/var attribute",
    "[dense-reader][budget-too-small][fixed-var]") {
  // Create default array.
  reset_config();
  create_default_array_1d_fixed_string();

  // Write a fragment.
  int subarray[] = {1, NUM_CELLS};
  std::vector<int> a1_data(NUM_CELLS);
  std::iota(a1_data.begin(), a1_data.end(), 1);
  uint64_t a1_data_size = a1_data.size() * sizeof(int);
  std::string a2_data;
  std::vector<uint64_t> a2_offsets(NUM_CELLS);
  for (uint64_t i = 0; i < NUM_CELLS; i++) {
    a2_offsets[i] = a2_data.size();
    a2_data += std::to_string(i + 1);
  }
  uint64_t a2_data_size = a2_data.size();
  uint64_t a2_offsets_size = a2_offsets.size() * sizeof(uint64_t);
  write_1d_fragment_fixed_strings(
      subarray,
      a1_data.data(),
      &a1_data_size,
      a2_data.data(),
      &a2_data_size,
      a2_offsets.data(),
      &a2_offsets_size);

  // Each var tiles are 91 and 100 bytes respectively, this will only allow to
  // load one as the budget is split across two potential reads. Fixed tiles are
  // both 40 so they both fit in the budget.
  total_budget_ = "1100";
  update_config();

  // Try to read.
  int a1_data_r[NUM_CELLS] = {0};
  uint64_t a1_data_r_size = sizeof(a1_data_r);
  char a2_data_r[NUM_CELLS * 2] = {0};
  uint64_t a2_data_r_size = sizeof(a2_data_r);
  uint64_t a2_offsets_r[NUM_CELLS] = {0};
  uint64_t a2_offsets_r_size = sizeof(a2_offsets_r);
  read_fixed_strings(
      subarray,
      a1_data_r,
      &a1_data_r_size,
      a2_data_r,
      &a2_data_r_size,
      a2_offsets_r,
      &a2_offsets_r_size,
      0,
      false,
      false,
      "DenseReader: Cannot process a single tile, increase memory budget");

  // Now read with QC set for a1 only.
  read_fixed_strings(
      subarray,
      a1_data_r,
      &a1_data_r_size,
      a2_data_r,
      &a2_data_r_size,
      a2_offsets_r,
      &a2_offsets_r_size,
      0,
      true,
      false,
      "DenseReader: Cannot process a single tile, increase memory budget");

  // Now read with QC set for a2 only.
  read_fixed_strings(
      subarray,
      a1_data_r,
      &a1_data_r_size,
      a2_data_r,
      &a2_data_r_size,
      a2_offsets_r,
      &a2_offsets_r_size,
      0,
      false,
      true,
      "DenseReader: Cannot process a single tile, increase memory budget");

  // Now read with QC set for a1 and a2, should fail.
  read_fixed_strings(
      subarray,
      a1_data_r,
      &a1_data_r_size,
      a2_data_r,
      &a2_data_r_size,
      a2_offsets_r,
      &a2_offsets_r_size,
      0,
      true,
      true,
      "DenseReader: Cannot process a single tile because of query "
      "condition, increase memory budget");
}

#define LARGE_NUM_CELLS 50

TEST_CASE_METHOD(
    CDenseFx,
    "Dense reader: fixed/var attribute, many loops",
    "[dense-reader][many-loops][fixed-var]") {
  // Create default array.
  reset_config();
  create_default_array_1d_fixed_string();

  // Write a fragment.
  int subarray[] = {1, LARGE_NUM_CELLS};
  std::vector<int> a1_data(LARGE_NUM_CELLS);
  std::iota(a1_data.begin(), a1_data.end(), 1);
  uint64_t a1_data_size = a1_data.size() * sizeof(int);
  std::string a2_data;
  std::vector<uint64_t> a2_offsets(LARGE_NUM_CELLS);
  for (uint64_t i = 0; i < LARGE_NUM_CELLS; i++) {
    a2_offsets[i] = a2_data.size();
    a2_data += std::to_string(i + 1);
  }
  uint64_t a2_data_size = a2_data.size();
  uint64_t a2_offsets_size = a2_offsets.size() * sizeof(uint64_t);
  write_1d_fragment_fixed_strings(
      subarray,
      a1_data.data(),
      &a1_data_size,
      a2_data.data(),
      &a2_data_size,
      a2_offsets.data(),
      &a2_offsets_size);

  // First var tiles is 99 and subequent are 108 bytes, this will only allow to
  // load two tiles the first loop and one on the subsequents.
  tile_upper_memory_limit_ = "416";
  update_config();

  // Try to read.
  int a1_data_r[LARGE_NUM_CELLS] = {0};
  uint64_t a1_data_r_size = sizeof(a1_data_r);
  char a2_data_r[LARGE_NUM_CELLS * 2] = {0};
  uint64_t a2_data_r_size = sizeof(a2_data_r);
  uint64_t a2_offsets_r[LARGE_NUM_CELLS] = {0};
  uint64_t a2_offsets_r_size = sizeof(a2_offsets_r);
  read_fixed_strings(
      subarray,
      a1_data_r,
      &a1_data_r_size,
      a2_data_r,
      &a2_data_r_size,
      a2_offsets_r,
      &a2_offsets_r_size,
      4);

  CHECK(a1_data_r_size == a1_data_size);
  CHECK(!std::memcmp(a1_data.data(), a1_data_r, a1_data_size));
  CHECK(a2_data_r_size == a2_data_size);
  CHECK(!std::memcmp(a2_data.data(), a2_data_r, a2_data_size));
  CHECK(a2_offsets_r_size == a2_offsets_size);
  CHECK(!std::memcmp(a2_offsets.data(), a2_offsets_r, a2_offsets_size));
}

TEST_CASE_METHOD(
    CDenseFx,
    "Dense reader: fixed schema evolution",
    "[dense-reader][schema-evolution][fixed]") {
  // Create default array.
  reset_config();
  create_default_array_1d();

  // Write a fragment.
  int subarray[] = {1, NUM_CELLS};
  std::vector<int> data(NUM_CELLS);
  std::iota(data.begin(), data.end(), 1);
  uint64_t data_size = data.size() * sizeof(int);
  write_1d_fragment(subarray, data.data(), &data_size);

  // Evolve array.
  evolve_default_array_1d();

  // Try to read.
  int data_r[NUM_CELLS] = {0};
  uint64_t data_r_size = sizeof(data_r);
  int data_r_b[NUM_CELLS] = {0};
  uint64_t data_r_b_size = sizeof(data_r_b);
  read_evolved(subarray, data_r, &data_r_size, data_r_b, &data_r_b_size);

  // Validate cells.
  for (int i = 0; i < NUM_CELLS; i++) {
    CHECK(data_r[i] == i + 1);
    CHECK(data_r_b[i] == 7);
  }
}

TEST_CASE_METHOD(
    CDenseFx,
    "Dense reader: var schema evolution",
    "[dense-reader][schema-evolution][var]") {
  // Create default array.
  reset_config();
  create_default_array_1d_string();

  // Write a fragment.
  int subarray[] = {1, NUM_CELLS};
  std::string data;
  std::vector<uint64_t> offsets(NUM_CELLS);
  for (uint64_t i = 0; i < NUM_CELLS; i++) {
    offsets[i] = data.size();
    data += std::to_string(i + 1);
  }
  uint64_t data_size = data.size();
  uint64_t offsets_size = offsets.size() * sizeof(uint64_t);
  write_1d_fragment_strings(
      subarray, data.data(), &data_size, offsets.data(), &offsets_size);

  // Evolve array.
  evolve_default_array_1d_string();

  // Try to read.
  char data_r[NUM_CELLS * 2] = {0};
  uint64_t data_r_size = sizeof(data_r);
  uint64_t offsets_r[NUM_CELLS] = {0};
  uint64_t offsets_r_size = sizeof(offsets_r);
  char data_r_b[NUM_CELLS * 2] = {0};
  uint64_t data_r_b_size = sizeof(data_r_b);
  uint64_t offsets_r_b[NUM_CELLS] = {0};
  uint64_t offsets_r_b_size = sizeof(offsets_r_b);
  read_strings_evolved(
      subarray,
      data_r,
      &data_r_size,
      offsets_r,
      &offsets_r_size,
      data_r_b,
      &data_r_b_size,
      offsets_r_b,
      &offsets_r_b_size);

  // Validate cells.
  char c_data[] = "1234567891011121314151617181920";
  uint64_t c_data_size = sizeof(c_data) - 1;
  uint64_t c_offsets[] = {0,  1,  2,  3,  4,  5,  6,  7,  8,  9,
                          11, 13, 15, 17, 19, 21, 23, 25, 27, 29};
  uint64_t c_offsets_size = sizeof(c_offsets);
  CHECK(data_r_size == c_data_size);
  CHECK(!std::memcmp(c_data, data_r, c_data_size));
  CHECK(offsets_r_size == c_offsets_size);
  CHECK(!std::memcmp(c_offsets, offsets_r, c_offsets_size));

  char c_data_b[] = "77777777777777777777";
  uint64_t c_data_b_size = sizeof(c_data_b) - 1;
  uint64_t c_offsets_b[] = {0,  1,  2,  3,  4,  5,  6,  7,  8,  9,
                            10, 11, 12, 13, 14, 15, 16, 17, 18, 19};
  uint64_t c_offsets_b_size = sizeof(c_offsets_b);
  CHECK(data_r_b_size == c_data_b_size);
  CHECK(!std::memcmp(c_data_b, data_r_b, c_data_b_size));
  CHECK(offsets_r_b_size == c_offsets_b_size);
  CHECK(!std::memcmp(c_offsets_b, offsets_r_b, c_offsets_b_size));
}
