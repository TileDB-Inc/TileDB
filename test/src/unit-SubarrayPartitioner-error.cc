/**
 * @file unit-SubarrayPartitioner-error.cc
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
 * Tests the `SubarrayPartitioner` class for errors.
 */

#include "test/support/src/helpers.h"
#include "test/support/src/vfs_helpers.h"
#include "tiledb/sm/c_api/tiledb_struct_def.h"
#include "tiledb/sm/subarray/subarray_partitioner.h"

#ifdef _WIN32
#include "tiledb/sm/filesystem/win.h"
#else
#include "tiledb/sm/filesystem/posix.h"
#endif

#include <catch2/catch_test_macros.hpp>
#include <iostream>

using namespace tiledb::common;
using namespace tiledb::sm;
using namespace tiledb::test;

/* ********************************* */
/*         STRUCT DEFINITION         */
/* ********************************* */

struct SubarrayPartitionerErrorFx {
  tiledb_ctx_t* ctx_;
  tiledb_vfs_t* vfs_;
  const std::vector<std::unique_ptr<SupportedFs>> fs_vec_;
  std::string temp_dir_;
  std::string array_name_;
  const char* ARRAY_NAME = "subarray_partitioner_error";
  tiledb_array_t* array_ = nullptr;
  uint64_t memory_budget_ = 1024 * 1024 * 1024;
  uint64_t memory_budget_var_ = 1024 * 1024 * 1024;

  SubarrayPartitionerErrorFx();
  ~SubarrayPartitionerErrorFx();
};

SubarrayPartitionerErrorFx::SubarrayPartitionerErrorFx()
    : fs_vec_(vfs_test_get_fs_vec()) {
  // Initialize vfs test
  REQUIRE(vfs_test_init(fs_vec_, &ctx_, &vfs_).ok());

// Create temporary directory based on the supported filesystem
#ifdef _WIN32
  SupportedFsLocal windows_fs;
  temp_dir_ = windows_fs.file_prefix() + windows_fs.temp_dir();
#else
  SupportedFsLocal posix_fs;
  temp_dir_ = posix_fs.file_prefix() + posix_fs.temp_dir();
#endif

  create_dir(temp_dir_, ctx_, vfs_);

  array_name_ = temp_dir_ + ARRAY_NAME;
  int rc = tiledb_array_alloc(ctx_, array_name_.c_str(), &array_);
  CHECK(rc == TILEDB_OK);
}

SubarrayPartitionerErrorFx::~SubarrayPartitionerErrorFx() {
  tiledb_array_free(&array_);
  remove_dir(temp_dir_, ctx_, vfs_);
  tiledb_ctx_free(&ctx_);
  tiledb_vfs_free(&vfs_);
}

/* ********************************* */
/*                TESTS              */
/* ********************************* */

TEST_CASE_METHOD(
    SubarrayPartitionerErrorFx,
    "SubarrayPartitioner (Error): 1D, result and memory budget, "
    "setters/getters",
    "[SubarrayPartitioner][error][1D][budget][set_get]") {
  uint64_t domain[] = {1, 100};
  uint64_t tile_extent = 10;
  create_array(
      ctx_,
      array_name_,
      TILEDB_DENSE,
      {"d"},
      {TILEDB_UINT64},
      {domain},
      {&tile_extent},
      {"a", "b"},
      {TILEDB_INT32, TILEDB_INT32},
      {1, TILEDB_VAR_NUM},
      {tiledb::test::Compressor(TILEDB_FILTER_LZ4, -1),
       tiledb::test::Compressor(TILEDB_FILTER_LZ4, -1)},
      TILEDB_ROW_MAJOR,
      TILEDB_ROW_MAJOR,
      2);

  open_array(ctx_, array_, TILEDB_READ);

  Subarray subarray;
  SubarrayRanges<uint64_t> ranges = {};
  Layout subarray_layout = Layout::GLOBAL_ORDER;
  create_subarray(array_->array_, ranges, subarray_layout, &subarray);
  ThreadPool tp(4);
  Config config;
  SubarrayPartitioner subarray_partitioner(
      &config,
      subarray,
      memory_budget_,
      memory_budget_var_,
      0,
      &tp,
      &g_helper_stats,
      g_helper_logger());
  uint64_t budget, budget_off, budget_val;

  auto st = subarray_partitioner.get_result_budget("a", &budget);
  CHECK(!st.ok());
  st = subarray_partitioner.set_result_budget(nullptr, 10);
  CHECK(!st.ok());
  st = subarray_partitioner.set_result_budget("foo", 10);
  CHECK(!st.ok());
  st = subarray_partitioner.set_result_budget("a", 10, 10);
  CHECK(!st.ok());
  st = subarray_partitioner.set_result_budget("a", 10);
  CHECK(st.ok());
  st = subarray_partitioner.get_result_budget(nullptr, &budget);
  CHECK(!st.ok());
  st = subarray_partitioner.get_result_budget("foo", &budget);
  CHECK(!st.ok());
  st = subarray_partitioner.get_result_budget("a", &budget_off, &budget_val);
  CHECK(!st.ok());
  st = subarray_partitioner.get_result_budget("a", &budget);
  CHECK(st.ok());
  CHECK(budget == 10);
  st = subarray_partitioner.get_result_budget(TILEDB_COORDS, &budget);
  CHECK(!st.ok());
  st = subarray_partitioner.set_result_budget(TILEDB_COORDS, 11);
  CHECK(st.ok());
  st = subarray_partitioner.get_result_budget(TILEDB_COORDS, &budget);
  CHECK(st.ok());
  CHECK(budget == 11);
  st = subarray_partitioner.get_result_budget("b", &budget_off, &budget_val);
  CHECK(!st.ok());
  st = subarray_partitioner.set_result_budget(nullptr, 100, 101);
  CHECK(!st.ok());
  st = subarray_partitioner.set_result_budget("foo", 100, 101);
  CHECK(!st.ok());
  st = subarray_partitioner.set_result_budget("foo", 100);
  CHECK(!st.ok());
  st = subarray_partitioner.set_result_budget("b", 100, 101);
  CHECK(st.ok());
  st = subarray_partitioner.set_result_budget("d", 1000, 1010);
  CHECK(!st.ok());
  st = subarray_partitioner.set_result_budget("d", 1000);
  CHECK(st.ok());
  st =
      subarray_partitioner.get_result_budget(nullptr, &budget_off, &budget_val);
  CHECK(!st.ok());
  st = subarray_partitioner.get_result_budget("foo", &budget_off, &budget_val);
  CHECK(!st.ok());
  st = subarray_partitioner.get_result_budget("b", &budget);
  CHECK(!st.ok());
  st = subarray_partitioner.get_result_budget("b", &budget_off, &budget_val);
  CHECK(st.ok());
  CHECK(budget_off == 100);
  CHECK(budget_val == 101);
  st = subarray_partitioner.set_result_budget(TILEDB_COORDS, 100, 101);
  CHECK(!st.ok());
  st = subarray_partitioner.get_result_budget(
      TILEDB_COORDS, &budget_off, &budget_val);
  CHECK(!st.ok());
  st = subarray_partitioner.get_result_budget("d", &budget);
  CHECK(st.ok());
  CHECK(budget == 1000);

  uint64_t memory_budget, memory_budget_var, memory_budget_validity;
  st = subarray_partitioner.get_memory_budget(
      &memory_budget, &memory_budget_var, &memory_budget_validity);
  CHECK(st.ok());
  CHECK(memory_budget == memory_budget_);
  CHECK(memory_budget_var == memory_budget_var_);
  st = subarray_partitioner.set_memory_budget(16, 16, 0);
  CHECK(st.ok());
  st = subarray_partitioner.get_memory_budget(
      &memory_budget, &memory_budget_var, &memory_budget_validity);
  CHECK(st.ok());
  CHECK(memory_budget == 16);
  CHECK(memory_budget_var == 16);

  close_array(ctx_, array_);
}
