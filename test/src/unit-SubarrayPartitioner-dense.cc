/**
 * @file unit-SubarrayPartitioner-dense.cc
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
 * Tests the `SubarrayPartitioner` class for dense arrays.
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

struct SubarrayPartitionerDenseFx {
  tiledb_ctx_t* ctx_;
  tiledb_vfs_t* vfs_;
  const std::vector<std::unique_ptr<SupportedFs>> fs_vec_;
  std::string temp_dir_;
  std::string array_name_;
  const char* ARRAY_NAME = "subarray_partitioner_dense";
  tiledb_array_t* array_ = nullptr;
  uint64_t memory_budget_ = 1024 * 1024 * 1024;
  uint64_t memory_budget_var_ = 1024 * 1024 * 1024;

  SubarrayPartitionerDenseFx();
  ~SubarrayPartitionerDenseFx();

  /**
   * Helper function that creates a default 1D array, with the
   * input tile and cell order.
   */
  void create_default_1d_array(
      tiledb_layout_t tile_order, tiledb_layout_t cell_order);

  /**
   * Helper function that creates a default 2D array, with the
   * input tile and cell order.
   */
  void create_default_2d_array(
      tiledb_layout_t tile_order, tiledb_layout_t cell_order);

  /** Helper function that writes to the default 1D array. */
  void write_default_1d_array();

  /** Helper function that writes to the default 2D array. */
  void write_default_2d_array();

  /**
   * Helper function to test the subarray partitioner for the given arguments.
   */
  template <class T>
  void test_subarray_partitioner(
      Layout subarray_layout,
      const SubarrayRanges<T>& ranges,
      const std::vector<SubarrayRanges<T>>& partitions,
      const std::string& attr,  // Attribute to set the budget for
      uint64_t budget,
      bool unsplittable = false);

  /**
   * Helper function to test the subarray partitioner for the given arguments.
   * This is different from the above in that it tests the memory
   * budget.
   */
  template <class T>
  void test_subarray_partitioner(
      Layout subarray_layout,
      const SubarrayRanges<T>& ranges,
      const std::vector<SubarrayRanges<T>>& partitions,
      uint64_t budget,
      uint64_t budget_var,
      bool unsplittable = false);
};

SubarrayPartitionerDenseFx::SubarrayPartitionerDenseFx()
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

SubarrayPartitionerDenseFx::~SubarrayPartitionerDenseFx() {
  tiledb_array_free(&array_);
  remove_dir(temp_dir_, ctx_, vfs_);
  tiledb_ctx_free(&ctx_);
  tiledb_vfs_free(&vfs_);
}

void SubarrayPartitionerDenseFx::create_default_1d_array(
    tiledb_layout_t tile_order, tiledb_layout_t cell_order) {
  uint64_t domain[] = {1, 10};
  uint64_t tile_extent = 2;
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
      tile_order,
      cell_order,
      2);
}

void SubarrayPartitionerDenseFx::create_default_2d_array(
    tiledb_layout_t tile_order, tiledb_layout_t cell_order) {
  uint64_t domain[] = {1, 4};
  uint64_t tile_extent = 2;
  create_array(
      ctx_,
      array_name_,
      TILEDB_DENSE,
      {"d1", "d2"},
      {TILEDB_UINT64, TILEDB_UINT64},
      {domain, domain},
      {&tile_extent, &tile_extent},
      {"a", "b"},
      {TILEDB_INT32, TILEDB_INT32},
      {1, TILEDB_VAR_NUM},
      {tiledb::test::Compressor(TILEDB_FILTER_LZ4, -1),
       tiledb::test::Compressor(TILEDB_FILTER_LZ4, -1)},
      tile_order,
      cell_order,
      2);
}

void SubarrayPartitionerDenseFx::write_default_1d_array() {
  tiledb::test::QueryBuffers buffers;
  std::vector<int> a = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
  uint64_t a_size = a.size() * sizeof(int);
  std::vector<uint64_t> b_off = {
      0,
      sizeof(int),
      3 * sizeof(int),
      6 * sizeof(int),
      9 * sizeof(int),
      11 * sizeof(int),
      15 * sizeof(int),
      16 * sizeof(int),
      17 * sizeof(int),
      18 * sizeof(int)};
  uint64_t b_off_size = b_off.size() * sizeof(uint64_t);
  std::vector<int> b_val = {
      1, 2, 2, 3, 3, 3, 4, 4, 4, 5, 5, 6, 6, 6, 6, 7, 8, 9, 10};
  uint64_t b_val_size = b_val.size() * sizeof(int);
  buffers["a"] = tiledb::test::QueryBuffer({&a[0], a_size, nullptr, 0});
  buffers["b"] =
      tiledb::test::QueryBuffer({&b_off[0], b_off_size, &b_val[0], b_val_size});
  write_array(ctx_, array_name_, TILEDB_GLOBAL_ORDER, buffers);
}

void SubarrayPartitionerDenseFx::write_default_2d_array() {
  tiledb::test::QueryBuffers buffers;
  std::vector<int> a = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};
  uint64_t a_size = a.size() * sizeof(int);
  std::vector<uint64_t> b_off = {
      0,
      sizeof(int),
      3 * sizeof(int),
      6 * sizeof(int),
      9 * sizeof(int),
      11 * sizeof(int),
      15 * sizeof(int),
      17 * sizeof(int),
      20 * sizeof(int),
      21 * sizeof(int),
      23 * sizeof(int),
      24 * sizeof(int),
      25 * sizeof(int),
      27 * sizeof(int),
      28 * sizeof(int),
      29 * sizeof(int)};
  uint64_t b_off_size = b_off.size() * sizeof(uint64_t);
  std::vector<int> b_val = {1,  2,  2,  3,  3,  3,  4,  4,  4, 5, 5,
                            6,  6,  6,  6,  7,  7,  8,  8,  8, 9, 10,
                            10, 11, 12, 13, 13, 14, 15, 16, 16};
  uint64_t b_val_size = b_val.size() * sizeof(int);
  buffers["a"] = tiledb::test::QueryBuffer({&a[0], a_size, nullptr, 0});
  buffers["b"] =
      tiledb::test::QueryBuffer({&b_off[0], b_off_size, &b_val[0], b_val_size});
  write_array(ctx_, array_name_, TILEDB_GLOBAL_ORDER, buffers);
}

template <class T>
void SubarrayPartitionerDenseFx::test_subarray_partitioner(
    Layout subarray_layout,
    const SubarrayRanges<T>& ranges,
    const std::vector<SubarrayRanges<T>>& partitions,
    const std::string& attr,
    uint64_t budget,
    bool unsplittable) {
  Subarray subarray;
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
  auto st = subarray_partitioner.set_result_budget(attr.c_str(), budget);
  CHECK(st.ok());

  check_partitions(subarray_partitioner, partitions, unsplittable);
}

template <class T>
void SubarrayPartitionerDenseFx::test_subarray_partitioner(
    Layout subarray_layout,
    const SubarrayRanges<T>& ranges,
    const std::vector<SubarrayRanges<T>>& partitions,
    uint64_t budget,
    uint64_t budget_var,
    bool unsplittable) {
  Subarray subarray;
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

  // Note: this is necessary, otherwise the subarray partitioner does
  // not check if the memory budget is exceeded for attributes whose
  // result budget is not set.
  auto st = subarray_partitioner.set_result_budget(TILEDB_COORDS, 1000000);
  CHECK(st.ok());
  st = subarray_partitioner.set_result_budget("a", 1000000);
  CHECK(st.ok());
  st = subarray_partitioner.set_result_budget("b", 1000000, 1000000);
  CHECK(st.ok());

  st = subarray_partitioner.set_memory_budget(budget, budget_var, 0);
  CHECK(st.ok());

  check_partitions(subarray_partitioner, partitions, unsplittable);
}

/* ********************************* */
/*                TESTS              */
/* ********************************* */

TEST_CASE_METHOD(
    SubarrayPartitionerDenseFx,
    "SubarrayPartitioner (Dense): 1D, single-range, empty array",
    "[SubarrayPartitioner][dense][1D][1R][empty_array]") {
  Layout subarray_layout;
  SubarrayRanges<uint64_t> ranges = {};
  std::vector<SubarrayRanges<uint64_t>> partitions = {{{1, 10}}};
  uint64_t budget = 1000 * sizeof(uint64_t);
  std::string attr = TILEDB_COORDS;
  bool unsplittable = false;

  create_default_1d_array(TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR);
  open_array(ctx_, array_, TILEDB_READ);

  // subarray: global
  subarray_layout = Layout::GLOBAL_ORDER;
  test_subarray_partitioner(
      subarray_layout, ranges, partitions, attr, budget, unsplittable);

  // subarray: row
  subarray_layout = Layout::ROW_MAJOR;
  test_subarray_partitioner(
      subarray_layout, ranges, partitions, attr, budget, unsplittable);

  // subarray: col
  subarray_layout = Layout::COL_MAJOR;
  test_subarray_partitioner(
      subarray_layout, ranges, partitions, attr, budget, unsplittable);

  // subarray: unordered
  subarray_layout = Layout::UNORDERED;
  test_subarray_partitioner(
      subarray_layout, ranges, partitions, attr, budget, unsplittable);

  close_array(ctx_, array_);
}

TEST_CASE_METHOD(
    SubarrayPartitionerDenseFx,
    "SubarrayPartitioner (Dense): 1D, single-range, whole subarray fits",
    "[SubarrayPartitioner][dense][1D][1R][whole_subarray_fits]") {
  Layout subarray_layout;
  SubarrayRanges<uint64_t> ranges = {};
  std::vector<SubarrayRanges<uint64_t>> partitions = {{{1, 10}}};
  uint64_t budget = 1000 * sizeof(uint64_t);
  std::string attr = TILEDB_COORDS;
  bool unsplittable = false;

  create_default_1d_array(TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR);
  write_default_1d_array();
  open_array(ctx_, array_, TILEDB_READ);

  // subarray: global
  subarray_layout = Layout::GLOBAL_ORDER;
  test_subarray_partitioner(
      subarray_layout, ranges, partitions, attr, budget, unsplittable);

  // subarray: row
  subarray_layout = Layout::ROW_MAJOR;
  test_subarray_partitioner(
      subarray_layout, ranges, partitions, attr, budget, unsplittable);

  // subarray: col
  subarray_layout = Layout::COL_MAJOR;
  test_subarray_partitioner(
      subarray_layout, ranges, partitions, attr, budget, unsplittable);

  // subarray: unordered
  subarray_layout = Layout::UNORDERED;
  test_subarray_partitioner(
      subarray_layout, ranges, partitions, attr, budget, unsplittable);

  close_array(ctx_, array_);
}

TEST_CASE_METHOD(
    SubarrayPartitionerDenseFx,
    "SubarrayPartitioner (Dense): 1D, single-range, split once",
    "[SubarrayPartitioner][dense][1D][1R][split_once]") {
  Layout subarray_layout;
  SubarrayRanges<uint64_t> ranges = {{2, 5}};
  std::vector<SubarrayRanges<uint64_t>> partitions;
  uint64_t budget = 3 * sizeof(int);
  std::string attr = "a";
  bool unsplittable = false;

  create_default_1d_array(TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR);
  write_default_1d_array();
  open_array(ctx_, array_, TILEDB_READ);

  // subarray: global
  subarray_layout = Layout::GLOBAL_ORDER;
  partitions = {{{2, 2}}, {{3, 5}}};
  test_subarray_partitioner(
      subarray_layout, ranges, partitions, attr, budget, unsplittable);

  // subarray: row
  subarray_layout = Layout::ROW_MAJOR;
  partitions = {{{2, 3}}, {{4, 5}}};
  test_subarray_partitioner(
      subarray_layout, ranges, partitions, attr, budget, unsplittable);

  // subarray: col
  subarray_layout = Layout::COL_MAJOR;
  test_subarray_partitioner(
      subarray_layout, ranges, partitions, attr, budget, unsplittable);

  // subarray: unordered
  subarray_layout = Layout::UNORDERED;
  test_subarray_partitioner(
      subarray_layout, ranges, partitions, attr, budget, unsplittable);

  close_array(ctx_, array_);
}

TEST_CASE_METHOD(
    SubarrayPartitionerDenseFx,
    "SubarrayPartitioner (Dense): 1D, single-range, unsplittable at once",
    "[SubarrayPartitioner][dense][1D][1R][unsplittable_at_once]") {
  Layout subarray_layout;
  SubarrayRanges<uint64_t> ranges = {{4, 4}};
  std::vector<SubarrayRanges<uint64_t>> partitions = {{{4, 4}}};
  uint64_t budget = 1;
  std::string attr = "a";
  bool unsplittable = true;

  create_default_1d_array(TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR);
  write_default_1d_array();
  open_array(ctx_, array_, TILEDB_READ);

  // subarray: global
  subarray_layout = Layout::GLOBAL_ORDER;
  test_subarray_partitioner(
      subarray_layout, ranges, partitions, attr, budget, unsplittable);

  // subarray: row
  subarray_layout = Layout::ROW_MAJOR;
  test_subarray_partitioner(
      subarray_layout, ranges, partitions, attr, budget, unsplittable);

  // subarray: col
  subarray_layout = Layout::COL_MAJOR;
  test_subarray_partitioner(
      subarray_layout, ranges, partitions, attr, budget, unsplittable);

  // subarray: unordered
  subarray_layout = Layout::UNORDERED;
  test_subarray_partitioner(
      subarray_layout, ranges, partitions, attr, budget, unsplittable);

  close_array(ctx_, array_);
}

TEST_CASE_METHOD(
    SubarrayPartitionerDenseFx,
    "SubarrayPartitioner (Dense): 1D, single-range, split multiple",
    "[SubarrayPartitioner][dense][1D][1R][split_multiple]") {
  Layout subarray_layout;
  SubarrayRanges<uint64_t> ranges = {{1, 6}};
  std::vector<SubarrayRanges<uint64_t>> partitions;
  uint64_t budget = 2 * sizeof(int);
  std::string attr = "a";
  bool unsplittable = false;

  create_default_1d_array(TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR);
  write_default_1d_array();
  open_array(ctx_, array_, TILEDB_READ);

  // subarray: global
  subarray_layout = Layout::GLOBAL_ORDER;
  partitions = {{{1, 2}}, {{3, 4}}, {{5, 6}}};
  test_subarray_partitioner(
      subarray_layout, ranges, partitions, attr, budget, unsplittable);

  // subarray: row
  partitions = {{{1, 2}}, {{3, 3}}, {{4, 5}}, {{6, 6}}};
  subarray_layout = Layout::ROW_MAJOR;
  test_subarray_partitioner(
      subarray_layout, ranges, partitions, attr, budget, unsplittable);

  // subarray: col
  subarray_layout = Layout::COL_MAJOR;
  test_subarray_partitioner(
      subarray_layout, ranges, partitions, attr, budget, unsplittable);

  // subarray: unordered
  subarray_layout = Layout::UNORDERED;
  test_subarray_partitioner(
      subarray_layout, ranges, partitions, attr, budget, unsplittable);

  close_array(ctx_, array_);
}

TEST_CASE_METHOD(
    SubarrayPartitionerDenseFx,
    "SubarrayPartitioner (Dense): 1D, single-range, unsplittable after "
    "multiple",
    "[SubarrayPartitioner][dense][1D][1R][unsplittable_after_multiple]") {
  Layout subarray_layout;
  SubarrayRanges<uint64_t> ranges = {{1, 6}};
  std::vector<SubarrayRanges<uint64_t>> partitions = {{{1, 1}}};
  uint64_t budget = 1;
  std::string attr = "a";
  bool unsplittable = true;

  create_default_1d_array(TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR);
  write_default_1d_array();
  open_array(ctx_, array_, TILEDB_READ);

  // subarray: global
  subarray_layout = Layout::GLOBAL_ORDER;
  test_subarray_partitioner(
      subarray_layout, ranges, partitions, attr, budget, unsplittable);

  // subarray: row
  subarray_layout = Layout::ROW_MAJOR;
  test_subarray_partitioner(
      subarray_layout, ranges, partitions, attr, budget, unsplittable);

  // subarray: col
  subarray_layout = Layout::COL_MAJOR;
  test_subarray_partitioner(
      subarray_layout, ranges, partitions, attr, budget, unsplittable);

  // subarray: unordered
  subarray_layout = Layout::UNORDERED;
  test_subarray_partitioner(
      subarray_layout, ranges, partitions, attr, budget, unsplittable);

  close_array(ctx_, array_);
}

TEST_CASE_METHOD(
    SubarrayPartitionerDenseFx,
    "SubarrayPartitioner (Dense): 1D, single-range, unsplittable but ok after "
    "budget reset",
    "[SubarrayPartitioner][dense][1D][1R][unsplittable_but_then_ok]") {
  create_default_1d_array(TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR);
  write_default_1d_array();
  open_array(ctx_, array_, TILEDB_READ);

  Subarray subarray;
  SubarrayRanges<uint64_t> ranges = {{2, 6}};
  Layout subarray_layout = Layout::GLOBAL_ORDER;
  std::vector<SubarrayRanges<uint64_t>> partitions = {{{2, 2}}};
  std::vector<SubarrayRanges<uint64_t>> partitions_after = {{{3, 6}}};

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
  auto st = subarray_partitioner.set_result_budget("a", 100 * sizeof(int));
  CHECK(st.ok());
  st = subarray_partitioner.set_result_budget("b", 1, 1);
  CHECK(st.ok());

  check_partitions(subarray_partitioner, partitions, true);

  st = subarray_partitioner.set_result_budget("b", 100, 100);
  CHECK(st.ok());

  check_partitions(subarray_partitioner, partitions_after, false);

  close_array(ctx_, array_);
}

TEST_CASE_METHOD(
    SubarrayPartitionerDenseFx,
    "SubarrayPartitioner (Dense): 1D, multi-range, whole subarray fits",
    "[SubarrayPartitioner][dense][1D][MR][whole_subarray_fits]") {
  Layout subarray_layout;
  SubarrayRanges<uint64_t> ranges = {{2, 3, 5, 8, 9, 10}};
  std::vector<SubarrayRanges<uint64_t>> partitions = {{{2, 3, 5, 8, 9, 10}}};
  uint64_t budget = 1000000;
  std::string attr = "a";
  bool unsplittable = false;

  create_default_1d_array(TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR);
  write_default_1d_array();
  open_array(ctx_, array_, TILEDB_READ);

  // subarray: row
  subarray_layout = Layout::ROW_MAJOR;
  test_subarray_partitioner(
      subarray_layout, ranges, partitions, attr, budget, unsplittable);

  // subarray: col
  subarray_layout = Layout::COL_MAJOR;
  test_subarray_partitioner(
      subarray_layout, ranges, partitions, attr, budget, unsplittable);

  // subarray: unordered
  subarray_layout = Layout::UNORDERED;
  test_subarray_partitioner(
      subarray_layout, ranges, partitions, attr, budget, unsplittable);

  close_array(ctx_, array_);
}

TEST_CASE_METHOD(
    SubarrayPartitionerDenseFx,
    "SubarrayPartitioner (Dense): 1D, multi-range, split once",
    "[SubarrayPartitioner][dense][1D][MR][split_once]") {
  Layout subarray_layout;
  SubarrayRanges<uint64_t> ranges = {{2, 3, 5, 8, 9, 10}};
  std::vector<SubarrayRanges<uint64_t>> partitions = {
      {{2, 3, 5, 8}}, {{9, 10}}};
  uint64_t budget = 7 * sizeof(int);
  std::string attr = "a";
  bool unsplittable = false;

  create_default_1d_array(TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR);
  write_default_1d_array();
  open_array(ctx_, array_, TILEDB_READ);

  // subarray: row
  subarray_layout = Layout::ROW_MAJOR;
  test_subarray_partitioner(
      subarray_layout, ranges, partitions, attr, budget, unsplittable);

  // subarray: col
  subarray_layout = Layout::COL_MAJOR;
  test_subarray_partitioner(
      subarray_layout, ranges, partitions, attr, budget, unsplittable);

  // subarray: unordered
  subarray_layout = Layout::UNORDERED;
  test_subarray_partitioner(
      subarray_layout, ranges, partitions, attr, budget, unsplittable);

  close_array(ctx_, array_);
}

TEST_CASE_METHOD(
    SubarrayPartitionerDenseFx,
    "SubarrayPartitioner (Dense): 1D, multi-range, split multiple",
    "[SubarrayPartitioner][dense][1D][MR][split_multiple]") {
  Layout subarray_layout;
  SubarrayRanges<uint64_t> ranges = {{2, 3, 5, 8, 9, 10}};
  std::vector<SubarrayRanges<uint64_t>> partitions = {
      {{2, 3}}, {{5, 8}}, {{9, 10}}};
  uint64_t budget = 4 * sizeof(int);
  std::string attr = "a";
  bool unsplittable = false;

  create_default_1d_array(TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR);
  write_default_1d_array();
  open_array(ctx_, array_, TILEDB_READ);

  // subarray: row
  subarray_layout = Layout::ROW_MAJOR;
  test_subarray_partitioner(
      subarray_layout, ranges, partitions, attr, budget, unsplittable);

  // subarray: col
  subarray_layout = Layout::COL_MAJOR;
  test_subarray_partitioner(
      subarray_layout, ranges, partitions, attr, budget, unsplittable);

  // subarray: unordered
  subarray_layout = Layout::UNORDERED;
  test_subarray_partitioner(
      subarray_layout, ranges, partitions, attr, budget, unsplittable);

  close_array(ctx_, array_);
}

TEST_CASE_METHOD(
    SubarrayPartitionerDenseFx,
    "SubarrayPartitioner (Dense): 1D, multi-range, split multiple finer",
    "[SubarrayPartitioner][dense][1D][MR][split_multiple_finer]") {
  Layout subarray_layout;
  SubarrayRanges<uint64_t> ranges = {{2, 3, 5, 8, 9, 10}};
  std::vector<SubarrayRanges<uint64_t>> partitions = {
      {{2, 3}}, {{5, 6}}, {{7, 8}}, {{9, 10}}};
  uint64_t budget = 2 * sizeof(int);
  std::string attr = "a";
  bool unsplittable = false;

  create_default_1d_array(TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR);
  write_default_1d_array();
  open_array(ctx_, array_, TILEDB_READ);

  // subarray: row
  subarray_layout = Layout::ROW_MAJOR;
  test_subarray_partitioner(
      subarray_layout, ranges, partitions, attr, budget, unsplittable);

  // subarray: col
  subarray_layout = Layout::COL_MAJOR;
  test_subarray_partitioner(
      subarray_layout, ranges, partitions, attr, budget, unsplittable);

  // subarray: unordered
  subarray_layout = Layout::UNORDERED;
  test_subarray_partitioner(
      subarray_layout, ranges, partitions, attr, budget, unsplittable);

  close_array(ctx_, array_);
}

TEST_CASE_METHOD(
    SubarrayPartitionerDenseFx,
    "SubarrayPartitioner (Dense): 1D, multi-range, unsplittable",
    "[SubarrayPartitioner][dense][1D][MR][unsplittable]") {
  Layout subarray_layout;
  SubarrayRanges<uint64_t> ranges = {{2, 3, 5, 8, 9, 10}};
  std::vector<SubarrayRanges<uint64_t>> partitions = {{{2, 2}}};
  uint64_t budget = 0;
  std::string attr = "a";
  bool unsplittable = true;

  create_default_1d_array(TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR);
  write_default_1d_array();
  open_array(ctx_, array_, TILEDB_READ);

  // subarray: row
  subarray_layout = Layout::ROW_MAJOR;
  test_subarray_partitioner(
      subarray_layout, ranges, partitions, attr, budget, unsplittable);

  // subarray: col
  subarray_layout = Layout::COL_MAJOR;
  test_subarray_partitioner(
      subarray_layout, ranges, partitions, attr, budget, unsplittable);

  // subarray: unordered
  subarray_layout = Layout::UNORDERED;
  test_subarray_partitioner(
      subarray_layout, ranges, partitions, attr, budget, unsplittable);

  close_array(ctx_, array_);
}

TEST_CASE_METHOD(
    SubarrayPartitionerDenseFx,
    "SubarrayPartitioner (Dense): 1D, single-range, memory budget",
    "[SubarrayPartitioner][dense][1D][1R][memory_budget]") {
  Layout subarray_layout;
  SubarrayRanges<uint64_t> ranges = {};
  std::vector<SubarrayRanges<uint64_t>> partitions = {};
  uint64_t budget = 16;
  uint64_t budget_var = 100000;
  bool unsplittable = false;

  create_default_1d_array(TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR);
  write_default_1d_array();
  open_array(ctx_, array_, TILEDB_READ);

  // subarray: global
  subarray_layout = Layout::GLOBAL_ORDER;
  partitions = {
      {{1, 2}},
      {{3, 4}},
      {{5, 6}},
      {{7, 8}},
      {{9, 10}},
  };
  test_subarray_partitioner(
      subarray_layout, ranges, partitions, budget, budget_var, unsplittable);

  // subarray: row
  subarray_layout = Layout::ROW_MAJOR;
  partitions = {
      {{1, 2}},
      {{3, 3}},
      {{4, 4}},
      {{5, 5}},
      {{6, 6}},
      {{7, 7}},
      {{8, 8}},
      {{9, 10}},
  };
  test_subarray_partitioner(
      subarray_layout, ranges, partitions, budget, budget_var, unsplittable);

  // subarray: col
  subarray_layout = Layout::COL_MAJOR;
  test_subarray_partitioner(
      subarray_layout, ranges, partitions, budget, budget_var, unsplittable);

  // subarray: unordered
  subarray_layout = Layout::UNORDERED;
  test_subarray_partitioner(
      subarray_layout, ranges, partitions, budget, budget_var, unsplittable);

  close_array(ctx_, array_);
}

TEST_CASE_METHOD(
    SubarrayPartitionerDenseFx,
    "SubarrayPartitioner (Dense): 2D, single-range, whole subarray fits",
    "[SubarrayPartitioner][dense][2D][1R][whole_subarray_fits]") {
  Layout subarray_layout;
  SubarrayRanges<uint64_t> ranges = {{1, 4}, {1, 4}};
  std::vector<SubarrayRanges<uint64_t>> partitions = {{{1, 4}, {1, 4}}};
  uint64_t budget = 1000 * sizeof(uint64_t);
  std::string attr = TILEDB_COORDS;
  bool unsplittable = false;

  SECTION("# tile: row, cell: row") {
    create_default_2d_array(TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR);
    write_default_2d_array();
    open_array(ctx_, array_, TILEDB_READ);

    // subarray: global
    subarray_layout = Layout::GLOBAL_ORDER;
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget, unsplittable);

    // subarray: row
    subarray_layout = Layout::ROW_MAJOR;
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget, unsplittable);

    // subarray: col
    subarray_layout = Layout::COL_MAJOR;
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget, unsplittable);

    // subarray: unordered
    subarray_layout = Layout::UNORDERED;
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget, unsplittable);
  }

  SECTION("# tile: row, cell: col") {
    create_default_2d_array(TILEDB_ROW_MAJOR, TILEDB_COL_MAJOR);
    write_default_2d_array();
    open_array(ctx_, array_, TILEDB_READ);

    // subarray: global
    subarray_layout = Layout::GLOBAL_ORDER;
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget, unsplittable);

    // subarray: row
    subarray_layout = Layout::ROW_MAJOR;
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget, unsplittable);

    // subarray: col
    subarray_layout = Layout::COL_MAJOR;
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget, unsplittable);

    // subarray: unordered
    subarray_layout = Layout::UNORDERED;
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget, unsplittable);
  }

  SECTION("# tile: col, cell: row") {
    create_default_2d_array(TILEDB_COL_MAJOR, TILEDB_ROW_MAJOR);
    write_default_2d_array();
    open_array(ctx_, array_, TILEDB_READ);

    // subarray: global
    subarray_layout = Layout::GLOBAL_ORDER;
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget, unsplittable);

    // subarray: row
    subarray_layout = Layout::ROW_MAJOR;
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget, unsplittable);

    // subarray: col
    subarray_layout = Layout::COL_MAJOR;
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget, unsplittable);

    // subarray: unordered
    subarray_layout = Layout::UNORDERED;
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget, unsplittable);
  }

  SECTION("# tile: col, cell: col") {
    create_default_2d_array(TILEDB_COL_MAJOR, TILEDB_COL_MAJOR);
    write_default_2d_array();
    open_array(ctx_, array_, TILEDB_READ);

    // subarray: global
    subarray_layout = Layout::GLOBAL_ORDER;
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget, unsplittable);

    // subarray: row
    subarray_layout = Layout::ROW_MAJOR;
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget, unsplittable);

    // subarray: col
    subarray_layout = Layout::COL_MAJOR;
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget, unsplittable);

    // subarray: unordered
    subarray_layout = Layout::UNORDERED;
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget, unsplittable);
  }

  close_array(ctx_, array_);
}

TEST_CASE_METHOD(
    SubarrayPartitionerDenseFx,
    "SubarrayPartitioner (Dense): 2D, single-range, unsplittable",
    "[SubarrayPartitioner][dense][2D][1R][unsplittable]") {
  Layout subarray_layout;
  SubarrayRanges<uint64_t> ranges = {{2, 4}, {2, 4}};
  std::vector<SubarrayRanges<uint64_t>> partitions = {{{2, 2}, {2, 2}}};
  uint64_t budget = 0 * sizeof(uint64_t);
  std::string attr = TILEDB_COORDS;
  bool unsplittable = true;

  SECTION("# tile: row, cell: row") {
    create_default_2d_array(TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR);
    write_default_2d_array();
    open_array(ctx_, array_, TILEDB_READ);

    // subarray: global
    subarray_layout = Layout::GLOBAL_ORDER;
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget, unsplittable);

    // subarray: row
    subarray_layout = Layout::ROW_MAJOR;
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget, unsplittable);

    // subarray: col
    subarray_layout = Layout::COL_MAJOR;
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget, unsplittable);

    // subarray: unordered
    subarray_layout = Layout::UNORDERED;
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget, unsplittable);
  }

  SECTION("# tile: row, cell: col") {
    create_default_2d_array(TILEDB_ROW_MAJOR, TILEDB_COL_MAJOR);
    write_default_2d_array();
    open_array(ctx_, array_, TILEDB_READ);

    // subarray: global
    subarray_layout = Layout::GLOBAL_ORDER;
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget, unsplittable);

    // subarray: row
    subarray_layout = Layout::ROW_MAJOR;
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget, unsplittable);

    // subarray: col
    subarray_layout = Layout::COL_MAJOR;
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget, unsplittable);

    // subarray: unordered
    subarray_layout = Layout::UNORDERED;
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget, unsplittable);
  }

  SECTION("# tile: col, cell: row") {
    create_default_2d_array(TILEDB_COL_MAJOR, TILEDB_ROW_MAJOR);
    write_default_2d_array();
    open_array(ctx_, array_, TILEDB_READ);

    // subarray: global
    subarray_layout = Layout::GLOBAL_ORDER;
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget, unsplittable);

    // subarray: row
    subarray_layout = Layout::ROW_MAJOR;
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget, unsplittable);

    // subarray: col
    subarray_layout = Layout::COL_MAJOR;
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget, unsplittable);

    // subarray: unordered
    subarray_layout = Layout::UNORDERED;
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget, unsplittable);
  }

  SECTION("# tile: col, cell: col") {
    create_default_2d_array(TILEDB_COL_MAJOR, TILEDB_COL_MAJOR);
    write_default_2d_array();
    open_array(ctx_, array_, TILEDB_READ);

    // subarray: global
    subarray_layout = Layout::GLOBAL_ORDER;
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget, unsplittable);

    // subarray: row
    subarray_layout = Layout::ROW_MAJOR;
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget, unsplittable);

    // subarray: col
    subarray_layout = Layout::COL_MAJOR;
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget, unsplittable);

    // subarray: unordered
    subarray_layout = Layout::UNORDERED;
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget, unsplittable);
  }

  close_array(ctx_, array_);
}

TEST_CASE_METHOD(
    SubarrayPartitionerDenseFx,
    "SubarrayPartitioner (Dense): 2D, single-range, split multiple",
    "[SubarrayPartitioner][dense][2D][1R][split_multiple]") {
  Layout subarray_layout;
  SubarrayRanges<uint64_t> ranges = {{2, 4}, {2, 4}};
  std::vector<SubarrayRanges<uint64_t>> partitions;
  ;
  uint64_t budget = 2 * sizeof(int);
  std::string attr = "a";
  bool unsplittable = false;

  SECTION("# tile: row, cell: row") {
    create_default_2d_array(TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR);
    write_default_2d_array();
    open_array(ctx_, array_, TILEDB_READ);

    // subarray: global
    subarray_layout = Layout::GLOBAL_ORDER;
    partitions = {
        {{2, 2}, {2, 2}},
        {{2, 2}, {3, 4}},
        {{3, 4}, {2, 2}},
        {{3, 3}, {3, 4}},
        {{4, 4}, {3, 4}},
    };
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget, unsplittable);

    // subarray: row
    subarray_layout = Layout::ROW_MAJOR;
    partitions = {
        {{2, 2}, {2, 3}},
        {{2, 2}, {4, 4}},
        {{3, 3}, {2, 3}},
        {{3, 3}, {4, 4}},
        {{4, 4}, {2, 3}},
        {{4, 4}, {4, 4}},
    };
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget, unsplittable);

    // subarray: col
    subarray_layout = Layout::COL_MAJOR;
    partitions = {
        {{2, 3}, {2, 2}},
        {{4, 4}, {2, 2}},
        {{2, 3}, {3, 3}},
        {{4, 4}, {3, 3}},
        {{2, 3}, {4, 4}},
        {{4, 4}, {4, 4}},
    };
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget, unsplittable);

    // subarray: unordered
    subarray_layout = Layout::UNORDERED;
    partitions = {
        {{2, 2}, {2, 3}},
        {{2, 2}, {4, 4}},
        {{3, 3}, {2, 3}},
        {{3, 3}, {4, 4}},
        {{4, 4}, {2, 3}},
        {{4, 4}, {4, 4}},
    };
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget, unsplittable);
  }

  SECTION("# tile: row, cell: col") {
    create_default_2d_array(TILEDB_ROW_MAJOR, TILEDB_COL_MAJOR);
    write_default_2d_array();
    open_array(ctx_, array_, TILEDB_READ);

    // subarray: global
    subarray_layout = Layout::GLOBAL_ORDER;
    partitions = {
        {{2, 2}, {2, 2}},
        {{2, 2}, {3, 4}},
        {{3, 4}, {2, 2}},
        {{3, 4}, {3, 3}},
        {{3, 4}, {4, 4}},
    };
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget, unsplittable);

    // subarray: row
    subarray_layout = Layout::ROW_MAJOR;
    partitions = {
        {{2, 2}, {2, 3}},
        {{2, 2}, {4, 4}},
        {{3, 3}, {2, 3}},
        {{3, 3}, {4, 4}},
        {{4, 4}, {2, 3}},
        {{4, 4}, {4, 4}},
    };
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget, unsplittable);

    // subarray: col
    subarray_layout = Layout::COL_MAJOR;
    partitions = {
        {{2, 3}, {2, 2}},
        {{4, 4}, {2, 2}},
        {{2, 3}, {3, 3}},
        {{4, 4}, {3, 3}},
        {{2, 3}, {4, 4}},
        {{4, 4}, {4, 4}},
    };
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget, unsplittable);

    // subarray: unordered
    subarray_layout = Layout::UNORDERED;
    partitions = {
        {{2, 3}, {2, 2}},
        {{4, 4}, {2, 2}},
        {{2, 3}, {3, 3}},
        {{4, 4}, {3, 3}},
        {{2, 3}, {4, 4}},
        {{4, 4}, {4, 4}},
    };
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget, unsplittable);
  }

  SECTION("# tile: col, cell: row") {
    create_default_2d_array(TILEDB_COL_MAJOR, TILEDB_ROW_MAJOR);
    write_default_2d_array();
    open_array(ctx_, array_, TILEDB_READ);

    // subarray: global
    subarray_layout = Layout::GLOBAL_ORDER;
    partitions = {
        {{2, 2}, {2, 2}},
        {{3, 4}, {2, 2}},
        {{2, 2}, {3, 4}},
        {{3, 3}, {3, 4}},
        {{4, 4}, {3, 4}},
    };
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget, unsplittable);

    // subarray: row
    subarray_layout = Layout::ROW_MAJOR;
    partitions = {
        {{2, 2}, {2, 3}},
        {{2, 2}, {4, 4}},
        {{3, 3}, {2, 3}},
        {{3, 3}, {4, 4}},
        {{4, 4}, {2, 3}},
        {{4, 4}, {4, 4}},
    };
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget, unsplittable);

    // subarray: col
    subarray_layout = Layout::COL_MAJOR;
    partitions = {
        {{2, 3}, {2, 2}},
        {{4, 4}, {2, 2}},
        {{2, 3}, {3, 3}},
        {{4, 4}, {3, 3}},
        {{2, 3}, {4, 4}},
        {{4, 4}, {4, 4}},
    };
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget, unsplittable);

    // subarray: unordered
    subarray_layout = Layout::UNORDERED;
    partitions = {
        {{2, 2}, {2, 3}},
        {{2, 2}, {4, 4}},
        {{3, 3}, {2, 3}},
        {{3, 3}, {4, 4}},
        {{4, 4}, {2, 3}},
        {{4, 4}, {4, 4}},
    };
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget, unsplittable);
  }

  SECTION("# tile: col, cell: col") {
    create_default_2d_array(TILEDB_COL_MAJOR, TILEDB_COL_MAJOR);
    write_default_2d_array();
    open_array(ctx_, array_, TILEDB_READ);

    // subarray: global
    subarray_layout = Layout::GLOBAL_ORDER;
    partitions = {
        {{2, 2}, {2, 2}},
        {{3, 4}, {2, 2}},
        {{2, 2}, {3, 4}},
        {{3, 4}, {3, 3}},
        {{3, 4}, {4, 4}},
    };
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget, unsplittable);

    // subarray: row
    subarray_layout = Layout::ROW_MAJOR;
    partitions = {
        {{2, 2}, {2, 3}},
        {{2, 2}, {4, 4}},
        {{3, 3}, {2, 3}},
        {{3, 3}, {4, 4}},
        {{4, 4}, {2, 3}},
        {{4, 4}, {4, 4}},
    };
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget, unsplittable);

    // subarray: col
    subarray_layout = Layout::COL_MAJOR;
    partitions = {
        {{2, 3}, {2, 2}},
        {{4, 4}, {2, 2}},
        {{2, 3}, {3, 3}},
        {{4, 4}, {3, 3}},
        {{2, 3}, {4, 4}},
        {{4, 4}, {4, 4}},
    };
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget, unsplittable);

    // subarray: unordered
    subarray_layout = Layout::UNORDERED;
    partitions = {
        {{2, 3}, {2, 2}},
        {{4, 4}, {2, 2}},
        {{2, 3}, {3, 3}},
        {{4, 4}, {3, 3}},
        {{2, 3}, {4, 4}},
        {{4, 4}, {4, 4}},
    };
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget, unsplittable);
  }

  close_array(ctx_, array_);
}

TEST_CASE_METHOD(
    SubarrayPartitionerDenseFx,
    "SubarrayPartitioner (Dense): 2D, multi-range, whole_subarray_fits",
    "[SubarrayPartitioner][dense][2D][MR][whole_subarray_fits]") {
  Layout subarray_layout;
  SubarrayRanges<uint64_t> ranges = {{1, 2, 3, 3, 4, 4}, {2, 3, 4, 4}};
  std::vector<SubarrayRanges<uint64_t>> partitions = {
      {{1, 2, 3, 3, 4, 4}, {2, 3, 4, 4}}};
  uint64_t budget = 100 * sizeof(int);
  std::string attr = "a";
  bool unsplittable = false;

  SECTION("# tile: row, cell: row") {
    create_default_2d_array(TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR);
    write_default_2d_array();
    open_array(ctx_, array_, TILEDB_READ);

    // subarray: row
    subarray_layout = Layout::ROW_MAJOR;
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget, unsplittable);

    // subarray: col
    subarray_layout = Layout::COL_MAJOR;
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget, unsplittable);

    // subarray: unordered
    subarray_layout = Layout::UNORDERED;
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget, unsplittable);
  }

  SECTION("# tile: row, cell: col") {
    create_default_2d_array(TILEDB_ROW_MAJOR, TILEDB_COL_MAJOR);
    write_default_2d_array();
    open_array(ctx_, array_, TILEDB_READ);

    // subarray: row
    subarray_layout = Layout::ROW_MAJOR;
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget, unsplittable);

    // subarray: col
    subarray_layout = Layout::COL_MAJOR;
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget, unsplittable);

    // subarray: unordered
    subarray_layout = Layout::UNORDERED;
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget, unsplittable);
  }

  SECTION("# tile: col, cell: row") {
    create_default_2d_array(TILEDB_COL_MAJOR, TILEDB_ROW_MAJOR);
    write_default_2d_array();
    open_array(ctx_, array_, TILEDB_READ);

    // subarray: row
    subarray_layout = Layout::ROW_MAJOR;
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget, unsplittable);

    // subarray: col
    subarray_layout = Layout::COL_MAJOR;
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget, unsplittable);

    // subarray: unordered
    subarray_layout = Layout::UNORDERED;
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget, unsplittable);
  }

  SECTION("# tile: col, cell: col") {
    create_default_2d_array(TILEDB_COL_MAJOR, TILEDB_COL_MAJOR);
    write_default_2d_array();
    open_array(ctx_, array_, TILEDB_READ);

    // subarray: row
    subarray_layout = Layout::ROW_MAJOR;
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget, unsplittable);

    // subarray: col
    subarray_layout = Layout::COL_MAJOR;
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget, unsplittable);

    // subarray: unordered
    subarray_layout = Layout::UNORDERED;
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget, unsplittable);
  }

  close_array(ctx_, array_);
}

TEST_CASE_METHOD(
    SubarrayPartitionerDenseFx,
    "SubarrayPartitioner (Dense): 2D, multi-range, split once",
    "[SubarrayPartitioner][dense][2D][MR][split_once]") {
  Layout subarray_layout;
  SubarrayRanges<uint64_t> ranges;
  std::vector<SubarrayRanges<uint64_t>> partitions;
  uint64_t budget = 0;
  std::string attr = "a";
  bool unsplittable = false;

  SECTION("# tile: row, cell: row") {
    create_default_2d_array(TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR);
    write_default_2d_array();
    open_array(ctx_, array_, TILEDB_READ);

    // subarray: row
    subarray_layout = Layout::ROW_MAJOR;
    partitions = {
        {{1, 2, 3, 3}, {2, 3, 4, 4}},
        {{4, 4}, {2, 3, 4, 4}},
    };
    ranges = {{1, 2, 3, 3, 4, 4}, {2, 3, 4, 4}};
    budget = 9 * sizeof(int);
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget, unsplittable);

    // subarray: col
    subarray_layout = Layout::COL_MAJOR;
    partitions = {
        {{1, 2, 3, 3, 4, 4}, {2, 3}},
        {{1, 2, 3, 3, 4, 4}, {4, 4}},
    };
    ranges = {{1, 2, 3, 3, 4, 4}, {2, 3, 4, 4}};
    budget = 9 * sizeof(int);
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget, unsplittable);

    // subarray: unordered
    subarray_layout = Layout::UNORDERED;
    partitions = {
        {{1, 2, 3, 3}, {2, 3, 4, 4}},
        {{4, 4}, {2, 3, 4, 4}},
    };
    ranges = {{1, 2, 3, 3, 4, 4}, {2, 3, 4, 4}};
    budget = 9 * sizeof(int);
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget, unsplittable);
  }

  SECTION("# tile: row, cell: col") {
    create_default_2d_array(TILEDB_ROW_MAJOR, TILEDB_COL_MAJOR);
    write_default_2d_array();
    open_array(ctx_, array_, TILEDB_READ);

    // subarray: row
    subarray_layout = Layout::ROW_MAJOR;
    partitions = {
        {{1, 2, 3, 3}, {2, 3, 4, 4}},
        {{4, 4}, {2, 3, 4, 4}},
    };
    ranges = {{1, 2, 3, 3, 4, 4}, {2, 3, 4, 4}};
    budget = 9 * sizeof(int);
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget, unsplittable);

    // subarray: col
    subarray_layout = Layout::COL_MAJOR;
    partitions = {
        {{1, 2, 3, 3, 4, 4}, {2, 3}},
        {{1, 2, 3, 3, 4, 4}, {4, 4}},
    };
    ranges = {{1, 2, 3, 3, 4, 4}, {2, 3, 4, 4}};
    budget = 9 * sizeof(int);
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget, unsplittable);

    // subarray: unordered
    subarray_layout = Layout::UNORDERED;
    partitions = {
        {{1, 2, 3, 3, 4, 4}, {2, 3}},
        {{1, 2, 3, 3, 4, 4}, {4, 4}},
    };
    ranges = {{1, 2, 3, 3, 4, 4}, {2, 3, 4, 4}};
    budget = 9 * sizeof(int);
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget, unsplittable);
  }

  SECTION("# tile: col, cell: row") {
    create_default_2d_array(TILEDB_COL_MAJOR, TILEDB_ROW_MAJOR);
    write_default_2d_array();
    open_array(ctx_, array_, TILEDB_READ);

    // subarray: row
    subarray_layout = Layout::ROW_MAJOR;
    partitions = {
        {{1, 2, 3, 3}, {2, 3, 4, 4}},
        {{4, 4}, {2, 3, 4, 4}},
    };
    ranges = {{1, 2, 3, 3, 4, 4}, {2, 3, 4, 4}};
    budget = 9 * sizeof(int);
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget, unsplittable);

    // subarray: col
    subarray_layout = Layout::COL_MAJOR;
    partitions = {
        {{1, 2, 3, 4}, {1, 2, 3, 3}},
        {{1, 2, 3, 4}, {4, 4}},
    };
    ranges = {{1, 2, 3, 4}, {1, 2, 3, 3, 4, 4}};
    budget = 12 * sizeof(int);
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget, unsplittable);

    // subarray: unordered
    subarray_layout = Layout::UNORDERED;
    partitions = {
        {{1, 2, 3, 3}, {2, 3, 4, 4}},
        {{4, 4}, {2, 3, 4, 4}},
    };
    ranges = {{1, 2, 3, 3, 4, 4}, {2, 3, 4, 4}};
    budget = 9 * sizeof(int);
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget, unsplittable);
  }

  SECTION("# tile: col, cell: col") {
    create_default_2d_array(TILEDB_COL_MAJOR, TILEDB_COL_MAJOR);
    write_default_2d_array();
    open_array(ctx_, array_, TILEDB_READ);

    // subarray: row
    subarray_layout = Layout::ROW_MAJOR;
    partitions = {
        {{1, 2, 3, 3}, {2, 3, 4, 4}},
        {{4, 4}, {2, 3, 4, 4}},
    };
    ranges = {{1, 2, 3, 3, 4, 4}, {2, 3, 4, 4}};
    budget = 9 * sizeof(int);
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget, unsplittable);

    // subarray: col
    subarray_layout = Layout::COL_MAJOR;
    partitions = {
        {{1, 2, 3, 4}, {1, 2, 3, 3}},
        {{1, 2, 3, 4}, {4, 4}},
    };
    ranges = {{1, 2, 3, 4}, {1, 2, 3, 3, 4, 4}};
    budget = 12 * sizeof(int);
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget, unsplittable);

    // subarray: unordered
    subarray_layout = Layout::UNORDERED;
    partitions = {
        {{1, 2, 3, 4}, {1, 2, 3, 3}},
        {{1, 2, 3, 4}, {4, 4}},
    };
    ranges = {{1, 2, 3, 4}, {1, 2, 3, 3, 4, 4}};
    budget = 12 * sizeof(int);
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget, unsplittable);
  }

  close_array(ctx_, array_);
}

TEST_CASE_METHOD(
    SubarrayPartitionerDenseFx,
    "SubarrayPartitioner (Dense): 2D, multi-range, calibrate",
    "[SubarrayPartitioner][dense][2D][MR][calibrate]") {
  Layout subarray_layout;
  SubarrayRanges<uint64_t> ranges = {{1, 2, 3, 3, 4, 4}, {2, 3, 4, 4}};
  std::vector<SubarrayRanges<uint64_t>> partitions = {
      {{1, 2, 3, 3, 4, 4}, {2, 3, 4, 4}}};
  uint64_t budget = 100 * sizeof(int);
  std::string attr = "a";
  bool unsplittable = false;

  SECTION("# tile: row, cell: row") {
    create_default_2d_array(TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR);
    write_default_2d_array();
    open_array(ctx_, array_, TILEDB_READ);

    // subarray: row
    subarray_layout = Layout::ROW_MAJOR;
    partitions = {
        {{1, 2, 3, 3}, {2, 3, 4, 4}},
        {{4, 4}, {2, 3, 4, 4}},
    };
    ranges = {{1, 2, 3, 3, 4, 4}, {2, 3, 4, 4}};
    budget = 11 * sizeof(int);
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget, unsplittable);

    // subarray: col
    subarray_layout = Layout::COL_MAJOR;
    partitions = {
        {{1, 2, 3, 3, 4, 4}, {2, 3}},
        {{1, 2, 3, 3, 4, 4}, {4, 4}},
    };
    ranges = {{1, 2, 3, 3, 4, 4}, {2, 3, 4, 4}};
    budget = 11 * sizeof(int);
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget, unsplittable);

    // subarray: unordered
    subarray_layout = Layout::UNORDERED;
    partitions = {
        {{1, 2, 3, 3}, {2, 3, 4, 4}},
        {{4, 4}, {2, 3, 4, 4}},
    };
    ranges = {{1, 2, 3, 3, 4, 4}, {2, 3, 4, 4}};
    budget = 11 * sizeof(int);
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget, unsplittable);
  }

  SECTION("# tile: row, cell: col") {
    create_default_2d_array(TILEDB_ROW_MAJOR, TILEDB_COL_MAJOR);
    write_default_2d_array();
    open_array(ctx_, array_, TILEDB_READ);

    // subarray: row
    subarray_layout = Layout::ROW_MAJOR;
    partitions = {
        {{1, 2, 3, 3}, {2, 3, 4, 4}},
        {{4, 4}, {2, 3, 4, 4}},
    };
    ranges = {{1, 2, 3, 3, 4, 4}, {2, 3, 4, 4}};
    budget = 11 * sizeof(int);
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget, unsplittable);

    // subarray: col
    subarray_layout = Layout::COL_MAJOR;
    partitions = {
        {{1, 2, 3, 3, 4, 4}, {2, 3}},
        {{1, 2, 3, 3, 4, 4}, {4, 4}},
    };
    ranges = {{1, 2, 3, 3, 4, 4}, {2, 3, 4, 4}};
    budget = 11 * sizeof(int);
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget, unsplittable);

    // subarray: unordered
    subarray_layout = Layout::UNORDERED;
    partitions = {
        {{1, 2, 3, 3, 4, 4}, {2, 3}},
        {{1, 2, 3, 3, 4, 4}, {4, 4}},
    };
    ranges = {{1, 2, 3, 3, 4, 4}, {2, 3, 4, 4}};
    budget = 11 * sizeof(int);
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget, unsplittable);
  }

  SECTION("# tile: col, cell: row") {
    create_default_2d_array(TILEDB_COL_MAJOR, TILEDB_ROW_MAJOR);
    write_default_2d_array();
    open_array(ctx_, array_, TILEDB_READ);

    // subarray: row
    subarray_layout = Layout::ROW_MAJOR;
    partitions = {
        {{1, 2, 3, 3}, {2, 3, 4, 4}},
        {{4, 4}, {2, 3, 4, 4}},
    };
    ranges = {{1, 2, 3, 3, 4, 4}, {2, 3, 4, 4}};
    budget = 11 * sizeof(int);
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget, unsplittable);

    // subarray: col
    subarray_layout = Layout::COL_MAJOR;
    partitions = {
        {{1, 2, 3, 4}, {1, 2}},
        {{1, 2, 3, 4}, {3, 3, 4, 4}},
    };
    ranges = {{1, 2, 3, 4}, {1, 2, 3, 3, 4, 4}};
    budget = 10 * sizeof(int);
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget, unsplittable);

    // subarray: unordered
    subarray_layout = Layout::UNORDERED;
    partitions = {
        {{1, 2, 3, 3}, {2, 3, 4, 4}},
        {{4, 4}, {2, 3, 4, 4}},
    };
    ranges = {{1, 2, 3, 3, 4, 4}, {2, 3, 4, 4}};
    budget = 11 * sizeof(int);
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget, unsplittable);
  }

  SECTION("# tile: col, cell: col") {
    create_default_2d_array(TILEDB_COL_MAJOR, TILEDB_COL_MAJOR);
    write_default_2d_array();
    open_array(ctx_, array_, TILEDB_READ);

    // subarray: row
    subarray_layout = Layout::ROW_MAJOR;
    partitions = {
        {{1, 2, 3, 3}, {2, 3, 4, 4}},
        {{4, 4}, {2, 3, 4, 4}},
    };
    ranges = {{1, 2, 3, 3, 4, 4}, {2, 3, 4, 4}};
    budget = 11 * sizeof(int);
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget, unsplittable);

    // subarray: col
    subarray_layout = Layout::COL_MAJOR;
    partitions = {
        {{1, 2, 3, 4}, {1, 2}},
        {{1, 2, 3, 4}, {3, 3, 4, 4}},
    };
    ranges = {{1, 2, 3, 4}, {1, 2, 3, 3, 4, 4}};
    budget = 10 * sizeof(int);
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget, unsplittable);

    // subarray: unordered
    subarray_layout = Layout::UNORDERED;
    partitions = {
        {{1, 2, 3, 4}, {1, 2}},
        {{1, 2, 3, 4}, {3, 3, 4, 4}},
    };
    ranges = {{1, 2, 3, 4}, {1, 2, 3, 3, 4, 4}};
    budget = 10 * sizeof(int);
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget, unsplittable);
  }

  close_array(ctx_, array_);
}

TEST_CASE_METHOD(
    SubarrayPartitionerDenseFx,
    "SubarrayPartitioner (Dense): 2D, multi-range, split multiple finer",
    "[SubarrayPartitioner][dense][2D][MR][split_multiple_finer]") {
  Layout subarray_layout;
  SubarrayRanges<uint64_t> ranges;
  std::vector<SubarrayRanges<uint64_t>> partitions;
  uint64_t budget = 100 * sizeof(int);
  std::string attr = "a";
  bool unsplittable = false;

  SECTION("# tile: row, cell: row") {
    create_default_2d_array(TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR);
    write_default_2d_array();
    open_array(ctx_, array_, TILEDB_READ);

    // subarray: row
    subarray_layout = Layout::ROW_MAJOR;
    partitions = {
        {{1, 1}, {2, 3, 4, 4}},
        {{2, 2}, {2, 3, 4, 4}},
        {{3, 3}, {2, 3, 4, 4}},
        {{4, 4}, {2, 3, 4, 4}},
    };
    ranges = {{1, 2, 3, 3, 4, 4}, {2, 3, 4, 4}};
    budget = 3 * sizeof(int);
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget, unsplittable);

    // subarray: col
    subarray_layout = Layout::COL_MAJOR;
    partitions = {
        {{1, 2, 3, 3}, {2, 2}},
        {{4, 4}, {2, 2}},
        {{1, 2, 3, 3}, {3, 3}},
        {{4, 4}, {3, 3}},
        {{1, 2, 3, 3}, {4, 4}},
        {{4, 4}, {4, 4}},
    };
    ranges = {{1, 2, 3, 3, 4, 4}, {2, 3, 4, 4}};
    budget = 3 * sizeof(int);
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget, unsplittable);

    // subarray: unordered
    subarray_layout = Layout::UNORDERED;
    partitions = {
        {{1, 1}, {2, 3}},
        {{2, 2}, {2, 3}},
        {{1, 2}, {4, 4}},
        {{3, 3}, {2, 3, 4, 4}},
        {{4, 4}, {2, 3, 4, 4}},
    };
    ranges = {{1, 2, 3, 3, 4, 4}, {2, 3, 4, 4}};
    budget = 3 * sizeof(int);
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget, unsplittable);
  }

  SECTION("# tile: row, cell: col") {
    create_default_2d_array(TILEDB_ROW_MAJOR, TILEDB_COL_MAJOR);
    write_default_2d_array();
    open_array(ctx_, array_, TILEDB_READ);

    // subarray: row
    subarray_layout = Layout::ROW_MAJOR;
    partitions = {
        {{1, 1}, {2, 3, 4, 4}},
        {{2, 2}, {2, 3, 4, 4}},
        {{3, 3}, {2, 3, 4, 4}},
        {{4, 4}, {2, 3, 4, 4}},
    };
    ranges = {{1, 2, 3, 3, 4, 4}, {2, 3, 4, 4}};
    budget = 3 * sizeof(int);
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget, unsplittable);

    // subarray: col
    subarray_layout = Layout::COL_MAJOR;
    partitions = {
        {{1, 2, 3, 3}, {2, 2}},
        {{4, 4}, {2, 2}},
        {{1, 2, 3, 3}, {3, 3}},
        {{4, 4}, {3, 3}},
        {{1, 2, 3, 3}, {4, 4}},
        {{4, 4}, {4, 4}},
    };
    ranges = {{1, 2, 3, 3, 4, 4}, {2, 3, 4, 4}};
    budget = 3 * sizeof(int);
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget, unsplittable);

    // subarray: unordered
    subarray_layout = Layout::UNORDERED;
    partitions = {
        {{1, 2}, {2, 2}},
        {{1, 2}, {3, 3}},
        {{3, 3}, {2, 3}},
        {{4, 4}, {2, 3}},
        {{1, 2, 3, 3}, {4, 4}},
        {{4, 4}, {4, 4}},
    };
    ranges = {{1, 2, 3, 3, 4, 4}, {2, 3, 4, 4}};
    budget = 3 * sizeof(int);
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget, unsplittable);
  }

  SECTION("# tile: col, cell: row") {
    create_default_2d_array(TILEDB_COL_MAJOR, TILEDB_ROW_MAJOR);
    write_default_2d_array();
    open_array(ctx_, array_, TILEDB_READ);

    // subarray: row
    subarray_layout = Layout::ROW_MAJOR;
    partitions = {
        {{1, 1}, {2, 3, 4, 4}},
        {{2, 2}, {2, 3, 4, 4}},
        {{3, 3}, {2, 3, 4, 4}},
        {{4, 4}, {2, 3, 4, 4}},
    };
    ranges = {{1, 2, 3, 3, 4, 4}, {2, 3, 4, 4}};
    budget = 3 * sizeof(int);
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget, unsplittable);

    // subarray: col
    subarray_layout = Layout::COL_MAJOR;
    partitions = {
        {{1, 2, 3, 4}, {1, 1}},
        {{1, 2, 3, 4}, {2, 2}},
        {{1, 2, 3, 4}, {3, 3}},
        {{1, 2, 3, 4}, {4, 4}},
    };
    ranges = {{1, 2, 3, 4}, {1, 2, 3, 3, 4, 4}};
    budget = 4 * sizeof(int);
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget, unsplittable);

    // subarray: unordered
    subarray_layout = Layout::UNORDERED;
    partitions = {
        {{1, 1}, {2, 3}},
        {{2, 2}, {2, 3}},
        {{1, 2}, {4, 4}},
        {{3, 3}, {2, 3, 4, 4}},
        {{4, 4}, {2, 3, 4, 4}},
    };
    ranges = {{1, 2, 3, 3, 4, 4}, {2, 3, 4, 4}};
    budget = 3 * sizeof(int);
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget, unsplittable);
  }

  SECTION("# tile: col, cell: col") {
    create_default_2d_array(TILEDB_COL_MAJOR, TILEDB_COL_MAJOR);
    write_default_2d_array();
    open_array(ctx_, array_, TILEDB_READ);

    // subarray: row
    subarray_layout = Layout::ROW_MAJOR;
    partitions = {
        {{1, 1}, {2, 3, 4, 4}},
        {{2, 2}, {2, 3, 4, 4}},
        {{3, 3}, {2, 3, 4, 4}},
        {{4, 4}, {2, 3, 4, 4}},
    };
    ranges = {{1, 2, 3, 3, 4, 4}, {2, 3, 4, 4}};
    budget = 3 * sizeof(int);
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget, unsplittable);

    // subarray: col
    subarray_layout = Layout::COL_MAJOR;
    partitions = {
        {{1, 2, 3, 4}, {1, 1}},
        {{1, 2, 3, 4}, {2, 2}},
        {{1, 2, 3, 4}, {3, 3}},
        {{1, 2, 3, 4}, {4, 4}},
    };
    ranges = {{1, 2, 3, 4}, {1, 2, 3, 3, 4, 4}};
    budget = 4 * sizeof(int);
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget, unsplittable);

    // subarray: unordered
    subarray_layout = Layout::UNORDERED;
    partitions = {
        {{1, 2}, {1, 2}},
        {{3, 4}, {1, 2}},
        {{1, 2, 3, 4}, {3, 3}},
        {{1, 2, 3, 4}, {4, 4}},
    };
    ranges = {{1, 2, 3, 4}, {1, 2, 3, 3, 4, 4}};
    budget = 4 * sizeof(int);
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget, unsplittable);
  }

  close_array(ctx_, array_);
}

TEST_CASE_METHOD(
    SubarrayPartitionerDenseFx,
    "SubarrayPartitioner (Dense): 2D, multi-range, unsplittable",
    "[SubarrayPartitioner][dense][2D][MR][unsplittable]") {
  Layout subarray_layout;
  std::vector<SubarrayRanges<uint64_t>> partitions = {{{1, 1}, {2, 2}}};
  SubarrayRanges<uint64_t> ranges = {{1, 2, 3, 3, 4, 4}, {2, 3, 4, 4}};
  uint64_t budget = 0 * sizeof(int);
  std::string attr = "a";
  bool unsplittable = true;

  SECTION("# tile: row, cell: row") {
    create_default_2d_array(TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR);
    write_default_2d_array();
    open_array(ctx_, array_, TILEDB_READ);

    // subarray: row
    subarray_layout = Layout::ROW_MAJOR;
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget, unsplittable);

    // subarray: col
    subarray_layout = Layout::COL_MAJOR;
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget, unsplittable);

    // subarray: unordered
    subarray_layout = Layout::UNORDERED;
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget, unsplittable);
  }

  SECTION("# tile: row, cell: col") {
    create_default_2d_array(TILEDB_ROW_MAJOR, TILEDB_COL_MAJOR);
    write_default_2d_array();
    open_array(ctx_, array_, TILEDB_READ);

    // subarray: row
    subarray_layout = Layout::ROW_MAJOR;
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget, unsplittable);

    // subarray: col
    subarray_layout = Layout::COL_MAJOR;
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget, unsplittable);

    // subarray: unordered
    subarray_layout = Layout::UNORDERED;
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget, unsplittable);
  }

  SECTION("# tile: col, cell: row") {
    create_default_2d_array(TILEDB_COL_MAJOR, TILEDB_ROW_MAJOR);
    write_default_2d_array();
    open_array(ctx_, array_, TILEDB_READ);

    // subarray: row
    subarray_layout = Layout::ROW_MAJOR;
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget, unsplittable);

    // subarray: col
    subarray_layout = Layout::COL_MAJOR;
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget, unsplittable);

    // subarray: unordered
    subarray_layout = Layout::UNORDERED;
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget, unsplittable);
  }

  SECTION("# tile: col, cell: col") {
    create_default_2d_array(TILEDB_COL_MAJOR, TILEDB_COL_MAJOR);
    write_default_2d_array();
    open_array(ctx_, array_, TILEDB_READ);

    // subarray: row
    subarray_layout = Layout::ROW_MAJOR;
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget, unsplittable);

    // subarray: col
    subarray_layout = Layout::COL_MAJOR;
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget, unsplittable);

    // subarray: unordered
    subarray_layout = Layout::UNORDERED;
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget, unsplittable);
  }

  close_array(ctx_, array_);
}
