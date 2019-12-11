/**
 * @file unit-SubarrayPartitioner-sparse.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2019 TileDB, Inc.
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
 * Tests the `SubarrayPartitioner` class for sparse arrays.
 */

#include "test/src/helpers.h"
#include "tiledb/sm/c_api/tiledb_struct_def.h"
#include "tiledb/sm/subarray/subarray_partitioner.h"

#ifdef _WIN32
#include "tiledb/sm/filesystem/win.h"
#else
#include "tiledb/sm/filesystem/posix.h"
#endif

#include <catch.hpp>
#include <iostream>

using namespace tiledb::sm;
using namespace tiledb::test;

/* ********************************* */
/*         STRUCT DEFINITION         */
/* ********************************* */

struct SubarrayPartitionerSparseFx {
  tiledb_ctx_t* ctx_;
  tiledb_vfs_t* vfs_;
  bool s3_supported_, hdfs_supported_;
  std::string temp_dir_;
  const std::string s3_bucket_name_ =
      "s3://" + random_bucket_name("tiledb") + "/";
  std::string array_name_;
  const char* ARRAY_NAME = "subarray_partitioner_sparse";
  tiledb_array_t* array_ = nullptr;
  uint64_t memory_budget_ = 1024 * 1024 * 1024;
  uint64_t memory_budget_var_ = 1024 * 1024 * 1024;

  SubarrayPartitionerSparseFx();
  ~SubarrayPartitionerSparseFx();

  /**
   * Helper function that creates a default 1D array, with the
   * input tile and cell order.
   */
  void create_default_1d_array(
      tiledb_layout_t tile_order, tiledb_layout_t cell_order);

  /**
   * Helper function that creates a default 1D array, with float
   * dimension and the input tile and cell order.
   */
  void create_default_1d_float_array(
      tiledb_layout_t tile_order, tiledb_layout_t cell_order);

  /**
   * Helper function that creates a default 2D array, with the
   * input tile and cell order.
   */
  void create_default_2d_array(
      tiledb_layout_t tile_order, tiledb_layout_t cell_order);

  /** Helper function that writes to the default 1D array. */
  void write_default_1d_array();

  /** Helper function that writes to a second default 1D array. */
  void write_default_1d_array_2();

  /** Helper function that writes to the default 1D float array. */
  void write_default_1d_float_array();

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

SubarrayPartitionerSparseFx::SubarrayPartitionerSparseFx() {
  ctx_ = nullptr;
  vfs_ = nullptr;
  hdfs_supported_ = false;
  s3_supported_ = false;

  get_supported_fs(&s3_supported_, &hdfs_supported_);
  create_ctx_and_vfs(s3_supported_, &ctx_, &vfs_);
  create_s3_bucket(s3_bucket_name_, s3_supported_, ctx_, vfs_);

// Create temporary directory based on the supported filesystem
#ifdef _WIN32
  temp_dir_ = tiledb::sm::Win::current_dir() + "\\tiledb_test\\";
#else
  temp_dir_ = "file://" + tiledb::sm::Posix::current_dir() + "/tiledb_test/";
#endif
  if (s3_supported_)
    temp_dir_ = s3_bucket_name_ + "tiledb/test/";
  // Disabling this for now, as this test takes a long time on HDFS
  // if (hdfs_supported_)
  //  temp_dir_ = "hdfs:///tiledb_test/";

  create_dir(temp_dir_, ctx_, vfs_);

  array_name_ = temp_dir_ + ARRAY_NAME;
  int rc = tiledb_array_alloc(ctx_, array_name_.c_str(), &array_);
  CHECK(rc == TILEDB_OK);
}

SubarrayPartitionerSparseFx::~SubarrayPartitionerSparseFx() {
  tiledb_array_free(&array_);
  remove_dir(temp_dir_, ctx_, vfs_);
  tiledb_ctx_free(&ctx_);
  tiledb_vfs_free(&vfs_);
}

void SubarrayPartitionerSparseFx::create_default_1d_array(
    tiledb_layout_t tile_order, tiledb_layout_t cell_order) {
  uint64_t domain[] = {1, 100};
  uint64_t tile_extent = 10;
  create_array(
      ctx_,
      array_name_,
      TILEDB_SPARSE,
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

void SubarrayPartitionerSparseFx::create_default_1d_float_array(
    tiledb_layout_t tile_order, tiledb_layout_t cell_order) {
  float domain[] = {1.0f, 100.0f};
  float tile_extent = 100.0f;
  create_array(
      ctx_,
      array_name_,
      TILEDB_SPARSE,
      {"d"},
      {TILEDB_FLOAT32},
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

void SubarrayPartitionerSparseFx::create_default_2d_array(
    tiledb_layout_t tile_order, tiledb_layout_t cell_order) {
  uint64_t domain[] = {1, 10};
  uint64_t tile_extent = 2;
  create_array(
      ctx_,
      array_name_,
      TILEDB_SPARSE,
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

void SubarrayPartitionerSparseFx::write_default_1d_array() {
  tiledb::test::QueryBuffers buffers;
  std::vector<uint64_t> coords = {2, 4, 5, 10, 12, 18};
  uint64_t coords_size = coords.size() * sizeof(uint64_t);
  std::vector<int> a = {1, 2, 3, 4, 5, 6};
  uint64_t a_size = a.size() * sizeof(int);
  std::vector<uint64_t> b_off = {0,
                                 sizeof(int),
                                 3 * sizeof(int),
                                 6 * sizeof(int),
                                 9 * sizeof(int),
                                 11 * sizeof(int)};
  uint64_t b_off_size = b_off.size() * sizeof(uint64_t);
  std::vector<int> b_val = {1, 2, 2, 3, 3, 3, 4, 4, 4, 5, 5, 6, 6, 6, 6};
  uint64_t b_val_size = b_val.size() * sizeof(int);
  buffers[TILEDB_COORDS] =
      tiledb::test::QueryBuffer({&coords[0], coords_size, nullptr, 0});
  buffers["a"] = tiledb::test::QueryBuffer({&a[0], a_size, nullptr, 0});
  buffers["b"] =
      tiledb::test::QueryBuffer({&b_off[0], b_off_size, &b_val[0], b_val_size});
  write_array(ctx_, array_name_, TILEDB_UNORDERED, buffers);
}

void SubarrayPartitionerSparseFx::write_default_1d_array_2() {
  tiledb::test::QueryBuffers buffers;
  std::vector<uint64_t> coords = {2, 4, 5, 10, 12, 18, 25, 27, 33, 40};
  uint64_t coords_size = coords.size() * sizeof(uint64_t);
  std::vector<int> a = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
  uint64_t a_size = a.size() * sizeof(int);
  std::vector<uint64_t> b_off = {0,
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
  buffers[TILEDB_COORDS] =
      tiledb::test::QueryBuffer({&coords[0], coords_size, nullptr, 0});
  buffers["a"] = tiledb::test::QueryBuffer({&a[0], a_size, nullptr, 0});
  buffers["b"] =
      tiledb::test::QueryBuffer({&b_off[0], b_off_size, &b_val[0], b_val_size});
  write_array(ctx_, array_name_, TILEDB_UNORDERED, buffers);
}

void SubarrayPartitionerSparseFx::write_default_1d_float_array() {
  tiledb::test::QueryBuffers buffers;
  std::vector<float> coords = {2.0f, 4.0f, 5.0f, 10.0f, 12.0f, 18.0f};
  uint64_t coords_size = coords.size() * sizeof(float);
  std::vector<int> a = {1, 2, 3, 4, 5, 6};
  uint64_t a_size = a.size() * sizeof(int);
  std::vector<uint64_t> b_off = {0,
                                 sizeof(int),
                                 3 * sizeof(int),
                                 6 * sizeof(int),
                                 9 * sizeof(int),
                                 11 * sizeof(int)};
  uint64_t b_off_size = b_off.size() * sizeof(uint64_t);
  std::vector<int> b_val = {1, 2, 2, 3, 3, 3, 4, 4, 4, 5, 5, 6, 6, 6, 6};
  uint64_t b_val_size = b_val.size() * sizeof(int);
  buffers[TILEDB_COORDS] =
      tiledb::test::QueryBuffer({&coords[0], coords_size, nullptr, 0});
  buffers["a"] = tiledb::test::QueryBuffer({&a[0], a_size, nullptr, 0});
  buffers["b"] =
      tiledb::test::QueryBuffer({&b_off[0], b_off_size, &b_val[0], b_val_size});
  write_array(ctx_, array_name_, TILEDB_UNORDERED, buffers);
}

void SubarrayPartitionerSparseFx::write_default_2d_array() {
  tiledb::test::QueryBuffers buffers;
  std::vector<uint64_t> coords = {1, 2, 2, 5, 3, 3, 3, 9, 4, 1, 4, 7};
  uint64_t coords_size = coords.size() * sizeof(uint64_t);
  std::vector<int> a = {1, 2, 3, 4, 5, 6};
  uint64_t a_size = a.size() * sizeof(int);
  std::vector<uint64_t> b_off = {0,
                                 sizeof(int),
                                 3 * sizeof(int),
                                 6 * sizeof(int),
                                 9 * sizeof(int),
                                 11 * sizeof(int)};
  uint64_t b_off_size = b_off.size() * sizeof(uint64_t);
  std::vector<int> b_val = {1, 2, 2, 3, 3, 3, 4, 4, 4, 5, 5, 6, 6, 6, 6};
  uint64_t b_val_size = b_val.size() * sizeof(int);
  buffers[TILEDB_COORDS] =
      tiledb::test::QueryBuffer({&coords[0], coords_size, nullptr, 0});
  buffers["a"] = tiledb::test::QueryBuffer({&a[0], a_size, nullptr, 0});
  buffers["b"] =
      tiledb::test::QueryBuffer({&b_off[0], b_off_size, &b_val[0], b_val_size});
  write_array(ctx_, array_name_, TILEDB_UNORDERED, buffers);
}

template <class T>
void SubarrayPartitionerSparseFx::test_subarray_partitioner(
    Layout subarray_layout,
    const SubarrayRanges<T>& ranges,
    const std::vector<SubarrayRanges<T>>& partitions,
    const std::string& attr,
    uint64_t budget,
    bool unsplittable) {
  Subarray subarray;
  create_subarray(array_->array_, ranges, subarray_layout, &subarray);

  SubarrayPartitioner subarray_partitioner(
      subarray, memory_budget_, memory_budget_var_);
  auto st = subarray_partitioner.set_result_budget(attr.c_str(), budget);
  CHECK(st.ok());

  check_partitions(subarray_partitioner, partitions, unsplittable);
}

template <class T>
void SubarrayPartitionerSparseFx::test_subarray_partitioner(
    Layout subarray_layout,
    const SubarrayRanges<T>& ranges,
    const std::vector<SubarrayRanges<T>>& partitions,
    uint64_t budget,
    uint64_t budget_var,
    bool unsplittable) {
  Subarray subarray;
  create_subarray(array_->array_, ranges, subarray_layout, &subarray);

  SubarrayPartitioner subarray_partitioner(
      subarray, memory_budget_, memory_budget_var_);

  // Note: this is necessary, otherwise the subarray partitioner does
  // not check if the memory budget is exceeded for attributes whose
  // result budget is not set.
  auto st = subarray_partitioner.set_result_budget(TILEDB_COORDS, 1000000);
  CHECK(st.ok());
  st = subarray_partitioner.set_result_budget("a", 1000000);
  CHECK(st.ok());
  st = subarray_partitioner.set_result_budget("b", 1000000, 1000000);
  CHECK(st.ok());

  st = subarray_partitioner.set_memory_budget(budget, budget_var);
  CHECK(st.ok());

  check_partitions(subarray_partitioner, partitions, unsplittable);
}

/* ********************************* */
/*                TESTS              */
/* ********************************* */

TEST_CASE_METHOD(
    SubarrayPartitionerSparseFx,
    "SubarrayPartitioner (Sparse): 1D, single-range, empty array",
    "[SubarrayPartitioner][sparse][1D][1R][empty_array]") {
  Layout subarray_layout;
  SubarrayRanges<uint64_t> ranges = {};
  std::vector<SubarrayRanges<uint64_t>> partitions = {{{1, 100}}};
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
    SubarrayPartitionerSparseFx,
    "SubarrayPartitioner (Sparse): 1D, single-range, whole subarray fits",
    "[SubarrayPartitioner][sparse][1D][1R][whole_subarray_fits]") {
  Layout subarray_layout;
  SubarrayRanges<uint64_t> ranges = {};
  std::vector<SubarrayRanges<uint64_t>> partitions = {{{1, 100}}};
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
    SubarrayPartitionerSparseFx,
    "SubarrayPartitioner (Sparse): 1D, single-range, split once",
    "[SubarrayPartitioner][sparse][1D][1R][split_once]") {
  Layout subarray_layout;
  SubarrayRanges<uint64_t> ranges = {{3, 11}};
  std::vector<SubarrayRanges<uint64_t>> partitions = {{{1, 100}}};
  uint64_t budget = 3 * sizeof(int);
  std::string attr = "a";
  bool unsplittable = false;

  create_default_1d_array(TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR);
  write_default_1d_array();
  open_array(ctx_, array_, TILEDB_READ);

  // subarray: global
  subarray_layout = Layout::GLOBAL_ORDER;
  partitions = {{{3, 6}}, {{7, 10}}, {{11, 11}}};
  test_subarray_partitioner(
      subarray_layout, ranges, partitions, attr, budget, unsplittable);

  // subarray: row
  subarray_layout = Layout::ROW_MAJOR;
  partitions = {{{3, 7}}, {{8, 11}}};
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
    SubarrayPartitionerSparseFx,
    "SubarrayPartitioner (Sparse): 1D, single-range, unsplittable at once",
    "[SubarrayPartitioner][sparse][1D][1R][unsplittable_at_once]") {
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
    SubarrayPartitionerSparseFx,
    "SubarrayPartitioner (Sparse): 1D, single-range, split multiple",
    "[SubarrayPartitioner][sparse][1D][1R][split_multiple]") {
  Layout subarray_layout;
  SubarrayRanges<uint64_t> ranges = {{2, 18}};
  std::vector<SubarrayRanges<uint64_t>> partitions = {
      {{2, 4}}, {{5, 6}}, {{7, 10}}, {{11, 18}}};
  uint64_t budget = 2 * sizeof(int);
  std::string attr = "a";
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
    SubarrayPartitionerSparseFx,
    "SubarrayPartitioner (Sparse): 1D, single-range, unsplittable after "
    "multiple",
    "[SubarrayPartitioner][sparse][1D][1R][unsplittable_after_multiple]") {
  Layout subarray_layout;
  SubarrayRanges<uint64_t> ranges = {{2, 18}};
  std::vector<SubarrayRanges<uint64_t>> partitions = {{{2, 2}}};
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
    SubarrayPartitionerSparseFx,
    "SubarrayPartitioner (Sparse): 1D, single-range, unsplittable but ok after "
    "budget reset",
    "[SubarrayPartitioner][sparse][1D][1R][unsplittable_but_then_ok]") {
  create_default_1d_array(TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR);
  write_default_1d_array();
  open_array(ctx_, array_, TILEDB_READ);

  Subarray subarray;
  SubarrayRanges<uint64_t> ranges = {{2, 18}};
  Layout subarray_layout = Layout::GLOBAL_ORDER;
  std::vector<SubarrayRanges<uint64_t>> partitions = {{{2, 2}}};
  std::vector<SubarrayRanges<uint64_t>> partitions_after = {
      {{3, 3}}, {{4, 4}}, {{5, 6}}, {{7, 10}}, {{11, 18}}};

  create_subarray(array_->array_, ranges, subarray_layout, &subarray);

  SubarrayPartitioner subarray_partitioner(
      subarray, memory_budget_, memory_budget_var_);
  auto st = subarray_partitioner.set_result_budget("a", 100);
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
    SubarrayPartitionerSparseFx,
    "SubarrayPartitioner (Sparse): 1D, single-range, float, split multiple",
    "[SubarrayPartitioner][sparse][1D][1R][float][split_multiple]") {
  Layout subarray_layout;
  auto max = std::numeric_limits<float>::max();
  SubarrayRanges<float> ranges = {{2.0f, 18.0f}};
  std::vector<SubarrayRanges<float>> partitions = {
      {{2.0f, 4.0f}},
      {{std::nextafter(4.0f, max), 6.0f}},
      {{std::nextafter(6.0f, max), 10.0f}},
      {{std::nextafter(10.0f, max), 18.0f}}};
  uint64_t budget = 2 * sizeof(int);
  std::string attr = "a";
  bool unsplittable = false;

  create_default_1d_float_array(TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR);
  write_default_1d_float_array();
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
    SubarrayPartitionerSparseFx,
    "SubarrayPartitioner (Sparse): 1D, single-range, float, unsplittable after "
    "multiple",
    "[SubarrayPartitioner][sparse][1D][1R][float][unsplittable_after_"
    "multiple]") {
  Layout subarray_layout;
  SubarrayRanges<float> ranges = {{2.0f, 18.0f}};
  std::vector<SubarrayRanges<float>> partitions = {{{2.0f, 2.0f}}};
  uint64_t budget = 0;
  std::string attr = "a";
  bool unsplittable = true;

  create_default_1d_float_array(TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR);
  write_default_1d_float_array();
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
    SubarrayPartitionerSparseFx,
    "SubarrayPartitioner (Sparse): 1D, single-range, float, whole subarray "
    "fits",
    "[SubarrayPartitioner][sparse][1D][1R][float][whole_subarray_fits]") {
  Layout subarray_layout;
  SubarrayRanges<float> ranges = {{2.0f, 18.0f}};
  std::vector<SubarrayRanges<float>> partitions = {{{2.0f, 18.0f}}};
  uint64_t budget = 100000;
  std::string attr = "a";
  bool unsplittable = false;

  create_default_1d_float_array(TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR);
  write_default_1d_float_array();
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
    SubarrayPartitionerSparseFx,
    "SubarrayPartitioner (Sparse): 1D, single-range, memory budget",
    "[SubarrayPartitioner][sparse][1D][1R][memory_budget]") {
  Layout subarray_layout;
  SubarrayRanges<uint64_t> ranges = {};
  std::vector<SubarrayRanges<uint64_t>> partitions = {};
  uint64_t budget = 16;
  uint64_t budget_var = 100000;
  bool unsplittable = false;

  create_default_1d_array(TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR);
  write_default_1d_array_2();
  open_array(ctx_, array_, TILEDB_READ);

  // subarray: global
  subarray_layout = Layout::GLOBAL_ORDER;
  partitions = {
      {{1, 3}},
      {{4, 4}},
      {{5, 5}},
      {{6, 10}},
      {{11, 20}},
      {{21, 30}},
      {{31, 40}},
      {{41, 100}},
  };
  test_subarray_partitioner(
      subarray_layout, ranges, partitions, budget, budget_var, unsplittable);

  // subarray: row
  subarray_layout = Layout::ROW_MAJOR;
  partitions = {
      {{1, 4}},
      {{5, 7}},
      {{8, 10}},
      {{11, 13}},
      {{14, 19}},
      {{20, 25}},
      {{26, 32}},
      {{33, 38}},
      {{39, 50}},
      {{51, 100}},
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
    SubarrayPartitionerSparseFx,
    "SubarrayPartitioner (Sparse): 1D, multi-range, whole subarray fits",
    "[SubarrayPartitioner][sparse][1D][MR][whole_subarray_fits]") {
  Layout subarray_layout;
  SubarrayRanges<uint64_t> ranges = {{5, 10, 25, 27, 33, 50}};
  std::vector<SubarrayRanges<uint64_t>> partitions = {
      {{5, 10, 25, 27, 33, 50}}};
  uint64_t budget = 100000;
  std::string attr = "a";
  bool unsplittable = false;

  create_default_1d_array(TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR);
  write_default_1d_array_2();
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
    SubarrayPartitionerSparseFx,
    "SubarrayPartitioner (Sparse): 1D, multi-range, split once",
    "[SubarrayPartitioner][sparse][1D][MR][split_once]") {
  Layout subarray_layout;
  SubarrayRanges<uint64_t> ranges = {{5, 10, 25, 27, 33, 50}};
  std::vector<SubarrayRanges<uint64_t>> partitions = {{{5, 10, 25, 27}},
                                                      {{33, 50}}};
  uint64_t budget = 4 * sizeof(int);
  std::string attr = "a";
  bool unsplittable = false;

  create_default_1d_array(TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR);
  write_default_1d_array_2();
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
    SubarrayPartitionerSparseFx,
    "SubarrayPartitioner (Sparse): 1D, multi-range, split multiple",
    "[SubarrayPartitioner][sparse][1D][MR][split_multiple]") {
  Layout subarray_layout;
  SubarrayRanges<uint64_t> ranges = {{5, 10, 25, 27, 33, 50}};
  std::vector<SubarrayRanges<uint64_t>> partitions = {
      {{5, 10}}, {{25, 27}}, {{33, 50}}};
  uint64_t budget = 2 * sizeof(int);
  std::string attr = "a";
  bool unsplittable = false;

  create_default_1d_array(TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR);
  write_default_1d_array_2();
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
    SubarrayPartitionerSparseFx,
    "SubarrayPartitioner (Sparse): 1D, multi-range, split multiple finer",
    "[SubarrayPartitioner][sparse][1D][MR][split_multiple_finer]") {
  Layout subarray_layout;
  SubarrayRanges<uint64_t> ranges = {{5, 10, 25, 27, 33, 40}};
  std::vector<SubarrayRanges<uint64_t>> partitions = {
      {{5, 7}},
      {{8, 10}},
      {{25, 26}},
      {{27, 27}},
      {{33, 36}},
      {{37, 40}},
  };
  uint64_t budget = 2 * sizeof(int) - 1;
  std::string attr = "a";
  bool unsplittable = false;

  create_default_1d_array(TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR);
  write_default_1d_array_2();
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
    SubarrayPartitionerSparseFx,
    "SubarrayPartitioner (Sparse): 1D, multi-range, unsplittable",
    "[SubarrayPartitioner][sparse][1D][MR][unsplittable]") {
  Layout subarray_layout;
  SubarrayRanges<uint64_t> ranges = {{5, 10, 25, 27, 33, 40}};
  std::vector<SubarrayRanges<uint64_t>> partitions = {{{5, 5}}};
  uint64_t budget = 1;
  std::string attr = "a";
  bool unsplittable = true;

  create_default_1d_array(TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR);
  write_default_1d_array_2();
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
    SubarrayPartitionerSparseFx,
    "SubarrayPartitioner (Sparse): 2D, single-range, whole subarray fits",
    "[SubarrayPartitioner][sparse][2D][1R][whole_subarray_fits]") {
  Layout subarray_layout;
  SubarrayRanges<uint64_t> ranges = {{2, 10}, {2, 10}};
  std::vector<SubarrayRanges<uint64_t>> partitions = {{{2, 10}, {2, 10}}};
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
    SubarrayPartitionerSparseFx,
    "SubarrayPartitioner (Sparse): 2D, single-range, split multiple",
    "[SubarrayPartitioner][sparse][2D][1R][split_multiple]") {
  Layout subarray_layout;
  SubarrayRanges<uint64_t> ranges = {{3, 4}, {1, 10}};
  std::vector<SubarrayRanges<uint64_t>> partitions = {};
  std::string attr = TILEDB_COORDS;
  uint64_t budget = 2 * sizeof(uint64_t);

  SECTION("# tile: row, cell: row") {
    create_default_2d_array(TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR);
    write_default_2d_array();
    open_array(ctx_, array_, TILEDB_READ);

    // subarray: global
    subarray_layout = Layout::GLOBAL_ORDER;
    partitions = {
        {{3, 3}, {1, 2}},
        {{4, 4}, {1, 2}},
        {{3, 4}, {3, 4}},
        {{3, 4}, {5, 6}},
        {{3, 3}, {7, 8}},
        {{4, 4}, {7, 8}},
        {{3, 4}, {9, 10}},
    };
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget);

    // subarray: row
    subarray_layout = Layout::ROW_MAJOR;
    partitions = {
        {{3, 3}, {1, 5}},
        {{3, 3}, {6, 10}},
        {{4, 4}, {1, 5}},
        {{4, 4}, {6, 10}},
    };
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget);

    // subarray: col
    subarray_layout = Layout::COL_MAJOR;
    partitions = {
        {{3, 4}, {1, 1}},
        {{3, 4}, {2, 2}},
        {{3, 4}, {3, 3}},
        {{3, 4}, {4, 5}},
        {{3, 4}, {6, 7}},
        {{3, 4}, {8, 8}},
        {{3, 4}, {9, 10}},
    };
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget);

    // subarray: unordered
    subarray_layout = Layout::UNORDERED;
    partitions = {
        {{3, 3}, {1, 5}},
        {{3, 3}, {6, 10}},
        {{4, 4}, {1, 5}},
        {{4, 4}, {6, 10}},
    };
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget);
  }

  SECTION("# tile: row, cell: col") {
    create_default_2d_array(TILEDB_ROW_MAJOR, TILEDB_COL_MAJOR);
    write_default_2d_array();
    open_array(ctx_, array_, TILEDB_READ);

    // subarray: global
    subarray_layout = Layout::GLOBAL_ORDER;
    partitions = {
        {{3, 4}, {1, 1}},
        {{3, 4}, {2, 2}},
        {{3, 4}, {3, 4}},
        {{3, 4}, {5, 6}},
        {{3, 4}, {7, 7}},
        {{3, 4}, {8, 8}},
        {{3, 4}, {9, 10}},
    };
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget);

    // subarray: row
    subarray_layout = Layout::ROW_MAJOR;
    partitions = {
        {{3, 3}, {1, 5}},
        {{3, 3}, {6, 10}},
        {{4, 4}, {1, 5}},
        {{4, 4}, {6, 10}},
    };
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget);

    // subarray: col
    subarray_layout = Layout::COL_MAJOR;
    partitions = {
        {{3, 4}, {1, 1}},
        {{3, 4}, {2, 2}},
        {{3, 4}, {3, 3}},
        {{3, 4}, {4, 5}},
        {{3, 4}, {6, 7}},
        {{3, 4}, {8, 8}},
        {{3, 4}, {9, 10}},
    };
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget);

    // subarray: unordered
    subarray_layout = Layout::UNORDERED;
    partitions = {
        {{3, 4}, {1, 1}},
        {{3, 4}, {2, 2}},
        {{3, 4}, {3, 3}},
        {{3, 4}, {4, 5}},
        {{3, 4}, {6, 7}},
        {{3, 4}, {8, 8}},
        {{3, 4}, {9, 10}},
    };
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget);
  }

  SECTION("# tile: col, cell: row") {
    create_default_2d_array(TILEDB_COL_MAJOR, TILEDB_ROW_MAJOR);
    write_default_2d_array();
    open_array(ctx_, array_, TILEDB_READ);

    // subarray: global
    subarray_layout = Layout::GLOBAL_ORDER;
    partitions = {
        {{3, 4}, {1, 2}},
        {{3, 4}, {3, 4}},
        {{3, 4}, {5, 6}},
        {{3, 3}, {7, 8}},
        {{4, 4}, {7, 8}},
        {{3, 4}, {9, 10}},
    };
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget);

    // subarray: row
    subarray_layout = Layout::ROW_MAJOR;
    partitions = {
        {{3, 3}, {1, 3}},
        {{3, 3}, {4, 5}},
        {{3, 3}, {6, 10}},
        {{4, 4}, {1, 5}},
        {{4, 4}, {6, 10}},
    };
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget);

    // subarray: col
    subarray_layout = Layout::COL_MAJOR;
    partitions = {
        {{3, 4}, {1, 2}},
        {{3, 4}, {3, 3}},
        {{3, 4}, {4, 5}},
        {{3, 4}, {6, 7}},
        {{3, 4}, {8, 8}},
        {{3, 4}, {9, 10}},
    };
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget);

    // subarray: unordered
    subarray_layout = Layout::UNORDERED;
    partitions = {
        {{3, 3}, {1, 3}},
        {{3, 3}, {4, 5}},
        {{3, 3}, {6, 10}},
        {{4, 4}, {1, 5}},
        {{4, 4}, {6, 10}},
    };
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget);
  }

  SECTION("# tile: col, cell: col") {
    create_default_2d_array(TILEDB_COL_MAJOR, TILEDB_COL_MAJOR);
    write_default_2d_array();
    open_array(ctx_, array_, TILEDB_READ);

    // subarray: global
    subarray_layout = Layout::GLOBAL_ORDER;
    partitions = {
        {{3, 4}, {1, 2}},
        {{3, 4}, {3, 4}},
        {{3, 4}, {5, 6}},
        {{3, 4}, {7, 7}},
        {{3, 4}, {8, 8}},
        {{3, 4}, {9, 10}},
    };
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget);

    // subarray: row
    subarray_layout = Layout::ROW_MAJOR;
    partitions = {
        {{3, 3}, {1, 3}},
        {{3, 3}, {4, 5}},
        {{3, 3}, {6, 10}},
        {{4, 4}, {1, 5}},
        {{4, 4}, {6, 10}},
    };
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget);

    // subarray: col
    subarray_layout = Layout::COL_MAJOR;
    partitions = {
        {{3, 4}, {1, 2}},
        {{3, 4}, {3, 3}},
        {{3, 4}, {4, 5}},
        {{3, 4}, {6, 7}},
        {{3, 4}, {8, 8}},
        {{3, 4}, {9, 10}},
    };
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget);

    // subarray: unordered
    subarray_layout = Layout::UNORDERED;
    partitions = {
        {{3, 4}, {1, 2}},
        {{3, 4}, {3, 3}},
        {{3, 4}, {4, 5}},
        {{3, 4}, {6, 7}},
        {{3, 4}, {8, 8}},
        {{3, 4}, {9, 10}},
    };
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget);
  }

  close_array(ctx_, array_);
}

TEST_CASE_METHOD(
    SubarrayPartitionerSparseFx,
    "SubarrayPartitioner (Sparse): 2D, single-range, unsplittable",
    "[SubarrayPartitioner][sparse][2D][1R][unsplittable]") {
  Layout subarray_layout;
  SubarrayRanges<uint64_t> ranges = {{2, 10}, {2, 10}};
  std::vector<SubarrayRanges<uint64_t>> partitions = {{{2, 2}, {2, 2}}};
  uint64_t budget = 0;
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
    SubarrayPartitionerSparseFx,
    "SubarrayPartitioner (Sparse): 2D, multi-range, whole subarray fits",
    "[SubarrayPartitioner][sparse][2D][MR][whole_subarray_fits]") {
  Layout subarray_layout;
  SubarrayRanges<uint64_t> ranges = {{1, 2, 3, 3, 4, 4}, {2, 3, 4, 5}};
  std::vector<SubarrayRanges<uint64_t>> partitions = {
      {{1, 2, 3, 3, 4, 4}, {2, 3, 4, 5}}};
  uint64_t budget = 10000;
  std::string attr = "a";

  SECTION("# tile: row, cell: row") {
    create_default_2d_array(TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR);
    write_default_2d_array();
    open_array(ctx_, array_, TILEDB_READ);

    // subarray: row
    subarray_layout = Layout::ROW_MAJOR;
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget);

    // subarray: col
    subarray_layout = Layout::COL_MAJOR;
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget);

    // subarray: unordered
    subarray_layout = Layout::UNORDERED;
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget);
  }

  SECTION("# tile: row, cell: col") {
    create_default_2d_array(TILEDB_ROW_MAJOR, TILEDB_COL_MAJOR);
    write_default_2d_array();
    open_array(ctx_, array_, TILEDB_READ);

    // subarray: row
    subarray_layout = Layout::ROW_MAJOR;
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget);

    // subarray: col
    subarray_layout = Layout::COL_MAJOR;
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget);

    // subarray: unordered
    subarray_layout = Layout::UNORDERED;
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget);
  }

  SECTION("# tile: col, cell: row") {
    create_default_2d_array(TILEDB_COL_MAJOR, TILEDB_ROW_MAJOR);
    write_default_2d_array();
    open_array(ctx_, array_, TILEDB_READ);

    // subarray: row
    subarray_layout = Layout::ROW_MAJOR;
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget);

    // subarray: col
    subarray_layout = Layout::COL_MAJOR;
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget);

    // subarray: unordered
    subarray_layout = Layout::UNORDERED;
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget);
  }

  SECTION("# tile: col, cell: col") {
    create_default_2d_array(TILEDB_COL_MAJOR, TILEDB_COL_MAJOR);
    write_default_2d_array();
    open_array(ctx_, array_, TILEDB_READ);

    // subarray: row
    subarray_layout = Layout::ROW_MAJOR;
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget);

    // subarray: col
    subarray_layout = Layout::COL_MAJOR;
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget);

    // subarray: unordered
    subarray_layout = Layout::UNORDERED;
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget);
  }

  close_array(ctx_, array_);
}

TEST_CASE_METHOD(
    SubarrayPartitionerSparseFx,
    "SubarrayPartitioner (Sparse): 2D, multi-range, split once",
    "[SubarrayPartitioner][sparse][2D][MR][split_once]") {
  Layout subarray_layout;
  std::vector<SubarrayRanges<uint64_t>> partitions = {};
  SubarrayRanges<uint64_t> ranges;
  std::string attr = "a";
  uint64_t budget = 4 * sizeof(int);

  SECTION("# tile: row, cell: row") {
    create_default_2d_array(TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR);
    write_default_2d_array();
    open_array(ctx_, array_, TILEDB_READ);

    // subarray: row
    subarray_layout = Layout::ROW_MAJOR;
    ranges = {{1, 2, 3, 3, 4, 4}, {2, 5, 6, 9}};
    partitions = {{{1, 2, 3, 3}, {2, 5, 6, 9}}, {{4, 4}, {2, 5, 6, 9}}};
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget);

    // subarray: col
    subarray_layout = Layout::COL_MAJOR;
    ranges = {{1, 2, 3, 4}, {1, 2, 3, 5, 7, 9}};
    partitions = {
        {{1, 2, 3, 4}, {1, 2}}, {{1, 2, 3, 4}, {3, 5}}, {{1, 2, 3, 4}, {7, 9}}};
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget);

    // subarray: unordered
    subarray_layout = Layout::UNORDERED;
    ranges = {{1, 2, 3, 3, 4, 4}, {2, 5, 6, 9}};
    partitions = {{{1, 2, 3, 3}, {2, 5, 6, 9}}, {{4, 4}, {2, 5, 6, 9}}};
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget);
  }

  SECTION("# tile: row, cell: col") {
    create_default_2d_array(TILEDB_ROW_MAJOR, TILEDB_COL_MAJOR);
    write_default_2d_array();
    open_array(ctx_, array_, TILEDB_READ);

    // subarray: row
    subarray_layout = Layout::ROW_MAJOR;
    ranges = {{1, 2, 3, 3, 4, 4}, {2, 5, 6, 9}};
    partitions = {{{1, 2, 3, 3}, {2, 5, 6, 9}}, {{4, 4}, {2, 5, 6, 9}}};
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget);

    // subarray: col
    subarray_layout = Layout::COL_MAJOR;
    ranges = {{1, 2, 3, 4}, {1, 2, 3, 5, 7, 9}};
    partitions = {
        {{1, 2, 3, 4}, {1, 2}}, {{1, 2, 3, 4}, {3, 5}}, {{1, 2, 3, 4}, {7, 9}}};
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget);

    // subarray: unordered
    subarray_layout = Layout::UNORDERED;
    ranges = {{1, 2, 3, 4}, {1, 2, 3, 5, 7, 9}};
    partitions = {
        {{1, 2, 3, 4}, {1, 2}}, {{1, 2, 3, 4}, {3, 5}}, {{1, 2, 3, 4}, {7, 9}}};
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget);
  }

  SECTION("# tile: col, cell: row") {
    create_default_2d_array(TILEDB_COL_MAJOR, TILEDB_ROW_MAJOR);
    write_default_2d_array();
    open_array(ctx_, array_, TILEDB_READ);

    // subarray: row
    subarray_layout = Layout::ROW_MAJOR;
    ranges = {{1, 2, 3, 3, 4, 4}, {2, 5, 6, 9}};
    partitions = {{{1, 2, 3, 3}, {2, 5, 6, 9}}, {{4, 4}, {2, 5, 6, 9}}};
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget);

    // subarray: col
    subarray_layout = Layout::COL_MAJOR;
    ranges = {{1, 2, 3, 4}, {1, 2, 3, 5, 7, 9}};
    partitions = {{{1, 2, 3, 4}, {1, 2, 3, 5}}, {{1, 2, 3, 4}, {7, 9}}};
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget);

    // subarray: unordered
    subarray_layout = Layout::UNORDERED;
    ranges = {{1, 2, 3, 3, 4, 4}, {2, 5, 6, 9}};
    partitions = {{{1, 2, 3, 3}, {2, 5, 6, 9}}, {{4, 4}, {2, 5, 6, 9}}};
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget);
  }

  SECTION("# tile: col, cell: col") {
    create_default_2d_array(TILEDB_COL_MAJOR, TILEDB_COL_MAJOR);
    write_default_2d_array();
    open_array(ctx_, array_, TILEDB_READ);

    // subarray: row
    subarray_layout = Layout::ROW_MAJOR;
    ranges = {{1, 2, 3, 3, 4, 4}, {2, 5, 6, 9}};
    partitions = {{{1, 2, 3, 3}, {2, 5, 6, 9}}, {{4, 4}, {2, 5, 6, 9}}};
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget);

    // subarray: col
    subarray_layout = Layout::COL_MAJOR;
    ranges = {{1, 2, 3, 4}, {1, 2, 3, 5, 7, 9}};
    partitions = {{{1, 2, 3, 4}, {1, 2, 3, 5}}, {{1, 2, 3, 4}, {7, 9}}};
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget);

    // subarray: unordered
    subarray_layout = Layout::UNORDERED;
    ranges = {{1, 2, 3, 4}, {1, 2, 3, 5, 7, 9}};
    partitions = {{{1, 2, 3, 4}, {1, 2, 3, 5}}, {{1, 2, 3, 4}, {7, 9}}};
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget);
  }

  close_array(ctx_, array_);
}

/**
 * Tests subarray range calibration, such that the ranges involved
 * in the next partition fall in the same slab (or fall in a single
 * slab in case the subarray layout is UNORDERED).
 */
TEST_CASE_METHOD(
    SubarrayPartitionerSparseFx,
    "SubarrayPartitioner (Sparse): 2D, multi-range, calibrate",
    "[SubarrayPartitioner][sparse][2D][MR][calibrate]") {
  Layout subarray_layout;
  std::vector<SubarrayRanges<uint64_t>> partitions = {};
  SubarrayRanges<uint64_t> ranges;
  std::string attr = "a";
  uint64_t budget = 5 * sizeof(int);

  SECTION("# tile: row, cell: row") {
    create_default_2d_array(TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR);
    write_default_2d_array();
    open_array(ctx_, array_, TILEDB_READ);

    // subarray: row
    subarray_layout = Layout::ROW_MAJOR;
    ranges = {{1, 2, 3, 3, 4, 4}, {2, 5, 6, 9}};
    partitions = {{{1, 2, 3, 3}, {2, 5, 6, 9}}, {{4, 4}, {2, 5, 6, 9}}};
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget);

    // subarray: col
    subarray_layout = Layout::COL_MAJOR;
    ranges = {{1, 2, 3, 4}, {1, 2, 3, 5, 7, 9}};
    partitions = {{{1, 2, 3, 4}, {1, 2, 3, 5}}, {{1, 2, 3, 4}, {7, 9}}};
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget);

    // subarray: unordered
    subarray_layout = Layout::UNORDERED;
    ranges = {{1, 2, 3, 3, 4, 4}, {2, 5, 6, 9}};
    partitions = {{{1, 2, 3, 3}, {2, 5, 6, 9}}, {{4, 4}, {2, 5, 6, 9}}};
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget);
  }

  SECTION("# tile: row, cell: col") {
    create_default_2d_array(TILEDB_ROW_MAJOR, TILEDB_COL_MAJOR);
    write_default_2d_array();
    open_array(ctx_, array_, TILEDB_READ);

    // subarray: row
    subarray_layout = Layout::ROW_MAJOR;
    ranges = {{1, 2, 3, 3, 4, 4}, {2, 5, 6, 9}};
    partitions = {{{1, 2, 3, 3}, {2, 5, 6, 9}}, {{4, 4}, {2, 5, 6, 9}}};
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget);

    // subarray: col
    subarray_layout = Layout::COL_MAJOR;
    ranges = {{1, 2, 3, 4}, {1, 2, 3, 5, 7, 9}};
    partitions = {{{1, 2, 3, 4}, {1, 2, 3, 5}}, {{1, 2, 3, 4}, {7, 9}}};
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget);

    // subarray: unordered
    subarray_layout = Layout::UNORDERED;
    ranges = {{1, 2, 3, 4}, {1, 2, 3, 5, 7, 9}};
    partitions = {{{1, 2, 3, 4}, {1, 2, 3, 5}}, {{1, 2, 3, 4}, {7, 9}}};
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget);
  }

  SECTION("# tile: col, cell: row") {
    create_default_2d_array(TILEDB_COL_MAJOR, TILEDB_ROW_MAJOR);
    write_default_2d_array();
    open_array(ctx_, array_, TILEDB_READ);

    // subarray: row
    subarray_layout = Layout::ROW_MAJOR;
    ranges = {{1, 2, 3, 3, 4, 4}, {2, 5, 6, 9}};
    partitions = {{{1, 2, 3, 3, 4, 4}, {2, 5, 6, 9}}};
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget);

    // subarray: col
    subarray_layout = Layout::COL_MAJOR;
    ranges = {{1, 2, 3, 4}, {2, 5, 7, 9}};
    partitions = {{{1, 2, 3, 4}, {2, 5, 7, 9}}};
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget);

    // subarray: unordered
    subarray_layout = Layout::UNORDERED;
    ranges = {{1, 2, 3, 3, 4, 4}, {2, 5, 6, 9}};
    partitions = {{{1, 2, 3, 3, 4, 4}, {2, 5, 6, 9}}};
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget);
  }

  SECTION("# tile: col, cell: col") {
    create_default_2d_array(TILEDB_COL_MAJOR, TILEDB_COL_MAJOR);
    write_default_2d_array();
    open_array(ctx_, array_, TILEDB_READ);

    // subarray: row
    subarray_layout = Layout::ROW_MAJOR;
    ranges = {{1, 2, 3, 3, 4, 4}, {2, 5, 6, 9}};
    partitions = {{{1, 2, 3, 3, 4, 4}, {2, 5, 6, 9}}};
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget);

    // subarray: col
    subarray_layout = Layout::COL_MAJOR;
    ranges = {{1, 2, 3, 4}, {2, 5, 7, 9}};
    partitions = {{{1, 2, 3, 4}, {2, 5, 7, 9}}};
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget);

    // subarray: unordered
    subarray_layout = Layout::UNORDERED;
    ranges = {{1, 2, 3, 4}, {2, 5, 7, 9}};
    partitions = {{{1, 2, 3, 4}, {2, 5, 7, 9}}};
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget);
  }

  close_array(ctx_, array_);
}

TEST_CASE_METHOD(
    SubarrayPartitionerSparseFx,
    "SubarrayPartitioner (Sparse): 2D, multi-range, unsplittable",
    "[SubarrayPartitioner][sparse][2D][MR][unsplittable]") {
  Layout subarray_layout;
  std::vector<SubarrayRanges<uint64_t>> partitions = {{{1, 1}, {2, 2}}};
  SubarrayRanges<uint64_t> ranges = {{1, 2, 3, 3, 4, 4}, {2, 5, 6, 9}};
  std::string attr = "a";
  uint64_t budget = 0;
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

TEST_CASE_METHOD(
    SubarrayPartitionerSparseFx,
    "SubarrayPartitioner (Sparse): 2D, multi-range, split multiple finer",
    "[SubarrayPartitioner][sparse][2D][MR][split_multiple_finer]") {
  Layout subarray_layout;
  std::vector<SubarrayRanges<uint64_t>> partitions = {};
  SubarrayRanges<uint64_t> ranges = {{1, 2, 3, 3, 4, 4}, {2, 5, 6, 9}};
  std::string attr = "a";
  uint64_t budget = sizeof(int);

  SECTION("# tile: row, cell: row") {
    create_default_2d_array(TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR);
    write_default_2d_array();
    open_array(ctx_, array_, TILEDB_READ);

    // subarray: row
    subarray_layout = Layout::ROW_MAJOR;
    partitions = {
        {{1, 1}, {2, 5, 6, 9}},
        {{2, 2}, {2, 5, 6, 9}},
        {{3, 3}, {2, 5}},
        {{3, 3}, {6, 9}},
        {{4, 4}, {2, 5}},
        {{4, 4}, {6, 9}},
    };
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget);

    // subarray: col
    subarray_layout = Layout::COL_MAJOR;
    partitions = {
        {{1, 2, 3, 3}, {2, 2}},
        {{4, 4}, {2, 2}},
        {{1, 2, 3, 3}, {3, 3}},
        {{4, 4}, {3, 3}},
        {{1, 2, 3, 3, 4, 4}, {4, 5}},
        {{1, 2, 3, 3, 4, 4}, {6, 7}},
        {{1, 2, 3, 3, 4, 4}, {8, 8}},
        {{1, 2, 3, 3, 4, 4}, {9, 9}},
    };
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget);

    // subarray: unordered
    subarray_layout = Layout::UNORDERED;
    partitions = {
        {{1, 1}, {2, 5}},
        {{2, 2}, {2, 5}},
        {{1, 2}, {6, 9}},
        {{3, 3}, {2, 5}},
        {{3, 3}, {6, 9}},
        {{4, 4}, {2, 5}},
        {{4, 4}, {6, 9}},
    };
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget);
  }

  SECTION("# tile: row, cell: col") {
    create_default_2d_array(TILEDB_ROW_MAJOR, TILEDB_COL_MAJOR);
    write_default_2d_array();
    open_array(ctx_, array_, TILEDB_READ);

    // subarray: row
    subarray_layout = Layout::ROW_MAJOR;
    partitions = {
        {{1, 1}, {2, 5, 6, 9}},
        {{2, 2}, {2, 5, 6, 9}},
        {{3, 3}, {2, 5}},
        {{3, 3}, {6, 9}},
        {{4, 4}, {2, 5}},
        {{4, 4}, {6, 9}},
    };
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget);

    // subarray: col
    subarray_layout = Layout::COL_MAJOR;
    partitions = {
        {{1, 2, 3, 3}, {2, 2}},
        {{4, 4}, {2, 2}},
        {{1, 2, 3, 3}, {3, 3}},
        {{4, 4}, {3, 3}},
        {{1, 2, 3, 3, 4, 4}, {4, 5}},
        {{1, 2, 3, 3, 4, 4}, {6, 7}},
        {{1, 2, 3, 3, 4, 4}, {8, 8}},
        {{1, 2, 3, 3, 4, 4}, {9, 9}},
    };
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget);

    // subarray: unordered
    subarray_layout = Layout::UNORDERED;
    partitions = {
        {{1, 2}, {2, 3}},
        {{1, 2}, {4, 5}},
        {{3, 3}, {2, 5}},
        {{4, 4}, {2, 5}},
        {{1, 2, 3, 3}, {6, 9}},
        {{4, 4}, {6, 9}},
    };
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget);
  }

  SECTION("# tile: col, cell: row") {
    create_default_2d_array(TILEDB_COL_MAJOR, TILEDB_ROW_MAJOR);
    write_default_2d_array();
    open_array(ctx_, array_, TILEDB_READ);

    // subarray: row
    subarray_layout = Layout::ROW_MAJOR;
    partitions = {
        {{1, 1}, {2, 5, 6, 9}},
        {{2, 2}, {2, 3}},
        {{2, 2}, {4, 5}},
        {{2, 2}, {6, 9}},
        {{3, 3}, {2, 3}},
        {{3, 3}, {4, 5}},
        {{3, 3}, {6, 9}},
        {{4, 4}, {2, 5}},
        {{4, 4}, {6, 9}},
    };
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget);

    // subarray: col
    subarray_layout = Layout::COL_MAJOR;
    partitions = {
        {{1, 2, 3, 3, 4, 4}, {2, 2}},
        {{1, 2, 3, 3, 4, 4}, {3, 3}},
        {{1, 2, 3, 3, 4, 4}, {4, 4}},
        {{1, 2, 3, 3, 4, 4}, {5, 5}},
        {{1, 2, 3, 3, 4, 4}, {6, 7}},
        {{1, 2, 3, 3, 4, 4}, {8, 8}},
        {{1, 2, 3, 3, 4, 4}, {9, 9}},
    };
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget);

    // subarray: unordered
    subarray_layout = Layout::UNORDERED;
    partitions = {
        {{1, 1}, {2, 5}},
        {{2, 2}, {2, 3}},
        {{2, 2}, {4, 5}},
        {{1, 2}, {6, 9}},
        {{3, 3}, {2, 3}},
        {{3, 3}, {4, 5}},
        {{3, 3}, {6, 9}},
        {{4, 4}, {2, 5}},
        {{4, 4}, {6, 9}},
    };
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget);
  }

  SECTION("# tile: col, cell: col") {
    create_default_2d_array(TILEDB_COL_MAJOR, TILEDB_COL_MAJOR);
    write_default_2d_array();
    open_array(ctx_, array_, TILEDB_READ);

    // subarray: row
    subarray_layout = Layout::ROW_MAJOR;
    partitions = {
        {{1, 1}, {2, 5, 6, 9}},
        {{2, 2}, {2, 3}},
        {{2, 2}, {4, 5}},
        {{2, 2}, {6, 9}},
        {{3, 3}, {2, 3}},
        {{3, 3}, {4, 5}},
        {{3, 3}, {6, 9}},
        {{4, 4}, {2, 5}},
        {{4, 4}, {6, 9}},
    };
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget);

    // subarray: col
    subarray_layout = Layout::COL_MAJOR;
    partitions = {
        {{1, 2, 3, 3, 4, 4}, {2, 2}},
        {{1, 2, 3, 3, 4, 4}, {3, 3}},
        {{1, 2, 3, 3, 4, 4}, {4, 4}},
        {{1, 2, 3, 3, 4, 4}, {5, 5}},
        {{1, 2, 3, 3, 4, 4}, {6, 7}},
        {{1, 2, 3, 3, 4, 4}, {8, 8}},
        {{1, 2, 3, 3, 4, 4}, {9, 9}},
    };
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget);

    // subarray: unordered
    subarray_layout = Layout::UNORDERED;
    partitions = {
        {{1, 2}, {2, 3}},
        {{1, 2}, {4, 5}},
        {{3, 3}, {2, 3}},
        {{3, 3}, {4, 5}},
        {{4, 4}, {2, 5}},
        {{1, 2, 3, 3}, {6, 9}},
        {{4, 4}, {6, 9}},
    };
    test_subarray_partitioner(
        subarray_layout, ranges, partitions, attr, budget);
  }

  close_array(ctx_, array_);
}
