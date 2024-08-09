/**
 * @file unit-CellSlabIter.cc
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
 * Tests the `CellSlabIter` class.
 */

#include "test/support/src/helpers.h"
#include "test/support/src/vfs_helpers.h"
#include "tiledb/sm/c_api/tiledb_struct_def.h"
#include "tiledb/sm/query/legacy/cell_slab_iter.h"

#ifdef _WIN32
#include "tiledb/sm/filesystem/win.h"
#else
#include "tiledb/sm/filesystem/posix.h"
#endif

#include <catch2/catch_test_macros.hpp>
#include <iostream>

using namespace tiledb::sm;
using namespace tiledb::test;

/* ********************************* */
/*         STRUCT DEFINITION         */
/* ********************************* */

struct CellSlabIterFx {
  tiledb_ctx_t* ctx_;
  tiledb_vfs_t* vfs_;

  const std::vector<std::unique_ptr<SupportedFs>> fs_vec_;

  std::string temp_dir_;
  std::string array_name_;
  const char* ARRAY_NAME = "cell_slab_iter";
  tiledb_array_t* array_ = nullptr;

  CellSlabIterFx();
  ~CellSlabIterFx();

  template <class T>
  void check_iter(
      const Subarray& subarray, const std::vector<CellSlab<T>>& c_cell_slabs);
};

CellSlabIterFx::CellSlabIterFx()
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

CellSlabIterFx::~CellSlabIterFx() {
  tiledb_array_free(&array_);
  remove_dir(temp_dir_, ctx_, vfs_);
  tiledb_ctx_free(&ctx_);
  tiledb_vfs_free(&vfs_);
}

template <class T>
void CellSlabIterFx::check_iter(
    const Subarray& subarray, const std::vector<CellSlab<T>>& c_cell_slabs) {
  CellSlabIter<T> iter(&subarray);
  CHECK(iter.end());
  CHECK(iter.begin().ok());
  auto cell_slab = iter.cell_slab();
  CHECK(cell_slab == c_cell_slabs[0]);

  for (size_t i = 1; i < c_cell_slabs.size(); ++i) {
    ++iter;
    cell_slab = iter.cell_slab();
    CHECK(cell_slab == c_cell_slabs[i]);
    CHECK(!iter.end());
  }

  ++iter;
  CHECK(iter.end());
}

/* ********************************* */
/*                TESTS              */
/* ********************************* */

TEST_CASE_METHOD(
    CellSlabIterFx, "CellSlabIter: Empty iterator", "[CellSlabIter][empty]") {
  CellSlabIter<int32_t> iter;
  CHECK(iter.end());
  CHECK(iter.begin().ok());
  CHECK(iter.end());
}

TEST_CASE_METHOD(
    CellSlabIterFx, "CellSlabIter: Error checks", "[CellSlabIter][error]") {
  // Create array
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

  // Create subarray
  open_array(ctx_, array_, TILEDB_READ);
  Subarray subarray;
  SubarrayRanges<uint64_t> ranges = {};
  Layout subarray_layout = Layout::ROW_MAJOR;
  create_subarray(array_->array_, ranges, subarray_layout, &subarray);

  // Datatype mismatch
  CellSlabIter<int32_t> iter(&subarray);
  CHECK(iter.end());
  CHECK(!iter.begin().ok());

  // Create subarray
  Subarray subarray_2;
  subarray_layout = Layout::GLOBAL_ORDER;
  create_subarray(array_->array_, ranges, subarray_layout, &subarray_2);

  // Invalid layout
  CellSlabIter<uint64_t> iter2(&subarray_2);
  CHECK(iter2.end());
  CHECK(!iter2.begin().ok());

  close_array(ctx_, array_);
}

TEST_CASE_METHOD(
    CellSlabIterFx,
    "CellSlabIter: Test 1D ranges",
    "[CellSlabIter][ranges][1d]") {
  // Create array
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

  // Create subarray
  open_array(ctx_, array_, TILEDB_READ);
  Subarray subarray;
  SubarrayRanges<uint64_t> ranges = {{5, 15, 3, 5, 10, 20, 6, 36}};
  Layout subarray_layout = Layout::ROW_MAJOR;
  create_subarray(array_->array_, ranges, subarray_layout, &subarray);

  CellSlabIter<uint64_t> iter(&subarray);
  CHECK(iter.end());
  CHECK(iter.begin().ok());
  auto iter_ranges = iter.ranges();

  std::vector<CellSlabIter<uint64_t>::Range> c_ranges = {
      {5, 10, 0},
      {11, 15, 1},
      {3, 5, 0},
      {10, 10, 0},
      {11, 20, 1},
      {6, 10, 0},
      {11, 20, 1},
      {21, 30, 2},
      {31, 36, 3}};
  CHECK(iter_ranges.size() == 1);
  CHECK(iter_ranges[0].size() == c_ranges.size());
  CHECK(std::equal(
      iter_ranges[0].begin(), iter_ranges[0].end(), c_ranges.begin()));

  close_array(ctx_, array_);
}

TEST_CASE_METHOD(
    CellSlabIterFx,
    "CellSlabIter: Test 2D ranges",
    "[CellSlabIter][ranges][2d]") {
  // Create array
  uint64_t domain[] = {1, 10};
  uint64_t tile_extent_1 = 5;
  uint64_t tile_extent_2 = 2;
  create_array(
      ctx_,
      array_name_,
      TILEDB_DENSE,
      {"d1", "d2"},
      {TILEDB_UINT64, TILEDB_UINT64},
      {domain, domain},
      {&tile_extent_1, &tile_extent_2},
      {"a", "b"},
      {TILEDB_INT32, TILEDB_INT32},
      {1, TILEDB_VAR_NUM},
      {tiledb::test::Compressor(TILEDB_FILTER_LZ4, -1),
       tiledb::test::Compressor(TILEDB_FILTER_LZ4, -1)},
      TILEDB_ROW_MAJOR,
      TILEDB_ROW_MAJOR,
      2);

  // Create subarray
  open_array(ctx_, array_, TILEDB_READ);
  Subarray subarray;
  SubarrayRanges<uint64_t> ranges = {{5, 8, 3, 5}, {5, 8, 3, 5}};
  Layout subarray_layout = Layout::ROW_MAJOR;
  create_subarray(array_->array_, ranges, subarray_layout, &subarray);

  CellSlabIter<uint64_t> iter(&subarray);
  CHECK(iter.end());
  CHECK(iter.begin().ok());
  auto iter_ranges = iter.ranges();

  CHECK(iter_ranges.size() == 2);
  std::vector<CellSlabIter<uint64_t>::Range> c_ranges_1 = {
      {5, 5, 0}, {6, 8, 1}, {3, 5, 0}};
  CHECK(iter_ranges[0].size() == c_ranges_1.size());
  CHECK(std::equal(
      iter_ranges[0].begin(), iter_ranges[0].end(), c_ranges_1.begin()));

  std::vector<CellSlabIter<uint64_t>::Range> c_ranges_2 = {
      {5, 6, 2}, {7, 8, 3}, {3, 4, 1}, {5, 5, 2}};
  CHECK(iter_ranges[1].size() == c_ranges_2.size());
  CHECK(std::equal(
      iter_ranges[1].begin(), iter_ranges[1].end(), c_ranges_2.begin()));

  close_array(ctx_, array_);
}

TEST_CASE_METHOD(
    CellSlabIterFx,
    "CellSlabIter: Test 1D slabs",
    "[CellSlabIter][slabs][1d]") {
  // Create array
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

  // Create subarray
  open_array(ctx_, array_, TILEDB_READ);
  Subarray subarray;
  SubarrayRanges<uint64_t> ranges = {{5, 15, 3, 5, 11, 14}};
  Layout subarray_layout = Layout::ROW_MAJOR;
  create_subarray(array_->array_, ranges, subarray_layout, &subarray);
  CHECK_NOTHROW(subarray.compute_tile_coords<uint64_t>());

  uint64_t tile_coords_0[] = {0};
  uint64_t tile_coords_1[] = {1};

  // Check iterator
  std::vector<CellSlab<uint64_t>> c_cell_slabs = {
      CellSlab<uint64_t>(tile_coords_0, {5}, 6),
      CellSlab<uint64_t>(tile_coords_1, {11}, 5),
      CellSlab<uint64_t>(tile_coords_0, {3}, 3),
      CellSlab<uint64_t>(tile_coords_1, {11}, 4),
  };
  check_iter<uint64_t>(subarray, c_cell_slabs);

  close_array(ctx_, array_);
}

TEST_CASE_METHOD(
    CellSlabIterFx,
    "CellSlabIter: Test 2D slabs",
    "[CellSlabIter][slabs][2d]") {
  // Create array
  uint64_t domain_1[] = {1, 10};
  uint64_t domain_2[] = {1, 9};
  uint64_t tile_extent_1 = 5;
  uint64_t tile_extent_2 = 3;
  create_array(
      ctx_,
      array_name_,
      TILEDB_DENSE,
      {"d1", "d2"},
      {TILEDB_UINT64, TILEDB_UINT64},
      {domain_1, domain_2},
      {&tile_extent_1, &tile_extent_2},
      {"a", "b"},
      {TILEDB_INT32, TILEDB_INT32},
      {1, TILEDB_VAR_NUM},
      {tiledb::test::Compressor(TILEDB_FILTER_LZ4, -1),
       tiledb::test::Compressor(TILEDB_FILTER_LZ4, -1)},
      TILEDB_ROW_MAJOR,
      TILEDB_ROW_MAJOR,
      2);

  Layout subarray_layout = Layout::ROW_MAJOR;
  std::vector<CellSlab<uint64_t>> c_cell_slabs;
  uint64_t tile_coords_0_0[] = {0, 0};
  uint64_t tile_coords_0_1[] = {0, 1};
  uint64_t tile_coords_0_2[] = {0, 2};
  uint64_t tile_coords_1_0[] = {1, 0};
  uint64_t tile_coords_1_1[] = {1, 1};
  uint64_t tile_coords_1_2[] = {1, 2};

  SECTION("- row-major") {
    subarray_layout = Layout::ROW_MAJOR;
    c_cell_slabs = {
        CellSlab<uint64_t>(tile_coords_0_0, {2, 1}, 2),
        CellSlab<uint64_t>(tile_coords_0_1, {2, 5}, 2),
        CellSlab<uint64_t>(tile_coords_0_2, {2, 7}, 2),
        CellSlab<uint64_t>(tile_coords_0_0, {3, 1}, 2),
        CellSlab<uint64_t>(tile_coords_0_1, {3, 5}, 2),
        CellSlab<uint64_t>(tile_coords_0_2, {3, 7}, 2),
        CellSlab<uint64_t>(tile_coords_0_0, {4, 1}, 2),
        CellSlab<uint64_t>(tile_coords_0_1, {4, 5}, 2),
        CellSlab<uint64_t>(tile_coords_0_2, {4, 7}, 2),
        CellSlab<uint64_t>(tile_coords_0_0, {3, 1}, 2),
        CellSlab<uint64_t>(tile_coords_0_1, {3, 5}, 2),
        CellSlab<uint64_t>(tile_coords_0_2, {3, 7}, 2),
        CellSlab<uint64_t>(tile_coords_0_0, {4, 1}, 2),
        CellSlab<uint64_t>(tile_coords_0_1, {4, 5}, 2),
        CellSlab<uint64_t>(tile_coords_0_2, {4, 7}, 2),
        CellSlab<uint64_t>(tile_coords_0_0, {5, 1}, 2),
        CellSlab<uint64_t>(tile_coords_0_1, {5, 5}, 2),
        CellSlab<uint64_t>(tile_coords_0_2, {5, 7}, 2),
        CellSlab<uint64_t>(tile_coords_1_0, {6, 1}, 2),
        CellSlab<uint64_t>(tile_coords_1_1, {6, 5}, 2),
        CellSlab<uint64_t>(tile_coords_1_2, {6, 7}, 2),
        CellSlab<uint64_t>(tile_coords_1_0, {7, 1}, 2),
        CellSlab<uint64_t>(tile_coords_1_1, {7, 5}, 2),
        CellSlab<uint64_t>(tile_coords_1_2, {7, 7}, 2),
        CellSlab<uint64_t>(tile_coords_1_0, {8, 1}, 2),
        CellSlab<uint64_t>(tile_coords_1_1, {8, 5}, 2),
        CellSlab<uint64_t>(tile_coords_1_2, {8, 7}, 2),
        CellSlab<uint64_t>(tile_coords_1_0, {9, 1}, 2),
        CellSlab<uint64_t>(tile_coords_1_1, {9, 5}, 2),
        CellSlab<uint64_t>(tile_coords_1_2, {9, 7}, 2),
    };
  }

  SECTION("- col-major") {
    subarray_layout = Layout::COL_MAJOR;
    c_cell_slabs = {
        CellSlab<uint64_t>(tile_coords_0_0, {2, 1}, 3),
        CellSlab<uint64_t>(tile_coords_0_0, {3, 1}, 3),
        CellSlab<uint64_t>(tile_coords_1_0, {6, 1}, 4),
        CellSlab<uint64_t>(tile_coords_0_0, {2, 2}, 3),
        CellSlab<uint64_t>(tile_coords_0_0, {3, 2}, 3),
        CellSlab<uint64_t>(tile_coords_1_0, {6, 2}, 4),
        CellSlab<uint64_t>(tile_coords_0_1, {2, 5}, 3),
        CellSlab<uint64_t>(tile_coords_0_1, {3, 5}, 3),
        CellSlab<uint64_t>(tile_coords_1_1, {6, 5}, 4),
        CellSlab<uint64_t>(tile_coords_0_1, {2, 6}, 3),
        CellSlab<uint64_t>(tile_coords_0_1, {3, 6}, 3),
        CellSlab<uint64_t>(tile_coords_1_1, {6, 6}, 4),
        CellSlab<uint64_t>(tile_coords_0_2, {2, 7}, 3),
        CellSlab<uint64_t>(tile_coords_0_2, {3, 7}, 3),
        CellSlab<uint64_t>(tile_coords_1_2, {6, 7}, 4),
        CellSlab<uint64_t>(tile_coords_0_2, {2, 8}, 3),
        CellSlab<uint64_t>(tile_coords_0_2, {3, 8}, 3),
        CellSlab<uint64_t>(tile_coords_1_2, {6, 8}, 4),
    };
  }

  open_array(ctx_, array_, TILEDB_READ);

  Subarray subarray;
  SubarrayRanges<uint64_t> ranges = {
      {2, 4, 3, 9},
      {1, 2, 5, 8},
  };
  create_subarray(array_->array_, ranges, subarray_layout, &subarray);
  CHECK_NOTHROW(subarray.compute_tile_coords<uint64_t>());

  check_iter<uint64_t>(subarray, c_cell_slabs);

  close_array(ctx_, array_);
}
