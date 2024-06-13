/**
 * @file unit-Subarray.cc
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
 * Tests the `Subarray` class.
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

#include <test/support/tdb_catch.h>
#include <iostream>

using namespace tiledb::sm;
using namespace tiledb::test;

/* ********************************* */
/*         STRUCT DEFINITION         */
/* ********************************* */

struct SubarrayFx {
  tiledb_ctx_t* ctx_;
  tiledb_vfs_t* vfs_;
  const std::vector<std::unique_ptr<SupportedFs>> fs_vec_;
  std::string temp_dir_;
  std::string array_name_;
  const char* ARRAY_NAME = "subarray";
  tiledb_array_t* array_ = nullptr;

  SubarrayFx();
  ~SubarrayFx();
};

SubarrayFx::SubarrayFx()
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

SubarrayFx::~SubarrayFx() {
  tiledb_array_free(&array_);
  remove_dir(temp_dir_, ctx_, vfs_);
  tiledb_ctx_free(&ctx_);
  tiledb_vfs_free(&vfs_);
}

/* ********************************* */
/*                TESTS              */
/* ********************************* */

TEST_CASE_METHOD(
    SubarrayFx,
    "Subarray: Test tile coords, 1D",
    "[Subarray][1d][tile_coords]") {
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
  SubarrayRanges<uint64_t> ranges = {{5, 7, 6, 15, 33, 43}};
  Layout subarray_layout = Layout::ROW_MAJOR;
  create_subarray(array_->array_, ranges, subarray_layout, &subarray);
  CHECK_NOTHROW(subarray.compute_tile_coords<uint64_t>());

  // Prepare correct tile coordinates
  std::vector<std::vector<uint8_t>> c_tile_coords;
  std::vector<uint8_t> tile_coords_el;
  auto coords_size = sizeof(uint64_t);
  tile_coords_el.resize(coords_size);
  uint64_t tile_coords_0 = 0;
  uint64_t tile_coords_1 = 1;
  uint64_t tile_coords_3 = 3;
  uint64_t tile_coords_4 = 4;
  std::memcpy(&tile_coords_el[0], &tile_coords_0, sizeof(uint64_t));
  c_tile_coords.push_back(tile_coords_el);
  std::memcpy(&tile_coords_el[0], &tile_coords_1, sizeof(uint64_t));
  c_tile_coords.push_back(tile_coords_el);
  std::memcpy(&tile_coords_el[0], &tile_coords_3, sizeof(uint64_t));
  c_tile_coords.push_back(tile_coords_el);
  std::memcpy(&tile_coords_el[0], &tile_coords_4, sizeof(uint64_t));
  c_tile_coords.push_back(tile_coords_el);

  // Check tile coordinates
  auto tile_coords = subarray.tile_coords();
  CHECK(tile_coords == c_tile_coords);

  // Check tile coordinates ptr
  std::vector<uint8_t> aux_tile_coords;
  aux_tile_coords.resize(coords_size);
  auto tile_coords_ptr =
      subarray.tile_coords_ptr<uint64_t>({1}, &aux_tile_coords);
  CHECK(tile_coords_ptr[0] == tile_coords_1);
  tile_coords_ptr = subarray.tile_coords_ptr<uint64_t>({4}, &aux_tile_coords);
  CHECK(tile_coords_ptr[0] == tile_coords_4);
  tile_coords_ptr = subarray.tile_coords_ptr<uint64_t>({10}, &aux_tile_coords);
  CHECK(tile_coords_ptr == nullptr);

  close_array(ctx_, array_);
}

TEST_CASE_METHOD(
    SubarrayFx,
    "Subarray: Test tile coords, 2D",
    "[Subarray][2d][tile_coords]") {
  tiledb_layout_t tile_order = TILEDB_ROW_MAJOR;
  uint64_t domain[] = {1, 10};
  uint64_t tile_extent_1 = 2;
  uint64_t tile_extent_2 = 5;
  std::vector<std::vector<uint8_t>> c_tile_coords;
  std::vector<uint8_t> tile_coords_el;
  auto coords_size = 2 * sizeof(uint64_t);
  uint64_t tile_coords_0_0[] = {0, 0};
  uint64_t tile_coords_0_1[] = {0, 1};
  uint64_t tile_coords_2_0[] = {2, 0};
  uint64_t tile_coords_2_1[] = {2, 1};
  uint64_t tile_coords_3_0[] = {3, 0};
  uint64_t tile_coords_3_1[] = {3, 1};
  uint64_t tile_coords_4_0[] = {4, 0};
  uint64_t tile_coords_4_1[] = {4, 1};

  SECTION("tile: row") {
    tile_order = TILEDB_ROW_MAJOR;
    tile_coords_el.resize(coords_size);
    std::memcpy(&tile_coords_el[0], tile_coords_0_0, 2 * sizeof(uint64_t));
    c_tile_coords.push_back(tile_coords_el);
    std::memcpy(&tile_coords_el[0], tile_coords_0_1, 2 * sizeof(uint64_t));
    c_tile_coords.push_back(tile_coords_el);
    std::memcpy(&tile_coords_el[0], tile_coords_2_0, 2 * sizeof(uint64_t));
    c_tile_coords.push_back(tile_coords_el);
    std::memcpy(&tile_coords_el[0], tile_coords_2_1, 2 * sizeof(uint64_t));
    c_tile_coords.push_back(tile_coords_el);
    std::memcpy(&tile_coords_el[0], tile_coords_3_0, 2 * sizeof(uint64_t));
    c_tile_coords.push_back(tile_coords_el);
    std::memcpy(&tile_coords_el[0], tile_coords_3_1, 2 * sizeof(uint64_t));
    c_tile_coords.push_back(tile_coords_el);
    std::memcpy(&tile_coords_el[0], tile_coords_4_0, 2 * sizeof(uint64_t));
    c_tile_coords.push_back(tile_coords_el);
    std::memcpy(&tile_coords_el[0], tile_coords_4_1, 2 * sizeof(uint64_t));
    c_tile_coords.push_back(tile_coords_el);
  }

  SECTION("tile: col") {
    tile_order = TILEDB_COL_MAJOR;
    tile_coords_el.resize(coords_size);
    std::memcpy(&tile_coords_el[0], tile_coords_0_0, 2 * sizeof(uint64_t));
    c_tile_coords.push_back(tile_coords_el);
    std::memcpy(&tile_coords_el[0], tile_coords_2_0, 2 * sizeof(uint64_t));
    c_tile_coords.push_back(tile_coords_el);
    std::memcpy(&tile_coords_el[0], tile_coords_3_0, 2 * sizeof(uint64_t));
    c_tile_coords.push_back(tile_coords_el);
    std::memcpy(&tile_coords_el[0], tile_coords_4_0, 2 * sizeof(uint64_t));
    c_tile_coords.push_back(tile_coords_el);
    std::memcpy(&tile_coords_el[0], tile_coords_0_1, 2 * sizeof(uint64_t));
    c_tile_coords.push_back(tile_coords_el);
    std::memcpy(&tile_coords_el[0], tile_coords_2_1, 2 * sizeof(uint64_t));
    c_tile_coords.push_back(tile_coords_el);
    std::memcpy(&tile_coords_el[0], tile_coords_3_1, 2 * sizeof(uint64_t));
    c_tile_coords.push_back(tile_coords_el);
    std::memcpy(&tile_coords_el[0], tile_coords_4_1, 2 * sizeof(uint64_t));
    c_tile_coords.push_back(tile_coords_el);
  }

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
      tile_order,
      TILEDB_ROW_MAJOR,
      2);

  open_array(ctx_, array_, TILEDB_READ);

  Subarray subarray;
  SubarrayRanges<uint64_t> ranges = {{2, 2, 6, 10}, {2, 6, 5, 10}};
  Layout subarray_layout = Layout::ROW_MAJOR;
  create_subarray(array_->array_, ranges, subarray_layout, &subarray);
  CHECK_NOTHROW(subarray.compute_tile_coords<uint64_t>());

  auto tile_coords = subarray.tile_coords();
  CHECK(tile_coords == c_tile_coords);

  // Check tile coordinates ptr
  std::vector<uint8_t> aux_tile_coords;
  aux_tile_coords.resize(coords_size);
  auto tile_coords_ptr =
      subarray.tile_coords_ptr<uint64_t>({2, 0}, &aux_tile_coords);
  CHECK(tile_coords_ptr[0] == tile_coords_2_0[0]);
  CHECK(tile_coords_ptr[1] == tile_coords_2_0[1]);
  tile_coords_ptr =
      subarray.tile_coords_ptr<uint64_t>({3, 1}, &aux_tile_coords);
  CHECK(tile_coords_ptr[0] == tile_coords_3_1[0]);
  CHECK(tile_coords_ptr[1] == tile_coords_3_1[1]);
  tile_coords_ptr =
      subarray.tile_coords_ptr<uint64_t>({10, 10}, &aux_tile_coords);
  CHECK(tile_coords_ptr == nullptr);

  close_array(ctx_, array_);
}

TEST_CASE_METHOD(
    SubarrayFx,
    "Subarray: Test crop to tile, 2D",
    "[Subarray][2d][crop_to_tile]") {
  uint64_t domain[] = {1, 10};
  uint64_t tile_extent_1 = 2;
  uint64_t tile_extent_2 = 5;
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

  open_array(ctx_, array_, TILEDB_READ);

  Subarray subarray;
  SubarrayRanges<uint64_t> ranges = {{2, 10, 6, 10}, {2, 6, 5, 10}};
  Layout subarray_layout = Layout::ROW_MAJOR;
  create_subarray(array_->array_, ranges, subarray_layout, &subarray);

  std::vector<uint64_t> tile_coords = {1, 0};
  std::vector<uint64_t> c_range_0_0 = {3, 4};
  std::vector<uint64_t> c_range_1_0 = {2, 5};
  std::vector<uint64_t> c_range_1_1 = {5, 5};
  auto cropped_subarray =
      subarray.crop_to_tile(&tile_coords[0], Layout::ROW_MAJOR);
  const Range* range = nullptr;
  CHECK(cropped_subarray.range_num() == 2);
  CHECK_NOTHROW(cropped_subarray.get_range(0, 0, &range));
  CHECK(!memcmp(range->data(), &c_range_0_0[0], 2 * sizeof(uint64_t)));
  CHECK_NOTHROW(cropped_subarray.get_range(1, 0, &range));
  CHECK(!memcmp(range->data(), &c_range_1_0[0], 2 * sizeof(uint64_t)));
  CHECK_NOTHROW(cropped_subarray.get_range(1, 1, &range));
  CHECK(!memcmp(range->data(), &c_range_1_1[0], 2 * sizeof(uint64_t)));

  auto tile_cell_num = subarray.tile_cell_num(&tile_coords[0]);
  CHECK(tile_cell_num == cropped_subarray.cell_num());

  close_array(ctx_, array_);
}

void verify_expanded_coordinates_2D(
    Subarray* const subarray,
    const uint64_t range_idx_start,
    const uint64_t range_idx_end,
    const uint64_t expected_range_idx_start,
    const uint64_t expected_range_idx_end,
    const std::vector<uint64_t>& expected_start_coords,
    const std::vector<uint64_t>& expected_end_coords) {
  std::vector<uint64_t> start_coords;
  std::vector<uint64_t> end_coords;
  subarray->get_expanded_coordinates(
      range_idx_start, range_idx_end, &start_coords, &end_coords);
  REQUIRE(start_coords == expected_start_coords);
  REQUIRE(end_coords == expected_end_coords);
  REQUIRE(subarray->range_idx(start_coords) == expected_range_idx_start);
  REQUIRE(subarray->range_idx(end_coords) == expected_range_idx_end);

  // Build a map from each inclusive range index between
  // `range_idx_start` and `range_idx_end` that maps to a bool.
  std::unordered_map<uint64_t, bool> range_idx_found;
  for (uint64_t i = range_idx_start; i <= range_idx_end; ++i) {
    range_idx_found[i] = false;
  }

  // Iterate through every coordinate between the start and end
  // coordinate. If the flattened index is in `range_idx_found`,
  // set the value to `true`.
  for (uint64_t x = start_coords[0]; x <= end_coords[0]; ++x) {
    for (uint64_t y = start_coords[1]; y <= end_coords[1]; ++y) {
      const uint64_t range_idx = subarray->range_idx({x, y});
      if (range_idx_found.count(range_idx) == 1) {
        range_idx_found[range_idx] = true;
      }
    }
  }

  // Verify all flattened ranges are contained within the 2D
  // space between `start_coords` and `end_coords`.
  for (uint64_t i = range_idx_start; i <= range_idx_end; ++i) {
    REQUIRE(range_idx_found[i] == true);
  }
}

TEST_CASE_METHOD(
    SubarrayFx,
    "Subarray: Test get_expanded_coordinates, row-major, 2D",
    "[Subarray][2d][row_major][get_expanded_coordinates]") {
  uint64_t domain[] = {1, 4};
  uint64_t tile_extent_1 = 1;
  uint64_t tile_extent_2 = 1;
  create_array(
      ctx_,
      array_name_,
      TILEDB_DENSE,
      {"x", "y"},
      {TILEDB_UINT64, TILEDB_UINT64},
      {domain, domain},
      {&tile_extent_1, &tile_extent_2},
      {"a"},
      {TILEDB_INT32},
      {1},
      {tiledb::test::Compressor(TILEDB_FILTER_LZ4, -1)},
      TILEDB_ROW_MAJOR,
      TILEDB_ROW_MAJOR,
      1);

  open_array(ctx_, array_, TILEDB_READ);

  /**
   * Populate the subarray with non-coalesced point ranges
   * on each cell. This will populate the 2D subarray ranges as:
   * 0  1  2  3
   * 4  5  6  7
   * 8  9  10 11
   * 12 13 14 15
   */
  Subarray subarray;
  std::vector<uint64_t> d1_ranges;
  std::vector<uint64_t> d2_ranges;
  for (uint64_t i = domain[0]; i <= domain[1]; ++i) {
    d1_ranges.emplace_back(i);
    d1_ranges.emplace_back(i);
    d2_ranges.emplace_back(i);
    d2_ranges.emplace_back(i);
  }
  SubarrayRanges<uint64_t> ranges = {d1_ranges, d2_ranges};
  Layout subarray_layout = Layout::ROW_MAJOR;
  create_subarray(array_->array_, ranges, subarray_layout, &subarray, false);

  // We must compute range offsets before invoking
  // `get_expanded_coordinates`.
  subarray.compute_range_offsets();

  // The flattened, inclusive range [1, 2] does not expand
  // the coordinates when calibrating.
  verify_expanded_coordinates_2D(&subarray, 1, 2, 1, 2, {0, 1}, {0, 2});

  // The flattened, inclusive range [4, 6] does not expand
  // the coordinates when calibrating.
  verify_expanded_coordinates_2D(&subarray, 4, 6, 4, 6, {1, 0}, {1, 2});

  // The flattened, inclusive range [8, 8] does not expand
  // the coordinates when calibrating.
  verify_expanded_coordinates_2D(&subarray, 8, 8, 8, 8, {2, 0}, {2, 0});

  // The flattened, inclusive range [1, 7] must have
  // a starting coordinate of (0, 0) and an ending coordinate
  // of (1, 3) to contain ranges [0, 7].
  verify_expanded_coordinates_2D(&subarray, 1, 7, 0, 7, {0, 0}, {1, 3});

  // The flattened, inclusive range [5, 10] must have
  // a starting coordinate of (1, 0) and an ending coordinate
  // of (2, 3) to contain ranges [4, 11].
  verify_expanded_coordinates_2D(&subarray, 5, 10, 4, 11, {1, 0}, {2, 3});

  close_array(ctx_, array_);
}

TEST_CASE_METHOD(
    SubarrayFx,
    "Subarray: Test get_expanded_coordinates, col-major, 2D",
    "[Subarray][2d][col_major][get_expanded_coordinates]") {
  uint64_t domain[] = {1, 4};
  uint64_t tile_extent_1 = 1;
  uint64_t tile_extent_2 = 1;
  create_array(
      ctx_,
      array_name_,
      TILEDB_DENSE,
      {"x", "y"},
      {TILEDB_UINT64, TILEDB_UINT64},
      {domain, domain},
      {&tile_extent_1, &tile_extent_2},
      {"a"},
      {TILEDB_INT32},
      {1},
      {tiledb::test::Compressor(TILEDB_FILTER_LZ4, -1)},
      TILEDB_ROW_MAJOR,
      TILEDB_ROW_MAJOR,
      1);

  open_array(ctx_, array_, TILEDB_READ);

  /**
   * Populate the subarray with non-coalesced point ranges
   * on each cell. This will populate the 2D subarray ranges as:
   * 0 4 8  12
   * 1 5 9  13
   * 2 6 10 14
   * 3 7 11 15
   */
  Subarray subarray;
  std::vector<uint64_t> d1_ranges;
  std::vector<uint64_t> d2_ranges;
  for (uint64_t i = domain[0]; i <= domain[1]; ++i) {
    d1_ranges.emplace_back(i);
    d1_ranges.emplace_back(i);
    d2_ranges.emplace_back(i);
    d2_ranges.emplace_back(i);
  }
  SubarrayRanges<uint64_t> ranges = {d1_ranges, d2_ranges};
  Layout subarray_layout = Layout::COL_MAJOR;
  create_subarray(array_->array_, ranges, subarray_layout, &subarray, false);

  // We must compute range offsets before invoking
  // `get_expanded_coordinates`.
  subarray.compute_range_offsets();

  // The flattened, inclusive range [1, 2] does not expand
  // the coordinates when calibrating.
  verify_expanded_coordinates_2D(&subarray, 1, 2, 1, 2, {1, 0}, {2, 0});

  // The flattened, inclusive range [4, 6] does not expand
  // the coordinates when calibrating.
  verify_expanded_coordinates_2D(&subarray, 4, 6, 4, 6, {0, 1}, {2, 1});

  // The flattened, inclusive range [8, 8] does not expand
  // the coordinates when calibrating.
  verify_expanded_coordinates_2D(&subarray, 8, 8, 8, 8, {0, 2}, {0, 2});

  // The flattened, inclusive range [1, 7] must have
  // a starting coordinate of (0, 0) and an ending coordinate
  // of (3, 1) to contain ranges [0, 7].
  verify_expanded_coordinates_2D(&subarray, 1, 7, 0, 7, {0, 0}, {3, 1});

  // The flattened, inclusive range [5, 10] must have
  // a starting coordinate of (0, 1) and an ending coordinate
  // of (3, 2) to contain ranges [4, 11].
  verify_expanded_coordinates_2D(&subarray, 5, 10, 4, 11, {0, 1}, {3, 2});

  close_array(ctx_, array_);
}

TEST_CASE_METHOD(
    SubarrayFx,
    "Subarray: Test get_expanded_coordinates, unordered, 2D",
    "[Subarray][2d][unordered][get_expanded_coordinates]") {
  uint64_t domain[] = {1, 4};
  uint64_t tile_extent_1 = 1;
  uint64_t tile_extent_2 = 1;
  create_array(
      ctx_,
      array_name_,
      TILEDB_DENSE,
      {"x", "y"},
      {TILEDB_UINT64, TILEDB_UINT64},
      {domain, domain},
      {&tile_extent_1, &tile_extent_2},
      {"a"},
      {TILEDB_INT32},
      {1},
      {tiledb::test::Compressor(TILEDB_FILTER_LZ4, -1)},
      TILEDB_ROW_MAJOR,
      TILEDB_ROW_MAJOR,
      1);

  open_array(ctx_, array_, TILEDB_READ);

  /**
   * Populate the subarray with non-coalesced point ranges
   * on each cell. This will populate the 2D subarray ranges as:
   * 0  1  2  3
   * 4  5  6  7
   * 8  9  10 11
   * 12 13 14 15
   */
  Subarray subarray;
  std::vector<uint64_t> d1_ranges;
  std::vector<uint64_t> d2_ranges;
  for (uint64_t i = domain[0]; i <= domain[1]; ++i) {
    d1_ranges.emplace_back(i);
    d1_ranges.emplace_back(i);
    d2_ranges.emplace_back(i);
    d2_ranges.emplace_back(i);
  }
  SubarrayRanges<uint64_t> ranges = {d1_ranges, d2_ranges};
  Layout subarray_layout = Layout::UNORDERED;
  create_subarray(array_->array_, ranges, subarray_layout, &subarray, false);

  // We must compute range offsets before invoking
  // `get_expanded_coordinates`.
  subarray.compute_range_offsets();

  // The flattened, inclusive range [1, 2] does not expand
  // the coordinates when calibrating.
  verify_expanded_coordinates_2D(&subarray, 1, 2, 1, 2, {0, 1}, {0, 2});

  // The flattened, inclusive range [4, 6] does not expand
  // the coordinates when calibrating.
  verify_expanded_coordinates_2D(&subarray, 4, 6, 4, 6, {1, 0}, {1, 2});

  // The flattened, inclusive range [8, 8] does not expand
  // the coordinates when calibrating.
  verify_expanded_coordinates_2D(&subarray, 8, 8, 8, 8, {2, 0}, {2, 0});

  // The flattened, inclusive range [1, 7] must have
  // a starting coordinate of (0, 0) and an ending coordinate
  // of (1, 3) to contain ranges [0, 7].
  verify_expanded_coordinates_2D(&subarray, 1, 7, 0, 7, {0, 0}, {1, 3});

  // The flattened, inclusive range [5, 10] must have
  // a starting coordinate of (1, 0) and an ending coordinate
  // of (2, 3) to contain ranges [4, 11].
  verify_expanded_coordinates_2D(&subarray, 5, 10, 4, 11, {1, 0}, {2, 3});

  close_array(ctx_, array_);
}

void verify_expanded_coordinates_3D(
    Subarray* const subarray,
    const uint64_t range_idx_start,
    const uint64_t range_idx_end,
    const uint64_t expected_range_idx_start,
    const uint64_t expected_range_idx_end,
    const std::vector<uint64_t>& expected_start_coords,
    const std::vector<uint64_t>& expected_end_coords) {
  std::vector<uint64_t> start_coords;
  std::vector<uint64_t> end_coords;
  subarray->get_expanded_coordinates(
      range_idx_start, range_idx_end, &start_coords, &end_coords);
  REQUIRE(start_coords == expected_start_coords);
  REQUIRE(end_coords == expected_end_coords);
  REQUIRE(subarray->range_idx(start_coords) == expected_range_idx_start);
  REQUIRE(subarray->range_idx(end_coords) == expected_range_idx_end);

  // Build a map from each inclusive range index between
  // `range_idx_start` and `range_idx_end` that maps to a bool.
  std::unordered_map<uint64_t, bool> range_idx_found;
  for (uint64_t i = range_idx_start; i <= range_idx_end; ++i) {
    range_idx_found[i] = false;
  }

  // Iterate through every coordinate between the start and end
  // coordinate. If the flattened index is in `range_idx_found`,
  // set the value to `true`.
  for (uint64_t x = start_coords[0]; x <= end_coords[0]; ++x) {
    for (uint64_t y = start_coords[1]; y <= end_coords[1]; ++y) {
      for (uint64_t z = start_coords[2]; z <= end_coords[2]; ++z) {
        const uint64_t range_idx = subarray->range_idx({x, y, z});
        if (range_idx_found.count(range_idx) == 1) {
          range_idx_found[range_idx] = true;
        }
      }
    }
  }

  // Verify all flattened ranges are contained within the 2D
  // space between `start_coords` and `end_coords`.
  for (uint64_t i = range_idx_start; i <= range_idx_end; ++i) {
    REQUIRE(range_idx_found[i] == true);
  }
}

TEST_CASE_METHOD(
    SubarrayFx,
    "Subarray: Test get_expanded_coordinates, row-major, 3D",
    "[Subarray][3d][row_major][get_expanded_coordinates]") {
  uint64_t domain[] = {1, 4};
  uint64_t tile_extent_1 = 1;
  uint64_t tile_extent_2 = 1;
  uint64_t tile_extent_3 = 1;
  create_array(
      ctx_,
      array_name_,
      TILEDB_DENSE,
      {"x", "y", "z"},
      {TILEDB_UINT64, TILEDB_UINT64, TILEDB_UINT64},
      {domain, domain, domain},
      {&tile_extent_1, &tile_extent_2, &tile_extent_3},
      {"a"},
      {TILEDB_INT32},
      {1},
      {tiledb::test::Compressor(TILEDB_FILTER_LZ4, -1)},
      TILEDB_ROW_MAJOR,
      TILEDB_ROW_MAJOR,
      1);

  open_array(ctx_, array_, TILEDB_READ);

  /**
   * Populate the subarray with non-coalesced point ranges
   * on each cell. This will populate the 3D subarray ranges as:
   *
   * z == 0
   * 0  4  8  12
   * 16 20 24 28
   * 32 36 40 44
   * 48 52 56 60
   *
   * z == 1
   * 1  5  9  13
   * 17 21 25 29
   * 33 37 41 45
   * 49 53 57 61
   *
   * z == 2
   * 2  6  10 14
   * 18 22 26 30
   * 34 38 42 46
   * 50 54 58 62
   *
   * z == 3
   * 3  7  11 15
   * 19 23 27 31
   * 35 39 43 47
   * 51 55 59 63
   */
  Subarray subarray;
  std::vector<uint64_t> d1_ranges;
  std::vector<uint64_t> d2_ranges;
  std::vector<uint64_t> d3_ranges;
  for (uint64_t i = domain[0]; i <= domain[1]; ++i) {
    d1_ranges.emplace_back(i);
    d1_ranges.emplace_back(i);
    d2_ranges.emplace_back(i);
    d2_ranges.emplace_back(i);
    d3_ranges.emplace_back(i);
    d3_ranges.emplace_back(i);
  }
  SubarrayRanges<uint64_t> ranges = {d1_ranges, d2_ranges, d3_ranges};
  Layout subarray_layout = Layout::ROW_MAJOR;
  create_subarray(array_->array_, ranges, subarray_layout, &subarray, false);

  // We must compute range offsets before invoking
  // `get_expanded_coordinates`.
  subarray.compute_range_offsets();

  // The flattened, inclusive range [0, 4] only expands
  // on the last dimension.
  verify_expanded_coordinates_3D(&subarray, 0, 4, 0, 7, {0, 0, 0}, {0, 1, 3});

  // The flattened, inclusive range [56, 59] does not expand
  // the coordinates when calibrating.
  verify_expanded_coordinates_3D(
      &subarray, 56, 59, 56, 59, {3, 2, 0}, {3, 2, 3});

  // The flattened, inclusive range [16, 18] does not expand
  // the coordinates when calibrating.
  verify_expanded_coordinates_3D(
      &subarray, 16, 18, 16, 18, {1, 0, 0}, {1, 0, 2});

  // The flattened, inclusive range [37, 57] must have
  // a starting coordinate of (2, 0, 0) and an ending coordinate
  // of (3, 3, 3) to contain ranges [32, 63]. This ensures
  // expansion along both the "y" and "z" dimension, leaving the
  // "x" dimension untouched.
  verify_expanded_coordinates_3D(
      &subarray, 37, 57, 32, 63, {2, 0, 0}, {3, 3, 3});

  close_array(ctx_, array_);
}

TEST_CASE_METHOD(
    SubarrayFx,
    "Subarray: Test get_expanded_coordinates, col-major, 3D",
    "[Subarray][3d][col_major][get_expanded_coordinates]") {
  uint64_t domain[] = {1, 4};
  uint64_t tile_extent_1 = 1;
  uint64_t tile_extent_2 = 1;
  uint64_t tile_extent_3 = 1;
  create_array(
      ctx_,
      array_name_,
      TILEDB_DENSE,
      {"x", "y", "z"},
      {TILEDB_UINT64, TILEDB_UINT64, TILEDB_UINT64},
      {domain, domain, domain},
      {&tile_extent_1, &tile_extent_2, &tile_extent_3},
      {"a"},
      {TILEDB_INT32},
      {1},
      {tiledb::test::Compressor(TILEDB_FILTER_LZ4, -1)},
      TILEDB_ROW_MAJOR,
      TILEDB_ROW_MAJOR,
      1);

  open_array(ctx_, array_, TILEDB_READ);

  /**
   * Populate the subarray with non-coalesced point ranges
   * on each cell. This will populate the 3D subarray ranges as:
   *
   * z == 0
   * 0  16 32 48
   * 4  20 36 52
   * 8  24 40 56
   * 12 28 44 60
   *
   * z == 1
   * 1  17 33 49
   * 5  21 37 53
   * 9  25 41 57
   * 13 29 45 61
   *
   * z == 2
   * 2  18 34 50
   * 6  22 38 54
   * 10 26 42 58
   * 14 30 46 62
   *
   * z == 3
   * 3  19 35 51
   * 7  23 39 55
   * 11 27 43 59
   * 15 31 47 63
   */
  Subarray subarray;
  std::vector<uint64_t> d1_ranges;
  std::vector<uint64_t> d2_ranges;
  std::vector<uint64_t> d3_ranges;
  for (uint64_t i = domain[0]; i <= domain[1]; ++i) {
    d1_ranges.emplace_back(i);
    d1_ranges.emplace_back(i);
    d2_ranges.emplace_back(i);
    d2_ranges.emplace_back(i);
    d3_ranges.emplace_back(i);
    d3_ranges.emplace_back(i);
  }
  SubarrayRanges<uint64_t> ranges = {d1_ranges, d2_ranges, d3_ranges};
  Layout subarray_layout = Layout::COL_MAJOR;
  create_subarray(array_->array_, ranges, subarray_layout, &subarray, false);

  // We must compute range offsets before invoking
  // `get_expanded_coordinates`.
  subarray.compute_range_offsets();

  // The flattened, inclusive range [0, 4] only expands
  // on the last dimension.
  verify_expanded_coordinates_3D(&subarray, 0, 4, 0, 7, {0, 0, 0}, {3, 1, 0});

  // The flattened, inclusive range [56, 59] does not expand
  // the coordinates when calibrating.
  verify_expanded_coordinates_3D(
      &subarray, 56, 59, 56, 59, {0, 2, 3}, {3, 2, 3});

  // The flattened, inclusive range [16, 18] does not expand
  // the coordinates when calibrating.
  verify_expanded_coordinates_3D(
      &subarray, 16, 18, 16, 18, {0, 0, 1}, {2, 0, 1});

  // The flattened, inclusive range [37, 57] must have
  // a starting coordinate of (0, 0, 2) and an ending coordinate
  // of (3, 3, 3) to contain ranges [32, 63]. This ensures
  // expansion along both the "x" and "y" dimension, leaving the
  // "z" dimension untouched.
  verify_expanded_coordinates_3D(
      &subarray, 37, 57, 32, 63, {0, 0, 2}, {3, 3, 3});

  close_array(ctx_, array_);
}

TEST_CASE_METHOD(
    SubarrayFx,
    "Subarray: Test get_expanded_coordinates, unordered, 3D",
    "[Subarray][3d][unordered][get_expanded_coordinates]") {
  uint64_t domain[] = {1, 4};
  uint64_t tile_extent_1 = 1;
  uint64_t tile_extent_2 = 1;
  uint64_t tile_extent_3 = 1;
  create_array(
      ctx_,
      array_name_,
      TILEDB_DENSE,
      {"x", "y", "z"},
      {TILEDB_UINT64, TILEDB_UINT64, TILEDB_UINT64},
      {domain, domain, domain},
      {&tile_extent_1, &tile_extent_2, &tile_extent_3},
      {"a"},
      {TILEDB_INT32},
      {1},
      {tiledb::test::Compressor(TILEDB_FILTER_LZ4, -1)},
      TILEDB_ROW_MAJOR,
      TILEDB_ROW_MAJOR,
      1);

  open_array(ctx_, array_, TILEDB_READ);

  /**
   * Populate the subarray with non-coalesced point ranges
   * on each cell. This will populate the 3D subarray ranges as:
   *
   * z == 0
   * 0  4  8  12
   * 16 20 24 28
   * 32 36 40 44
   * 48 52 56 60
   *
   * z == 1
   * 1  5  9  13
   * 17 21 25 29
   * 33 37 41 45
   * 49 53 57 61
   *
   * z == 2
   * 2  6  10 14
   * 18 22 26 30
   * 34 38 42 46
   * 50 54 58 62
   *
   * z == 3
   * 3  7  11 15
   * 19 23 27 31
   * 35 39 43 47
   * 51 55 59 63
   */
  Subarray subarray;
  std::vector<uint64_t> d1_ranges;
  std::vector<uint64_t> d2_ranges;
  std::vector<uint64_t> d3_ranges;
  for (uint64_t i = domain[0]; i <= domain[1]; ++i) {
    d1_ranges.emplace_back(i);
    d1_ranges.emplace_back(i);
    d2_ranges.emplace_back(i);
    d2_ranges.emplace_back(i);
    d3_ranges.emplace_back(i);
    d3_ranges.emplace_back(i);
  }
  SubarrayRanges<uint64_t> ranges = {d1_ranges, d2_ranges, d3_ranges};
  Layout subarray_layout = Layout::UNORDERED;
  create_subarray(array_->array_, ranges, subarray_layout, &subarray, false);

  // We must compute range offsets before invoking
  // `get_expanded_coordinates`.
  subarray.compute_range_offsets();

  // The flattened, inclusive range [56, 59] does not expand
  // the coordinates when calibrating.
  verify_expanded_coordinates_3D(
      &subarray, 56, 59, 56, 59, {3, 2, 0}, {3, 2, 3});

  // The flattened, inclusive range [16, 18] does not expand
  // the coordinates when calibrating.
  verify_expanded_coordinates_3D(
      &subarray, 16, 18, 16, 18, {1, 0, 0}, {1, 0, 2});

  // The flattened, inclusive range [37, 57] must have
  // a starting coordinate of (2, 0, 0) and an ending coordinate
  // of (3, 3, 3) to contain ranges [32, 63]. This ensures
  // expansion along both the "y" and "z" dimension, leaving the
  // "x" dimension untouched.
  verify_expanded_coordinates_3D(
      &subarray, 37, 57, 32, 63, {2, 0, 0}, {3, 3, 3});

  close_array(ctx_, array_);
}

TEST_CASE_METHOD(
    SubarrayFx, "Subarray: round-trip attribute ranges", "[Subarray]") {
  // Create array
  uint64_t domain[2]{0, 3};
  uint64_t tile_extent{4};
  create_array(
      ctx_,
      array_name_,
      TILEDB_DENSE,
      {"x"},
      {TILEDB_UINT64},
      {domain},
      {&tile_extent},
      {"a", "b"},
      {TILEDB_INT64, TILEDB_FLOAT64},
      {1, 1},
      {tiledb::test::Compressor(TILEDB_FILTER_LZ4, -1),
       tiledb::test::Compressor(TILEDB_FILTER_LZ4, -1)},
      TILEDB_ROW_MAJOR,
      TILEDB_ROW_MAJOR,
      4);
  open_array(ctx_, array_, TILEDB_READ);

  // Create subarray
  tiledb_subarray_t* subarray;
  tiledb_subarray_alloc(ctx_, array_, &subarray);

  // Set attribute ranges
  int64_t range_data[6]{-10, -8, -5, 0, -2, 7};
  std::vector<Range> input_ranges{
      Range(&range_data[0], &range_data[1], sizeof(int64_t)),
      Range(&range_data[2], &range_data[3], sizeof(int64_t)),
      Range(&range_data[4], &range_data[5], sizeof(int64_t))};
  subarray->subarray_->set_attribute_ranges("b", input_ranges);

  // Get attribute ranges and verify results
  const auto& output_ranges = subarray->subarray_->get_attribute_ranges("b");
  for (uint32_t ii = 0; ii < 3; ++ii) {
    CHECK(input_ranges[ii] == output_ranges[ii]);
  }
}
