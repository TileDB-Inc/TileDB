/**
 * @file unit-Subarray.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2020 TileDB, Inc.
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

#include "test/src/helpers.h"
#include "test/src/vfs_helpers.h"
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
  subarray.compute_tile_coords<uint64_t>();

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
  subarray.compute_tile_coords<uint64_t>();

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
  CHECK(cropped_subarray.get_range(0, 0, &range).ok());
  CHECK(!memcmp(range->data(), &c_range_0_0[0], 2 * sizeof(uint64_t)));
  CHECK(cropped_subarray.get_range(1, 0, &range).ok());
  CHECK(!memcmp(range->data(), &c_range_1_0[0], 2 * sizeof(uint64_t)));
  CHECK(cropped_subarray.get_range(1, 1, &range).ok());
  CHECK(!memcmp(range->data(), &c_range_1_1[0], 2 * sizeof(uint64_t)));

  close_array(ctx_, array_);
}
