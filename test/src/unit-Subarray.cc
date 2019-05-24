/**
 * @file unit-Subarray.cc
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
 * Tests the `Subarray` class.
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

/* ********************************* */
/*         STRUCT DEFINITION         */
/* ********************************* */

struct SubarrayFx {
  tiledb_ctx_t* ctx_;
  tiledb_vfs_t* vfs_;
  bool s3_supported_, hdfs_supported_;
  std::string temp_dir_;
  const std::string s3_bucket_name_ =
      "s3://" + random_bucket_name("tiledb") + "/";
  std::string array_name_;
  const char* ARRAY_NAME = "subarray";
  tiledb_array_t* array_ = nullptr;

  SubarrayFx();
  ~SubarrayFx();
};

SubarrayFx::SubarrayFx() {
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
      {::Compressor(TILEDB_FILTER_LZ4, -1),
       ::Compressor(TILEDB_FILTER_LZ4, -1)},
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
      {::Compressor(TILEDB_FILTER_LZ4, -1),
       ::Compressor(TILEDB_FILTER_LZ4, -1)},
      TILEDB_ROW_MAJOR,
      TILEDB_ROW_MAJOR,
      2);

  open_array(ctx_, array_, TILEDB_READ);

  Subarray subarray;
  SubarrayRanges<uint64_t> ranges = {{2, 2, 6, 10}, {2, 6, 5, 10}};
  Layout subarray_layout = Layout::ROW_MAJOR;
  create_subarray(array_->array_, ranges, subarray_layout, &subarray);
  subarray.compute_tile_coords<uint64_t>();

  // Prepare correct tile coordinates
  std::vector<std::vector<uint8_t>> c_tile_coords;
  std::vector<uint8_t> tile_coords_el;
  auto coords_size = 2 * sizeof(uint64_t);
  tile_coords_el.resize(coords_size);
  uint64_t tile_coords_0_0[] = {0, 0};
  uint64_t tile_coords_0_1[] = {0, 1};
  uint64_t tile_coords_2_0[] = {2, 0};
  uint64_t tile_coords_2_1[] = {2, 1};
  uint64_t tile_coords_3_0[] = {3, 0};
  uint64_t tile_coords_3_1[] = {3, 1};
  uint64_t tile_coords_4_0[] = {4, 0};
  uint64_t tile_coords_4_1[] = {4, 1};
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
