/**
 * @file unit-capi-dense_array_2.cc
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
 * Tests the C API for dense arrays.
 */

#include "test/support/src/helpers.h"
#include "test/support/src/vfs_helpers.h"
#include "tiledb/sm/c_api/tiledb.h"
#include "tiledb/sm/c_api/tiledb_struct_def.h"

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

struct CDenseArrayFx {
  tiledb_ctx_t* ctx_;
  tiledb_vfs_t* vfs_;

  // Vector of supported filsystems
  const std::vector<std::unique_ptr<SupportedFs>> fs_vec_;

  std::string temp_dir_;
  std::string array_name_;
  const char* ARRAY_NAME = "capi_dense_array";
  tiledb_array_t* array_ = nullptr;

  void create_default_array_1d();
  void create_default_array_2d();
  void write_default_array_1d();
  void write_default_array_2d();

  CDenseArrayFx();
  ~CDenseArrayFx();
};

CDenseArrayFx::CDenseArrayFx()
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

CDenseArrayFx::~CDenseArrayFx() {
  tiledb_array_free(&array_);
  remove_dir(temp_dir_, ctx_, vfs_);
  tiledb_ctx_free(&ctx_);
  tiledb_vfs_free(&vfs_);
}

void CDenseArrayFx::create_default_array_1d() {
  uint64_t domain[] = {1, 10};
  uint64_t tile_extent = 5;
  create_array(
      ctx_,
      array_name_,
      TILEDB_DENSE,
      {"d"},
      {TILEDB_UINT64},
      {domain},
      {&tile_extent},
      {"a", "b", "c"},
      {TILEDB_INT32, TILEDB_CHAR, TILEDB_FLOAT32},
      {1, TILEDB_VAR_NUM, 2},
      {tiledb::test::Compressor(TILEDB_FILTER_NONE, -1),
       tiledb::test::Compressor(TILEDB_FILTER_ZSTD, -1),
       tiledb::test::Compressor(TILEDB_FILTER_LZ4, -1)},
      TILEDB_ROW_MAJOR,
      TILEDB_ROW_MAJOR,
      2);
}

void CDenseArrayFx::create_default_array_2d() {
  uint64_t domain[] = {1, 4, 1, 4};
  uint64_t tile_extent = 2;
  create_array(
      ctx_,
      array_name_,
      TILEDB_DENSE,
      {"d1", "d2"},
      {TILEDB_UINT64, TILEDB_UINT64},
      {&domain[0], &domain[2]},
      {&tile_extent, &tile_extent},
      {"a", "b", "c"},
      {TILEDB_INT32, TILEDB_CHAR, TILEDB_FLOAT32},
      {1, TILEDB_VAR_NUM, 2},
      {tiledb::test::Compressor(TILEDB_FILTER_NONE, -1),
       tiledb::test::Compressor(TILEDB_FILTER_ZSTD, -1),
       tiledb::test::Compressor(TILEDB_FILTER_LZ4, -1)},
      TILEDB_ROW_MAJOR,
      TILEDB_ROW_MAJOR,
      2);
}

void CDenseArrayFx::write_default_array_1d() {
  tiledb::test::QueryBuffers buffers;
  std::vector<int> a = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
  uint64_t a_size = a.size() * sizeof(int);
  std::vector<uint64_t> b_off = {
      0,
      sizeof(char),
      3 * sizeof(char),
      6 * sizeof(char),
      9 * sizeof(char),
      11 * sizeof(char),
      15 * sizeof(char),
      16 * sizeof(char),
      17 * sizeof(char),
      18 * sizeof(char)};
  uint64_t b_off_size = b_off.size() * sizeof(uint64_t);
  std::vector<char> b_val;
  b_val = {
      'a',
      'b',
      'b',
      'c',
      'c',
      'c',
      'd',
      'd',
      'd',
      'e',
      'e',
      'f',
      'f',
      'f',
      'f',
      'g',
      'h',
      'i',
      'j'};
  uint64_t b_val_size = b_val.size() * sizeof(char);
  std::vector<float> c = {1.1f, 1.2f, 2.1f, 2.2f, 3.1f,  3.2f, 4.1f,
                          4.2f, 5.1f, 5.2f, 6.1f, 6.2f,  7.1f, 7.2f,
                          8.1f, 8.2f, 9.1f, 9.2f, 10.1f, 10.2f};
  uint64_t c_size = c.size() * sizeof(float);
  buffers["a"] = tiledb::test::QueryBuffer({&a[0], a_size, nullptr, 0});
  buffers["b"] =
      tiledb::test::QueryBuffer({&b_off[0], b_off_size, &b_val[0], b_val_size});
  buffers["c"] = tiledb::test::QueryBuffer({&c[0], c_size, nullptr, 0});
  write_array(ctx_, array_name_, TILEDB_GLOBAL_ORDER, buffers);
}

void CDenseArrayFx::write_default_array_2d() {
  std::vector<int> a = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};
  uint64_t a_size = a.size() * sizeof(int);
  std::vector<uint64_t> b_off = {
      0,
      sizeof(char),
      3 * sizeof(char),
      6 * sizeof(char),
      9 * sizeof(char),
      11 * sizeof(char),
      15 * sizeof(char),
      16 * sizeof(char),
      17 * sizeof(char),
      18 * sizeof(char),
      19 * sizeof(char),
      21 * sizeof(char),
      22 * sizeof(char),
      23 * sizeof(char),
      24 * sizeof(char),
      27 * sizeof(char)};
  uint64_t b_off_size = b_off.size() * sizeof(uint64_t);
  std::vector<char> b_val = {'a', 'b', 'b', 'c', 'c', 'c', 'd', 'd', 'd', 'e',
                             'e', 'f', 'f', 'f', 'f', 'g', 'h', 'i', 'j', 'k',
                             'k', 'l', 'm', 'n', 'o', 'o', 'o', 'p', 'p'};
  uint64_t b_val_size = b_val.size() * sizeof(char);
  std::vector<float> c = {1.1f,  1.2f,  2.1f,  2.2f,  3.1f,  3.2f,  4.1f,
                          4.2f,  5.1f,  5.2f,  6.1f,  6.2f,  7.1f,  7.2f,
                          8.1f,  8.2f,  9.1f,  9.2f,  10.1f, 10.2f, 11.1f,
                          11.2f, 12.1f, 12.2f, 13.1f, 13.2f, 14.1f, 14.2f,
                          15.1f, 15.2f, 16.1f, 16.2f};
  uint64_t c_size = c.size() * sizeof(float);
  tiledb::test::QueryBuffers buffers;
  buffers["a"] = tiledb::test::QueryBuffer({&a[0], a_size, nullptr, 0});
  buffers["b"] =
      tiledb::test::QueryBuffer({&b_off[0], b_off_size, &b_val[0], b_val_size});
  buffers["c"] = tiledb::test::QueryBuffer({&c[0], c_size, nullptr, 0});
  write_array(ctx_, array_name_, TILEDB_ROW_MAJOR, buffers);
}

/* ********************************* */
/*                TESTS              */
/* ********************************* */

TEST_CASE_METHOD(
    CDenseArrayFx,
    "Dense array: 1D, full read",
    "[capi][dense2][1D][full_read]") {
  // Create and write array
  create_default_array_1d();
  write_default_array_1d();

  // Prepare buffers that will store the results
  std::vector<int> a(10);
  uint64_t a_size = a.size() * sizeof(int);
  std::vector<uint64_t> b_off(10);
  uint64_t b_off_size = b_off.size() * sizeof(uint64_t);
  std::vector<char> b_val(19);
  uint64_t b_val_size = b_val.size() * sizeof(char);
  std::vector<float> c(20);
  uint64_t c_size = c.size() * sizeof(float);
  tiledb::test::QueryBuffers buffers;
  buffers["a"] = tiledb::test::QueryBuffer({&a[0], a_size, nullptr, 0});
  buffers["b"] =
      tiledb::test::QueryBuffer({&b_off[0], b_off_size, &b_val[0], b_val_size});
  buffers["c"] = tiledb::test::QueryBuffer({&c[0], c_size, nullptr, 0});
  std::vector<uint64_t> coords_dim1(10);
  uint64_t coords_size = coords_dim1.size() * sizeof(uint64_t);
  buffers["d"] =
      tiledb::test::QueryBuffer({&coords_dim1[0], coords_size, nullptr, 0});

  // Open array
  open_array(ctx_, array_, TILEDB_READ);

  // Create subarray
  SubarrayRanges<uint64_t> ranges = {{1, 10}};
  read_array(ctx_, array_, ranges, TILEDB_ROW_MAJOR, buffers);

  // Check results
  std::vector<int> c_a = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
  std::vector<uint64_t> c_b_off = {
      0,
      sizeof(char),
      3 * sizeof(char),
      6 * sizeof(char),
      9 * sizeof(char),
      11 * sizeof(char),
      15 * sizeof(char),
      16 * sizeof(char),
      17 * sizeof(char),
      18 * sizeof(char)};
  std::vector<char> c_b_val;
  c_b_val = {
      'a',
      'b',
      'b',
      'c',
      'c',
      'c',
      'd',
      'd',
      'd',
      'e',
      'e',
      'f',
      'f',
      'f',
      'f',
      'g',
      'h',
      'i',
      'j'};
  std::vector<float> c_c = {1.1f, 1.2f, 2.1f, 2.2f, 3.1f,  3.2f, 4.1f,
                            4.2f, 5.1f, 5.2f, 6.1f, 6.2f,  7.1f, 7.2f,
                            8.1f, 8.2f, 9.1f, 9.2f, 10.1f, 10.2f};
  std::vector<uint64_t> c_coords = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
  CHECK(a == c_a);
  CHECK(b_off == c_b_off);
  CHECK(b_val == c_b_val);
  CHECK(c == c_c);
  CHECK(coords_dim1 == c_coords);

  // Clean up
  close_array(ctx_, array_);
}

TEST_CASE_METHOD(
    CDenseArrayFx,
    "Dense array: 2D, full read",
    "[capi][dense2][2D][full_read]") {
  // Create and write array
  create_default_array_2d();
  write_default_array_2d();

  // Prepare buffers that will store the results
  std::vector<int> a(16);
  uint64_t a_size = a.size() * sizeof(int);
  std::vector<uint64_t> b_off(16);
  uint64_t b_off_size = b_off.size() * sizeof(uint64_t);
  std::vector<char> b_val(50);
  uint64_t b_val_size = b_val.size() * sizeof(char);
  std::vector<float> c(32);
  uint64_t c_size = c.size() * sizeof(float);
  tiledb::test::QueryBuffers buffers;
  buffers["a"] = tiledb::test::QueryBuffer({&a[0], a_size, nullptr, 0});
  buffers["b"] =
      tiledb::test::QueryBuffer({&b_off[0], b_off_size, &b_val[0], b_val_size});
  buffers["c"] = tiledb::test::QueryBuffer({&c[0], c_size, nullptr, 0});
  std::vector<uint64_t> coords_dim1(16);
  std::vector<uint64_t> coords_dim2(16);
  uint64_t coords_size = coords_dim1.size() * sizeof(uint64_t);
  buffers["d1"] =
      tiledb::test::QueryBuffer({&coords_dim1[0], coords_size, nullptr, 0});
  buffers["d2"] =
      tiledb::test::QueryBuffer({&coords_dim2[0], coords_size, nullptr, 0});

  // Open array
  open_array(ctx_, array_, TILEDB_READ);

  tiledb_layout_t subarray_layout = TILEDB_ROW_MAJOR;
  std::vector<int> c_a = {};
  std::vector<uint64_t> c_b_off = {};
  std::vector<char> c_b_val = {};
  std::vector<float> c_c = {};
  std::vector<uint64_t> c_coords_dim1 = {};
  std::vector<uint64_t> c_coords_dim2 = {};

  SECTION("- subarray: row") {
    subarray_layout = TILEDB_ROW_MAJOR;
    c_a = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};
    c_b_off = {
        0,
        sizeof(char),
        3 * sizeof(char),
        6 * sizeof(char),
        9 * sizeof(char),
        11 * sizeof(char),
        15 * sizeof(char),
        16 * sizeof(char),
        17 * sizeof(char),
        18 * sizeof(char),
        19 * sizeof(char),
        21 * sizeof(char),
        22 * sizeof(char),
        23 * sizeof(char),
        24 * sizeof(char),
        27 * sizeof(char)};
    c_b_val = {'a', 'b', 'b', 'c', 'c', 'c', 'd', 'd', 'd', 'e',
               'e', 'f', 'f', 'f', 'f', 'g', 'h', 'i', 'j', 'k',
               'k', 'l', 'm', 'n', 'o', 'o', 'o', 'p', 'p'};
    c_c = {1.1f,  1.2f,  2.1f,  2.2f,  3.1f,  3.2f,  4.1f,  4.2f,
           5.1f,  5.2f,  6.1f,  6.2f,  7.1f,  7.2f,  8.1f,  8.2f,
           9.1f,  9.2f,  10.1f, 10.2f, 11.1f, 11.2f, 12.1f, 12.2f,
           13.1f, 13.2f, 14.1f, 14.2f, 15.1f, 15.2f, 16.1f, 16.2f};
    c_coords_dim1 = {1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3, 4, 4, 4, 4};
    c_coords_dim2 = {1, 2, 3, 4, 1, 2, 3, 4, 1, 2, 3, 4, 1, 2, 3, 4};
  }

  SECTION("- subarray: col") {
    subarray_layout = TILEDB_COL_MAJOR;
    c_a = {1, 5, 9, 13, 2, 6, 10, 14, 3, 7, 11, 15, 4, 8, 12, 16};
    c_b_off = {
        0,
        sizeof(char),
        3 * sizeof(char),
        4 * sizeof(char),
        5 * sizeof(char),
        7 * sizeof(char),
        11 * sizeof(char),
        12 * sizeof(char),
        13 * sizeof(char),
        16 * sizeof(char),
        17 * sizeof(char),
        19 * sizeof(char),
        22 * sizeof(char),
        25 * sizeof(char),
        26 * sizeof(char),
        27 * sizeof(char)};
    c_b_val = {'a', 'e', 'e', 'i', 'm', 'b', 'b', 'f', 'f', 'f',
               'f', 'j', 'n', 'c', 'c', 'c', 'g', 'k', 'k', 'o',
               'o', 'o', 'd', 'd', 'd', 'h', 'l', 'p', 'p'};
    c_c = {1.1f, 1.2f, 5.1f, 5.2f, 9.1f,  9.2f,  13.1f, 13.2f,
           2.1f, 2.2f, 6.1f, 6.2f, 10.1f, 10.2f, 14.1f, 14.2f,
           3.1f, 3.2f, 7.1f, 7.2f, 11.1f, 11.2f, 15.1f, 15.2f,
           4.1f, 4.2f, 8.1f, 8.2f, 12.1f, 12.2f, 16.1f, 16.2f};
    c_coords_dim1 = {1, 2, 3, 4, 1, 2, 3, 4, 1, 2, 3, 4, 1, 2, 3, 4};
    c_coords_dim2 = {1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3, 4, 4, 4, 4};
  }

  SECTION("- subarray: global") {
    subarray_layout = TILEDB_GLOBAL_ORDER;
    c_a = {1, 2, 5, 6, 3, 4, 7, 8, 9, 10, 13, 14, 11, 12, 15, 16};
    c_b_off = {
        0,
        sizeof(char),
        3 * sizeof(char),
        5 * sizeof(char),
        9 * sizeof(char),
        12 * sizeof(char),
        15 * sizeof(char),
        16 * sizeof(char),
        17 * sizeof(char),
        18 * sizeof(char),
        19 * sizeof(char),
        20 * sizeof(char),
        21 * sizeof(char),
        23 * sizeof(char),
        24 * sizeof(char),
        27 * sizeof(char)};
    c_b_val = {'a', 'b', 'b', 'e', 'e', 'f', 'f', 'f', 'f', 'c',
               'c', 'c', 'd', 'd', 'd', 'g', 'h', 'i', 'j', 'm',
               'n', 'k', 'k', 'l', 'o', 'o', 'o', 'p', 'p'};
    c_c = {1.1f,  1.2f,  2.1f,  2.2f,  5.1f,  5.2f,  6.1f,  6.2f,
           3.1f,  3.2f,  4.1f,  4.2f,  7.1f,  7.2f,  8.1f,  8.2f,
           9.1f,  9.2f,  10.1f, 10.2f, 13.1f, 13.2f, 14.1f, 14.2f,
           11.1f, 11.2f, 12.1f, 12.2f, 15.1f, 15.2f, 16.1f, 16.2f};
    c_coords_dim1 = {1, 1, 2, 2, 1, 1, 2, 2, 3, 3, 4, 4, 3, 3, 4, 4};
    c_coords_dim2 = {1, 2, 1, 2, 3, 4, 3, 4, 1, 2, 1, 2, 3, 4, 3, 4};
  }

  // Read
  SubarrayRanges<uint64_t> ranges = {{1, 4}, {1, 4}};
  read_array(ctx_, array_, ranges, subarray_layout, buffers);

  // Check results
  CHECK(a == c_a);
  CHECK(b_off == c_b_off);
  b_val.resize(c_b_val.size());
  CHECK(b_val == c_b_val);
  CHECK(c == c_c);
  CHECK(coords_dim1 == c_coords_dim1);
  CHECK(coords_dim2 == c_coords_dim2);

  // Clean up
  close_array(ctx_, array_);
}

TEST_CASE_METHOD(
    CDenseArrayFx,
    "Dense array: 2D, error, multi-range global reads",
    "[capi][dense2][2D][error][global][MR]") {
  // Create and write array
  create_default_array_2d();
  write_default_array_2d();

  // Open array
  open_array(ctx_, array_, TILEDB_READ);

  // Create subarray
  SubarrayRanges<uint64_t> ranges = {{1, 4, 1, 3}, {1, 4}};
  (void)ranges;

  // Create query
  tiledb_query_t* query;
  int rc = tiledb_query_alloc(ctx_, array_, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_GLOBAL_ORDER);
  CHECK(rc == TILEDB_OK);

  // Set a multi-range subarray
  tiledb_subarray_t* subarray;
  rc = tiledb_subarray_alloc(ctx_, array_, &subarray);
  uint64_t start = 1, end = 1;
  rc = tiledb_subarray_add_range(ctx_, subarray, 0, &start, &end, nullptr);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_add_range(ctx_, subarray, 0, &start, &end, nullptr);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray_t(ctx_, query, subarray);
  CHECK(rc == TILEDB_OK);

  // Submit
  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_ERR);

  // Clean up
  close_array(ctx_, array_);
  tiledb_query_free(&query);
  tiledb_subarray_free(&subarray);
}
