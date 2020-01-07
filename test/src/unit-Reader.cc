/**
 * @file unit-Reader.cc
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
 * Tests the `Reader` class.
 */

#include "test/src/helpers.h"
#include "tiledb/sm/c_api/tiledb_struct_def.h"
#include "tiledb/sm/query/reader.h"

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

struct ReaderFx {
  tiledb_ctx_t* ctx_;
  tiledb_vfs_t* vfs_;
  bool s3_supported_, hdfs_supported_;
  std::string temp_dir_;
  const std::string s3_bucket_name_ =
      "s3://" + random_bucket_name("tiledb") + "/";
  std::string array_name_;
  const char* ARRAY_NAME = "reader";
  tiledb_array_t* array_ = nullptr;

  ReaderFx();
  ~ReaderFx();
};

ReaderFx::ReaderFx() {
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

ReaderFx::~ReaderFx() {
  tiledb_array_free(&array_);
  remove_dir(temp_dir_, ctx_, vfs_);
  tiledb_ctx_free(&ctx_);
  tiledb_vfs_free(&vfs_);
}

/* ********************************* */
/*                TESTS              */
/* ********************************* */

TEST_CASE_METHOD(
    ReaderFx,
    "Reader: Compute result space tiles, 2D",
    "[Reader][2d][compute_result_space_tiles]") {
  Reader reader;
  unsigned dim_num = 2;
  std::vector<int32_t> domain = {1, 10, 1, 15};
  std::vector<int32_t> tile_extents = {2, 5};
  Layout layout = Layout::ROW_MAJOR;

  // Tile coords
  int32_t tile_coords_1_0[] = {1, 0};
  int32_t tile_coords_1_2[] = {1, 2};
  int32_t tile_coords_2_0[] = {2, 0};
  int32_t tile_coords_2_2[] = {2, 2};
  int32_t tile_coords_3_0[] = {3, 0};
  int32_t tile_coords_3_2[] = {3, 2};

  // Initialize tile coordinates
  std::vector<uint8_t> tile_coords_el;
  size_t coords_size = dim_num * sizeof(int32_t);
  tile_coords_el.resize(coords_size);
  std::vector<std::vector<uint8_t>> tile_coords;
  std::memcpy(&tile_coords_el[0], &tile_coords_1_0[0], coords_size);
  tile_coords.push_back(tile_coords_el);
  std::memcpy(&tile_coords_el[0], &tile_coords_1_2[0], coords_size);
  tile_coords.push_back(tile_coords_el);
  std::memcpy(&tile_coords_el[0], &tile_coords_2_0[0], coords_size);
  tile_coords.push_back(tile_coords_el);
  std::memcpy(&tile_coords_el[0], &tile_coords_2_2[0], coords_size);
  tile_coords.push_back(tile_coords_el);
  std::memcpy(&tile_coords_el[0], &tile_coords_3_0[0], coords_size);
  tile_coords.push_back(tile_coords_el);
  std::memcpy(&tile_coords_el[0], &tile_coords_3_2[0], coords_size);
  tile_coords.push_back(tile_coords_el);

  // Initialize tile domains
  std::vector<int32_t> domain_slice_1 = {3, 4, 1, 12};
  std::vector<int32_t> domain_slice_2 = {4, 5, 2, 4};
  std::vector<int32_t> domain_slice_3 = {5, 7, 1, 9};
  std::vector<TileDomain<int32_t>> frag_tile_domains;
  frag_tile_domains.emplace_back(TileDomain<int32_t>(
      3, dim_num, &domain[0], &domain_slice_3[0], &tile_extents[0], layout));
  frag_tile_domains.emplace_back(TileDomain<int32_t>(
      2, dim_num, &domain[0], &domain_slice_2[0], &tile_extents[0], layout));
  frag_tile_domains.emplace_back(TileDomain<int32_t>(
      1, dim_num, &domain[0], &domain_slice_1[0], &tile_extents[0], layout));
  TileDomain<int32_t> array_tile_domain(
      UINT32_MAX, dim_num, &domain[0], &domain[0], &tile_extents[0], layout);

  // Compute result space tiles map
  std::map<const int32_t*, ResultSpaceTile<int32_t>> result_space_tiles;
  Reader::compute_result_space_tiles<int32_t>(
      tile_coords, array_tile_domain, frag_tile_domains, &result_space_tiles);
  CHECK(result_space_tiles.size() == 6);

  // Result tiles for fragment #1
  ResultTile result_tile_1_0_1(1, 0, dim_num);
  ResultTile result_tile_1_2_1(1, 2, dim_num);

  // Result tiles for fragment #2
  ResultTile result_tile_1_0_2(2, 0, dim_num);

  // Result tiles for fragment #3
  ResultTile result_tile_2_0_3(3, 0, dim_num);
  ResultTile result_tile_3_0_3(3, 2, dim_num);

  // Initialize frag domains
  typedef std::pair<unsigned, const int32_t*> FragDomain;
  std::vector<FragDomain> frag_domain_1_0 = {FragDomain(2, &domain_slice_2[0]),
                                             FragDomain(1, &domain_slice_1[0])};
  std::vector<FragDomain> frag_domain_1_2 = {FragDomain(1, &domain_slice_1[0])};
  std::vector<FragDomain> frag_domain_2_0 = {FragDomain(3, &domain_slice_3[0])};
  std::vector<FragDomain> frag_domain_3_0 = {FragDomain(3, &domain_slice_3[0])};
  std::vector<FragDomain> frag_domain_2_2 = {};
  std::vector<FragDomain> frag_domain_3_2 = {};

  // Initialize result tiles
  std::map<unsigned, ResultTile> result_tiles_1_0 = {
      std::pair<unsigned, ResultTile>(1, result_tile_1_0_1),
      std::pair<unsigned, ResultTile>(2, result_tile_1_0_2),
  };
  std::map<unsigned, ResultTile> result_tiles_1_2 = {
      std::pair<unsigned, ResultTile>(1, result_tile_1_2_1),
  };
  std::map<unsigned, ResultTile> result_tiles_2_0 = {
      std::pair<unsigned, ResultTile>(3, result_tile_2_0_3),
  };
  std::map<unsigned, ResultTile> result_tiles_3_0 = {
      std::pair<unsigned, ResultTile>(3, result_tile_3_0_3),
  };
  std::map<unsigned, ResultTile> result_tiles_2_2 = {};
  std::map<unsigned, ResultTile> result_tiles_3_2 = {};

  // Initialize result_space_tiles
  ResultSpaceTile<int32_t> rst_1_0 = {
      {3, 1}, frag_domain_1_0, result_tiles_1_0};
  ResultSpaceTile<int32_t> rst_1_2 = {
      {3, 11}, frag_domain_1_2, result_tiles_1_2};
  ResultSpaceTile<int32_t> rst_2_0 = {
      {5, 1}, frag_domain_2_0, result_tiles_2_0};
  ResultSpaceTile<int32_t> rst_2_2 = {
      {5, 11}, frag_domain_2_2, result_tiles_2_2};
  ResultSpaceTile<int32_t> rst_3_0 = {
      {7, 1}, frag_domain_3_0, result_tiles_3_0};
  ResultSpaceTile<int32_t> rst_3_2 = {
      {7, 11}, frag_domain_3_2, result_tiles_3_2};

  // Prepare correct space tiles map
  std::map<const int32_t*, ResultSpaceTile<int32_t>> c_result_space_tiles;
  c_result_space_tiles[(const int32_t*)&(tile_coords[0][0])] = rst_1_0;
  c_result_space_tiles[(const int32_t*)&(tile_coords[1][0])] = rst_1_2;
  c_result_space_tiles[(const int32_t*)&(tile_coords[2][0])] = rst_2_0;
  c_result_space_tiles[(const int32_t*)&(tile_coords[3][0])] = rst_2_2;
  c_result_space_tiles[(const int32_t*)&(tile_coords[4][0])] = rst_3_0;
  c_result_space_tiles[(const int32_t*)&(tile_coords[5][0])] = rst_3_2;

  // Check correctness
  CHECK(result_space_tiles == c_result_space_tiles);
}
