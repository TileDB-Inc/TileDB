/**
 * @file unit-Reader.cc
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
 * Tests the `Reader` class.
 */

#include "test/src/helpers.h"
#include "test/src/vfs_helpers.h"
#include "tiledb/common/dynamic_memory/dynamic_memory.h"
#include "tiledb/common/heap_memory.h"
#include "tiledb/common/logger.h"
#include "tiledb/sm/c_api/tiledb_struct_def.h"
#include "tiledb/sm/misc/types.h"
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
  const std::vector<std::unique_ptr<SupportedFs>> fs_vec_;
  std::string temp_dir_;
  std::string array_name_;
  const char* ARRAY_NAME = "reader";
  tiledb_array_t* array_ = nullptr;

  ReaderFx();
  ~ReaderFx();
};

ReaderFx::ReaderFx()
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
  Config config;
  std::unordered_map<std::string, tiledb::sm::QueryBuffer> buffers;
  Subarray subarray;
  QueryCondition condition;
  ThreadPool tp_cpu, tp_io;
  StorageManager storage_manager(
      &tp_cpu, &tp_io, &g_helper_stats, g_helper_logger());
  Array array(URI("my_array"), &storage_manager);
  Reader reader(
      &g_helper_stats,
      g_helper_logger(),
      nullptr,
      &array,
      config,
      buffers,
      subarray,
      Layout::ROW_MAJOR,
      condition);
  unsigned dim_num = 2;
  auto size = 2 * sizeof(int32_t);
  int32_t domain_vec[] = {1, 10, 1, 15};
  NDRange domain = {Range(&domain_vec[0], size), Range(&domain_vec[2], size)};
  std::vector<int32_t> tile_extents_vec = {2, 5};
  std::vector<ByteVecValue> tile_extents(2);
  tile_extents[0].assign_as<int32_t>(tile_extents_vec[0]);
  tile_extents[1].assign_as<int32_t>(tile_extents_vec[1]);
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

  NDRange ds1 = {Range(&domain_slice_1[0], size),
                 Range(&domain_slice_1[2], size)};
  NDRange ds2 = {Range(&domain_slice_2[0], size),
                 Range(&domain_slice_2[2], size)};
  NDRange ds3 = {Range(&domain_slice_3[0], size),
                 Range(&domain_slice_3[2], size)};
  NDRange dsd = domain;

  std::vector<TileDomain<int32_t>> frag_tile_domains;
  frag_tile_domains.emplace_back(
      TileDomain<int32_t>(3, domain, ds3, tile_extents, layout));
  frag_tile_domains.emplace_back(
      TileDomain<int32_t>(2, domain, ds2, tile_extents, layout));
  frag_tile_domains.emplace_back(
      TileDomain<int32_t>(1, domain, ds1, tile_extents, layout));
  TileDomain<int32_t> array_tile_domain(
      UINT32_MAX, domain, dsd, tile_extents, layout);

  Dimension d1("d1", Datatype::INT32);
  d1.set_domain(domain_vec);
  d1.set_tile_extent(&tile_extents_vec[0]);
  Dimension d2("d2", Datatype::INT32);
  d2.set_domain(&domain_vec[2]);
  d2.set_tile_extent(&tile_extents_vec[1]);
  Domain dom;
  CHECK(dom.add_dimension(&d1).ok());
  CHECK(dom.add_dimension(&d2).ok());

  auto schema = tdb::make_shared<ArraySchema>(HERE());
  CHECK(schema->set_domain(&dom).ok());

  std::vector<tdb_shared_ptr<FragmentMetadata>> fragments;
  for (uint64_t i = 0; i < frag_tile_domains.size() + 1; i++) {
    tdb_shared_ptr<FragmentMetadata> fragment =
        tdb::make_shared<FragmentMetadata>(
            HERE(),
            nullptr,
            nullptr,
            schema,
            URI(),
            std::make_pair<uint64_t, uint64_t>(0, 0),
            true);
    fragments.emplace_back(std::move(fragment));
  }

  // Compute result space tiles map
  std::map<const int32_t*, ResultSpaceTile<int32_t>> result_space_tiles;
  Reader::compute_result_space_tiles<int32_t>(
      fragments,
      tile_coords,
      array_tile_domain,
      frag_tile_domains,
      result_space_tiles);
  CHECK(result_space_tiles.size() == 6);

  // Result tiles for fragment #1
  ResultTile result_tile_1_0_1(1, 0, *(schema.get()));
  ResultTile result_tile_1_2_1(1, 2, *(schema.get()));

  // Result tiles for fragment #2
  ResultTile result_tile_1_0_2(2, 0, *(schema.get()));

  // Result tiles for fragment #3
  ResultTile result_tile_2_0_3(3, 0, *(schema.get()));
  ResultTile result_tile_3_0_3(3, 2, *(schema.get()));

  // Initialize result_space_tiles
  ResultSpaceTile<int32_t> rst_1_0;
  rst_1_0.set_start_coords({3, 1});
  rst_1_0.append_frag_domain(2, ds2);
  rst_1_0.append_frag_domain(1, ds1);
  rst_1_0.set_result_tile(1, result_tile_1_0_1);
  rst_1_0.set_result_tile(2, result_tile_1_0_2);
  ResultSpaceTile<int32_t> rst_1_2;
  rst_1_2.set_start_coords({3, 11});
  rst_1_2.append_frag_domain(1, ds1);
  rst_1_2.set_result_tile(1, result_tile_1_2_1);
  ResultSpaceTile<int32_t> rst_2_0;
  rst_2_0.set_start_coords({5, 1});
  rst_2_0.append_frag_domain(3, ds3);
  rst_2_0.set_result_tile(3, result_tile_2_0_3);
  ResultSpaceTile<int32_t> rst_2_2;
  rst_2_2.set_start_coords({5, 11});
  ResultSpaceTile<int32_t> rst_3_0;
  rst_3_0.set_start_coords({7, 1});
  rst_3_0.append_frag_domain(3, ds3);
  rst_3_0.set_result_tile(3, result_tile_3_0_3);
  ResultSpaceTile<int32_t> rst_3_2;
  rst_3_2.set_start_coords({7, 11});

  // Check correctness
  CHECK(result_space_tiles[(const int32_t*)&(tile_coords[0][0])] == rst_1_0);
  CHECK(result_space_tiles[(const int32_t*)&(tile_coords[1][0])] == rst_1_2);
  CHECK(result_space_tiles[(const int32_t*)&(tile_coords[2][0])] == rst_2_0);
  CHECK(result_space_tiles[(const int32_t*)&(tile_coords[3][0])] == rst_2_2);
  CHECK(result_space_tiles[(const int32_t*)&(tile_coords[4][0])] == rst_3_0);
  CHECK(result_space_tiles[(const int32_t*)&(tile_coords[5][0])] == rst_3_2);
}
