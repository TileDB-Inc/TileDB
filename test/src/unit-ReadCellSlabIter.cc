/**
 * @file unit-ReadCellSlabIter.cc
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
 * Tests the `ReadCellSlabIter` class.
 */

#include "test/src/helpers.h"
#include "tiledb/sm/c_api/tiledb_struct_def.h"
#include "tiledb/sm/query/read_cell_slab_iter.h"

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

struct ReadCellSlabIterFx {
  tiledb_ctx_t* ctx_;
  tiledb_vfs_t* vfs_;
  bool s3_supported_, hdfs_supported_, azure_supported_;
  std::string temp_dir_;
  const std::string s3_bucket_name_ = "s3://" + random_name("tiledb") + "/";
  const std::string azure_container_name_ =
      "azure://" + random_name("tiledb") + "/";
  std::string array_name_;
  const char* ARRAY_NAME = "read_cell_slab_iter";
  tiledb_array_t* array_ = nullptr;

  ReadCellSlabIterFx();
  ~ReadCellSlabIterFx();

  template <class T>
  void check_iter(
      ReadCellSlabIter<T>* iter,
      const std::vector<std::vector<uint64_t>>& c_result_cell_slabs);
  template <class T>
  void create_result_space_tiles(
      const Domain* dom,
      const NDRange& dom_ndrange,
      Layout layout,
      const std::vector<NDRange>& domain_slices,
      const std::vector<std::vector<uint8_t>>& tile_coords,
      std::map<const T*, ResultSpaceTile<T>>* result_space_tiles);
};

ReadCellSlabIterFx::ReadCellSlabIterFx() {
  ctx_ = nullptr;
  vfs_ = nullptr;
  hdfs_supported_ = false;
  s3_supported_ = false;
  azure_supported_ = false;

  get_supported_fs(&s3_supported_, &hdfs_supported_, &azure_supported_);
  create_ctx_and_vfs(s3_supported_, azure_supported_, &ctx_, &vfs_);
  create_s3_bucket(s3_bucket_name_, s3_supported_, ctx_, vfs_);
  create_azure_container(azure_container_name_, azure_supported_, ctx_, vfs_);

// Create temporary directory based on the supported filesystem
#ifdef _WIN32
  temp_dir_ = tiledb::sm::Win::current_dir() + "\\tiledb_test\\";
#else
  temp_dir_ = "file://" + tiledb::sm::Posix::current_dir() + "/tiledb_test/";
#endif
  if (s3_supported_)
    temp_dir_ = s3_bucket_name_ + "tiledb/test/";
  if (azure_supported_)
    temp_dir_ = azure_container_name_ + "tiledb/test/";
  // Removing this for now as these tests take too long on HDFS
  // if (hdfs_supported_)
  //   temp_dir_ = "hdfs:///tiledb_test/";
  create_dir(temp_dir_, ctx_, vfs_);

  array_name_ = temp_dir_ + ARRAY_NAME;
  int rc = tiledb_array_alloc(ctx_, array_name_.c_str(), &array_);
  CHECK(rc == TILEDB_OK);
}

ReadCellSlabIterFx::~ReadCellSlabIterFx() {
  tiledb_array_free(&array_);
  remove_dir(temp_dir_, ctx_, vfs_);
  tiledb_ctx_free(&ctx_);
  tiledb_vfs_free(&vfs_);
}

template <class T>
void ReadCellSlabIterFx::check_iter(
    ReadCellSlabIter<T>* iter,
    const std::vector<std::vector<uint64_t>>& c_result_cell_slabs) {
  CHECK(iter->end());
  CHECK(iter->begin().ok());
  for (const auto& rcs : c_result_cell_slabs) {
    auto result_cell_slab = iter->result_cell_slab();
    // result_cell_slab.print();

    if (rcs[0] == UINT64_MAX) {
      CHECK(result_cell_slab.tile_ == nullptr);
    } else {
      CHECK(result_cell_slab.tile_ != nullptr);
      CHECK(result_cell_slab.tile_->frag_idx() == rcs[0]);
      CHECK(result_cell_slab.tile_->tile_idx() == rcs[1]);
    }
    CHECK(result_cell_slab.start_ == rcs[2]);
    CHECK(result_cell_slab.length_ == rcs[3]);
    CHECK(!iter->end());
    ++(*iter);
  }

  CHECK(iter->end());
}

template <class T>
void ReadCellSlabIterFx::create_result_space_tiles(
    const Domain* dom,
    const NDRange& dom_ndrange,
    Layout layout,
    const std::vector<NDRange>& domain_slices,
    const std::vector<std::vector<uint8_t>>& tile_coords,
    std::map<const T*, ResultSpaceTile<T>>* result_space_tiles) {
  auto domain = dom->domain();
  const auto& tile_extents = dom->tile_extents();
  std::vector<TileDomain<T>> frag_tile_domains;
  for (size_t i = 0; i < domain_slices.size(); ++i) {
    frag_tile_domains.emplace_back(
        (unsigned)(domain_slices.size() - i),
        domain,
        domain_slices[i],
        tile_extents,
        layout);
  }
  TileDomain<T> array_tile_domain(
      UINT32_MAX, domain, dom_ndrange, tile_extents, layout);
  Reader::compute_result_space_tiles<T>(
      dom,
      tile_coords,
      array_tile_domain,
      frag_tile_domains,
      result_space_tiles);
}

/* ********************************* */
/*                TESTS              */
/* ********************************* */

TEST_CASE_METHOD(
    ReadCellSlabIterFx,
    "ReadCellSlabIter: Empty iterator",
    "[ReadCellSlabIter][empty]") {
  Subarray* subarray = nullptr;
  std::map<const int32_t*, ResultSpaceTile<int32_t>> result_space_tiles;
  std::vector<ResultCoords> result_coords;
  ReadCellSlabIter<int32_t> iter(subarray, &result_space_tiles, &result_coords);
  CHECK(iter.end());
  CHECK(iter.begin().ok());
  CHECK(iter.end());
}

TEST_CASE_METHOD(
    ReadCellSlabIterFx,
    "ReadCellSlabIter: Test 1D slabs, 1 fragment, full overlap",
    "[ReadCellSlabIter][slabs][1d][1f][full_overlap]") {
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
  SubarrayRanges<uint64_t> ranges = {{5, 15}};
  Layout subarray_layout = Layout::ROW_MAJOR;
  create_subarray(array_->array_, ranges, subarray_layout, &subarray);
  subarray.compute_tile_coords<uint64_t>();

  // Create result space tiles
  std::vector<uint64_t> slice = {1, 100};
  NDRange ds = {Range(&slice[0], 2 * sizeof(uint64_t))};
  std::vector<NDRange> domain_slices = {ds};
  const auto& tile_coords = subarray.tile_coords();
  std::map<const uint64_t*, ResultSpaceTile<uint64_t>> result_space_tiles;
  auto dom = array_->array_->array_schema()->domain();
  create_result_space_tiles(
      dom,
      dom->domain(),
      subarray_layout,
      domain_slices,
      tile_coords,
      &result_space_tiles);

  // Check iterator
  std::vector<ResultCoords> result_coords;
  ReadCellSlabIter<uint64_t> iter(
      &subarray, &result_space_tiles, &result_coords);
  std::vector<std::vector<uint64_t>> c_result_cell_slabs = {
      {1, 0, 4, 6},
      {1, 1, 0, 5},
  };
  check_iter<uint64_t>(&iter, c_result_cell_slabs);

  close_array(ctx_, array_);
}

TEST_CASE_METHOD(
    ReadCellSlabIterFx,
    "ReadCellSlabIter: Test 1D slabs, 1 fragment, no overlap",
    "[ReadCellSlabIter][slabs][1d][1f][no_overlap]") {
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
  SubarrayRanges<uint64_t> ranges = {{5, 15}};
  Layout subarray_layout = Layout::ROW_MAJOR;
  create_subarray(array_->array_, ranges, subarray_layout, &subarray);
  subarray.compute_tile_coords<uint64_t>();

  // Create result space tiles
  std::vector<uint64_t> slice = {20, 30};
  NDRange ds = {Range(&slice[0], 2 * sizeof(uint64_t))};
  std::vector<NDRange> domain_slices = {ds};
  const auto& tile_coords = subarray.tile_coords();
  std::map<const uint64_t*, ResultSpaceTile<uint64_t>> result_space_tiles;
  auto dom = array_->array_->array_schema()->domain();
  create_result_space_tiles(
      dom,
      dom->domain(),
      subarray_layout,
      domain_slices,
      tile_coords,
      &result_space_tiles);

  // Check iterator
  std::vector<ResultCoords> result_coords;
  ReadCellSlabIter<uint64_t> iter(
      &subarray, &result_space_tiles, &result_coords);
  std::vector<std::vector<uint64_t>> c_result_cell_slabs = {
      {UINT64_MAX, 0, 4, 6},
      {UINT64_MAX, 1, 0, 5},
  };
  check_iter<uint64_t>(&iter, c_result_cell_slabs);

  close_array(ctx_, array_);
}

TEST_CASE_METHOD(
    ReadCellSlabIterFx,
    "ReadCellSlabIter: Test 1D slabs, 2 fragments",
    "[ReadCellSlabIter][slabs][1d][2f]") {
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
  subarray.compute_tile_coords<uint64_t>();

  // Create result space tiles
  std::vector<uint64_t> slice_1 = {5, 12};
  std::vector<uint64_t> slice_2 = {4, 15};
  auto size = 2 * sizeof(uint64_t);
  NDRange ds1 = {Range(&slice_1[0], size)};
  NDRange ds2 = {Range(&slice_2[0], size)};
  std::vector<NDRange> domain_slices = {ds1, ds2};
  const auto& tile_coords = subarray.tile_coords();
  std::map<const uint64_t*, ResultSpaceTile<uint64_t>> result_space_tiles;
  auto dom = array_->array_->array_schema()->domain();
  create_result_space_tiles(
      dom,
      dom->domain(),
      subarray_layout,
      domain_slices,
      tile_coords,
      &result_space_tiles);

  // Check iterator
  std::vector<ResultCoords> result_coords;
  ReadCellSlabIter<uint64_t> iter(
      &subarray, &result_space_tiles, &result_coords);
  std::vector<std::vector<uint64_t>> c_result_cell_slabs = {
      {2, 0, 4, 6},
      {2, 1, 0, 2},
      {1, 1, 2, 3},
      {UINT64_MAX, 0, 2, 1},
      {1, 0, 3, 1},
      {2, 0, 4, 1},
      {2, 1, 0, 2},
      {1, 1, 2, 2},
  };
  check_iter<uint64_t>(&iter, c_result_cell_slabs);

  close_array(ctx_, array_);
}

TEST_CASE_METHOD(
    ReadCellSlabIterFx,
    "ReadCellSlabIter: Test 1D slabs, 1 dense fragment, 2 sparse fragments",
    "[ReadCellSlabIter][slabs][1d][1df][2sf]") {
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
  SubarrayRanges<uint64_t> ranges = {{3, 15, 18, 20}};
  Layout subarray_layout = Layout::ROW_MAJOR;
  create_subarray(array_->array_, ranges, subarray_layout, &subarray);
  subarray.compute_tile_coords<uint64_t>();

  // Create result space tiles
  std::vector<uint64_t> slice = {3, 12};
  auto size = 2 * sizeof(uint64_t);
  NDRange ds = {Range(&slice[0], size)};
  std::vector<NDRange> domain_slices = {ds};
  const auto& tile_coords = subarray.tile_coords();
  std::map<const uint64_t*, ResultSpaceTile<uint64_t>> result_space_tiles;
  auto dom = array_->array_->array_schema()->domain();
  create_result_space_tiles(
      dom,
      dom->domain(),
      subarray_layout,
      domain_slices,
      tile_coords,
      &result_space_tiles);

  // Create result coordinates
  std::vector<ResultCoords> result_coords;
  ResultTile result_tile_2_0(2, 0, dom);
  ResultTile result_tile_3_0(3, 0, dom);
  ResultTile result_tile_3_1(3, 1, dom);

  result_tile_2_0.init_coord_tile("d", 0);
  result_tile_3_0.init_coord_tile("d", 0);
  result_tile_3_1.init_coord_tile("d", 0);

  std::vector<uint64_t> vec_2_0 = {1000, 3, 1000, 5};
  Buffer buff_2_0(&vec_2_0[0], vec_2_0.size() * sizeof(uint64_t));
  ChunkedBuffer chunked_buffer_2_0;
  REQUIRE(Tile::buffer_to_contiguous_fixed_chunks(
              buff_2_0, 0, sizeof(uint64_t), &chunked_buffer_2_0)
              .ok());
  Tile tile_2_0(
      Datatype::UINT64, sizeof(uint64_t), 0, &chunked_buffer_2_0, false);
  auto tile_tuple = result_tile_2_0.tile_tuple("d");
  REQUIRE(tile_tuple != nullptr);
  std::get<0>(*tile_tuple) = tile_2_0;

  std::vector<uint64_t> vec_3_0 = {1000, 1000, 8, 9};
  Buffer buff_3_0(&vec_3_0[0], vec_3_0.size() * sizeof(uint64_t));
  ChunkedBuffer chunked_buffer_3_0;
  REQUIRE(Tile::buffer_to_contiguous_fixed_chunks(
              buff_3_0, 0, sizeof(uint64_t), &chunked_buffer_3_0)
              .ok());
  Tile tile_3_0(
      Datatype::UINT64, sizeof(uint64_t), 0, &chunked_buffer_3_0, false);
  tile_tuple = result_tile_3_0.tile_tuple("d");
  REQUIRE(tile_tuple != nullptr);
  std::get<0>(*tile_tuple) = tile_3_0;

  std::vector<uint64_t> vec_3_1 = {1000, 12, 19, 1000};
  Buffer buff_3_1(&vec_3_1[0], vec_3_1.size() * sizeof(uint64_t));
  ChunkedBuffer chunked_buffer_3_1;
  REQUIRE(Tile::buffer_to_contiguous_fixed_chunks(
              buff_3_1, 0, sizeof(uint64_t), &chunked_buffer_3_1)
              .ok());
  Tile tile_3_1(
      Datatype::UINT64, sizeof(uint64_t), 0, &chunked_buffer_3_1, false);
  tile_tuple = result_tile_3_1.tile_tuple("d");
  REQUIRE(tile_tuple != nullptr);
  std::get<0>(*tile_tuple) = tile_3_1;

  result_coords.emplace_back(&result_tile_2_0, 1);
  result_coords.emplace_back(&result_tile_2_0, 3);
  result_coords.emplace_back(&result_tile_3_0, 2);
  result_coords.emplace_back(&result_tile_3_0, 3);
  result_coords.back().invalidate();
  result_coords.emplace_back(&result_tile_3_1, 1);
  result_coords.emplace_back(&result_tile_3_1, 2);

  // Check iterator
  ReadCellSlabIter<uint64_t> iter(
      &subarray, &result_space_tiles, &result_coords);
  std::vector<std::vector<uint64_t>> c_result_cell_slabs = {
      {2, 0, 1, 1},
      {1, 0, 3, 1},
      {2, 0, 3, 1},
      {1, 0, 5, 2},
      {3, 0, 2, 1},
      {1, 0, 8, 2},
      {1, 1, 0, 1},
      {3, 1, 1, 1},
      {UINT64_MAX, 1, 2, 3},
      {UINT64_MAX, 1, 7, 1},
      {3, 1, 2, 1},
      {UINT64_MAX, 1, 9, 1},
  };
  check_iter<uint64_t>(&iter, c_result_cell_slabs);

  close_array(ctx_, array_);
}

TEST_CASE_METHOD(
    ReadCellSlabIterFx,
    "ReadCellSlabIter: Test 2D slabs, 1 range, 1 dense fragment, full "
    "overlap",
    "[ReadCellSlabIter][slabs][2d][1r][1f][full_overlap]") {
  Layout subarray_layout = Layout::ROW_MAJOR;
  Layout tile_domain_layout = Layout::ROW_MAJOR;
  tiledb_layout_t tile_order = TILEDB_ROW_MAJOR;
  tiledb_layout_t cell_order = TILEDB_ROW_MAJOR;
  std::vector<std::vector<uint64_t>> c_result_cell_slabs = {};

  SECTION("- tile: row, cell: row, subarray: row") {
    tile_order = TILEDB_ROW_MAJOR;
    cell_order = TILEDB_ROW_MAJOR;
    subarray_layout = Layout::ROW_MAJOR;
    tile_domain_layout = Layout::ROW_MAJOR;
    c_result_cell_slabs = {
        {1, 0, 4, 2},
        {1, 1, 3, 3},
        {1, 0, 7, 2},
        {1, 1, 6, 3},
    };
  }

  SECTION("- tile: row, cell: col, subarray: row") {
    tile_order = TILEDB_ROW_MAJOR;
    cell_order = TILEDB_COL_MAJOR;
    subarray_layout = Layout::ROW_MAJOR;
    tile_domain_layout = Layout::ROW_MAJOR;
    c_result_cell_slabs = {
        {1, 0, 4, 2},
        {1, 1, 1, 3},
        {1, 0, 5, 2},
        {1, 1, 2, 3},
    };
  }

  SECTION("- tile: col, cell: row, subarray: row") {
    tile_order = TILEDB_COL_MAJOR;
    cell_order = TILEDB_ROW_MAJOR;
    subarray_layout = Layout::ROW_MAJOR;
    tile_domain_layout = Layout::COL_MAJOR;
    c_result_cell_slabs = {
        {1, 0, 4, 2},
        {1, 2, 3, 3},
        {1, 0, 7, 2},
        {1, 2, 6, 3},
    };
  }

  SECTION("- tile: col, cell: col, subarray: row") {
    tile_order = TILEDB_COL_MAJOR;
    cell_order = TILEDB_COL_MAJOR;
    subarray_layout = Layout::ROW_MAJOR;
    tile_domain_layout = Layout::COL_MAJOR;
    c_result_cell_slabs = {
        {1, 0, 4, 2},
        {1, 2, 1, 3},
        {1, 0, 5, 2},
        {1, 2, 2, 3},
    };
  }

  SECTION("- tile: row, cell: row, subarray: col") {
    tile_order = TILEDB_ROW_MAJOR;
    cell_order = TILEDB_ROW_MAJOR;
    subarray_layout = Layout::COL_MAJOR;
    tile_domain_layout = Layout::ROW_MAJOR;
    c_result_cell_slabs = {
        {1, 0, 4, 2},
        {1, 0, 5, 2},
        {1, 1, 3, 2},
        {1, 1, 4, 2},
        {1, 1, 5, 2},
    };
  }

  SECTION("- tile: row, cell: col, subarray: col") {
    tile_order = TILEDB_ROW_MAJOR;
    cell_order = TILEDB_COL_MAJOR;
    subarray_layout = Layout::COL_MAJOR;
    tile_domain_layout = Layout::ROW_MAJOR;
    c_result_cell_slabs = {
        {1, 0, 4, 2},
        {1, 0, 7, 2},
        {1, 1, 1, 2},
        {1, 1, 4, 2},
        {1, 1, 7, 2},
    };
  }

  SECTION("- tile: col, cell: row, subarray: col") {
    tile_order = TILEDB_COL_MAJOR;
    cell_order = TILEDB_ROW_MAJOR;
    subarray_layout = Layout::COL_MAJOR;
    tile_domain_layout = Layout::COL_MAJOR;
    c_result_cell_slabs = {
        {1, 0, 4, 2},
        {1, 0, 5, 2},
        {1, 2, 3, 2},
        {1, 2, 4, 2},
        {1, 2, 5, 2},
    };
  }

  SECTION("- tile: col, cell: col, subarray: col") {
    tile_order = TILEDB_COL_MAJOR;
    cell_order = TILEDB_COL_MAJOR;
    subarray_layout = Layout::COL_MAJOR;
    tile_domain_layout = Layout::COL_MAJOR;
    c_result_cell_slabs = {
        {1, 0, 4, 2},
        {1, 0, 7, 2},
        {1, 2, 1, 2},
        {1, 2, 4, 2},
        {1, 2, 7, 2},
    };
  }

  // Create array
  uint64_t domain[] = {1, 6, 1, 6};
  uint64_t tile_extents[] = {3, 3};
  create_array(
      ctx_,
      array_name_,
      TILEDB_DENSE,
      {"d1", "d2"},
      {TILEDB_UINT64, TILEDB_UINT64},
      {&domain[0], &domain[2]},
      {&tile_extents[0], &tile_extents[1]},
      {"a", "b"},
      {TILEDB_INT32, TILEDB_INT32},
      {1, TILEDB_VAR_NUM},
      {tiledb::test::Compressor(TILEDB_FILTER_LZ4, -1),
       tiledb::test::Compressor(TILEDB_FILTER_LZ4, -1)},
      tile_order,
      cell_order,
      2);

  // Create subarray
  open_array(ctx_, array_, TILEDB_READ);
  Subarray subarray;
  SubarrayRanges<uint64_t> ranges = {{2, 3}, {2, 6}};
  create_subarray(array_->array_, ranges, subarray_layout, &subarray);
  subarray.compute_tile_coords<uint64_t>();

  // Create result space tiles
  std::vector<uint64_t> slice = {1, 6, 1, 6};
  auto size = 2 * sizeof(uint64_t);
  NDRange ds = {Range(&slice[0], size), Range(&slice[2], size)};
  std::vector<NDRange> domain_slices = {ds};
  const auto& tile_coords = subarray.tile_coords();
  std::map<const uint64_t*, ResultSpaceTile<uint64_t>> result_space_tiles;
  auto dom = array_->array_->array_schema()->domain();
  create_result_space_tiles(
      dom,
      dom->domain(),
      tile_domain_layout,
      domain_slices,
      tile_coords,
      &result_space_tiles);

  // Create result coordinates
  std::vector<ResultCoords> result_coords;

  // Check iterator
  ReadCellSlabIter<uint64_t> iter(
      &subarray, &result_space_tiles, &result_coords);
  check_iter<uint64_t>(&iter, c_result_cell_slabs);

  close_array(ctx_, array_);
}

TEST_CASE_METHOD(
    ReadCellSlabIterFx,
    "ReadCellSlabIter: Test 2D slabs, 1 range, 1 dense fragment, no overlap",
    "[ReadCellSlabIter][slabs][2d][1r][1f][no_overlap]") {
  Layout subarray_layout = Layout::ROW_MAJOR;
  Layout tile_domain_layout = Layout::ROW_MAJOR;
  tiledb_layout_t tile_order = TILEDB_ROW_MAJOR;
  tiledb_layout_t cell_order = TILEDB_ROW_MAJOR;
  std::vector<std::vector<uint64_t>> c_result_cell_slabs = {};

  SECTION("- tile: row, cell: row, subarray: row") {
    tile_order = TILEDB_ROW_MAJOR;
    cell_order = TILEDB_ROW_MAJOR;
    subarray_layout = Layout::ROW_MAJOR;
    tile_domain_layout = Layout::ROW_MAJOR;
    c_result_cell_slabs = {
        {UINT64_MAX, 0, 4, 2},
        {UINT64_MAX, 1, 3, 3},
        {UINT64_MAX, 0, 7, 2},
        {UINT64_MAX, 1, 6, 3},
    };
  }

  SECTION("- tile: row, cell: col, subarray: row") {
    tile_order = TILEDB_ROW_MAJOR;
    cell_order = TILEDB_COL_MAJOR;
    subarray_layout = Layout::ROW_MAJOR;
    tile_domain_layout = Layout::ROW_MAJOR;
    c_result_cell_slabs = {
        {UINT64_MAX, 0, 4, 2},
        {UINT64_MAX, 1, 1, 3},
        {UINT64_MAX, 0, 5, 2},
        {UINT64_MAX, 1, 2, 3},
    };
  }

  SECTION("- tile: col, cell: row, subarray: row") {
    tile_order = TILEDB_COL_MAJOR;
    cell_order = TILEDB_ROW_MAJOR;
    subarray_layout = Layout::ROW_MAJOR;
    tile_domain_layout = Layout::COL_MAJOR;
    c_result_cell_slabs = {
        {UINT64_MAX, 0, 4, 2},
        {UINT64_MAX, 2, 3, 3},
        {UINT64_MAX, 0, 7, 2},
        {UINT64_MAX, 2, 6, 3},
    };
  }

  SECTION("- tile: col, cell: col, subarray: row") {
    tile_order = TILEDB_COL_MAJOR;
    cell_order = TILEDB_COL_MAJOR;
    subarray_layout = Layout::ROW_MAJOR;
    tile_domain_layout = Layout::COL_MAJOR;
    c_result_cell_slabs = {
        {UINT64_MAX, 0, 4, 2},
        {UINT64_MAX, 2, 1, 3},
        {UINT64_MAX, 0, 5, 2},
        {UINT64_MAX, 2, 2, 3},
    };
  }

  SECTION("- tile: row, cell: row, subarray: col") {
    tile_order = TILEDB_ROW_MAJOR;
    cell_order = TILEDB_ROW_MAJOR;
    subarray_layout = Layout::COL_MAJOR;
    tile_domain_layout = Layout::ROW_MAJOR;
    c_result_cell_slabs = {
        {UINT64_MAX, 0, 4, 2},
        {UINT64_MAX, 0, 5, 2},
        {UINT64_MAX, 1, 3, 2},
        {UINT64_MAX, 1, 4, 2},
        {UINT64_MAX, 1, 5, 2},
    };
  }

  SECTION("- tile: row, cell: col, subarray: col") {
    tile_order = TILEDB_ROW_MAJOR;
    cell_order = TILEDB_COL_MAJOR;
    subarray_layout = Layout::COL_MAJOR;
    tile_domain_layout = Layout::ROW_MAJOR;
    c_result_cell_slabs = {
        {UINT64_MAX, 0, 4, 2},
        {UINT64_MAX, 0, 7, 2},
        {UINT64_MAX, 1, 1, 2},
        {UINT64_MAX, 1, 4, 2},
        {UINT64_MAX, 1, 7, 2},
    };
  }

  SECTION("- tile: col, cell: row, subarray: col") {
    tile_order = TILEDB_COL_MAJOR;
    cell_order = TILEDB_ROW_MAJOR;
    subarray_layout = Layout::COL_MAJOR;
    tile_domain_layout = Layout::COL_MAJOR;
    c_result_cell_slabs = {
        {UINT64_MAX, 0, 4, 2},
        {UINT64_MAX, 0, 5, 2},
        {UINT64_MAX, 2, 3, 2},
        {UINT64_MAX, 2, 4, 2},
        {UINT64_MAX, 2, 5, 2},
    };
  }

  SECTION("- tile: col, cell: col, subarray: col") {
    tile_order = TILEDB_COL_MAJOR;
    cell_order = TILEDB_COL_MAJOR;
    subarray_layout = Layout::COL_MAJOR;
    tile_domain_layout = Layout::COL_MAJOR;
    c_result_cell_slabs = {
        {UINT64_MAX, 0, 4, 2},
        {UINT64_MAX, 0, 7, 2},
        {UINT64_MAX, 2, 1, 2},
        {UINT64_MAX, 2, 4, 2},
        {UINT64_MAX, 2, 7, 2},
    };
  }

  // Create array
  uint64_t domain[] = {1, 6, 1, 6};
  uint64_t tile_extents[] = {3, 3};
  create_array(
      ctx_,
      array_name_,
      TILEDB_DENSE,
      {"d1", "d2"},
      {TILEDB_UINT64, TILEDB_UINT64},
      {&domain[0], &domain[2]},
      {&tile_extents[0], &tile_extents[1]},
      {"a", "b"},
      {TILEDB_INT32, TILEDB_INT32},
      {1, TILEDB_VAR_NUM},
      {tiledb::test::Compressor(TILEDB_FILTER_LZ4, -1),
       tiledb::test::Compressor(TILEDB_FILTER_LZ4, -1)},
      tile_order,
      cell_order,
      2);

  // Create subarray
  open_array(ctx_, array_, TILEDB_READ);
  Subarray subarray;
  SubarrayRanges<uint64_t> ranges = {{2, 3}, {2, 6}};
  create_subarray(array_->array_, ranges, subarray_layout, &subarray);
  subarray.compute_tile_coords<uint64_t>();

  // Create result space tiles
  std::vector<uint64_t> slice = {6, 6, 6, 6};
  auto size = 2 * sizeof(uint64_t);
  NDRange ds = {Range(&slice[0], size), Range(&slice[2], size)};
  std::vector<NDRange> domain_slices = {ds};
  const auto& tile_coords = subarray.tile_coords();
  std::map<const uint64_t*, ResultSpaceTile<uint64_t>> result_space_tiles;
  auto dom = array_->array_->array_schema()->domain();
  create_result_space_tiles(
      dom,
      dom->domain(),
      tile_domain_layout,
      domain_slices,
      tile_coords,
      &result_space_tiles);

  // Create result coordinates
  std::vector<ResultCoords> result_coords;

  // Check iterator
  ReadCellSlabIter<uint64_t> iter(
      &subarray, &result_space_tiles, &result_coords);
  check_iter<uint64_t>(&iter, c_result_cell_slabs);

  close_array(ctx_, array_);
}

TEST_CASE_METHOD(
    ReadCellSlabIterFx,
    "ReadCellSlabIter: Test 2D slabs, 1 range, 1 dense fragment, partial "
    "overlap",
    "[ReadCellSlabIter][slabs][2d][1r][1f][partial_overlap]") {
  Layout subarray_layout = Layout::ROW_MAJOR;
  Layout tile_domain_layout = Layout::ROW_MAJOR;
  tiledb_layout_t tile_order = TILEDB_ROW_MAJOR;
  tiledb_layout_t cell_order = TILEDB_ROW_MAJOR;
  std::vector<std::vector<uint64_t>> c_result_cell_slabs = {};

  SECTION("- tile: row, cell: row, subarray: row") {
    tile_order = TILEDB_ROW_MAJOR;
    cell_order = TILEDB_ROW_MAJOR;
    subarray_layout = Layout::ROW_MAJOR;
    tile_domain_layout = Layout::ROW_MAJOR;
    c_result_cell_slabs = {
        {UINT64_MAX, 0, 4, 2},
        {UINT64_MAX, 1, 3, 3},
        {UINT64_MAX, 0, 7, 2},
        {UINT64_MAX, 1, 6, 1},
        {1, 0, 7, 2},
    };
  }

  SECTION("- tile: row, cell: col, subarray: row") {
    tile_order = TILEDB_ROW_MAJOR;
    cell_order = TILEDB_COL_MAJOR;
    subarray_layout = Layout::ROW_MAJOR;
    tile_domain_layout = Layout::ROW_MAJOR;
    c_result_cell_slabs = {
        {UINT64_MAX, 0, 4, 2},
        {UINT64_MAX, 1, 1, 3},
        {UINT64_MAX, 0, 5, 2},
        {UINT64_MAX, 1, 2, 1},
        {1, 0, 5, 2},
    };
  }

  SECTION("- tile: col, cell: row, subarray: row") {
    tile_order = TILEDB_COL_MAJOR;
    cell_order = TILEDB_ROW_MAJOR;
    subarray_layout = Layout::ROW_MAJOR;
    tile_domain_layout = Layout::COL_MAJOR;
    c_result_cell_slabs = {
        {UINT64_MAX, 0, 4, 2},
        {UINT64_MAX, 2, 3, 3},
        {UINT64_MAX, 0, 7, 2},
        {UINT64_MAX, 2, 6, 1},
        {1, 0, 7, 2},
    };
  }

  SECTION("- tile: col, cell: col, subarray: row") {
    tile_order = TILEDB_COL_MAJOR;
    cell_order = TILEDB_COL_MAJOR;
    subarray_layout = Layout::ROW_MAJOR;
    tile_domain_layout = Layout::COL_MAJOR;
    c_result_cell_slabs = {
        {UINT64_MAX, 0, 4, 2},
        {UINT64_MAX, 2, 1, 3},
        {UINT64_MAX, 0, 5, 2},
        {UINT64_MAX, 2, 2, 1},
        {1, 0, 5, 2},
    };
  }

  SECTION("- tile: row, cell: row, subarray: col") {
    tile_order = TILEDB_ROW_MAJOR;
    cell_order = TILEDB_ROW_MAJOR;
    subarray_layout = Layout::COL_MAJOR;
    tile_domain_layout = Layout::ROW_MAJOR;
    c_result_cell_slabs = {
        {UINT64_MAX, 0, 4, 2},
        {UINT64_MAX, 0, 5, 2},
        {UINT64_MAX, 1, 3, 2},
        {UINT64_MAX, 1, 4, 1},
        {1, 0, 7, 1},
        {UINT64_MAX, 1, 5, 1},
        {1, 0, 8, 1},
    };
  }

  SECTION("- tile: row, cell: col, subarray: col") {
    tile_order = TILEDB_ROW_MAJOR;
    cell_order = TILEDB_COL_MAJOR;
    subarray_layout = Layout::COL_MAJOR;
    tile_domain_layout = Layout::ROW_MAJOR;
    c_result_cell_slabs = {
        {UINT64_MAX, 0, 4, 2},
        {UINT64_MAX, 0, 7, 2},
        {UINT64_MAX, 1, 1, 2},
        {UINT64_MAX, 1, 4, 1},
        {1, 0, 5, 1},
        {UINT64_MAX, 1, 7, 1},
        {1, 0, 8, 1},
    };
  }

  SECTION("- tile: col, cell: row, subarray: col") {
    tile_order = TILEDB_COL_MAJOR;
    cell_order = TILEDB_ROW_MAJOR;
    subarray_layout = Layout::COL_MAJOR;
    tile_domain_layout = Layout::COL_MAJOR;
    c_result_cell_slabs = {
        {UINT64_MAX, 0, 4, 2},
        {UINT64_MAX, 0, 5, 2},
        {UINT64_MAX, 2, 3, 2},
        {UINT64_MAX, 2, 4, 1},
        {1, 0, 7, 1},
        {UINT64_MAX, 2, 5, 1},
        {1, 0, 8, 1},
    };
  }

  SECTION("- tile: col, cell: col, subarray: col") {
    tile_order = TILEDB_COL_MAJOR;
    cell_order = TILEDB_COL_MAJOR;
    subarray_layout = Layout::COL_MAJOR;
    tile_domain_layout = Layout::COL_MAJOR;
    c_result_cell_slabs = {
        {UINT64_MAX, 0, 4, 2},
        {UINT64_MAX, 0, 7, 2},
        {UINT64_MAX, 2, 1, 2},
        {UINT64_MAX, 2, 4, 1},
        {1, 0, 5, 1},
        {UINT64_MAX, 2, 7, 1},
        {1, 0, 8, 1},
    };
  }

  // Create array
  uint64_t domain[] = {1, 6, 1, 6};
  uint64_t tile_extents[] = {3, 3};
  create_array(
      ctx_,
      array_name_,
      TILEDB_DENSE,
      {"d1", "d2"},
      {TILEDB_UINT64, TILEDB_UINT64},
      {&domain[0], &domain[2]},
      {&tile_extents[0], &tile_extents[1]},
      {"a", "b"},
      {TILEDB_INT32, TILEDB_INT32},
      {1, TILEDB_VAR_NUM},
      {tiledb::test::Compressor(TILEDB_FILTER_LZ4, -1),
       tiledb::test::Compressor(TILEDB_FILTER_LZ4, -1)},
      tile_order,
      cell_order,
      2);

  // Create subarray
  open_array(ctx_, array_, TILEDB_READ);
  Subarray subarray;
  SubarrayRanges<uint64_t> ranges = {{2, 3}, {2, 6}};
  create_subarray(array_->array_, ranges, subarray_layout, &subarray);
  subarray.compute_tile_coords<uint64_t>();

  // Create result space tiles
  std::vector<uint64_t> slice = {3, 6, 5, 6};
  auto size = 2 * sizeof(uint64_t);
  NDRange ds = {Range(&slice[0], size), Range(&slice[2], size)};
  std::vector<NDRange> domain_slices = {ds};
  const auto& tile_coords = subarray.tile_coords();
  std::map<const uint64_t*, ResultSpaceTile<uint64_t>> result_space_tiles;
  auto dom = array_->array_->array_schema()->domain();
  create_result_space_tiles(
      dom,
      dom->domain(),
      tile_domain_layout,
      domain_slices,
      tile_coords,
      &result_space_tiles);

  // Create result coordinates
  std::vector<ResultCoords> result_coords;

  // Check iterator
  ReadCellSlabIter<uint64_t> iter(
      &subarray, &result_space_tiles, &result_coords);
  check_iter<uint64_t>(&iter, c_result_cell_slabs);

  close_array(ctx_, array_);
}

TEST_CASE_METHOD(
    ReadCellSlabIterFx,
    "ReadCellSlabIter: Test 2D slabs, multiple ranges, 2 dense fragments, "
    "1 sparse, overlap",
    "[ReadCellSlabIter][slabs][2d][mr][2df1sf]") {
  Layout subarray_layout = Layout::ROW_MAJOR;
  Layout tile_domain_layout = Layout::ROW_MAJOR;
  tiledb_layout_t tile_order = TILEDB_ROW_MAJOR;
  tiledb_layout_t cell_order = TILEDB_ROW_MAJOR;
  std::vector<std::vector<uint64_t>> c_result_cell_slabs = {};

  SECTION("- tile: row, cell: row, subarray: row") {
    tile_order = TILEDB_ROW_MAJOR;
    cell_order = TILEDB_ROW_MAJOR;
    subarray_layout = Layout::ROW_MAJOR;
    tile_domain_layout = Layout::ROW_MAJOR;
    c_result_cell_slabs = {
        {2, 0, 7, 1},
        {3, 0, 1, 1},
        {2, 1, 6, 1},
        {1, 1, 7, 2},
        {2, 2, 1, 2},
        {2, 3, 0, 1},
        {UINT64_MAX, 3, 1, 2},
        {2, 2, 4, 2},
        {2, 3, 3, 1},
        {3, 1, 0, 1},
        {3, 1, 2, 1},
    };
  }

  SECTION("- tile: row, cell: col, subarray: row") {
    tile_order = TILEDB_ROW_MAJOR;
    cell_order = TILEDB_COL_MAJOR;
    subarray_layout = Layout::ROW_MAJOR;
    tile_domain_layout = Layout::ROW_MAJOR;
    c_result_cell_slabs = {
        {2, 0, 5, 1},
        {3, 0, 1, 1},
        {2, 1, 2, 1},
        {1, 1, 5, 2},
        {2, 2, 3, 2},
        {2, 3, 0, 1},
        {UINT64_MAX, 3, 3, 2},
        {2, 2, 4, 2},
        {2, 3, 1, 1},
        {3, 1, 0, 1},
        {3, 1, 2, 1},
    };
  }

  SECTION("- tile: col, cell: row, subarray: row") {
    tile_order = TILEDB_COL_MAJOR;
    cell_order = TILEDB_ROW_MAJOR;
    subarray_layout = Layout::ROW_MAJOR;
    tile_domain_layout = Layout::COL_MAJOR;
    c_result_cell_slabs = {
        {2, 0, 7, 1},
        {3, 0, 1, 1},
        {2, 2, 6, 1},
        {1, 1, 7, 2},
        {2, 1, 1, 2},
        {2, 3, 0, 1},
        {UINT64_MAX, 3, 1, 2},
        {2, 1, 4, 2},
        {2, 3, 3, 1},
        {3, 1, 0, 1},
        {3, 1, 2, 1},
    };
  }

  SECTION("- tile: col, cell: col, subarray: row") {
    tile_order = TILEDB_COL_MAJOR;
    cell_order = TILEDB_COL_MAJOR;
    subarray_layout = Layout::ROW_MAJOR;
    tile_domain_layout = Layout::COL_MAJOR;
    c_result_cell_slabs = {
        {2, 0, 5, 1},
        {3, 0, 1, 1},
        {2, 2, 2, 1},
        {1, 1, 5, 2},
        {2, 1, 3, 2},
        {2, 3, 0, 1},
        {UINT64_MAX, 3, 3, 2},
        {2, 1, 4, 2},
        {2, 3, 1, 1},
        {3, 1, 0, 1},
        {3, 1, 2, 1},
    };
  }

  SECTION("- tile: row, cell: row, subarray: col") {
    tile_order = TILEDB_ROW_MAJOR;
    cell_order = TILEDB_ROW_MAJOR;
    subarray_layout = Layout::COL_MAJOR;
    tile_domain_layout = Layout::ROW_MAJOR;
    c_result_cell_slabs = {
        {2, 0, 7, 1},
        {2, 2, 1, 2},
        {3, 0, 1, 1},
        {2, 2, 2, 2},
        {2, 1, 6, 1},
        {2, 3, 0, 2},
        {1, 1, 7, 1},
        {UINT64_MAX, 3, 1, 1},
        {3, 1, 0, 1},
        {1, 1, 8, 1},
        {UINT64_MAX, 3, 2, 1},
        {3, 1, 2, 1},
    };
  }

  SECTION("- tile: row, cell: col, subarray: col") {
    tile_order = TILEDB_ROW_MAJOR;
    cell_order = TILEDB_COL_MAJOR;
    subarray_layout = Layout::COL_MAJOR;
    tile_domain_layout = Layout::ROW_MAJOR;
    c_result_cell_slabs = {
        {2, 0, 5, 1},
        {2, 2, 3, 2},
        {3, 0, 1, 1},
        {2, 2, 6, 2},
        {2, 1, 2, 1},
        {2, 3, 0, 2},
        {1, 1, 5, 1},
        {UINT64_MAX, 3, 3, 1},
        {3, 1, 0, 1},
        {1, 1, 8, 1},
        {UINT64_MAX, 3, 6, 1},
        {3, 1, 2, 1},
    };
  }

  SECTION("- tile: col, cell: row, subarray: col") {
    tile_order = TILEDB_COL_MAJOR;
    cell_order = TILEDB_ROW_MAJOR;
    subarray_layout = Layout::COL_MAJOR;
    tile_domain_layout = Layout::COL_MAJOR;
    c_result_cell_slabs = {
        {2, 0, 7, 1},
        {2, 1, 1, 2},
        {3, 0, 1, 1},
        {2, 1, 2, 2},
        {2, 2, 6, 1},
        {2, 3, 0, 2},
        {1, 1, 7, 1},
        {UINT64_MAX, 3, 1, 1},
        {3, 1, 0, 1},
        {1, 1, 8, 1},
        {UINT64_MAX, 3, 2, 1},
        {3, 1, 2, 1},
    };
  }

  SECTION("- tile: col, cell: col, subarray: col") {
    tile_order = TILEDB_COL_MAJOR;
    cell_order = TILEDB_COL_MAJOR;
    subarray_layout = Layout::COL_MAJOR;
    tile_domain_layout = Layout::COL_MAJOR;
    c_result_cell_slabs = {
        {2, 0, 5, 1},
        {2, 1, 3, 2},
        {3, 0, 1, 1},
        {2, 1, 6, 2},
        {2, 2, 2, 1},
        {2, 3, 0, 2},
        {1, 1, 5, 1},
        {UINT64_MAX, 3, 3, 1},
        {3, 1, 0, 1},
        {1, 1, 8, 1},
        {UINT64_MAX, 3, 6, 1},
        {3, 1, 2, 1},
    };
  }

  // Create array
  uint64_t domain[] = {1, 6, 1, 6};
  uint64_t tile_extents[] = {3, 3};
  create_array(
      ctx_,
      array_name_,
      TILEDB_DENSE,
      {"d1", "d2"},
      {TILEDB_UINT64, TILEDB_UINT64},
      {&domain[0], &domain[2]},
      {&tile_extents[0], &tile_extents[1]},
      {"a", "b"},
      {TILEDB_INT32, TILEDB_INT32},
      {1, TILEDB_VAR_NUM},
      {tiledb::test::Compressor(TILEDB_FILTER_LZ4, -1),
       tiledb::test::Compressor(TILEDB_FILTER_LZ4, -1)},
      tile_order,
      cell_order,
      2);

  // Create subarray
  open_array(ctx_, array_, TILEDB_READ);
  Subarray subarray;
  SubarrayRanges<uint64_t> ranges = {{3, 5}, {2, 4, 5, 6}};
  create_subarray(array_->array_, ranges, subarray_layout, &subarray);
  subarray.compute_tile_coords<uint64_t>();

  // Create result space tiles
  std::vector<uint64_t> slice_1 = {3, 5, 2, 4};
  std::vector<uint64_t> slice_2 = {2, 3, 1, 6};
  auto size = 2 * sizeof(uint64_t);
  NDRange ds1 = {Range(&slice_1[0], size), Range(&slice_1[2], size)};
  NDRange ds2 = {Range(&slice_2[0], size), Range(&slice_2[2], size)};
  std::vector<NDRange> domain_slices = {ds1, ds2};
  const auto& tile_coords = subarray.tile_coords();
  std::map<const uint64_t*, ResultSpaceTile<uint64_t>> result_space_tiles;
  auto dom = array_->array_->array_schema()->domain();
  create_result_space_tiles(
      dom,
      dom->domain(),
      tile_domain_layout,
      domain_slices,
      tile_coords,
      &result_space_tiles);

  // Create result coordinates
  std::vector<ResultCoords> result_coords;
  ResultTile result_tile_3_0(3, 0, dom);
  ResultTile result_tile_3_1(3, 1, dom);

  result_tile_3_0.init_coord_tile("d1", 0);
  result_tile_3_0.init_coord_tile("d2", 1);
  result_tile_3_1.init_coord_tile("d1", 0);
  result_tile_3_1.init_coord_tile("d2", 1);

  std::vector<uint64_t> vec_3_0_d1 = {1000, 3, 1000, 1000};
  Buffer buff_3_0_d1(&vec_3_0_d1[0], vec_3_0_d1.size() * sizeof(uint64_t));
  ChunkedBuffer chunked_buffer_3_0_d1;
  REQUIRE(Tile::buffer_to_contiguous_fixed_chunks(
              buff_3_0_d1, 0, sizeof(uint64_t), &chunked_buffer_3_0_d1)
              .ok());
  Tile tile_3_0_d1(
      Datatype::UINT64, sizeof(uint64_t), 0, &chunked_buffer_3_0_d1, false);
  auto tile_tuple = result_tile_3_0.tile_tuple("d1");
  REQUIRE(tile_tuple != nullptr);
  std::get<0>(*tile_tuple) = tile_3_0_d1;

  std::vector<uint64_t> vec_3_0_d2 = {1000, 3, 1000, 1000};
  Buffer buff_3_0_d2(&vec_3_0_d2[0], vec_3_0_d2.size() * sizeof(uint64_t));
  ChunkedBuffer chunked_buffer_3_0_d2;
  REQUIRE(Tile::buffer_to_contiguous_fixed_chunks(
              buff_3_0_d2, 0, sizeof(uint64_t), &chunked_buffer_3_0_d2)
              .ok());
  Tile tile_3_0_d2(
      Datatype::UINT64, sizeof(uint64_t), 0, &chunked_buffer_3_0_d2, false);
  tile_tuple = result_tile_3_0.tile_tuple("d2");
  REQUIRE(tile_tuple != nullptr);
  std::get<0>(*tile_tuple) = tile_3_0_d2;

  std::vector<uint64_t> vec_3_1_d1 = {5, 1000, 5, 1000};
  Buffer buff_3_1_d1(&vec_3_1_d1[0], vec_3_1_d1.size() * sizeof(uint64_t));
  ChunkedBuffer chunked_buffer_3_1_d1;
  REQUIRE(Tile::buffer_to_contiguous_fixed_chunks(
              buff_3_1_d1, 0, sizeof(uint64_t), &chunked_buffer_3_1_d1)
              .ok());
  Tile tile_3_1_d1(
      Datatype::UINT64, sizeof(uint64_t), 0, &chunked_buffer_3_1_d1, false);
  tile_tuple = result_tile_3_1.tile_tuple("d1");
  REQUIRE(tile_tuple != nullptr);
  std::get<0>(*tile_tuple) = tile_3_1_d1;

  std::vector<uint64_t> vec_3_1_d2 = {5, 1000, 6, 1000};
  Buffer buff_3_1_d2(&vec_3_1_d2[0], vec_3_1_d2.size() * sizeof(uint64_t));
  ChunkedBuffer chunked_buffer_3_1_d2;
  REQUIRE(Tile::buffer_to_contiguous_fixed_chunks(
              buff_3_1_d2, 0, sizeof(uint64_t), &chunked_buffer_3_1_d2)
              .ok());
  Tile tile_3_1_d2(
      Datatype::UINT64, sizeof(uint64_t), 0, &chunked_buffer_3_1_d2, false);
  tile_tuple = result_tile_3_1.tile_tuple("d2");
  REQUIRE(tile_tuple != nullptr);
  std::get<0>(*tile_tuple) = tile_3_1_d2;

  result_coords.emplace_back(&result_tile_3_0, 1);
  result_coords.emplace_back(&result_tile_3_1, 0);
  result_coords.emplace_back(&result_tile_3_1, 2);

  // Check iterator
  ReadCellSlabIter<uint64_t> iter(
      &subarray, &result_space_tiles, &result_coords);
  check_iter<uint64_t>(&iter, c_result_cell_slabs);

  close_array(ctx_, array_);
}