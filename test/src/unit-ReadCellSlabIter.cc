/**
 * @file unit-ReadCellSlabIter.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2024 TileDB, Inc.
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

#include "test/support/src/helpers.h"
#include "test/support/src/vfs_helpers.h"
#include "tiledb/api/c_api/array/array_api_internal.h"
#include "tiledb/common/common.h"
#include "tiledb/common/memory_tracker.h"
#include "tiledb/sm/array_schema/tile_domain.h"
#include "tiledb/sm/query/legacy/read_cell_slab_iter.h"
#include "tiledb/sm/query/legacy/reader.h"

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

struct ReadCellSlabIterFx {
  tiledb_ctx_t* ctx_;
  tiledb_vfs_t* vfs_;
  const std::vector<std::unique_ptr<SupportedFs>> fs_vec_;
  std::string temp_dir_;
  std::string array_name_;
  const char* ARRAY_NAME = "read_cell_slab_iter";
  tiledb_array_t* array_ = nullptr;

  shared_ptr<MemoryTracker> tracker_;

  ReadCellSlabIterFx();
  ~ReadCellSlabIterFx();

  template <class T>
  void check_iter(
      ReadCellSlabIter<T>* iter,
      const std::vector<std::vector<uint64_t>>& c_result_cell_slabs);
  template <class T>
  void create_result_space_tiles(
      const std::vector<shared_ptr<FragmentMetadata>>& fragments,
      const Domain& dom,
      const NDRange& dom_ndrange,
      Layout layout,
      const std::vector<NDRange>& domain_slices,
      const std::vector<std::vector<uint8_t>>& tile_coords,
      std::map<const T*, ResultSpaceTile<T>>& result_space_tiles);
};

ReadCellSlabIterFx::ReadCellSlabIterFx()
    : fs_vec_(vfs_test_get_fs_vec())
    , tracker_(tiledb::test::create_test_memory_tracker()) {
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
    const std::vector<shared_ptr<FragmentMetadata>>& fragments,
    const Domain& dom,
    const NDRange& dom_ndrange,
    Layout layout,
    const std::vector<NDRange>& domain_slices,
    const std::vector<std::vector<uint8_t>>& tile_coords,
    std::map<const T*, ResultSpaceTile<T>>& result_space_tiles) {
  auto domain = dom.domain();
  const auto& tile_extents = dom.tile_extents();
  std::vector<TileDomain<T>> frag_tile_domains;
  for (size_t i = 0; i < domain_slices.size(); ++i) {
    frag_tile_domains.emplace_back(
        (unsigned)(domain_slices.size() - i - 1),
        domain,
        domain_slices[i],
        tile_extents,
        layout);
  }
  TileDomain<T> array_tile_domain(
      UINT32_MAX, domain, dom_ndrange, tile_extents, layout);
  Reader::compute_result_space_tiles<T>(
      fragments,
      tile_coords,
      array_tile_domain,
      frag_tile_domains,
      result_space_tiles,
      tiledb::test::get_test_memory_tracker());
}

void set_result_tile_dim(
    const ArraySchema& array_schema,
    ResultTile& result_tile,
    std::string dim,
    uint64_t dim_idx,
    std::vector<uint64_t> v) {
  ResultTile::TileSizes tile_sizes(
      v.size() * sizeof(uint64_t),
      0,
      std::nullopt,
      std::nullopt,
      std::nullopt,
      std::nullopt);
  ResultTile::TileData tile_data{nullptr, nullptr, nullptr};
  result_tile.init_coord_tile(
      constants::format_version,
      array_schema,
      dim,
      tile_sizes,
      tile_data,
      dim_idx);
  auto tile_tuple = result_tile.tile_tuple(dim);
  REQUIRE(tile_tuple != nullptr);
  uint64_t* data = tile_tuple->fixed_tile().data_as<uint64_t>();
  for (uint64_t i = 0; i < v.size(); i++) {
    data[i] = v[i];
  }
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
  auto& array_schema = array_->array_schema_latest();
  Subarray subarray;
  SubarrayRanges<uint64_t> ranges = {{5, 15}};
  Layout subarray_layout = Layout::ROW_MAJOR;
  create_subarray(array_->array(), ranges, subarray_layout, &subarray);
  CHECK_NOTHROW(subarray.compute_tile_coords<uint64_t>());

  // Create result space tiles
  std::vector<uint64_t> slice = {1, 100};
  NDRange ds = {Range(&slice[0], 2 * sizeof(uint64_t))};
  std::vector<NDRange> domain_slices = {ds};
  const auto& tile_coords = subarray.tile_coords();
  std::map<const uint64_t*, ResultSpaceTile<uint64_t>> result_space_tiles;
  auto& dom{array_schema.domain()};

  std::vector<shared_ptr<FragmentMetadata>> fragments;
  shared_ptr<FragmentMetadata> fragment = make_shared<FragmentMetadata>(
      HERE(),
      nullptr,
      array_->array_schema_latest_ptr(),
      generate_fragment_uri(array_->array().get()),
      std::make_pair<uint64_t, uint64_t>(0, 0),
      tiledb::test::create_test_memory_tracker(),
      true);
  fragments.emplace_back(std::move(fragment));

  create_result_space_tiles(
      fragments,
      dom,
      dom.domain(),
      subarray_layout,
      domain_slices,
      tile_coords,
      result_space_tiles);

  // Check iterator
  std::vector<ResultCoords> result_coords;
  ReadCellSlabIter<uint64_t> iter(
      &subarray, &result_space_tiles, &result_coords);
  std::vector<std::vector<uint64_t>> c_result_cell_slabs = {
      {0, 0, 4, 6},
      {0, 1, 0, 5},
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
  auto& array_schema = array_->array_schema_latest();
  Subarray subarray;
  SubarrayRanges<uint64_t> ranges = {{5, 15}};
  Layout subarray_layout = Layout::ROW_MAJOR;
  create_subarray(array_->array(), ranges, subarray_layout, &subarray);
  CHECK_NOTHROW(subarray.compute_tile_coords<uint64_t>());

  // Create result space tiles
  std::vector<uint64_t> slice = {20, 30};
  NDRange ds = {Range(&slice[0], 2 * sizeof(uint64_t))};
  std::vector<NDRange> domain_slices = {ds};
  const auto& tile_coords = subarray.tile_coords();
  std::map<const uint64_t*, ResultSpaceTile<uint64_t>> result_space_tiles;
  auto& dom{array_schema.domain()};

  std::vector<shared_ptr<FragmentMetadata>> fragments;
  shared_ptr<FragmentMetadata> fragment = make_shared<FragmentMetadata>(
      HERE(),
      nullptr,
      array_->array_schema_latest_ptr(),
      generate_fragment_uri(array_->array().get()),
      std::make_pair<uint64_t, uint64_t>(0, 0),
      tiledb::test::create_test_memory_tracker(),
      true);
  fragments.emplace_back(std::move(fragment));

  create_result_space_tiles(
      fragments,
      dom,
      dom.domain(),
      subarray_layout,
      domain_slices,
      tile_coords,
      result_space_tiles);

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
  auto& array_schema = array_->array_schema_latest();
  Subarray subarray;
  SubarrayRanges<uint64_t> ranges = {{5, 15, 3, 5, 11, 14}};
  Layout subarray_layout = Layout::ROW_MAJOR;
  create_subarray(array_->array(), ranges, subarray_layout, &subarray);
  CHECK_NOTHROW(subarray.compute_tile_coords<uint64_t>());

  // Create result space tiles
  std::vector<uint64_t> slice_1 = {5, 12};
  std::vector<uint64_t> slice_2 = {4, 15};
  auto size = 2 * sizeof(uint64_t);
  NDRange ds1 = {Range(&slice_1[0], size)};
  NDRange ds2 = {Range(&slice_2[0], size)};
  std::vector<NDRange> domain_slices = {ds1, ds2};
  const auto& tile_coords = subarray.tile_coords();
  std::map<const uint64_t*, ResultSpaceTile<uint64_t>> result_space_tiles;
  auto& dom{array_schema.domain()};

  std::vector<shared_ptr<FragmentMetadata>> fragments;
  for (uint64_t i = 0; i < 2; i++) {
    shared_ptr<FragmentMetadata> fragment = make_shared<FragmentMetadata>(
        HERE(),
        nullptr,
        array_->array_schema_latest_ptr(),
        generate_fragment_uri(array_->array().get()),
        std::make_pair<uint64_t, uint64_t>(0, 0),
        tiledb::test::create_test_memory_tracker(),
        true);
    fragments.emplace_back(std::move(fragment));
  }

  create_result_space_tiles(
      fragments,
      dom,
      dom.domain(),
      subarray_layout,
      domain_slices,
      tile_coords,
      result_space_tiles);

  // Check iterator
  std::vector<ResultCoords> result_coords;
  ReadCellSlabIter<uint64_t> iter(
      &subarray, &result_space_tiles, &result_coords);
  std::vector<std::vector<uint64_t>> c_result_cell_slabs = {
      {1, 0, 4, 6},
      {1, 1, 0, 2},
      {0, 1, 2, 3},
      {UINT64_MAX, 0, 2, 1},
      {0, 0, 3, 1},
      {1, 0, 4, 1},
      {1, 1, 0, 2},
      {0, 1, 2, 2},
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
  auto& array_schema = array_->array_schema_latest();
  Subarray subarray;
  SubarrayRanges<uint64_t> ranges = {{3, 15, 18, 20}};
  Layout subarray_layout = Layout::ROW_MAJOR;
  create_subarray(array_->array(), ranges, subarray_layout, &subarray);
  CHECK_NOTHROW(subarray.compute_tile_coords<uint64_t>());

  // Create result space tiles
  std::vector<uint64_t> slice = {3, 12};
  auto size = 2 * sizeof(uint64_t);
  NDRange ds = {Range(&slice[0], size)};
  std::vector<NDRange> domain_slices = {ds};
  const auto& tile_coords = subarray.tile_coords();
  std::map<const uint64_t*, ResultSpaceTile<uint64_t>> result_space_tiles;
  auto& dom{array_schema.domain()};

  std::vector<shared_ptr<FragmentMetadata>> fragments;
  for (uint64_t i = 0; i < 2; i++) {
    shared_ptr<FragmentMetadata> fragment = make_shared<FragmentMetadata>(
        HERE(),
        nullptr,
        array_->array_schema_latest_ptr(),
        generate_fragment_uri(array_->array().get()),
        std::make_pair<uint64_t, uint64_t>(0, 0),
        tiledb::test::create_test_memory_tracker(),
        true);
    fragments.emplace_back(std::move(fragment));
  }

  create_result_space_tiles(
      fragments,
      dom,
      dom.domain(),
      subarray_layout,
      domain_slices,
      tile_coords,
      result_space_tiles);

  // Create result coordinates
  std::vector<ResultCoords> result_coords;
  ResultTile result_tile_2_0(
      1, 0, *fragments[0], tiledb::test::get_test_memory_tracker());
  ResultTile result_tile_3_0(
      2, 0, *fragments[0], tiledb::test::get_test_memory_tracker());
  ResultTile result_tile_3_1(
      2, 1, *fragments[1], tiledb::test::get_test_memory_tracker());

  set_result_tile_dim(
      array_schema, result_tile_2_0, "d", 0, {{1000, 3, 1000, 5}});
  set_result_tile_dim(
      array_schema, result_tile_3_0, "d", 0, {{1000, 1000, 8, 9}});
  set_result_tile_dim(
      array_schema, result_tile_3_1, "d", 0, {{1000, 12, 19, 1000}});

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
      {1, 0, 1, 1},
      {0, 0, 3, 1},
      {1, 0, 3, 1},
      {0, 0, 5, 2},
      {2, 0, 2, 1},
      {0, 0, 8, 2},
      {0, 1, 0, 1},
      {2, 1, 1, 1},
      {UINT64_MAX, 1, 2, 3},
      {UINT64_MAX, 1, 7, 1},
      {2, 1, 2, 1},
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
        {0, 0, 4, 2},
        {0, 1, 3, 3},
        {0, 0, 7, 2},
        {0, 1, 6, 3},
    };
  }

  SECTION("- tile: row, cell: col, subarray: row") {
    tile_order = TILEDB_ROW_MAJOR;
    cell_order = TILEDB_COL_MAJOR;
    subarray_layout = Layout::ROW_MAJOR;
    tile_domain_layout = Layout::ROW_MAJOR;
    c_result_cell_slabs = {
        {0, 0, 4, 2},
        {0, 1, 1, 3},
        {0, 0, 5, 2},
        {0, 1, 2, 3},
    };
  }

  SECTION("- tile: col, cell: row, subarray: row") {
    tile_order = TILEDB_COL_MAJOR;
    cell_order = TILEDB_ROW_MAJOR;
    subarray_layout = Layout::ROW_MAJOR;
    tile_domain_layout = Layout::COL_MAJOR;
    c_result_cell_slabs = {
        {0, 0, 4, 2},
        {0, 2, 3, 3},
        {0, 0, 7, 2},
        {0, 2, 6, 3},
    };
  }

  SECTION("- tile: col, cell: col, subarray: row") {
    tile_order = TILEDB_COL_MAJOR;
    cell_order = TILEDB_COL_MAJOR;
    subarray_layout = Layout::ROW_MAJOR;
    tile_domain_layout = Layout::COL_MAJOR;
    c_result_cell_slabs = {
        {0, 0, 4, 2},
        {0, 2, 1, 3},
        {0, 0, 5, 2},
        {0, 2, 2, 3},
    };
  }

  SECTION("- tile: row, cell: row, subarray: col") {
    tile_order = TILEDB_ROW_MAJOR;
    cell_order = TILEDB_ROW_MAJOR;
    subarray_layout = Layout::COL_MAJOR;
    tile_domain_layout = Layout::ROW_MAJOR;
    c_result_cell_slabs = {
        {0, 0, 4, 2},
        {0, 0, 5, 2},
        {0, 1, 3, 2},
        {0, 1, 4, 2},
        {0, 1, 5, 2},
    };
  }

  SECTION("- tile: row, cell: col, subarray: col") {
    tile_order = TILEDB_ROW_MAJOR;
    cell_order = TILEDB_COL_MAJOR;
    subarray_layout = Layout::COL_MAJOR;
    tile_domain_layout = Layout::ROW_MAJOR;
    c_result_cell_slabs = {
        {0, 0, 4, 2},
        {0, 0, 7, 2},
        {0, 1, 1, 2},
        {0, 1, 4, 2},
        {0, 1, 7, 2},
    };
  }

  SECTION("- tile: col, cell: row, subarray: col") {
    tile_order = TILEDB_COL_MAJOR;
    cell_order = TILEDB_ROW_MAJOR;
    subarray_layout = Layout::COL_MAJOR;
    tile_domain_layout = Layout::COL_MAJOR;
    c_result_cell_slabs = {
        {0, 0, 4, 2},
        {0, 0, 5, 2},
        {0, 2, 3, 2},
        {0, 2, 4, 2},
        {0, 2, 5, 2},
    };
  }

  SECTION("- tile: col, cell: col, subarray: col") {
    tile_order = TILEDB_COL_MAJOR;
    cell_order = TILEDB_COL_MAJOR;
    subarray_layout = Layout::COL_MAJOR;
    tile_domain_layout = Layout::COL_MAJOR;
    c_result_cell_slabs = {
        {0, 0, 4, 2},
        {0, 0, 7, 2},
        {0, 2, 1, 2},
        {0, 2, 4, 2},
        {0, 2, 7, 2},
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
  auto& array_schema = array_->array_schema_latest();
  Subarray subarray;
  SubarrayRanges<uint64_t> ranges = {{2, 3}, {2, 6}};
  create_subarray(array_->array(), ranges, subarray_layout, &subarray);
  CHECK_NOTHROW(subarray.compute_tile_coords<uint64_t>());

  // Create result space tiles
  std::vector<uint64_t> slice = {1, 6, 1, 6};
  auto size = 2 * sizeof(uint64_t);
  NDRange ds = {Range(&slice[0], size), Range(&slice[2], size)};
  std::vector<NDRange> domain_slices = {ds};
  const auto& tile_coords = subarray.tile_coords();
  std::map<const uint64_t*, ResultSpaceTile<uint64_t>> result_space_tiles;
  auto& dom{array_schema.domain()};

  std::vector<shared_ptr<FragmentMetadata>> fragments;
  shared_ptr<FragmentMetadata> fragment = make_shared<FragmentMetadata>(
      HERE(),
      nullptr,
      array_->array_schema_latest_ptr(),
      generate_fragment_uri(array_->array().get()),
      std::make_pair<uint64_t, uint64_t>(0, 0),
      tiledb::test::create_test_memory_tracker(),
      true);
  fragments.emplace_back(std::move(fragment));

  create_result_space_tiles(
      fragments,
      dom,
      dom.domain(),
      tile_domain_layout,
      domain_slices,
      tile_coords,
      result_space_tiles);

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
  auto& array_schema = array_->array_schema_latest();
  Subarray subarray;
  SubarrayRanges<uint64_t> ranges = {{2, 3}, {2, 6}};
  create_subarray(array_->array(), ranges, subarray_layout, &subarray);
  CHECK_NOTHROW(subarray.compute_tile_coords<uint64_t>());

  // Create result space tiles
  std::vector<uint64_t> slice = {6, 6, 6, 6};
  auto size = 2 * sizeof(uint64_t);
  NDRange ds = {Range(&slice[0], size), Range(&slice[2], size)};
  std::vector<NDRange> domain_slices = {ds};
  const auto& tile_coords = subarray.tile_coords();
  std::map<const uint64_t*, ResultSpaceTile<uint64_t>> result_space_tiles;
  auto& dom{array_schema.domain()};

  std::vector<shared_ptr<FragmentMetadata>> fragments;
  shared_ptr<FragmentMetadata> fragment = make_shared<FragmentMetadata>(
      HERE(),
      nullptr,
      array_->array_schema_latest_ptr(),
      generate_fragment_uri(array_->array().get()),
      std::make_pair<uint64_t, uint64_t>(0, 0),
      tiledb::test::create_test_memory_tracker(),
      true);
  fragments.emplace_back(std::move(fragment));

  create_result_space_tiles(
      fragments,
      dom,
      dom.domain(),
      tile_domain_layout,
      domain_slices,
      tile_coords,
      result_space_tiles);

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
        {0, 0, 7, 2},
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
        {0, 0, 5, 2},
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
        {0, 0, 7, 2},
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
        {0, 0, 5, 2},
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
        {0, 0, 7, 1},
        {UINT64_MAX, 1, 5, 1},
        {0, 0, 8, 1},
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
        {0, 0, 5, 1},
        {UINT64_MAX, 1, 7, 1},
        {0, 0, 8, 1},
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
        {0, 0, 7, 1},
        {UINT64_MAX, 2, 5, 1},
        {0, 0, 8, 1},
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
        {0, 0, 5, 1},
        {UINT64_MAX, 2, 7, 1},
        {0, 0, 8, 1},
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
  auto& array_schema = array_->array_schema_latest();
  Subarray subarray;
  SubarrayRanges<uint64_t> ranges = {{2, 3}, {2, 6}};
  create_subarray(array_->array(), ranges, subarray_layout, &subarray);
  CHECK_NOTHROW(subarray.compute_tile_coords<uint64_t>());

  // Create result space tiles
  std::vector<uint64_t> slice = {3, 6, 5, 6};
  auto size = 2 * sizeof(uint64_t);
  NDRange ds = {Range(&slice[0], size), Range(&slice[2], size)};
  std::vector<NDRange> domain_slices = {ds};
  const auto& tile_coords = subarray.tile_coords();
  std::map<const uint64_t*, ResultSpaceTile<uint64_t>> result_space_tiles;
  auto& dom{array_schema.domain()};

  std::vector<shared_ptr<FragmentMetadata>> fragments;
  shared_ptr<FragmentMetadata> fragment = make_shared<FragmentMetadata>(
      HERE(),
      nullptr,
      array_->array_schema_latest_ptr(),
      generate_fragment_uri(array_->array().get()),
      std::make_pair<uint64_t, uint64_t>(0, 0),
      tiledb::test::create_test_memory_tracker(),
      true);
  fragments.emplace_back(std::move(fragment));

  create_result_space_tiles(
      fragments,
      dom,
      dom.domain(),
      tile_domain_layout,
      domain_slices,
      tile_coords,
      result_space_tiles);

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
        {1, 0, 7, 1},
        {2, 0, 1, 1},
        {1, 1, 6, 1},
        {0, 1, 7, 2},
        {1, 2, 1, 2},
        {1, 3, 0, 1},
        {UINT64_MAX, 3, 1, 2},
        {1, 2, 4, 2},
        {1, 3, 3, 1},
        {2, 1, 0, 1},
        {2, 1, 2, 1},
    };
  }

  SECTION("- tile: row, cell: col, subarray: row") {
    tile_order = TILEDB_ROW_MAJOR;
    cell_order = TILEDB_COL_MAJOR;
    subarray_layout = Layout::ROW_MAJOR;
    tile_domain_layout = Layout::ROW_MAJOR;
    c_result_cell_slabs = {
        {1, 0, 5, 1},
        {2, 0, 1, 1},
        {1, 1, 2, 1},
        {0, 1, 5, 2},
        {1, 2, 3, 2},
        {1, 3, 0, 1},
        {UINT64_MAX, 3, 3, 2},
        {1, 2, 4, 2},
        {1, 3, 1, 1},
        {2, 1, 0, 1},
        {2, 1, 2, 1},
    };
  }

  SECTION("- tile: col, cell: row, subarray: row") {
    tile_order = TILEDB_COL_MAJOR;
    cell_order = TILEDB_ROW_MAJOR;
    subarray_layout = Layout::ROW_MAJOR;
    tile_domain_layout = Layout::COL_MAJOR;
    c_result_cell_slabs = {
        {1, 0, 7, 1},
        {2, 0, 1, 1},
        {1, 2, 6, 1},
        {0, 1, 7, 2},
        {1, 1, 1, 2},
        {1, 3, 0, 1},
        {UINT64_MAX, 3, 1, 2},
        {1, 1, 4, 2},
        {1, 3, 3, 1},
        {2, 1, 0, 1},
        {2, 1, 2, 1},
    };
  }

  SECTION("- tile: col, cell: col, subarray: row") {
    tile_order = TILEDB_COL_MAJOR;
    cell_order = TILEDB_COL_MAJOR;
    subarray_layout = Layout::ROW_MAJOR;
    tile_domain_layout = Layout::COL_MAJOR;
    c_result_cell_slabs = {
        {1, 0, 5, 1},
        {2, 0, 1, 1},
        {1, 2, 2, 1},
        {0, 1, 5, 2},
        {1, 1, 3, 2},
        {1, 3, 0, 1},
        {UINT64_MAX, 3, 3, 2},
        {1, 1, 4, 2},
        {1, 3, 1, 1},
        {2, 1, 0, 1},
        {2, 1, 2, 1},
    };
  }

  SECTION("- tile: row, cell: row, subarray: col") {
    tile_order = TILEDB_ROW_MAJOR;
    cell_order = TILEDB_ROW_MAJOR;
    subarray_layout = Layout::COL_MAJOR;
    tile_domain_layout = Layout::ROW_MAJOR;
    c_result_cell_slabs = {
        {1, 0, 7, 1},
        {1, 2, 1, 2},
        {2, 0, 1, 1},
        {1, 2, 2, 2},
        {1, 1, 6, 1},
        {1, 3, 0, 2},
        {0, 1, 7, 1},
        {UINT64_MAX, 3, 1, 1},
        {2, 1, 0, 1},
        {0, 1, 8, 1},
        {UINT64_MAX, 3, 2, 1},
        {2, 1, 2, 1},
    };
  }

  SECTION("- tile: row, cell: col, subarray: col") {
    tile_order = TILEDB_ROW_MAJOR;
    cell_order = TILEDB_COL_MAJOR;
    subarray_layout = Layout::COL_MAJOR;
    tile_domain_layout = Layout::ROW_MAJOR;
    c_result_cell_slabs = {
        {1, 0, 5, 1},
        {1, 2, 3, 2},
        {2, 0, 1, 1},
        {1, 2, 6, 2},
        {1, 1, 2, 1},
        {1, 3, 0, 2},
        {0, 1, 5, 1},
        {UINT64_MAX, 3, 3, 1},
        {2, 1, 0, 1},
        {0, 1, 8, 1},
        {UINT64_MAX, 3, 6, 1},
        {2, 1, 2, 1},
    };
  }

  SECTION("- tile: col, cell: row, subarray: col") {
    tile_order = TILEDB_COL_MAJOR;
    cell_order = TILEDB_ROW_MAJOR;
    subarray_layout = Layout::COL_MAJOR;
    tile_domain_layout = Layout::COL_MAJOR;
    c_result_cell_slabs = {
        {1, 0, 7, 1},
        {1, 1, 1, 2},
        {2, 0, 1, 1},
        {1, 1, 2, 2},
        {1, 2, 6, 1},
        {1, 3, 0, 2},
        {0, 1, 7, 1},
        {UINT64_MAX, 3, 1, 1},
        {2, 1, 0, 1},
        {0, 1, 8, 1},
        {UINT64_MAX, 3, 2, 1},
        {2, 1, 2, 1},
    };
  }

  SECTION("- tile: col, cell: col, subarray: col") {
    tile_order = TILEDB_COL_MAJOR;
    cell_order = TILEDB_COL_MAJOR;
    subarray_layout = Layout::COL_MAJOR;
    tile_domain_layout = Layout::COL_MAJOR;
    c_result_cell_slabs = {
        {1, 0, 5, 1},
        {1, 1, 3, 2},
        {2, 0, 1, 1},
        {1, 1, 6, 2},
        {1, 2, 2, 1},
        {1, 3, 0, 2},
        {0, 1, 5, 1},
        {UINT64_MAX, 3, 3, 1},
        {2, 1, 0, 1},
        {0, 1, 8, 1},
        {UINT64_MAX, 3, 6, 1},
        {2, 1, 2, 1},
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
  auto& array_schema = array_->array_schema_latest();
  Subarray subarray;
  SubarrayRanges<uint64_t> ranges = {{3, 5}, {2, 4, 5, 6}};
  create_subarray(array_->array(), ranges, subarray_layout, &subarray);
  CHECK_NOTHROW(subarray.compute_tile_coords<uint64_t>());

  // Create result space tiles
  std::vector<uint64_t> slice_1 = {3, 5, 2, 4};
  std::vector<uint64_t> slice_2 = {2, 3, 1, 6};
  auto size = 2 * sizeof(uint64_t);
  NDRange ds1 = {Range(&slice_1[0], size), Range(&slice_1[2], size)};
  NDRange ds2 = {Range(&slice_2[0], size), Range(&slice_2[2], size)};
  std::vector<NDRange> domain_slices = {ds1, ds2};
  const auto& tile_coords = subarray.tile_coords();
  std::map<const uint64_t*, ResultSpaceTile<uint64_t>> result_space_tiles;
  auto& dom{array_schema.domain()};

  std::vector<shared_ptr<FragmentMetadata>> fragments;
  for (uint64_t i = 0; i < 2; i++) {
    shared_ptr<FragmentMetadata> fragment = make_shared<FragmentMetadata>(
        HERE(),
        nullptr,
        array_->array_schema_latest_ptr(),
        generate_fragment_uri(array_->array().get()),
        std::make_pair<uint64_t, uint64_t>(0, 0),
        tiledb::test::create_test_memory_tracker(),
        true);
    fragments.emplace_back(std::move(fragment));
  }

  create_result_space_tiles(
      fragments,
      dom,
      dom.domain(),
      tile_domain_layout,
      domain_slices,
      tile_coords,
      result_space_tiles);

  // Create result coordinates
  std::vector<ResultCoords> result_coords;
  ResultTile result_tile_3_0(
      2, 0, *fragments[0], tiledb::test::get_test_memory_tracker());
  ResultTile result_tile_3_1(
      2, 1, *fragments[1], tiledb::test::get_test_memory_tracker());

  set_result_tile_dim(
      array_schema, result_tile_3_0, "d1", 0, {{1000, 3, 1000, 1000}});
  set_result_tile_dim(
      array_schema, result_tile_3_0, "d2", 1, {{1000, 3, 1000, 1000}});
  set_result_tile_dim(
      array_schema, result_tile_3_1, "d1", 0, {{5, 1000, 5, 1000}});
  set_result_tile_dim(
      array_schema, result_tile_3_1, "d2", 1, {{5, 1000, 6, 1000}});

  result_coords.emplace_back(&result_tile_3_0, 1);
  result_coords.emplace_back(&result_tile_3_1, 0);
  result_coords.emplace_back(&result_tile_3_1, 2);

  // Check iterator
  ReadCellSlabIter<uint64_t> iter(
      &subarray, &result_space_tiles, &result_coords);
  check_iter<uint64_t>(&iter, c_result_cell_slabs);

  close_array(ctx_, array_);
}
