/**
 * @file unit-result-coords.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022 TileDB, Inc.
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
 * Tests for the ResultCoords classes.
 */
#include "tiledb/sm/c_api/tiledb.h"
#include "tiledb/sm/c_api/tiledb_struct_def.h"

#include "test/src/helpers.h"
#include "tiledb/sm/query/readers/result_coords.h"
#include "tiledb/sm/query/readers/sparse_index_reader_base.h"

#ifdef _WIN32
#include "tiledb/sm/filesystem/win.h"
#else
#include "tiledb/sm/filesystem/posix.h"
#endif

#include <test/support/tdb_catch.h>

using namespace tiledb::sm;
using namespace tiledb::test;

struct CResultCoordsFx {
  tiledb_ctx_t* ctx_ = nullptr;
  tiledb_vfs_t* vfs_ = nullptr;
  std::string temp_dir_;
  std::string array_name_;
  const char* ARRAY_NAME = "test_result_coords";
  tiledb_array_t* array_;
  std::unique_ptr<FragmentMetadata> frag_md;

  CResultCoordsFx();
  ~CResultCoordsFx();

  GlobalOrderResultTile<uint8_t> make_tile_with_num_cells(uint64_t num_cells);
};

CResultCoordsFx::CResultCoordsFx() {
  tiledb_config_t* config;
  tiledb_error_t* error = nullptr;
  REQUIRE(tiledb_config_alloc(&config, &error) == TILEDB_OK);
  REQUIRE(error == nullptr);
  REQUIRE(tiledb_ctx_alloc(config, &ctx_) == TILEDB_OK);
  REQUIRE(error == nullptr);
  REQUIRE(tiledb_vfs_alloc(ctx_, config, &vfs_) == TILEDB_OK);
  tiledb_config_free(&config);

  // Create temporary directory based on the supported filesystem.
#ifdef _WIN32
  temp_dir_ = tiledb::sm::Win::current_dir() + "\\tiledb_test\\";
#else
  temp_dir_ = "file://" + tiledb::sm::Posix::current_dir() + "/tiledb_test/";
#endif
  create_dir(temp_dir_, ctx_, vfs_);
  array_name_ = temp_dir_ + ARRAY_NAME;

  int64_t domain[] = {1, 10};
  int64_t tile_extent = 5;
  // Create array
  create_array(
      ctx_,
      array_name_,
      TILEDB_SPARSE,
      {"d"},
      {TILEDB_INT64},
      {domain},
      {&tile_extent},
      {"a"},
      {TILEDB_STRING_ASCII},
      {1},
      {tiledb::test::Compressor(TILEDB_FILTER_NONE, -1)},
      TILEDB_ROW_MAJOR,
      TILEDB_ROW_MAJOR,
      5);

  // Open array for reading.
  auto rc = tiledb_array_alloc(ctx_, array_name_.c_str(), &array_);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array_, TILEDB_READ);
  REQUIRE(rc == TILEDB_OK);

  frag_md.reset(new FragmentMetadata(
      nullptr,
      nullptr,
      array_->array_->array_schema_latest_ptr(),
      URI(),
      std::make_pair<uint64_t, uint64_t>(0, 0),
      true));
}

CResultCoordsFx::~CResultCoordsFx() {
  // Clean up.
  REQUIRE(tiledb_array_close(ctx_, array_) == TILEDB_OK);
  tiledb_array_free(&array_);

  remove_dir(temp_dir_, ctx_, vfs_);
  tiledb_ctx_free(&ctx_);
  tiledb_vfs_free(&vfs_);
}

GlobalOrderResultTile<uint8_t> CResultCoordsFx::make_tile_with_num_cells(
    uint64_t num_cells) {
  GlobalOrderResultTile<uint8_t> result_tile(0, 0, false, false, *frag_md);
  result_tile.init_attr_tile(constants::coords, false, false);
  auto tile_tuple = result_tile.tile_tuple(constants::coords);
  Tile* const tile = &tile_tuple->fixed_tile();
  auto cell_size = sizeof(int64_t);
  REQUIRE(tile->init_unfiltered(
                  constants::format_version,
                  Datatype::INT64,
                  num_cells * cell_size,
                  cell_size,
                  0)
              .ok());

  return result_tile;
}

/* ********************************* */
/*                TESTS              */
/* ********************************* */

// Simple comparator class that will only look at pos_.
class Cmp {
 public:
  Cmp() {
  }

  bool operator()(
      const GlobalOrderResultCoords<uint8_t>& a,
      const GlobalOrderResultCoords<uint8_t>& b) const {
    if (a.pos_ >= b.pos_) {
      return true;
    }

    return false;
  }
};

TEST_CASE_METHOD(
    CResultCoordsFx,
    "GlobalOrderResultCoords: test max_slab_length",
    "[globalorderresultcoords][max_slab_length]") {
  auto tile = make_tile_with_num_cells(5);

  // Test max_slab_length with no bitmap.
  GlobalOrderResultCoords rc1(&tile, 1);
  REQUIRE(rc1.max_slab_length() == 4);

  // Test max_slab_length with bitmap 1.
  tile.bitmap() = {0, 1, 1, 1, 1};
  tile.count_cells();
  REQUIRE(rc1.max_slab_length() == 4);

  // Test max_slab_length with bitmap 2.
  tile.bitmap() = {0, 1, 1, 1, 0};
  tile.count_cells();
  REQUIRE(rc1.max_slab_length() == 3);

  // Test max_slab_length with bitmap 3.
  tile.bitmap() = {0, 1, 1, 1, 0};
  tile.count_cells();
  rc1.pos_ = 0;
  REQUIRE(rc1.max_slab_length() == 0);
}

TEST_CASE_METHOD(
    CResultCoordsFx,
    "GlobalOrderResultCoords: test max_slab_length with comparator",
    "[globalorderresultcoords][max_slab_length_with_comp]") {
  auto tile = make_tile_with_num_cells(5);
  Cmp cmp;

  // Test max_slab_length with no bitmap and comparator.
  GlobalOrderResultCoords rc1(&tile, 1);
  REQUIRE(rc1.max_slab_length(GlobalOrderResultCoords(&tile, 3), cmp) == 2);

  // Test max_slab_length with bitmap and comparator 1.
  tile.bitmap() = {0, 1, 1, 1, 1};
  tile.count_cells();
  REQUIRE(rc1.max_slab_length(GlobalOrderResultCoords(&tile, 10), cmp) == 4);
  REQUIRE(rc1.max_slab_length(GlobalOrderResultCoords(&tile, 3), cmp) == 2);

  // Test max_slab_length with bitmap and comparator 2.
  tile.bitmap() = {0, 1, 1, 1, 0};
  tile.count_cells();
  REQUIRE(rc1.max_slab_length(GlobalOrderResultCoords(&tile, 10), cmp) == 3);
  REQUIRE(rc1.max_slab_length(GlobalOrderResultCoords(&tile, 3), cmp) == 2);

  // Test max_slab_length with bitmap and comparator 3.
  tile.bitmap() = {0, 1, 1, 1, 0};
  tile.count_cells();
  rc1.pos_ = 0;
  REQUIRE(rc1.max_slab_length(GlobalOrderResultCoords(&tile, 3), cmp) == 0);

  auto tile2 = make_tile_with_num_cells(100);
  rc1.tile_ = &tile2;
  for (uint64_t i = 0; i < 100; i++) {
    for (uint64_t j = i + 1; j < 100; j++) {
      rc1.pos_ = i;
      REQUIRE(
          rc1.max_slab_length(GlobalOrderResultCoords(&tile2, j), cmp) ==
          j - i);
    }
  }
}

TEST_CASE_METHOD(
    CResultCoordsFx,
    "GlobalOrderResultCoords: advance_to_next_cell",
    "[globalorderresultcoords][advance_to_next_cell]") {
  auto tile = make_tile_with_num_cells(5);
  Cmp cmp;

  GlobalOrderResultCoords rc1(&tile, 0);
  tile.bitmap() = {0, 1, 1, 0, 1};
  tile.count_cells();
  REQUIRE(rc1.advance_to_next_cell() == true);
  REQUIRE(rc1.pos_ == 1);
  REQUIRE(rc1.advance_to_next_cell() == true);
  REQUIRE(rc1.pos_ == 2);
  REQUIRE(rc1.advance_to_next_cell() == true);
  REQUIRE(rc1.pos_ == 4);
  REQUIRE(rc1.advance_to_next_cell() == false);

  // Recreate to test that we don't move pos_ on the first call.
  GlobalOrderResultCoords rc2(&tile, 0);
  tile.bitmap() = {1, 1, 1, 0, 0};
  tile.count_cells();
  REQUIRE(rc2.advance_to_next_cell() == true);
  REQUIRE(rc2.pos_ == 0);
  REQUIRE(rc2.advance_to_next_cell() == true);
  REQUIRE(rc2.pos_ == 1);
  REQUIRE(rc2.advance_to_next_cell() == true);
  REQUIRE(rc2.pos_ == 2);
  REQUIRE(rc2.advance_to_next_cell() == false);
}