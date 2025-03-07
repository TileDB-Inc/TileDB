/**
 * @file unit-result-tile.cc
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
 * Tests for the ResultTile classes.
 */

#include "tiledb/api/c_api/array/array_api_internal.h"
#include "tiledb/common/memory_tracker.h"
#include "tiledb/sm/c_api/tiledb.h"
#include "tiledb/sm/misc/types.h"

#include "test/support/src/helpers.h"
#include "tiledb/sm/query/readers/sparse_index_reader_base.h"

#ifdef _WIN32
#include "tiledb/sm/filesystem/win.h"
#else
#include "tiledb/sm/filesystem/posix.h"
#endif

#include <test/support/tdb_catch.h>
#include <numeric>

using namespace tiledb::sm;
using namespace tiledb::test;

/* ********************************* */
/*                TESTS              */
/* ********************************* */

struct CResultTileFx {
  tiledb_ctx_t* ctx_ = nullptr;
  tiledb_vfs_t* vfs_ = nullptr;
  std::string temp_dir_;
  std::string array_name_;
  const char* ARRAY_NAME = "test_result_coords";
  tiledb_array_t* array_;
  std::shared_ptr<FragmentMetadata> frag_md_;
  shared_ptr<MemoryTracker> memory_tracker_;

  CResultTileFx();
  ~CResultTileFx();
};

CResultTileFx::CResultTileFx()
    : memory_tracker_(tiledb::test::get_test_memory_tracker()) {
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

  // Create array
  create_array(
      ctx_,
      array_name_,
      TILEDB_SPARSE,
      {"d1", "d2"},
      {TILEDB_STRING_ASCII, TILEDB_STRING_ASCII},
      {nullptr, nullptr},
      {nullptr, nullptr},
      {"a"},
      {TILEDB_STRING_ASCII},
      {1},
      {tiledb::test::Compressor(TILEDB_FILTER_NONE, -1)},
      TILEDB_ROW_MAJOR,
      TILEDB_ROW_MAJOR,
      100);

  // Open array for reading.
  auto rc = tiledb_array_alloc(ctx_, array_name_.c_str(), &array_);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array_, TILEDB_READ);
  REQUIRE(rc == TILEDB_OK);

  // Create test memory tracker.
  memory_tracker_ = tiledb::test::create_test_memory_tracker();

  frag_md_ = make_shared<FragmentMetadata>(
      HERE(),
      nullptr,
      array_->array_schema_latest_ptr(),
      generate_fragment_uri(array_->array().get()),
      std::make_pair<uint64_t, uint64_t>(0, 0),
      memory_tracker_,
      false);
}

CResultTileFx::~CResultTileFx() {
  // Clean up.
  REQUIRE(tiledb_array_close(ctx_, array_) == TILEDB_OK);
  tiledb_array_free(&array_);

  remove_dir(temp_dir_, ctx_, vfs_);
  tiledb_ctx_free(&ctx_);
  tiledb_vfs_free(&vfs_);
}

TEST_CASE_METHOD(
    CResultTileFx,
    "ResultTileWithBitmap: result_num_between_pos and "
    "pos_with_given_result_sum test",
    "[resulttilewithbitmap][pos_with_given_result_sum][pos_with_given_result_"
    "sum]") {
  tiledb_ctx_t* ctx = vanilla_context_c();

  tiledb_array_schema_t* array_schema;
  int rc = tiledb_array_schema_alloc(ctx, TILEDB_SPARSE, &array_schema);
  REQUIRE(rc == TILEDB_OK);

  // Create dimensions and domain
  tiledb_domain_t* domain;
  rc = tiledb_domain_alloc(ctx, &domain);
  REQUIRE(rc == TILEDB_OK);

  int dim_domain[] = {1, 4};
  int tile_extent = 2;
  tiledb_dimension_t* d;
  rc = tiledb_dimension_alloc(
      ctx, "d", TILEDB_INT32, &dim_domain[0], &tile_extent, &d);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_domain_add_dimension(ctx, domain, d);
  REQUIRE(rc == TILEDB_OK);
  tiledb_dimension_free(&d);

  // Set domain to schema
  rc = tiledb_array_schema_set_domain(ctx, array_schema, domain);
  REQUIRE(rc == TILEDB_OK);
  tiledb_domain_free(&domain);

  UnorderedWithDupsResultTile<uint8_t> tile(
      0, 0, *frag_md_, tiledb::test::get_test_memory_tracker());

  // Check the function with an empty bitmap.
  CHECK(tile.result_num_between_pos(2, 10) == 8);
  CHECK(tile.pos_with_given_result_sum(2, 8) == 9);

  // Check the functions with a bitmap.
  tile.alloc_bitmap();
  CHECK(tile.result_num_between_pos(2, 10) == 8);
  CHECK(tile.pos_with_given_result_sum(2, 8) == 9);

  tile.bitmap()[6] = 0;
  tile.count_cells();
  CHECK(tile.result_num_between_pos(2, 10) == 7);
  CHECK(tile.pos_with_given_result_sum(2, 8) == 10);
}

TEST_CASE_METHOD(
    CResultTileFx,
    "Test compute_results_count_sparse_string non overlapping",
    "[resulttile][compute_results_count_sparse_string][non-overlapping]") {
  bool first_dim = GENERATE(true, false);
  std::string dim_name = first_dim ? "d1" : "d2";
  uint64_t dim_idx = first_dim ? 0 : 1;
  uint64_t num_cells = 8;

  auto& array_schema = array_->array_schema_latest();
  FragmentMetadata frag_md(
      nullptr,
      array_->array_schema_latest_ptr(),
      generate_fragment_uri(array_->array().get()),
      std::make_pair<uint64_t, uint64_t>(0, 0),
      memory_tracker_,
      true);
  ResultTile rt(0, 0, frag_md, tiledb::test::get_test_memory_tracker());

  // Make sure cell_num() will return the correct value.
  if (!first_dim) {
    ResultTile::TileSizes tile_sizes(
        (num_cells + 1) * constants::cell_var_offset_size,
        0,
        0,
        0,
        std::nullopt,
        std::nullopt);
    ResultTile::TileData tile_data{nullptr, nullptr, nullptr};
    rt.init_coord_tile(
        constants::format_version,
        array_schema,
        "d1",
        tile_sizes,
        tile_data,
        0);
  }

  ResultTile::TileSizes tile_sizes(
      (num_cells + 1) * constants::cell_var_offset_size,
      0,
      num_cells,
      0,
      std::nullopt,
      std::nullopt);
  ResultTile::TileData tile_data{nullptr, nullptr, nullptr};
  rt.init_coord_tile(
      constants::format_version,
      array_schema,
      dim_name,
      tile_sizes,
      tile_data,
      dim_idx);
  auto tile_tuple = rt.tile_tuple(dim_name);
  Tile* const t = &tile_tuple->fixed_tile();
  Tile* const t_var = &tile_tuple->var_tile();

  // Initialize offsets, use 1 character strings.
  offsets_t* offsets = t->data_as<offsets_t>();
  for (uint64_t i = 0; i < num_cells + 1; i++) {
    offsets[i] = i;
  }

  // Initialize data, use incrementing single string values starting with 'a'.
  char* var = t_var->data_as<char>();
  for (uint64_t i = 0; i < num_cells; i++) {
    var[i] = 'a' + i;
  }

  // Initialize ranges.
  NDRange ranges;
  char temp[2];
  std::vector<uint8_t> exp_result_count;
  SECTION("- First and last cell included") {
    ranges.resize(2);
    temp[0] = temp[1] = 'a';
    ranges[0] = Range(temp, 2, 1);

    temp[0] = temp[1] = 'h';
    ranges[1] = Range(temp, 2, 1);

    exp_result_count = {1, 0, 0, 0, 0, 0, 0, 1};
  }

  SECTION("- Middle cells included") {
    ranges.resize(1);
    temp[0] = 'b';
    temp[1] = 'g';
    ranges[0] = Range(temp, 2, 1);

    exp_result_count = {0, 1, 1, 1, 1, 1, 1, 0};
  }

  tdb::pmr::vector<uint64_t> range_indexes(
      ranges.size(), memory_tracker_->get_resource(MemoryType::DIMENSIONS));
  std::iota(range_indexes.begin(), range_indexes.end(), 0);

  auto resource = tiledb::test::get_test_memory_tracker()->get_resource(
      MemoryType::RESULT_TILE_BITMAP);
  tdb::pmr::vector<uint8_t> result_count(num_cells, 1, resource);
  ResultTile::compute_results_count_sparse_string(
      &rt,
      dim_idx,
      ranges,
      range_indexes,
      result_count,
      Layout::ROW_MAJOR,
      0,
      num_cells);

  CHECK(memcmp(result_count.data(), exp_result_count.data(), num_cells) == 0);
}

TEST_CASE_METHOD(
    CResultTileFx,
    "Test compute_results_count_sparse_string overlapping",
    "[resulttile][compute_results_count_sparse_string][overlapping]") {
  bool first_dim = GENERATE(true, false);
  std::string dim_name = first_dim ? "d1" : "d2";
  uint64_t dim_idx = first_dim ? 0 : 1;
  uint64_t num_cells = 8;

  auto& array_schema = array_->array_schema_latest();
  FragmentMetadata frag_md(
      nullptr,
      array_->array_schema_latest_ptr(),
      generate_fragment_uri(array_->array().get()),
      std::make_pair<uint64_t, uint64_t>(0, 0),
      memory_tracker_,
      true);
  ResultTile rt(0, 0, frag_md, tiledb::test::get_test_memory_tracker());

  // Make sure cell_num() will return the correct value.
  if (!first_dim) {
    ResultTile::TileSizes tile_sizes(
        (num_cells + 1) * constants::cell_var_offset_size,
        0,
        0,
        0,
        std::nullopt,
        std::nullopt);
    ResultTile::TileData tile_data{nullptr, nullptr, nullptr};
    rt.init_coord_tile(
        constants::format_version,
        array_schema,
        "d1",
        tile_sizes,
        tile_data,
        0);
  }

  ResultTile::TileSizes tile_sizes(
      (num_cells + 1) * constants::cell_var_offset_size,
      0,
      num_cells,
      0,
      std::nullopt,
      std::nullopt);
  ResultTile::TileData tile_data{nullptr, nullptr, nullptr};
  rt.init_coord_tile(
      constants::format_version,
      array_schema,
      dim_name,
      tile_sizes,
      tile_data,
      dim_idx);
  auto tile_tuple = rt.tile_tuple(dim_name);
  Tile* const t = &tile_tuple->fixed_tile();
  Tile* const t_var = &tile_tuple->var_tile();

  // Initialize offsets, use 1 character strings.
  offsets_t* offsets = t->data_as<offsets_t>();
  for (uint64_t i = 0; i < num_cells + 1; i++) {
    offsets[i] = i;
  }

  // Initialize data, use incrementing single string values starting with 'a'.
  char* var = t_var->data_as<char>();
  for (uint64_t i = 0; i < num_cells; i++) {
    var[i] = 'a' + i;
  }

  // Initialize ranges.
  NDRange ranges;
  char temp[2];
  std::vector<uint64_t> exp_result_count{8};
  SECTION("- First and last cell included multiple times") {
    ranges.resize(5);
    temp[0] = temp[1] = 'a';
    ranges[0] = Range(temp, 2, 1);
    ranges[1] = Range(temp, 2, 1);
    ranges[2] = Range(temp, 2, 1);

    temp[0] = temp[1] = 'h';
    ranges[3] = Range(temp, 2, 1);
    ranges[4] = Range(temp, 2, 1);

    exp_result_count = {3, 0, 0, 0, 0, 0, 0, 2};
  }

  SECTION("- Middle cells included multiple times") {
    ranges.resize(2);
    temp[0] = 'b';
    temp[1] = 'g';
    ranges[0] = Range(temp, 2, 1);

    temp[0] = 'c';
    temp[1] = 'f';
    ranges[1] = Range(temp, 2, 1);

    exp_result_count = {0, 1, 2, 2, 2, 2, 1, 0};
  }

  SECTION("- Complex ranges") {
    ranges.resize(6);
    temp[0] = 'b';
    temp[1] = 'd';
    ranges[0] = Range(temp, 2, 1);

    temp[0] = temp[1] = 'c';
    ranges[1] = Range(temp, 2, 1);

    temp[0] = 'f';
    temp[1] = 'h';
    ranges[2] = Range(temp, 2, 1);

    temp[0] = temp[1] = 'g';
    ranges[3] = Range(temp, 2, 1);
    ranges[4] = Range(temp, 2, 1);

    temp[0] = temp[1] = 'h';
    ranges[5] = Range(temp, 2, 1);

    exp_result_count = {0, 1, 2, 1, 0, 1, 3, 2};
  }

  tdb::pmr::vector<uint64_t> range_indexes(
      ranges.size(), memory_tracker_->get_resource(MemoryType::DIMENSIONS));
  std::iota(range_indexes.begin(), range_indexes.end(), 0);

  auto resource = tiledb::test::get_test_memory_tracker()->get_resource(
      MemoryType::RESULT_TILE_BITMAP);
  tdb::pmr::vector<uint64_t> result_count(num_cells, 1, resource);
  ResultTile::compute_results_count_sparse_string(
      &rt,
      dim_idx,
      ranges,
      range_indexes,
      result_count,
      Layout::ROW_MAJOR,
      0,
      num_cells);

  CHECK(memcmp(result_count.data(), exp_result_count.data(), num_cells) == 0);
}
