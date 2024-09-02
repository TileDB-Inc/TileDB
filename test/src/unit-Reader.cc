/**
 * @file unit-Reader.cc
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
 * Tests the `Reader` class.
 */

#include "test/support/src/helpers.h"
#include "test/support/src/mem_helpers.h"
#include "test/support/src/vfs_helpers.h"
#include "tiledb/common/common.h"
#include "tiledb/common/dynamic_memory/dynamic_memory.h"
#include "tiledb/common/heap_memory.h"
#include "tiledb/common/logger.h"
#include "tiledb/common/memory_tracker.h"
#include "tiledb/sm/c_api/tiledb_struct_def.h"
#include "tiledb/sm/enums/array_type.h"
#include "tiledb/sm/enums/encryption_type.h"
#include "tiledb/sm/misc/types.h"
#include "tiledb/sm/query/legacy/reader.h"
#include "tiledb/type/range/range.h"

#ifdef _WIN32
#include "tiledb/sm/filesystem/win.h"
#else
#include "tiledb/sm/filesystem/posix.h"
#endif

#include <test/support/tdb_catch.h>
#include <iostream>

using namespace tiledb::sm;
using namespace tiledb::type;
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

  shared_ptr<MemoryTracker> tracker_;

  ReaderFx();
  ~ReaderFx();
};

ReaderFx::ReaderFx()
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

  int64_t dim_domain[] = {-1, 2};
  int64_t tile_extent = 2;

  // Create domain
  tiledb_domain_t* domain;
  auto rc = tiledb_domain_alloc(ctx_, &domain);
  REQUIRE(rc == TILEDB_OK);
  tiledb_dimension_t* dim;
  rc = tiledb_dimension_alloc(
      ctx_, "d1", TILEDB_INT64, dim_domain, &tile_extent, &dim);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_domain_add_dimension(ctx_, domain, dim);
  REQUIRE(rc == TILEDB_OK);

  // Create attribute
  tiledb_attribute_t* attr;
  rc = tiledb_attribute_alloc(ctx_, "a", TILEDB_INT32, &attr);
  REQUIRE(rc == TILEDB_OK);

  // Create array schema
  tiledb_array_schema_t* array_schema;
  rc = tiledb_array_schema_alloc(ctx_, TILEDB_SPARSE, &array_schema);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_cell_order(ctx_, array_schema, TILEDB_ROW_MAJOR);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_tile_order(ctx_, array_schema, TILEDB_ROW_MAJOR);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_domain(ctx_, array_schema, domain);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_schema_add_attribute(ctx_, array_schema, attr);
  REQUIRE(rc == TILEDB_OK);

  rc = tiledb_array_schema_check(ctx_, array_schema);
  REQUIRE(rc == TILEDB_OK);

  // Create array
  rc = tiledb_array_create(ctx_, array_name_.c_str(), array_schema);
  REQUIRE(rc == TILEDB_OK);
  tiledb_attribute_free(&attr);
  tiledb_dimension_free(&dim);
  tiledb_domain_free(&domain);
  tiledb_array_schema_free(&array_schema);
  CHECK(rc == TILEDB_OK);
}

ReaderFx::~ReaderFx() {
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
  uint64_t tmp_size = 0;
  Config config;
  Context context(config);
  LocalQueryStateMachine lq_state_machine{LocalQueryState::uninitialized};
  std::unordered_map<std::string, tiledb::sm::QueryBuffer> buffers;
  buffers.emplace(
      "a", tiledb::sm::QueryBuffer(nullptr, nullptr, &tmp_size, &tmp_size));
  std::unordered_map<std::string, tiledb::sm::QueryBuffer> aggregate_buffers;
  std::optional<QueryCondition> condition;
  ThreadPool tp_cpu(4), tp_io(4);
  Array array(context.resources(), URI(array_name_));
  CHECK(array.open(QueryType::READ, EncryptionType::NO_ENCRYPTION, nullptr, 0)
            .ok());
  Subarray subarray(&array, &g_helper_stats, g_helper_logger());
  DefaultChannelAggregates default_channel_aggregates;
  auto params = StrategyParams(
      context.resources(),
      array.memory_tracker(),
      tracker_,
      lq_state_machine,
      CancellationSource(context.storage_manager()),
      array.opened_array(),
      config,
      nullopt,
      buffers,
      aggregate_buffers,
      subarray,
      Layout::ROW_MAJOR,
      condition,
      default_channel_aggregates,
      false);
  Reader reader(&g_helper_stats, g_helper_logger(), params);
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

  NDRange ds1 = {
      Range(&domain_slice_1[0], size), Range(&domain_slice_1[2], size)};
  NDRange ds2 = {
      Range(&domain_slice_2[0], size), Range(&domain_slice_2[2], size)};
  NDRange ds3 = {
      Range(&domain_slice_3[0], size), Range(&domain_slice_3[2], size)};
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

  auto d1{make_shared<Dimension>(HERE(), "d1", Datatype::INT32, tracker_)};
  CHECK(d1->set_domain(domain_vec).ok());
  CHECK(d1->set_tile_extent(&tile_extents_vec[0]).ok());
  auto d2{make_shared<Dimension>(HERE(), "d2", Datatype::INT32, tracker_)};
  CHECK(d2->set_domain(&domain_vec[2]).ok());
  CHECK(d2->set_tile_extent(&tile_extents_vec[1]).ok());
  auto dom{make_shared<Domain>(HERE(), tracker_)};
  CHECK(dom->add_dimension(d1).ok());
  CHECK(dom->add_dimension(d2).ok());

  auto schema = make_shared<ArraySchema>(HERE(), ArrayType::DENSE, tracker_);
  CHECK(schema->set_domain(dom).ok());

  std::vector<shared_ptr<FragmentMetadata>> fragments;
  for (uint64_t i = 0; i < frag_tile_domains.size() + 1; i++) {
    shared_ptr<FragmentMetadata> fragment = make_shared<FragmentMetadata>(
        HERE(),
        nullptr,
        schema,
        generate_fragment_uri(&array),
        std::make_pair<uint64_t, uint64_t>(0, 0),
        tracker_,
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
      result_space_tiles,
      tiledb::test::get_test_memory_tracker());
  CHECK(result_space_tiles.size() == 6);

  // Initialize result_space_tiles
  ResultSpaceTile<int32_t> rst_1_0(tiledb::test::get_test_memory_tracker());
  rst_1_0.set_start_coords({3, 1});
  rst_1_0.append_frag_domain(2, ds2);
  rst_1_0.append_frag_domain(1, ds1);
  rst_1_0.set_result_tile(1, 0, *fragments[0]);
  rst_1_0.set_result_tile(2, 0, *fragments[1]);
  ResultSpaceTile<int32_t> rst_1_2(tiledb::test::get_test_memory_tracker());
  rst_1_2.set_start_coords({3, 11});
  rst_1_2.append_frag_domain(1, ds1);
  rst_1_2.set_result_tile(1, 2, *fragments[0]);
  ResultSpaceTile<int32_t> rst_2_0(tiledb::test::get_test_memory_tracker());
  rst_2_0.set_start_coords({5, 1});
  rst_2_0.append_frag_domain(3, ds3);
  rst_2_0.set_result_tile(3, 0, *fragments[2]);
  ResultSpaceTile<int32_t> rst_2_2(tiledb::test::get_test_memory_tracker());
  rst_2_2.set_start_coords({5, 11});
  ResultSpaceTile<int32_t> rst_3_0(tiledb::test::get_test_memory_tracker());
  rst_3_0.set_start_coords({7, 1});
  rst_3_0.append_frag_domain(3, ds3);
  rst_3_0.set_result_tile(3, 2, *fragments[2]);
  ResultSpaceTile<int32_t> rst_3_2(tiledb::test::get_test_memory_tracker());
  rst_3_2.set_start_coords({7, 11});

  // Check correctness
  CHECK(result_space_tiles.at((const int32_t*)&(tile_coords[0][0])) == rst_1_0);
  CHECK(result_space_tiles.at((const int32_t*)&(tile_coords[1][0])) == rst_1_2);
  CHECK(result_space_tiles.at((const int32_t*)&(tile_coords[2][0])) == rst_2_0);
  CHECK(result_space_tiles.at((const int32_t*)&(tile_coords[3][0])) == rst_2_2);
  CHECK(result_space_tiles.at((const int32_t*)&(tile_coords[4][0])) == rst_3_0);
  CHECK(result_space_tiles.at((const int32_t*)&(tile_coords[5][0])) == rst_3_2);
}
