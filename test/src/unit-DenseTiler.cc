/**
 * @file unit-DenseTiler.cc
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
 * Tests the `DenseTiler` class.
 */

#include "test/src/helpers.h"
#include "tiledb/sm/c_api/tiledb.h"
#include "tiledb/sm/c_api/tiledb_struct_def.h"
#include "tiledb/sm/cpp_api/tiledb"
#include "tiledb/sm/query/writers/dense_tiler.h"

#include <test/support/tdb_catch.h>
#include <iostream>

using namespace tiledb;
using namespace tiledb::sm;

struct DenseTilerFx {
  // Constants
  const int32_t fill_value = 0;

  // Aux structs
  struct DimensionInfo {
    std::string name;
    tiledb_datatype_t type;
    void* domain;
    void* tile_extent;
  };
  struct AttributeInfo {
    std::string name;
    tiledb_datatype_t type;
    uint32_t cell_val_num;
    bool nullable;
  };

  // Members used to create a subarray
  tiledb_ctx_t* ctx_;
  tiledb_array_t* array_;

  // Constructors/Destructors
  DenseTilerFx();
  ~DenseTilerFx();

  // Aux functions
  void remove_array(const std::string& array_name);
  void create_array(
      const std::string& array_name,
      const std::vector<DimensionInfo>& dim_info,
      const std::vector<AttributeInfo>& attr_info,
      tiledb_layout_t cell_order,
      tiledb_layout_t tile_order);
  void open_array(const std::string& array_name, tiledb_query_type_t type);
  void close_array();
  void add_ranges(
      const std::vector<const void*>& ranges,
      uint64_t range_size,
      tiledb::sm::Subarray* subarray);
  template <class T>
  bool check_tile(Tile& tile, const std::vector<T>& data);
};

DenseTilerFx::DenseTilerFx() {
  REQUIRE(tiledb_ctx_alloc(NULL, &ctx_) == TILEDB_OK);
  array_ = NULL;
}

DenseTilerFx::~DenseTilerFx() {
  close_array();
  tiledb_array_free(&array_);
  tiledb_ctx_free(&ctx_);
}

void DenseTilerFx::remove_array(const std::string& array_name) {
  tiledb::Context ctx;
  tiledb::VFS vfs(ctx);
  if (vfs.is_dir(array_name))
    vfs.remove_dir(array_name);
}

void DenseTilerFx::create_array(
    const std::string& array_name,
    const std::vector<DimensionInfo>& dim_info,
    const std::vector<AttributeInfo>& attr_info,
    tiledb_layout_t cell_order,
    tiledb_layout_t tile_order) {
  tiledb::Context ctx;

  // Clean array if it exists
  remove_array(array_name);

  // Create domain
  tiledb::Domain domain(ctx);
  for (const auto& di : dim_info) {
    auto d = tiledb::Dimension::create(
        ctx, di.name, di.type, di.domain, di.tile_extent);
    domain.add_dimension(d);
  }

  // Create array schema
  tiledb::ArraySchema schema(ctx, TILEDB_DENSE);
  schema.set_domain(domain);
  schema.set_cell_order(cell_order);
  schema.set_tile_order(tile_order);

  // Create attributes
  for (const auto& ai : attr_info) {
    auto a = tiledb::Attribute::create(ctx, ai.name, ai.type);
    a.set_nullable(ai.nullable);
    a.set_cell_val_num(ai.cell_val_num);
    schema.add_attribute(a);
  }

  // Create array
  tiledb::Array::create(array_name, schema);
}

void DenseTilerFx::add_ranges(
    const std::vector<const void*>& ranges,
    uint64_t range_size,
    tiledb::sm::Subarray* subarray) {
  for (size_t i = 0; i < ranges.size(); ++i)
    CHECK(subarray->add_range((uint32_t)i, Range(ranges[i], range_size)).ok());
}

void DenseTilerFx::open_array(
    const std::string& array_name, tiledb_query_type_t type) {
  close_array();
  REQUIRE(tiledb_array_alloc(ctx_, array_name.c_str(), &array_) == TILEDB_OK);
  REQUIRE(tiledb_array_open(ctx_, array_, type) == TILEDB_OK);
}

void DenseTilerFx::close_array() {
  if (array_ == NULL)
    return;

  int32_t is_open;
  REQUIRE(tiledb_array_is_open(ctx_, array_, &is_open) == TILEDB_OK);
  if (!is_open)
    return;

  REQUIRE(tiledb_array_close(ctx_, array_) == TILEDB_OK);
  tiledb_array_free(&array_);
  array_ = NULL;
}

template <class T>
bool DenseTilerFx::check_tile(Tile& tile, const std::vector<T>& data) {
  std::vector<T> tile_data(data.size());
  CHECK(tile.size() == data.size() * sizeof(T));
  CHECK(tile.read(&tile_data[0], 0, data.size() * sizeof(T)).ok());
  CHECK(tile_data == data);
  return tile_data == data;
}

TEST_CASE_METHOD(
    DenseTilerFx,
    "DenseTiler: Test initialization, 1D",
    "[DenseTiler][init][1d]") {
  // Create array
  std::string array_name = "dense_tiler";
  int32_t d_dom[] = {1, 10};
  int32_t d_ext = 5;
  create_array(
      array_name,
      {{"d", TILEDB_INT32, d_dom, &d_ext}},
      {{"a", TILEDB_INT32, 1, false}},
      TILEDB_ROW_MAJOR,
      TILEDB_ROW_MAJOR);

  // Create buffers
  std::unordered_map<std::string, QueryBuffer> buffers;
  std::vector<int32_t> buff_a = {1, 2, 3, 4};
  uint64_t buff_a_size = sizeof(buff_a);
  buffers["a"] = QueryBuffer(&buff_a[0], nullptr, &buff_a_size, nullptr);

  // Create subarray
  open_array(array_name, TILEDB_READ);
  int32_t sub1[] = {3, 6};
  tiledb::sm::Subarray subarray1(
      array_->array_.get(),
      Layout::ROW_MAJOR,
      &test::g_helper_stats,
      test::g_helper_logger());
  add_ranges({sub1}, sizeof(sub1), &subarray1);

  // Create DenseTiler
  DenseTiler<int32_t> tiler1(&buffers, &subarray1, &test::g_helper_stats);

  // Test correctness of initialization
  CHECK(tiler1.tile_num() == 2);
  CHECK(tiler1.first_sub_tile_coords() == std::vector<uint64_t>{0});
  CHECK(tiler1.sub_strides_el() == std::vector<uint64_t>{1});
  CHECK(tiler1.tile_strides_el() == std::vector<uint64_t>{1});
  CHECK(tiler1.sub_tile_coord_strides() == std::vector<uint64_t>{1});

  // Create new subarray
  close_array();
  open_array(array_name, TILEDB_READ);
  int32_t sub2[] = {6, 9};
  tiledb::sm::Subarray subarray2(
      array_->array_.get(),
      Layout::ROW_MAJOR,
      &test::g_helper_stats,
      test::g_helper_logger());
  add_ranges({sub2}, sizeof(sub2), &subarray2);

  // Create DenseTiler
  DenseTiler<int32_t> tiler2(&buffers, &subarray2, &test::g_helper_stats);

  // Test correctness of initialization
  CHECK(tiler2.tile_num() == 1);
  CHECK(tiler2.first_sub_tile_coords() == std::vector<uint64_t>{1});
  CHECK(tiler2.sub_strides_el() == std::vector<uint64_t>{1});
  CHECK(tiler2.tile_strides_el() == std::vector<uint64_t>{1});
  CHECK(tiler2.sub_tile_coord_strides() == std::vector<uint64_t>{1});

  // Clean up
  close_array();
  remove_array(array_name);
}

TEST_CASE_METHOD(
    DenseTilerFx,
    "DenseTiler: Test copy plan, 1D",
    "[DenseTiler][copy_plan][1d]") {
  // Create array
  std::string array_name = "dense_tiler";
  int32_t d_dom[] = {1, 10};
  int32_t d_ext = 5;
  create_array(
      array_name,
      {{"d", TILEDB_INT32, d_dom, &d_ext}},
      {{"a", TILEDB_INT32, 1, false}},
      TILEDB_ROW_MAJOR,
      TILEDB_ROW_MAJOR);

  // Create buffers
  std::unordered_map<std::string, QueryBuffer> buffers;
  std::vector<int32_t> buff_a = {1, 2, 3, 4};
  uint64_t buff_a_size = sizeof(buff_a);
  buffers["a"] = QueryBuffer(&buff_a[0], nullptr, &buff_a_size, nullptr);

  // Create subarray
  open_array(array_name, TILEDB_READ);
  int32_t sub1[] = {3, 6};
  tiledb::sm::Subarray subarray1(
      array_->array_.get(),
      Layout::ROW_MAJOR,
      &test::g_helper_stats,
      test::g_helper_logger());
  add_ranges({sub1}, sizeof(sub1), &subarray1);

  // Create DenseTiler
  DenseTiler<int32_t> tiler1(&buffers, &subarray1, &test::g_helper_stats);

  // Test correctness of copy plan for tile 0
  auto copy_plan1_0 = tiler1.copy_plan(0);
  CHECK(copy_plan1_0.copy_el_ == 3);
  CHECK(
      copy_plan1_0.dim_ranges_ == std::vector<std::array<uint64_t, 2>>{{0, 0}});
  CHECK(copy_plan1_0.sub_strides_el_ == std::vector<uint64_t>{1});
  CHECK(copy_plan1_0.tile_strides_el_ == std::vector<uint64_t>{1});
  CHECK(copy_plan1_0.sub_start_el_ == 0);
  CHECK(copy_plan1_0.tile_start_el_ == 2);
  CHECK(copy_plan1_0.first_d_ == 0);

  // Test correctness of copy plan for tile 1
  auto copy_plan1_1 = tiler1.copy_plan(1);
  CHECK(copy_plan1_1.copy_el_ == 1);
  CHECK(
      copy_plan1_1.dim_ranges_ == std::vector<std::array<uint64_t, 2>>{{0, 0}});
  CHECK(copy_plan1_1.sub_strides_el_ == std::vector<uint64_t>{1});
  CHECK(copy_plan1_1.tile_strides_el_ == std::vector<uint64_t>{1});
  CHECK(copy_plan1_1.sub_start_el_ == 3);
  CHECK(copy_plan1_1.tile_start_el_ == 0);
  CHECK(copy_plan1_1.first_d_ == 0);

  // Create new subarray
  close_array();
  open_array(array_name, TILEDB_READ);
  int32_t sub2[] = {7, 8};
  tiledb::sm::Subarray subarray2(
      array_->array_.get(),
      Layout::ROW_MAJOR,
      &test::g_helper_stats,
      test::g_helper_logger());
  add_ranges({sub2}, sizeof(sub2), &subarray2);

  // Create DenseTiler
  DenseTiler<int32_t> tiler2(&buffers, &subarray2, &test::g_helper_stats);

  // Test correctness of copy plan for tile 0
  auto copy_plan2 = tiler2.copy_plan(0);
  CHECK(copy_plan2.copy_el_ == 2);
  CHECK(copy_plan2.dim_ranges_ == std::vector<std::array<uint64_t, 2>>{{0, 0}});
  CHECK(copy_plan2.sub_strides_el_ == std::vector<uint64_t>{1});
  CHECK(copy_plan2.tile_strides_el_ == std::vector<uint64_t>{1});
  CHECK(copy_plan2.sub_start_el_ == 0);
  CHECK(copy_plan2.tile_start_el_ == 1);
  CHECK(copy_plan2.first_d_ == 0);

  // Create new subarray (col-major)
  close_array();
  open_array(array_name, TILEDB_READ);
  int32_t sub3[] = {7, 8};
  tiledb::sm::Subarray subarray3(
      array_->array_.get(),
      Layout::COL_MAJOR,
      &test::g_helper_stats,
      test::g_helper_logger());
  add_ranges({sub3}, sizeof(sub3), &subarray3);

  // Create DenseTiler
  DenseTiler<int32_t> tiler3(&buffers, &subarray3, &test::g_helper_stats);

  // Test correctness of copy plan for tile 0
  auto copy_plan3 = tiler3.copy_plan(0);
  CHECK(copy_plan3.copy_el_ == 2);
  CHECK(copy_plan3.dim_ranges_ == std::vector<std::array<uint64_t, 2>>{{0, 0}});
  CHECK(copy_plan3.sub_strides_el_ == std::vector<uint64_t>{1});
  CHECK(copy_plan3.tile_strides_el_ == std::vector<uint64_t>{1});
  CHECK(copy_plan3.sub_start_el_ == 0);
  CHECK(copy_plan3.tile_start_el_ == 1);
  CHECK(copy_plan1_0.first_d_ == 0);

  // Clean up
  close_array();
  remove_array(array_name);
}

TEST_CASE_METHOD(
    DenseTilerFx,
    "DenseTiler: Test get tile, 1D",
    "[DenseTiler][get_tile][1d]") {
  // Create array
  std::string array_name = "dense_tiler";
  int32_t d_dom[] = {1, 10};
  int32_t d_ext = 5;
  create_array(
      array_name,
      {{"d", TILEDB_INT32, d_dom, &d_ext}},
      {{"a", TILEDB_INT32, 1, false}},
      TILEDB_ROW_MAJOR,
      TILEDB_ROW_MAJOR);

  // Create buffers
  std::unordered_map<std::string, QueryBuffer> buffers;
  std::vector<int32_t> buff_a = {1, 2, 3, 4};
  uint64_t buff_a_size = sizeof(buff_a);
  buffers["a"] = QueryBuffer(&buff_a[0], nullptr, &buff_a_size, nullptr);

  // Create subarray
  open_array(array_name, TILEDB_READ);
  int32_t sub1[] = {3, 6};
  tiledb::sm::Subarray subarray1(
      array_->array_.get(),
      Layout::ROW_MAJOR,
      &test::g_helper_stats,
      test::g_helper_logger());
  add_ranges({sub1}, sizeof(sub1), &subarray1);

  // Create DenseTiler
  DenseTiler<int32_t> tiler1(&buffers, &subarray1, &test::g_helper_stats);

  // Test get tile 0
  WriterTile tile1_0(
      array_->array_->array_schema_latest(),
      array_->array_->array_schema_latest().domain().cell_num_per_tile(),
      false,
      false,
      sizeof(int32_t),
      Datatype::INT32);
  CHECK(!tiler1.get_tile(0, "foo", tile1_0).ok());
  CHECK(!tiler1.get_tile(10, "a", tile1_0).ok());
  CHECK(tiler1.get_tile(0, "a", tile1_0).ok());
  std::vector<int32_t> c_data1_0 = {fill_value, fill_value, 1, 2, 3};
  CHECK(check_tile<int32_t>(tile1_0.fixed_tile(), c_data1_0));

  // Test get tile 1
  WriterTile tile1_1(
      array_->array_->array_schema_latest(),
      array_->array_->array_schema_latest().domain().cell_num_per_tile(),
      false,
      false,
      sizeof(int32_t),
      Datatype::INT32);
  CHECK(tiler1.get_tile(1, "a", tile1_1).ok());
  std::vector<int32_t> c_data1_1 = {
      4, fill_value, fill_value, fill_value, fill_value};
  CHECK(check_tile<int32_t>(tile1_1.fixed_tile(), c_data1_1));

  // Create new subarray
  close_array();
  open_array(array_name, TILEDB_READ);
  int32_t sub2[] = {7, 10};
  tiledb::sm::Subarray subarray2(
      array_->array_.get(),
      Layout::ROW_MAJOR,
      &test::g_helper_stats,
      test::g_helper_logger());
  add_ranges({sub2}, sizeof(sub2), &subarray2);

  // Create DenseTiler
  DenseTiler<int32_t> tiler2(&buffers, &subarray2, &test::g_helper_stats);

  // Test get tile 0
  WriterTile tile2(
      array_->array_->array_schema_latest(),
      array_->array_->array_schema_latest().domain().cell_num_per_tile(),
      false,
      false,
      sizeof(int32_t),
      Datatype::INT32);
  CHECK(tiler2.get_tile(0, "a", tile2).ok());
  std::vector<int32_t> c_data2 = {fill_value, 1, 2, 3, 4};
  CHECK(check_tile<int32_t>(tile2.fixed_tile(), c_data2));

  // Create new subarray (col-major)
  close_array();
  open_array(array_name, TILEDB_READ);
  int32_t sub3[] = {7, 10};
  tiledb::sm::Subarray subarray3(
      array_->array_.get(),
      Layout::COL_MAJOR,
      &test::g_helper_stats,
      test::g_helper_logger());
  add_ranges({sub3}, sizeof(sub3), &subarray3);

  // Create DenseTiler
  DenseTiler<int32_t> tiler3(&buffers, &subarray3, &test::g_helper_stats);

  // Test get tile 0
  WriterTile tile3(
      array_->array_->array_schema_latest(),
      array_->array_->array_schema_latest().domain().cell_num_per_tile(),
      false,
      false,
      sizeof(int32_t),
      Datatype::INT32);
  CHECK(tiler3.get_tile(0, "a", tile3).ok());
  std::vector<int32_t> c_data3 = {fill_value, 1, 2, 3, 4};
  CHECK(check_tile<int32_t>(tile3.fixed_tile(), c_data3));

  // Clean up
  close_array();
  remove_array(array_name);
}

TEST_CASE_METHOD(
    DenseTilerFx,
    "DenseTiler: Test get tile, 1D, tile exceeding array domain",
    "[DenseTiler][get_tile][1d][exceeding_domain]") {
  // Create array
  std::string array_name = "dense_tiler";
  int32_t d_dom[] = {1, 8};
  int32_t d_ext = 5;
  create_array(
      array_name,
      {{"d", TILEDB_INT32, d_dom, &d_ext}},
      {{"a", TILEDB_INT32, 1, false}},
      TILEDB_ROW_MAJOR,
      TILEDB_ROW_MAJOR);

  // Create buffers
  std::unordered_map<std::string, QueryBuffer> buffers;
  std::vector<int32_t> buff_a = {1, 2, 3, 4};
  uint64_t buff_a_size = sizeof(buff_a);
  buffers["a"] = QueryBuffer(&buff_a[0], nullptr, &buff_a_size, nullptr);

  // Create subarray
  open_array(array_name, TILEDB_READ);
  int32_t sub1[] = {3, 6};
  tiledb::sm::Subarray subarray1(
      array_->array_.get(),
      Layout::ROW_MAJOR,
      &test::g_helper_stats,
      test::g_helper_logger());
  add_ranges({sub1}, sizeof(sub1), &subarray1);

  // Create DenseTiler
  DenseTiler<int32_t> tiler1(&buffers, &subarray1, &test::g_helper_stats);

  // Test get tile 0
  WriterTile tile1_0(
      array_->array_->array_schema_latest(),
      array_->array_->array_schema_latest().domain().cell_num_per_tile(),
      false,
      false,
      sizeof(int32_t),
      Datatype::INT32);
  CHECK(!tiler1.get_tile(0, "foo", tile1_0).ok());
  CHECK(!tiler1.get_tile(10, "a", tile1_0).ok());
  CHECK(tiler1.get_tile(0, "a", tile1_0).ok());
  std::vector<int32_t> c_data1_0 = {fill_value, fill_value, 1, 2, 3};
  CHECK(check_tile<int32_t>(tile1_0.fixed_tile(), c_data1_0));

  // Test get tile 1
  WriterTile tile1_1(
      array_->array_->array_schema_latest(),
      array_->array_->array_schema_latest().domain().cell_num_per_tile(),
      false,
      false,
      sizeof(int32_t),
      Datatype::INT32);
  CHECK(tiler1.get_tile(1, "a", tile1_1).ok());
  std::vector<int32_t> c_data1_1 = {
      4, fill_value, fill_value, fill_value, fill_value};
  CHECK(check_tile<int32_t>(tile1_1.fixed_tile(), c_data1_1));

  // Clean up
  close_array();
  remove_array(array_name);
}

TEST_CASE_METHOD(
    DenseTilerFx,
    "DenseTiler: Test get tile, 1D, negative domain",
    "[DenseTiler][get_tile][1d][exceeding_domain]") {
  // Create array
  std::string array_name = "dense_tiler";
  int32_t d_dom[] = {-4, 5};
  int32_t d_ext = 5;
  create_array(
      array_name,
      {{"d", TILEDB_INT32, d_dom, &d_ext}},
      {{"a", TILEDB_INT32, 1, false}},
      TILEDB_ROW_MAJOR,
      TILEDB_ROW_MAJOR);

  // Create buffers
  std::unordered_map<std::string, QueryBuffer> buffers;
  std::vector<int32_t> buff_a = {1, 2, 3, 4};
  uint64_t buff_a_size = sizeof(buff_a);
  buffers["a"] = QueryBuffer(&buff_a[0], nullptr, &buff_a_size, nullptr);

  // Create subarray
  open_array(array_name, TILEDB_READ);
  int32_t sub1[] = {-2, 1};
  tiledb::sm::Subarray subarray1(
      array_->array_.get(),
      Layout::ROW_MAJOR,
      &test::g_helper_stats,
      test::g_helper_logger());
  add_ranges({sub1}, sizeof(sub1), &subarray1);

  // Create DenseTiler
  DenseTiler<int32_t> tiler1(&buffers, &subarray1, &test::g_helper_stats);

  // Test get tile 0
  WriterTile tile1_0(
      array_->array_->array_schema_latest(),
      array_->array_->array_schema_latest().domain().cell_num_per_tile(),
      false,
      false,
      sizeof(int32_t),
      Datatype::INT32);
  CHECK(!tiler1.get_tile(0, "foo", tile1_0).ok());
  CHECK(!tiler1.get_tile(10, "a", tile1_0).ok());
  CHECK(tiler1.get_tile(0, "a", tile1_0).ok());
  std::vector<int32_t> c_data1_0 = {fill_value, fill_value, 1, 2, 3};
  CHECK(check_tile<int32_t>(tile1_0.fixed_tile(), c_data1_0));

  // Test get tile 1
  WriterTile tile1_1(
      array_->array_->array_schema_latest(),
      array_->array_->array_schema_latest().domain().cell_num_per_tile(),
      false,
      false,
      sizeof(int32_t),
      Datatype::INT32);
  CHECK(tiler1.get_tile(1, "a", tile1_1).ok());
  std::vector<int32_t> c_data1_1 = {
      4, fill_value, fill_value, fill_value, fill_value};
  CHECK(check_tile<int32_t>(tile1_1.fixed_tile(), c_data1_1));

  // Clean up
  close_array();
  remove_array(array_name);
}

TEST_CASE_METHOD(
    DenseTilerFx,
    "DenseTiler: Test initialization, 2D, (row, row)",
    "[DenseTiler][init][2d][row-row]") {
  // Create array
  std::string array_name = "dense_tiler";
  int32_t d_dom_1[] = {1, 10};
  int32_t d_ext_1 = 5;
  int32_t d_dom_2[] = {1, 30};
  int32_t d_ext_2 = 10;
  create_array(
      array_name,
      {{"d1", TILEDB_INT32, d_dom_1, &d_ext_1},
       {"d2", TILEDB_INT32, d_dom_2, &d_ext_2}},
      {{"a", TILEDB_INT32, 1, false}},
      TILEDB_ROW_MAJOR,
      TILEDB_ROW_MAJOR);

  // Create buffers
  std::unordered_map<std::string, QueryBuffer> buffers;
  std::vector<int32_t> buff_a = {1, 2, 3, 4};
  uint64_t buff_a_size = sizeof(buff_a);
  buffers["a"] = QueryBuffer(&buff_a[0], nullptr, &buff_a_size, nullptr);

  // Create subarray (multiple tiles)
  open_array(array_name, TILEDB_READ);
  int32_t sub1_0[] = {4, 6};
  int32_t sub1_1[] = {18, 22};
  tiledb::sm::Subarray subarray1(
      array_->array_.get(),
      Layout::ROW_MAJOR,
      &test::g_helper_stats,
      test::g_helper_logger());
  add_ranges({sub1_0, sub1_1}, sizeof(sub1_0), &subarray1);

  // Create DenseTiler
  DenseTiler<int32_t> tiler1(&buffers, &subarray1, &test::g_helper_stats);

  // Test correctness of initialization
  CHECK(tiler1.tile_num() == 4);
  CHECK(tiler1.first_sub_tile_coords() == std::vector<uint64_t>{0, 1});
  CHECK(tiler1.sub_strides_el() == std::vector<uint64_t>{5, 1});
  CHECK(tiler1.tile_strides_el() == std::vector<uint64_t>{10, 1});
  CHECK(tiler1.sub_tile_coord_strides() == std::vector<uint64_t>{2, 1});

  // Create subarray (single tile)
  close_array();
  open_array(array_name, TILEDB_READ);
  int32_t sub2_0[] = {7, 9};
  int32_t sub2_1[] = {23, 27};
  tiledb::sm::Subarray subarray2(
      array_->array_.get(),
      Layout::ROW_MAJOR,
      &test::g_helper_stats,
      test::g_helper_logger());
  add_ranges({sub2_0, sub2_1}, sizeof(sub2_0), &subarray2);

  // Create DenseTiler
  DenseTiler<int32_t> tiler2(&buffers, &subarray2, &test::g_helper_stats);

  // Test correctness of initialization
  CHECK(tiler2.tile_num() == 1);
  CHECK(tiler2.first_sub_tile_coords() == std::vector<uint64_t>{1, 2});
  CHECK(tiler2.sub_strides_el() == std::vector<uint64_t>{5, 1});
  CHECK(tiler2.tile_strides_el() == std::vector<uint64_t>{10, 1});
  CHECK(tiler2.sub_tile_coord_strides() == std::vector<uint64_t>{1, 1});

  // Create subarray (multiple tiles, col-major)
  close_array();
  open_array(array_name, TILEDB_READ);
  int32_t sub3_0[] = {4, 6};
  int32_t sub3_1[] = {18, 22};
  tiledb::sm::Subarray subarray3(
      array_->array_.get(),
      Layout::COL_MAJOR,
      &test::g_helper_stats,
      test::g_helper_logger());
  add_ranges({sub3_0, sub3_1}, sizeof(sub3_0), &subarray3);

  // Create DenseTiler
  DenseTiler<int32_t> tiler3(&buffers, &subarray3, &test::g_helper_stats);

  // Test correctness of initialization
  CHECK(tiler3.tile_num() == 4);
  CHECK(tiler3.first_sub_tile_coords() == std::vector<uint64_t>{0, 1});
  CHECK(tiler3.sub_strides_el() == std::vector<uint64_t>{1, 3});
  CHECK(tiler3.tile_strides_el() == std::vector<uint64_t>{10, 1});
  CHECK(tiler3.sub_tile_coord_strides() == std::vector<uint64_t>{2, 1});

  // Create subarray (single tile, col-major)
  close_array();
  open_array(array_name, TILEDB_READ);
  int32_t sub4_0[] = {7, 10};
  int32_t sub4_1[] = {23, 27};
  tiledb::sm::Subarray subarray4(
      array_->array_.get(),
      Layout::COL_MAJOR,
      &test::g_helper_stats,
      test::g_helper_logger());
  add_ranges({sub4_0, sub4_1}, sizeof(sub4_0), &subarray4);

  // Create DenseTiler
  DenseTiler<int32_t> tiler4(&buffers, &subarray4, &test::g_helper_stats);

  // Test correctness of initialization
  CHECK(tiler4.tile_num() == 1);
  CHECK(tiler4.first_sub_tile_coords() == std::vector<uint64_t>{1, 2});
  CHECK(tiler4.sub_strides_el() == std::vector<uint64_t>{1, 4});
  CHECK(tiler4.tile_strides_el() == std::vector<uint64_t>{10, 1});
  CHECK(tiler4.sub_tile_coord_strides() == std::vector<uint64_t>{1, 1});

  // Clean up
  close_array();
  remove_array(array_name);
}

TEST_CASE_METHOD(
    DenseTilerFx,
    "DenseTiler: Test initialization, 2D, (col, col)",
    "[DenseTiler][init][2d][col-col]") {
  // Create array
  std::string array_name = "dense_tiler";
  int32_t d_dom_1[] = {1, 10};
  int32_t d_ext_1 = 5;
  int32_t d_dom_2[] = {1, 30};
  int32_t d_ext_2 = 10;
  create_array(
      array_name,
      {{"d1", TILEDB_INT32, d_dom_1, &d_ext_1},
       {"d2", TILEDB_INT32, d_dom_2, &d_ext_2}},
      {{"a", TILEDB_INT32, 1, false}},
      TILEDB_COL_MAJOR,
      TILEDB_COL_MAJOR);

  // Create buffers
  std::unordered_map<std::string, QueryBuffer> buffers;
  std::vector<int32_t> buff_a = {1, 2, 3, 4};
  uint64_t buff_a_size = sizeof(buff_a);
  buffers["a"] = QueryBuffer(&buff_a[0], nullptr, &buff_a_size, nullptr);

  // Create subarray (multiple tiles)
  open_array(array_name, TILEDB_READ);
  int32_t sub1_0[] = {4, 6};
  int32_t sub1_1[] = {18, 22};
  tiledb::sm::Subarray subarray1(
      array_->array_.get(),
      Layout::ROW_MAJOR,
      &test::g_helper_stats,
      test::g_helper_logger());
  add_ranges({sub1_0, sub1_1}, sizeof(sub1_0), &subarray1);

  // Create DenseTiler
  DenseTiler<int32_t> tiler1(&buffers, &subarray1, &test::g_helper_stats);

  // Test correctness of initialization
  CHECK(tiler1.tile_num() == 4);
  CHECK(tiler1.first_sub_tile_coords() == std::vector<uint64_t>{0, 1});
  CHECK(tiler1.sub_strides_el() == std::vector<uint64_t>{5, 1});
  CHECK(tiler1.tile_strides_el() == std::vector<uint64_t>{1, 5});
  CHECK(tiler1.sub_tile_coord_strides() == std::vector<uint64_t>{1, 2});

  // Create subarray (single tile)
  close_array();
  open_array(array_name, TILEDB_READ);
  int32_t sub2_0[] = {7, 9};
  int32_t sub2_1[] = {23, 27};
  tiledb::sm::Subarray subarray2(
      array_->array_.get(),
      Layout::ROW_MAJOR,
      &test::g_helper_stats,
      test::g_helper_logger());
  add_ranges({sub2_0, sub2_1}, sizeof(sub2_0), &subarray2);

  // Create DenseTiler
  DenseTiler<int32_t> tiler2(&buffers, &subarray2, &test::g_helper_stats);

  // Test correctness of initialization
  CHECK(tiler2.tile_num() == 1);
  CHECK(tiler2.first_sub_tile_coords() == std::vector<uint64_t>{1, 2});
  CHECK(tiler2.sub_strides_el() == std::vector<uint64_t>{5, 1});
  CHECK(tiler2.tile_strides_el() == std::vector<uint64_t>{1, 5});
  CHECK(tiler2.sub_tile_coord_strides() == std::vector<uint64_t>{1, 1});

  // Create subarray (multiple tiles, col-major)
  close_array();
  open_array(array_name, TILEDB_READ);
  int32_t sub3_0[] = {4, 6};
  int32_t sub3_1[] = {18, 22};
  tiledb::sm::Subarray subarray3(
      array_->array_.get(),
      Layout::COL_MAJOR,
      &test::g_helper_stats,
      test::g_helper_logger());
  add_ranges({sub3_0, sub3_1}, sizeof(sub3_0), &subarray3);

  // Create DenseTiler
  DenseTiler<int32_t> tiler3(&buffers, &subarray3, &test::g_helper_stats);

  // Test correctness of initialization
  CHECK(tiler3.tile_num() == 4);
  CHECK(tiler3.first_sub_tile_coords() == std::vector<uint64_t>{0, 1});
  CHECK(tiler3.sub_strides_el() == std::vector<uint64_t>{1, 3});
  CHECK(tiler3.tile_strides_el() == std::vector<uint64_t>{1, 5});
  CHECK(tiler3.sub_tile_coord_strides() == std::vector<uint64_t>{1, 2});

  // Create subarray (single tile, col-major)
  close_array();
  open_array(array_name, TILEDB_READ);
  int32_t sub4_0[] = {7, 10};
  int32_t sub4_1[] = {23, 27};
  tiledb::sm::Subarray subarray4(
      array_->array_.get(),
      Layout::COL_MAJOR,
      &test::g_helper_stats,
      test::g_helper_logger());
  add_ranges({sub4_0, sub4_1}, sizeof(sub4_0), &subarray4);

  // Create DenseTiler
  DenseTiler<int32_t> tiler4(&buffers, &subarray4, &test::g_helper_stats);

  // Test correctness of initialization
  CHECK(tiler4.tile_num() == 1);
  CHECK(tiler4.first_sub_tile_coords() == std::vector<uint64_t>{1, 2});
  CHECK(tiler4.sub_strides_el() == std::vector<uint64_t>{1, 4});
  CHECK(tiler4.tile_strides_el() == std::vector<uint64_t>{1, 5});
  CHECK(tiler4.sub_tile_coord_strides() == std::vector<uint64_t>{1, 1});

  // Clean up
  close_array();
  remove_array(array_name);
}

TEST_CASE_METHOD(
    DenseTilerFx,
    "DenseTiler: Test copy plan, 2D, (row, row)",
    "[DenseTiler][copy_plan][2d][row-row]") {
  // Create array
  std::string array_name = "dense_tiler";
  int32_t d_dom_1[] = {1, 10};
  int32_t d_ext_1 = 5;
  int32_t d_dom_2[] = {1, 30};
  int32_t d_ext_2 = 10;
  create_array(
      array_name,
      {{"d1", TILEDB_INT32, d_dom_1, &d_ext_1},
       {"d2", TILEDB_INT32, d_dom_2, &d_ext_2}},
      {{"a", TILEDB_INT32, 1, false}},
      TILEDB_ROW_MAJOR,
      TILEDB_ROW_MAJOR);

  // Create buffers
  std::unordered_map<std::string, QueryBuffer> buffers;
  std::vector<int32_t> buff_a = {1, 2, 3, 4};
  uint64_t buff_a_size = sizeof(buff_a);
  buffers["a"] = QueryBuffer(&buff_a[0], nullptr, &buff_a_size, nullptr);

  // Create subarray (multiple tiles)
  open_array(array_name, TILEDB_READ);
  int32_t sub1_0[] = {4, 6};
  int32_t sub1_1[] = {18, 22};
  tiledb::sm::Subarray subarray1(
      array_->array_.get(),
      Layout::ROW_MAJOR,
      &test::g_helper_stats,
      test::g_helper_logger());
  add_ranges({sub1_0, sub1_1}, sizeof(sub1_0), &subarray1);

  // Create DenseTiler
  DenseTiler<int32_t> tiler1(&buffers, &subarray1, &test::g_helper_stats);

  // Test correctness of copy plan for tile 0
  auto copy_plan1_0 = tiler1.copy_plan(0);
  CHECK(copy_plan1_0.copy_el_ == 3);
  CHECK(
      copy_plan1_0.dim_ranges_ == std::vector<std::array<uint64_t, 2>>{{0, 1}});
  CHECK(copy_plan1_0.sub_strides_el_ == std::vector<uint64_t>{5, 1});
  CHECK(copy_plan1_0.tile_strides_el_ == std::vector<uint64_t>{10, 1});
  CHECK(copy_plan1_0.sub_start_el_ == 0);
  CHECK(copy_plan1_0.tile_start_el_ == 37);
  CHECK(copy_plan1_0.first_d_ == 0);

  // Test correctness of copy plan for tile 1
  auto copy_plan1_1 = tiler1.copy_plan(1);
  CHECK(copy_plan1_1.copy_el_ == 2);
  CHECK(
      copy_plan1_1.dim_ranges_ == std::vector<std::array<uint64_t, 2>>{{0, 1}});
  CHECK(copy_plan1_1.sub_strides_el_ == std::vector<uint64_t>{5, 1});
  CHECK(copy_plan1_1.tile_strides_el_ == std::vector<uint64_t>{10, 1});
  CHECK(copy_plan1_1.sub_start_el_ == 3);
  CHECK(copy_plan1_1.tile_start_el_ == 30);
  CHECK(copy_plan1_1.first_d_ == 0);

  // Test correctness of copy plan for tile 2
  auto copy_plan1_2 = tiler1.copy_plan(2);
  CHECK(copy_plan1_2.copy_el_ == 3);
  CHECK(
      copy_plan1_2.dim_ranges_ == std::vector<std::array<uint64_t, 2>>{{0, 0}});
  CHECK(copy_plan1_2.sub_strides_el_ == std::vector<uint64_t>{5, 1});
  CHECK(copy_plan1_2.tile_strides_el_ == std::vector<uint64_t>{10, 1});
  CHECK(copy_plan1_2.sub_start_el_ == 10);
  CHECK(copy_plan1_2.tile_start_el_ == 7);
  CHECK(copy_plan1_2.first_d_ == 0);

  // Test correctness of copy plan for tile 3
  auto copy_plan1_3 = tiler1.copy_plan(3);
  CHECK(copy_plan1_3.copy_el_ == 2);
  CHECK(
      copy_plan1_3.dim_ranges_ == std::vector<std::array<uint64_t, 2>>{{0, 0}});
  CHECK(copy_plan1_3.sub_strides_el_ == std::vector<uint64_t>{5, 1});
  CHECK(copy_plan1_3.tile_strides_el_ == std::vector<uint64_t>{10, 1});
  CHECK(copy_plan1_3.sub_start_el_ == 13);
  CHECK(copy_plan1_3.tile_start_el_ == 0);
  CHECK(copy_plan1_3.first_d_ == 0);

  // Create subarray (single tile)
  close_array();
  open_array(array_name, TILEDB_READ);
  int32_t sub2_0[] = {3, 5};
  int32_t sub2_1[] = {13, 18};
  tiledb::sm::Subarray subarray2(
      array_->array_.get(),
      Layout::ROW_MAJOR,
      &test::g_helper_stats,
      test::g_helper_logger());
  add_ranges({sub2_0, sub2_1}, sizeof(sub2_0), &subarray2);

  // Create DenseTiler
  DenseTiler<int32_t> tiler2(&buffers, &subarray2, &test::g_helper_stats);

  // Test correctness of copy plan for tile 0
  auto copy_plan2_0 = tiler2.copy_plan(0);
  CHECK(copy_plan2_0.copy_el_ == 6);
  CHECK(
      copy_plan2_0.dim_ranges_ == std::vector<std::array<uint64_t, 2>>{{0, 2}});
  CHECK(copy_plan2_0.sub_strides_el_ == std::vector<uint64_t>{6, 1});
  CHECK(copy_plan2_0.tile_strides_el_ == std::vector<uint64_t>{10, 1});
  CHECK(copy_plan2_0.sub_start_el_ == 0);
  CHECK(copy_plan2_0.tile_start_el_ == 22);
  CHECK(copy_plan2_0.first_d_ == 0);

  // Create subarray (multiple tiles, col-major)
  close_array();
  open_array(array_name, TILEDB_READ);
  int32_t sub3_0[] = {4, 6};
  int32_t sub3_1[] = {18, 22};
  tiledb::sm::Subarray subarray3(
      array_->array_.get(),
      Layout::COL_MAJOR,
      &test::g_helper_stats,
      test::g_helper_logger());
  add_ranges({sub3_0, sub3_1}, sizeof(sub3_0), &subarray3);

  // Create DenseTiler
  DenseTiler<int32_t> tiler3(&buffers, &subarray3, &test::g_helper_stats);

  // Test correctness of copy plan for tile 0
  auto copy_plan3_0 = tiler3.copy_plan(0);
  CHECK(copy_plan3_0.copy_el_ == 1);
  CHECK(
      copy_plan3_0.dim_ranges_ ==
      std::vector<std::array<uint64_t, 2>>{{0, 1}, {0, 2}});
  CHECK(copy_plan3_0.sub_strides_el_ == std::vector<uint64_t>{1, 3});
  CHECK(copy_plan3_0.tile_strides_el_ == std::vector<uint64_t>{10, 1});
  CHECK(copy_plan3_0.sub_start_el_ == 0);
  CHECK(copy_plan3_0.tile_start_el_ == 37);
  CHECK(copy_plan3_0.first_d_ == 0);

  // Test correctness of copy plan for tile 1
  auto copy_plan3_1 = tiler3.copy_plan(1);
  CHECK(copy_plan3_1.copy_el_ == 1);
  CHECK(
      copy_plan3_1.dim_ranges_ ==
      std::vector<std::array<uint64_t, 2>>{{0, 1}, {0, 1}});
  CHECK(copy_plan3_1.sub_strides_el_ == std::vector<uint64_t>{1, 3});
  CHECK(copy_plan3_1.tile_strides_el_ == std::vector<uint64_t>{10, 1});
  CHECK(copy_plan3_1.sub_start_el_ == 9);
  CHECK(copy_plan3_1.tile_start_el_ == 30);
  CHECK(copy_plan3_1.first_d_ == 0);

  // Test correctness of copy plan for tile 2
  auto copy_plan3_2 = tiler3.copy_plan(2);
  CHECK(copy_plan3_2.copy_el_ == 1);
  CHECK(
      copy_plan3_2.dim_ranges_ ==
      std::vector<std::array<uint64_t, 2>>{{0, 0}, {0, 2}});
  CHECK(copy_plan3_2.sub_strides_el_ == std::vector<uint64_t>{1, 3});
  CHECK(copy_plan3_2.tile_strides_el_ == std::vector<uint64_t>{10, 1});
  CHECK(copy_plan3_2.sub_start_el_ == 2);
  CHECK(copy_plan3_2.tile_start_el_ == 7);
  CHECK(copy_plan3_2.first_d_ == 0);

  // Test correctness of copy plan for tile 3
  auto copy_plan3_3 = tiler3.copy_plan(3);
  CHECK(copy_plan3_3.copy_el_ == 1);
  CHECK(
      copy_plan3_3.dim_ranges_ ==
      std::vector<std::array<uint64_t, 2>>{{0, 0}, {0, 1}});
  CHECK(copy_plan3_3.sub_strides_el_ == std::vector<uint64_t>{1, 3});
  CHECK(copy_plan3_3.tile_strides_el_ == std::vector<uint64_t>{10, 1});
  CHECK(copy_plan3_3.sub_start_el_ == 11);
  CHECK(copy_plan3_3.tile_start_el_ == 0);
  CHECK(copy_plan3_3.first_d_ == 0);

  // Create subarray (single tile, col-major)
  close_array();
  open_array(array_name, TILEDB_READ);
  int32_t sub4_0[] = {3, 5};
  int32_t sub4_1[] = {13, 18};
  tiledb::sm::Subarray subarray4(
      array_->array_.get(),
      Layout::COL_MAJOR,
      &test::g_helper_stats,
      test::g_helper_logger());
  add_ranges({sub4_0, sub4_1}, sizeof(sub4_0), &subarray4);

  // Create DenseTiler
  DenseTiler<int32_t> tiler4(&buffers, &subarray4, &test::g_helper_stats);

  // Test correctness of copy plan for tile 0
  auto copy_plan4_0 = tiler4.copy_plan(0);
  CHECK(copy_plan4_0.copy_el_ == 1);
  CHECK(
      copy_plan4_0.dim_ranges_ ==
      std::vector<std::array<uint64_t, 2>>{{0, 2}, {0, 5}});
  CHECK(copy_plan4_0.sub_strides_el_ == std::vector<uint64_t>{1, 3});
  CHECK(copy_plan4_0.tile_strides_el_ == std::vector<uint64_t>{10, 1});
  CHECK(copy_plan4_0.sub_start_el_ == 0);
  CHECK(copy_plan4_0.tile_start_el_ == 22);
  CHECK(copy_plan4_0.first_d_ == 0);

  // Clean up
  close_array();
  remove_array(array_name);
}

TEST_CASE_METHOD(
    DenseTilerFx,
    "DenseTiler: Test copy plan, 2D, (col, col)",
    "[DenseTiler][copy_plan][2d][col-col]") {
  // Create array
  std::string array_name = "dense_tiler";
  int32_t d_dom_1[] = {1, 10};
  int32_t d_ext_1 = 5;
  int32_t d_dom_2[] = {1, 30};
  int32_t d_ext_2 = 10;
  create_array(
      array_name,
      {{"d1", TILEDB_INT32, d_dom_1, &d_ext_1},
       {"d2", TILEDB_INT32, d_dom_2, &d_ext_2}},
      {{"a", TILEDB_INT32, 1, false}},
      TILEDB_COL_MAJOR,
      TILEDB_COL_MAJOR);

  // Create buffers
  std::unordered_map<std::string, QueryBuffer> buffers;
  std::vector<int32_t> buff_a = {1, 2, 3, 4};
  uint64_t buff_a_size = sizeof(buff_a);
  buffers["a"] = QueryBuffer(&buff_a[0], nullptr, &buff_a_size, nullptr);

  // Create subarray (multiple tiles)
  open_array(array_name, TILEDB_READ);
  int32_t sub1_0[] = {4, 6};
  int32_t sub1_1[] = {18, 22};
  tiledb::sm::Subarray subarray1(
      array_->array_.get(),
      Layout::ROW_MAJOR,
      &test::g_helper_stats,
      test::g_helper_logger());
  add_ranges({sub1_0, sub1_1}, sizeof(sub1_0), &subarray1);

  // Create DenseTiler
  DenseTiler<int32_t> tiler1(&buffers, &subarray1, &test::g_helper_stats);

  // Test correctness of copy plan for tile 0
  auto copy_plan1_0 = tiler1.copy_plan(0);
  CHECK(copy_plan1_0.copy_el_ == 1);
  CHECK(
      copy_plan1_0.dim_ranges_ ==
      std::vector<std::array<uint64_t, 2>>{{0, 1}, {0, 2}});
  CHECK(copy_plan1_0.sub_strides_el_ == std::vector<uint64_t>{5, 1});
  CHECK(copy_plan1_0.tile_strides_el_ == std::vector<uint64_t>{1, 5});
  CHECK(copy_plan1_0.sub_start_el_ == 0);
  CHECK(copy_plan1_0.tile_start_el_ == 38);
  CHECK(copy_plan1_0.first_d_ == 0);

  // Test correctness of copy plan for tile 1
  auto copy_plan1_1 = tiler1.copy_plan(1);
  CHECK(copy_plan1_1.copy_el_ == 1);
  CHECK(
      copy_plan1_1.dim_ranges_ ==
      std::vector<std::array<uint64_t, 2>>{{0, 0}, {0, 2}});
  CHECK(copy_plan1_1.sub_strides_el_ == std::vector<uint64_t>{5, 1});
  CHECK(copy_plan1_1.tile_strides_el_ == std::vector<uint64_t>{1, 5});
  CHECK(copy_plan1_1.sub_start_el_ == 10);
  CHECK(copy_plan1_1.tile_start_el_ == 35);
  CHECK(copy_plan1_1.first_d_ == 0);

  // Test correctness of copy plan for tile 2
  auto copy_plan1_2 = tiler1.copy_plan(2);
  CHECK(copy_plan1_2.copy_el_ == 1);
  CHECK(
      copy_plan1_2.dim_ranges_ ==
      std::vector<std::array<uint64_t, 2>>{{0, 1}, {0, 1}});
  CHECK(copy_plan1_2.sub_strides_el_ == std::vector<uint64_t>{5, 1});
  CHECK(copy_plan1_2.tile_strides_el_ == std::vector<uint64_t>{1, 5});
  CHECK(copy_plan1_2.sub_start_el_ == 3);
  CHECK(copy_plan1_2.tile_start_el_ == 3);
  CHECK(copy_plan1_2.first_d_ == 0);

  // Test correctness of copy plan for tile 3
  auto copy_plan1_3 = tiler1.copy_plan(3);
  CHECK(copy_plan1_3.copy_el_ == 1);
  CHECK(
      copy_plan1_3.dim_ranges_ ==
      std::vector<std::array<uint64_t, 2>>{{0, 0}, {0, 1}});
  CHECK(copy_plan1_3.sub_strides_el_ == std::vector<uint64_t>{5, 1});
  CHECK(copy_plan1_3.tile_strides_el_ == std::vector<uint64_t>{1, 5});
  CHECK(copy_plan1_3.sub_start_el_ == 13);
  CHECK(copy_plan1_3.tile_start_el_ == 0);
  CHECK(copy_plan1_3.first_d_ == 0);

  // Create subarray (single tile)
  close_array();
  open_array(array_name, TILEDB_READ);
  int32_t sub2_0[] = {3, 5};
  int32_t sub2_1[] = {13, 18};
  tiledb::sm::Subarray subarray2(
      array_->array_.get(),
      Layout::ROW_MAJOR,
      &test::g_helper_stats,
      test::g_helper_logger());
  add_ranges({sub2_0, sub2_1}, sizeof(sub2_0), &subarray2);

  // Create DenseTiler
  DenseTiler<int32_t> tiler2(&buffers, &subarray2, &test::g_helper_stats);

  // Test correctness of copy plan for tile 0
  auto copy_plan2_0 = tiler2.copy_plan(0);
  CHECK(copy_plan2_0.copy_el_ == 1);
  CHECK(
      copy_plan2_0.dim_ranges_ ==
      std::vector<std::array<uint64_t, 2>>{{0, 2}, {0, 5}});
  CHECK(copy_plan2_0.sub_strides_el_ == std::vector<uint64_t>{6, 1});
  CHECK(copy_plan2_0.tile_strides_el_ == std::vector<uint64_t>{1, 5});
  CHECK(copy_plan2_0.sub_start_el_ == 0);
  CHECK(copy_plan2_0.tile_start_el_ == 12);
  CHECK(copy_plan2_0.first_d_ == 0);

  // Create subarray (multiple tiles, col-major)
  close_array();
  open_array(array_name, TILEDB_READ);
  int32_t sub3_0[] = {4, 6};
  int32_t sub3_1[] = {18, 22};
  tiledb::sm::Subarray subarray3(
      array_->array_.get(),
      Layout::COL_MAJOR,
      &test::g_helper_stats,
      test::g_helper_logger());
  add_ranges({sub3_0, sub3_1}, sizeof(sub3_0), &subarray3);

  // Create DenseTiler
  DenseTiler<int32_t> tiler3(&buffers, &subarray3, &test::g_helper_stats);

  // Test correctness of copy plan for tile 0
  auto copy_plan3_0 = tiler3.copy_plan(0);
  CHECK(copy_plan3_0.copy_el_ == 2);
  CHECK(
      copy_plan3_0.dim_ranges_ == std::vector<std::array<uint64_t, 2>>{{0, 2}});
  CHECK(copy_plan3_0.sub_strides_el_ == std::vector<uint64_t>{1, 3});
  CHECK(copy_plan3_0.tile_strides_el_ == std::vector<uint64_t>{1, 5});
  CHECK(copy_plan3_0.sub_start_el_ == 0);
  CHECK(copy_plan3_0.tile_start_el_ == 38);
  CHECK(copy_plan3_0.first_d_ == 1);

  // Test correctness of copy plan for tile 1
  auto copy_plan3_1 = tiler3.copy_plan(1);
  CHECK(copy_plan3_1.copy_el_ == 1);
  CHECK(
      copy_plan3_1.dim_ranges_ == std::vector<std::array<uint64_t, 2>>{{0, 2}});
  CHECK(copy_plan3_1.sub_strides_el_ == std::vector<uint64_t>{1, 3});
  CHECK(copy_plan3_1.tile_strides_el_ == std::vector<uint64_t>{1, 5});
  CHECK(copy_plan3_1.sub_start_el_ == 2);
  CHECK(copy_plan3_1.tile_start_el_ == 35);
  CHECK(copy_plan3_1.first_d_ == 1);

  // Test correctness of copy plan for tile 2
  auto copy_plan3_2 = tiler3.copy_plan(2);
  CHECK(copy_plan3_2.copy_el_ == 2);
  CHECK(
      copy_plan3_2.dim_ranges_ == std::vector<std::array<uint64_t, 2>>{{0, 1}});
  CHECK(copy_plan3_2.sub_strides_el_ == std::vector<uint64_t>{1, 3});
  CHECK(copy_plan3_2.tile_strides_el_ == std::vector<uint64_t>{1, 5});
  CHECK(copy_plan3_2.sub_start_el_ == 9);
  CHECK(copy_plan3_2.tile_start_el_ == 3);
  CHECK(copy_plan3_2.first_d_ == 1);

  // Test correctness of copy plan for tile 3
  auto copy_plan3_3 = tiler3.copy_plan(3);
  CHECK(copy_plan3_3.copy_el_ == 1);
  CHECK(
      copy_plan3_3.dim_ranges_ == std::vector<std::array<uint64_t, 2>>{{0, 1}});
  CHECK(copy_plan3_3.sub_strides_el_ == std::vector<uint64_t>{1, 3});
  CHECK(copy_plan3_3.tile_strides_el_ == std::vector<uint64_t>{1, 5});
  CHECK(copy_plan3_3.sub_start_el_ == 11);
  CHECK(copy_plan3_3.tile_start_el_ == 0);
  CHECK(copy_plan3_3.first_d_ == 1);

  // Create subarray (single tile, col-major)
  close_array();
  open_array(array_name, TILEDB_READ);
  int32_t sub4_0[] = {3, 5};
  int32_t sub4_1[] = {13, 18};
  tiledb::sm::Subarray subarray4(
      array_->array_.get(),
      Layout::COL_MAJOR,
      &test::g_helper_stats,
      test::g_helper_logger());
  add_ranges({sub4_0, sub4_1}, sizeof(sub4_0), &subarray4);

  // Create DenseTiler
  DenseTiler<int32_t> tiler4(&buffers, &subarray4, &test::g_helper_stats);

  // Test correctness of copy plan for tile 0
  auto copy_plan4_0 = tiler4.copy_plan(0);
  CHECK(copy_plan4_0.copy_el_ == 3);
  CHECK(
      copy_plan4_0.dim_ranges_ == std::vector<std::array<uint64_t, 2>>{{0, 5}});
  CHECK(copy_plan4_0.sub_strides_el_ == std::vector<uint64_t>{1, 3});
  CHECK(copy_plan4_0.tile_strides_el_ == std::vector<uint64_t>{1, 5});
  CHECK(copy_plan4_0.sub_start_el_ == 0);
  CHECK(copy_plan4_0.tile_start_el_ == 12);
  CHECK(copy_plan4_0.first_d_ == 1);

  // Clean up
  close_array();
  remove_array(array_name);
}

TEST_CASE_METHOD(
    DenseTilerFx,
    "DenseTiler: Test copy plan optimization, 2D, (row, row)",
    "[DenseTiler][copy_plan][optimization][2d][row-row]") {
  // Create array
  std::string array_name = "dense_tiler";
  int32_t d_dom_1[] = {1, 10};
  int32_t d_ext_1 = 5;
  int32_t d_dom_2[] = {1, 30};
  int32_t d_ext_2 = 10;
  create_array(
      array_name,
      {{"d1", TILEDB_INT32, d_dom_1, &d_ext_1},
       {"d2", TILEDB_INT32, d_dom_2, &d_ext_2}},
      {{"a", TILEDB_INT32, 1, false}},
      TILEDB_ROW_MAJOR,
      TILEDB_ROW_MAJOR);

  // Create buffers
  std::unordered_map<std::string, QueryBuffer> buffers;
  std::vector<int32_t> buff_a = {1, 2, 3, 4};
  uint64_t buff_a_size = sizeof(buff_a);
  buffers["a"] = QueryBuffer(&buff_a[0], nullptr, &buff_a_size, nullptr);

  // Create subarray (multiple tiles)
  open_array(array_name, TILEDB_READ);
  int32_t sub1_0[] = {4, 9};
  int32_t sub1_1[] = {11, 20};
  tiledb::sm::Subarray subarray1(
      array_->array_.get(),
      Layout::ROW_MAJOR,
      &test::g_helper_stats,
      test::g_helper_logger());
  add_ranges({sub1_0, sub1_1}, sizeof(sub1_0), &subarray1);

  // Create DenseTiler
  DenseTiler<int32_t> tiler1(&buffers, &subarray1, &test::g_helper_stats);

  // Test correctness of copy plan for tile 0
  auto copy_plan1_0 = tiler1.copy_plan(0);
  CHECK(copy_plan1_0.copy_el_ == 20);
  CHECK(
      copy_plan1_0.dim_ranges_ == std::vector<std::array<uint64_t, 2>>{{0, 0}});
  CHECK(copy_plan1_0.sub_strides_el_ == std::vector<uint64_t>{10, 1});
  CHECK(copy_plan1_0.tile_strides_el_ == std::vector<uint64_t>{10, 1});
  CHECK(copy_plan1_0.sub_start_el_ == 0);
  CHECK(copy_plan1_0.tile_start_el_ == 30);
  CHECK(copy_plan1_0.first_d_ == 0);

  // Test correctness of copy plan for tile 1
  auto copy_plan1_1 = tiler1.copy_plan(1);
  CHECK(copy_plan1_1.copy_el_ == 40);
  CHECK(
      copy_plan1_1.dim_ranges_ == std::vector<std::array<uint64_t, 2>>{{0, 0}});
  CHECK(copy_plan1_1.sub_strides_el_ == std::vector<uint64_t>{10, 1});
  CHECK(copy_plan1_1.tile_strides_el_ == std::vector<uint64_t>{10, 1});
  CHECK(copy_plan1_1.sub_start_el_ == 20);
  CHECK(copy_plan1_1.tile_start_el_ == 0);
  CHECK(copy_plan1_1.first_d_ == 0);

  // Clean up
  close_array();
  remove_array(array_name);
}

TEST_CASE_METHOD(
    DenseTilerFx,
    "DenseTiler: Test copy plan optimization, 2D, (col, col)",
    "[DenseTiler][copy_plan][optimization][2d][col-col]") {
  // Create array
  std::string array_name = "dense_tiler";
  int32_t d_dom_1[] = {1, 10};
  int32_t d_ext_1 = 5;
  int32_t d_dom_2[] = {1, 30};
  int32_t d_ext_2 = 10;
  create_array(
      array_name,
      {{"d1", TILEDB_INT32, d_dom_1, &d_ext_1},
       {"d2", TILEDB_INT32, d_dom_2, &d_ext_2}},
      {{"a", TILEDB_INT32, 1, false}},
      TILEDB_COL_MAJOR,
      TILEDB_COL_MAJOR);

  // Create buffers
  std::unordered_map<std::string, QueryBuffer> buffers;
  std::vector<int32_t> buff_a = {1, 2, 3, 4};
  uint64_t buff_a_size = sizeof(buff_a);
  buffers["a"] = QueryBuffer(&buff_a[0], nullptr, &buff_a_size, nullptr);

  // Create subarray (multiple tiles)
  open_array(array_name, TILEDB_READ);
  int32_t sub1_0[] = {1, 5};
  int32_t sub1_1[] = {8, 12};
  tiledb::sm::Subarray subarray1(
      array_->array_.get(),
      Layout::COL_MAJOR,
      &test::g_helper_stats,
      test::g_helper_logger());
  add_ranges({sub1_0, sub1_1}, sizeof(sub1_0), &subarray1);

  // Create DenseTiler
  DenseTiler<int32_t> tiler1(&buffers, &subarray1, &test::g_helper_stats);

  // Test correctness of copy plan for tile 0
  auto copy_plan1_0 = tiler1.copy_plan(0);
  CHECK(copy_plan1_0.copy_el_ == 15);
  CHECK(
      copy_plan1_0.dim_ranges_ == std::vector<std::array<uint64_t, 2>>{{0, 0}});
  CHECK(copy_plan1_0.sub_strides_el_ == std::vector<uint64_t>{1, 5});
  CHECK(copy_plan1_0.tile_strides_el_ == std::vector<uint64_t>{1, 5});
  CHECK(copy_plan1_0.sub_start_el_ == 0);
  CHECK(copy_plan1_0.tile_start_el_ == 35);
  CHECK(copy_plan1_0.first_d_ == 1);

  // Test correctness of copy plan for tile 0
  auto copy_plan1_1 = tiler1.copy_plan(1);
  CHECK(copy_plan1_1.copy_el_ == 10);
  CHECK(
      copy_plan1_1.dim_ranges_ == std::vector<std::array<uint64_t, 2>>{{0, 0}});
  CHECK(copy_plan1_1.sub_strides_el_ == std::vector<uint64_t>{1, 5});
  CHECK(copy_plan1_1.tile_strides_el_ == std::vector<uint64_t>{1, 5});
  CHECK(copy_plan1_1.sub_start_el_ == 15);
  CHECK(copy_plan1_1.tile_start_el_ == 0);
  CHECK(copy_plan1_1.first_d_ == 1);

  // Clean up
  close_array();
  remove_array(array_name);
}

TEST_CASE_METHOD(
    DenseTilerFx,
    "DenseTiler: Test get tile, 2D, (row, row)",
    "[DenseTiler][get_tile][2d][row-row]") {
  // Create array
  std::string array_name = "dense_tiler";
  int32_t d_dom_1[] = {1, 10};
  int32_t d_ext_1 = 5;
  int32_t d_dom_2[] = {1, 30};
  int32_t d_ext_2 = 10;
  create_array(
      array_name,
      {{"d1", TILEDB_INT32, d_dom_1, &d_ext_1},
       {"d2", TILEDB_INT32, d_dom_2, &d_ext_2}},
      {{"a", TILEDB_INT32, 1, false}},
      TILEDB_ROW_MAJOR,
      TILEDB_ROW_MAJOR);

  // Create buffers
  std::unordered_map<std::string, QueryBuffer> buffers;
  std::vector<int32_t> buff_a = {
      1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15};
  uint64_t buff_a_size = sizeof(buff_a);
  buffers["a"] = QueryBuffer(&buff_a[0], nullptr, &buff_a_size, nullptr);

  // Create subarray (multiple tiles)
  open_array(array_name, TILEDB_READ);
  int32_t sub1_0[] = {4, 6};
  int32_t sub1_1[] = {18, 22};
  tiledb::sm::Subarray subarray1(
      array_->array_.get(),
      Layout::ROW_MAJOR,
      &test::g_helper_stats,
      test::g_helper_logger());
  add_ranges({sub1_0, sub1_1}, sizeof(sub1_0), &subarray1);

  // Create DenseTiler
  DenseTiler<int32_t> tiler1(&buffers, &subarray1, &test::g_helper_stats);

  // Test get tile 0
  WriterTile tile1_0(
      array_->array_->array_schema_latest(),
      array_->array_->array_schema_latest().domain().cell_num_per_tile(),
      false,
      false,
      sizeof(int32_t),
      Datatype::INT32);
  CHECK(tiler1.get_tile(0, "a", tile1_0).ok());
  std::vector<int32_t> c_data1_0(50);
  for (int i = 0; i <= 36; ++i)
    c_data1_0[i] = fill_value;
  for (int i = 37; i <= 39; ++i)
    c_data1_0[i] = i - 36;
  for (int i = 40; i <= 46; ++i)
    c_data1_0[i] = fill_value;
  for (int i = 47; i <= 49; ++i)
    c_data1_0[i] = i - 41;
  CHECK(check_tile<int32_t>(tile1_0.fixed_tile(), c_data1_0));

  // Test get tile 1
  WriterTile tile1_1(
      array_->array_->array_schema_latest(),
      array_->array_->array_schema_latest().domain().cell_num_per_tile(),
      false,
      false,
      sizeof(int32_t),
      Datatype::INT32);
  CHECK(tiler1.get_tile(1, "a", tile1_1).ok());
  std::vector<int32_t> c_data1_1(50);
  for (int i = 0; i <= 29; ++i)
    c_data1_1[i] = fill_value;
  for (int i = 30; i <= 31; ++i)
    c_data1_1[i] = i - 26;
  for (int i = 32; i <= 39; ++i)
    c_data1_1[i] = fill_value;
  for (int i = 40; i <= 41; ++i)
    c_data1_1[i] = i - 31;
  for (int i = 42; i <= 49; ++i)
    c_data1_1[i] = fill_value;
  CHECK(check_tile<int32_t>(tile1_1.fixed_tile(), c_data1_1));

  // Test get tile 2
  WriterTile tile1_2(
      array_->array_->array_schema_latest(),
      array_->array_->array_schema_latest().domain().cell_num_per_tile(),
      false,
      false,
      sizeof(int32_t),
      Datatype::INT32);
  CHECK(tiler1.get_tile(2, "a", tile1_2).ok());
  std::vector<int32_t> c_data1_2(50);
  for (int i = 0; i <= 6; ++i)
    c_data1_2[i] = fill_value;
  for (int i = 7; i <= 9; ++i)
    c_data1_2[i] = i + 4;
  for (int i = 10; i <= 49; ++i)
    c_data1_2[i] = fill_value;
  CHECK(check_tile<int32_t>(tile1_2.fixed_tile(), c_data1_2));

  // Test get tile 3
  WriterTile tile1_3(
      array_->array_->array_schema_latest(),
      array_->array_->array_schema_latest().domain().cell_num_per_tile(),
      false,
      false,
      sizeof(int32_t),
      Datatype::INT32);
  CHECK(tiler1.get_tile(3, "a", tile1_3).ok());
  std::vector<int32_t> c_data1_3(50);
  for (int i = 0; i <= 1; ++i)
    c_data1_3[i] = i + 14;
  for (int i = 2; i <= 49; ++i)
    c_data1_3[i] = fill_value;
  CHECK(check_tile<int32_t>(tile1_3.fixed_tile(), c_data1_3));

  // Create subarray (single tile)
  close_array();
  open_array(array_name, TILEDB_READ);
  int32_t sub2_0[] = {3, 5};
  int32_t sub2_1[] = {13, 18};
  tiledb::sm::Subarray subarray2(
      array_->array_.get(),
      Layout::ROW_MAJOR,
      &test::g_helper_stats,
      test::g_helper_logger());
  add_ranges({sub2_0, sub2_1}, sizeof(sub2_0), &subarray2);

  // Create DenseTiler
  buff_a = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18};
  buff_a_size = sizeof(buff_a);
  buffers["a"] = QueryBuffer(&buff_a[0], nullptr, &buff_a_size, nullptr);
  DenseTiler<int32_t> tiler2(&buffers, &subarray2, &test::g_helper_stats);

  // Test get tile 0
  WriterTile tile2_0(
      array_->array_->array_schema_latest(),
      array_->array_->array_schema_latest().domain().cell_num_per_tile(),
      false,
      false,
      sizeof(int32_t),
      Datatype::INT32);
  CHECK(tiler2.get_tile(0, "a", tile2_0).ok());
  std::vector<int32_t> c_data2_0(50);
  for (int i = 0; i <= 21; ++i)
    c_data2_0[i] = fill_value;
  for (int i = 22; i <= 27; ++i)
    c_data2_0[i] = i - 21;
  for (int i = 28; i <= 31; ++i)
    c_data2_0[i] = fill_value;
  for (int i = 32; i <= 37; ++i)
    c_data2_0[i] = i - 25;
  for (int i = 38; i <= 41; ++i)
    c_data2_0[i] = fill_value;
  for (int i = 42; i <= 47; ++i)
    c_data2_0[i] = i - 29;
  for (int i = 48; i <= 49; ++i)
    c_data2_0[i] = fill_value;
  CHECK(check_tile<int32_t>(tile2_0.fixed_tile(), c_data2_0));

  // Create subarray (multiple tiles, col-major)
  close_array();
  open_array(array_name, TILEDB_READ);
  int32_t sub3_0[] = {4, 6};
  int32_t sub3_1[] = {18, 22};
  tiledb::sm::Subarray subarray3(
      array_->array_.get(),
      Layout::COL_MAJOR,
      &test::g_helper_stats,
      test::g_helper_logger());
  add_ranges({sub3_0, sub3_1}, sizeof(sub3_0), &subarray3);

  // Create DenseTiler
  buff_a = {1, 6, 11, 2, 7, 12, 3, 8, 13, 4, 9, 14, 5, 10, 15};
  buff_a_size = sizeof(buff_a);
  buffers["a"] = QueryBuffer(&buff_a[0], nullptr, &buff_a_size, nullptr);
  DenseTiler<int32_t> tiler3(&buffers, &subarray3, &test::g_helper_stats);

  // Test get tile 0
  WriterTile tile3_0(
      array_->array_->array_schema_latest(),
      array_->array_->array_schema_latest().domain().cell_num_per_tile(),
      false,
      false,
      sizeof(int32_t),
      Datatype::INT32);
  CHECK(tiler3.get_tile(0, "a", tile3_0).ok());
  std::vector<int32_t> c_data3_0(50);
  for (int i = 0; i <= 36; ++i)
    c_data3_0[i] = fill_value;
  for (int i = 37; i <= 39; ++i)
    c_data3_0[i] = i - 36;
  for (int i = 40; i <= 46; ++i)
    c_data3_0[i] = fill_value;
  for (int i = 47; i <= 49; ++i)
    c_data3_0[i] = i - 41;
  CHECK(check_tile<int32_t>(tile3_0.fixed_tile(), c_data3_0));

  // Test get tile 1
  WriterTile tile3_1(
      array_->array_->array_schema_latest(),
      array_->array_->array_schema_latest().domain().cell_num_per_tile(),
      false,
      false,
      sizeof(int32_t),
      Datatype::INT32);
  CHECK(tiler3.get_tile(1, "a", tile3_1).ok());
  std::vector<int32_t> c_data3_1(50);
  for (int i = 0; i <= 29; ++i)
    c_data3_1[i] = fill_value;
  for (int i = 30; i <= 31; ++i)
    c_data3_1[i] = i - 26;
  for (int i = 32; i <= 39; ++i)
    c_data3_1[i] = fill_value;
  for (int i = 40; i <= 41; ++i)
    c_data3_1[i] = i - 31;
  for (int i = 42; i <= 49; ++i)
    c_data3_1[i] = fill_value;
  CHECK(check_tile<int32_t>(tile3_1.fixed_tile(), c_data3_1));

  // Test get tile 2
  WriterTile tile3_2(
      array_->array_->array_schema_latest(),
      array_->array_->array_schema_latest().domain().cell_num_per_tile(),
      false,
      false,
      sizeof(int32_t),
      Datatype::INT32);
  CHECK(tiler3.get_tile(2, "a", tile3_2).ok());
  std::vector<int32_t> c_data3_2(50);
  for (int i = 0; i <= 6; ++i)
    c_data3_2[i] = fill_value;
  for (int i = 7; i <= 9; ++i)
    c_data3_2[i] = i + 4;
  for (int i = 10; i <= 49; ++i)
    c_data3_2[i] = fill_value;
  CHECK(check_tile<int32_t>(tile3_2.fixed_tile(), c_data3_2));

  // Test get tile 3
  WriterTile tile3_3(
      array_->array_->array_schema_latest(),
      array_->array_->array_schema_latest().domain().cell_num_per_tile(),
      false,
      false,
      sizeof(int32_t),
      Datatype::INT32);
  CHECK(tiler3.get_tile(3, "a", tile3_3).ok());
  std::vector<int32_t> c_data3_3(50);
  for (int i = 0; i <= 1; ++i)
    c_data3_3[i] = i + 14;
  for (int i = 2; i <= 49; ++i)
    c_data3_3[i] = fill_value;
  CHECK(check_tile<int32_t>(tile3_3.fixed_tile(), c_data3_3));

  // Create subarray (single tile, col-major)
  close_array();
  open_array(array_name, TILEDB_READ);
  int32_t sub4_0[] = {3, 5};
  int32_t sub4_1[] = {13, 18};
  tiledb::sm::Subarray subarray4(
      array_->array_.get(),
      Layout::COL_MAJOR,
      &test::g_helper_stats,
      test::g_helper_logger());
  add_ranges({sub4_0, sub4_1}, sizeof(sub4_0), &subarray4);

  // Create DenseTiler
  buff_a = {1, 7, 13, 2, 8, 14, 3, 9, 15, 4, 10, 16, 5, 11, 17, 6, 12, 18};
  buff_a_size = sizeof(buff_a);
  buffers["a"] = QueryBuffer(&buff_a[0], nullptr, &buff_a_size, nullptr);
  DenseTiler<int32_t> tiler4(&buffers, &subarray4, &test::g_helper_stats);

  // Test get tile 0
  WriterTile tile4_0(
      array_->array_->array_schema_latest(),
      array_->array_->array_schema_latest().domain().cell_num_per_tile(),
      false,
      false,
      sizeof(int32_t),
      Datatype::INT32);
  CHECK(tiler4.get_tile(0, "a", tile4_0).ok());
  std::vector<int32_t> c_data4_0(50);
  for (int i = 0; i <= 21; ++i)
    c_data4_0[i] = fill_value;
  for (int i = 22; i <= 27; ++i)
    c_data4_0[i] = i - 21;
  for (int i = 28; i <= 31; ++i)
    c_data4_0[i] = fill_value;
  for (int i = 32; i <= 37; ++i)
    c_data4_0[i] = i - 25;
  for (int i = 38; i <= 41; ++i)
    c_data4_0[i] = fill_value;
  for (int i = 42; i <= 47; ++i)
    c_data4_0[i] = i - 29;
  for (int i = 48; i <= 49; ++i)
    c_data4_0[i] = fill_value;
  CHECK(check_tile<int32_t>(tile4_0.fixed_tile(), c_data4_0));

  // Clean up
  close_array();
  remove_array(array_name);
}

TEST_CASE_METHOD(
    DenseTilerFx,
    "DenseTiler: Test get tile, 2D, (col, col)",
    "[DenseTiler][get_tile][2d][col-col]") {
  // Create array
  std::string array_name = "dense_tiler";
  int32_t d_dom_1[] = {1, 10};
  int32_t d_ext_1 = 5;
  int32_t d_dom_2[] = {1, 30};
  int32_t d_ext_2 = 10;
  create_array(
      array_name,
      {{"d1", TILEDB_INT32, d_dom_1, &d_ext_1},
       {"d2", TILEDB_INT32, d_dom_2, &d_ext_2}},
      {{"a", TILEDB_INT32, 1, false}},
      TILEDB_COL_MAJOR,
      TILEDB_COL_MAJOR);

  // Create buffers
  std::unordered_map<std::string, QueryBuffer> buffers;
  std::vector<int32_t> buff_a = {
      1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15};
  uint64_t buff_a_size = sizeof(buff_a);
  buffers["a"] = QueryBuffer(&buff_a[0], nullptr, &buff_a_size, nullptr);

  // Create subarray (multiple tiles)
  open_array(array_name, TILEDB_READ);
  int32_t sub1_0[] = {4, 6};
  int32_t sub1_1[] = {18, 22};
  tiledb::sm::Subarray subarray1(
      array_->array_.get(),
      Layout::ROW_MAJOR,
      &test::g_helper_stats,
      test::g_helper_logger());
  add_ranges({sub1_0, sub1_1}, sizeof(sub1_0), &subarray1);

  // Create DenseTiler
  DenseTiler<int32_t> tiler1(&buffers, &subarray1, &test::g_helper_stats);

  // Test get tile 0
  WriterTile tile1_0(
      array_->array_->array_schema_latest(),
      array_->array_->array_schema_latest().domain().cell_num_per_tile(),
      false,
      false,
      sizeof(int32_t),
      Datatype::INT32);
  CHECK(tiler1.get_tile(0, "a", tile1_0).ok());
  std::vector<int32_t> c_data1_0(50);
  for (int i = 0; i <= 37; ++i)
    c_data1_0[i] = fill_value;
  c_data1_0[38] = 1;
  c_data1_0[39] = 6;
  for (int i = 40; i <= 42; ++i)
    c_data1_0[i] = fill_value;
  c_data1_0[43] = 2;
  c_data1_0[44] = 7;
  for (int i = 45; i <= 47; ++i)
    c_data1_0[i] = fill_value;
  c_data1_0[48] = 3;
  c_data1_0[49] = 8;
  CHECK(check_tile<int32_t>(tile1_0.fixed_tile(), c_data1_0));

  // Test get tile 1
  WriterTile tile1_1(
      array_->array_->array_schema_latest(),
      array_->array_->array_schema_latest().domain().cell_num_per_tile(),
      false,
      false,
      sizeof(int32_t),
      Datatype::INT32);
  CHECK(tiler1.get_tile(1, "a", tile1_1).ok());
  std::vector<int32_t> c_data1_1(50);
  for (int i = 0; i <= 34; ++i)
    c_data1_1[i] = fill_value;
  c_data1_1[35] = 11;
  for (int i = 36; i <= 39; ++i)
    c_data1_1[i] = fill_value;
  c_data1_1[40] = 12;
  for (int i = 41; i <= 44; ++i)
    c_data1_1[i] = fill_value;
  c_data1_1[45] = 13;
  for (int i = 46; i <= 49; ++i)
    c_data1_1[i] = fill_value;
  CHECK(check_tile<int32_t>(tile1_1.fixed_tile(), c_data1_1));

  // Test get tile 2
  WriterTile tile1_2(
      array_->array_->array_schema_latest(),
      array_->array_->array_schema_latest().domain().cell_num_per_tile(),
      false,
      false,
      sizeof(int32_t),
      Datatype::INT32);
  CHECK(tiler1.get_tile(2, "a", tile1_2).ok());
  std::vector<int32_t> c_data1_2(50);
  for (int i = 0; i <= 2; ++i)
    c_data1_2[i] = fill_value;
  c_data1_2[3] = 4;
  c_data1_2[4] = 9;
  for (int i = 5; i <= 7; ++i)
    c_data1_2[i] = fill_value;
  c_data1_2[8] = 5;
  c_data1_2[9] = 10;
  for (int i = 10; i <= 49; ++i)
    c_data1_2[i] = fill_value;
  CHECK(check_tile<int32_t>(tile1_2.fixed_tile(), c_data1_2));

  // Test get tile 3
  WriterTile tile1_3(
      array_->array_->array_schema_latest(),
      array_->array_->array_schema_latest().domain().cell_num_per_tile(),
      false,
      false,
      sizeof(int32_t),
      Datatype::INT32);
  CHECK(tiler1.get_tile(3, "a", tile1_3).ok());
  std::vector<int32_t> c_data1_3(50);
  c_data1_3[0] = 14;
  for (int i = 1; i <= 4; ++i)
    c_data1_3[i] = fill_value;
  c_data1_3[5] = 15;
  for (int i = 6; i <= 49; ++i)
    c_data1_3[i] = fill_value;
  CHECK(check_tile<int32_t>(tile1_3.fixed_tile(), c_data1_3));

  // Create subarray (single tile)
  close_array();
  open_array(array_name, TILEDB_READ);
  int32_t sub2_0[] = {3, 5};
  int32_t sub2_1[] = {13, 18};
  tiledb::sm::Subarray subarray2(
      array_->array_.get(),
      Layout::ROW_MAJOR,
      &test::g_helper_stats,
      test::g_helper_logger());
  add_ranges({sub2_0, sub2_1}, sizeof(sub2_0), &subarray2);

  // Create DenseTiler
  buff_a = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18};
  buff_a_size = sizeof(buff_a);
  buffers["a"] = QueryBuffer(&buff_a[0], nullptr, &buff_a_size, nullptr);
  DenseTiler<int32_t> tiler2(&buffers, &subarray2, &test::g_helper_stats);

  // Test get tile 0
  WriterTile tile2_0(
      array_->array_->array_schema_latest(),
      array_->array_->array_schema_latest().domain().cell_num_per_tile(),
      false,
      false,
      sizeof(int32_t),
      Datatype::INT32);
  CHECK(tiler2.get_tile(0, "a", tile2_0).ok());
  std::vector<int32_t> c_data2_0(50);
  for (int i = 0; i <= 11; ++i)
    c_data2_0[i] = fill_value;
  c_data2_0[12] = 1;
  c_data2_0[13] = 7;
  c_data2_0[14] = 13;
  for (int i = 15; i <= 16; ++i)
    c_data2_0[i] = fill_value;
  c_data2_0[17] = 2;
  c_data2_0[18] = 8;
  c_data2_0[19] = 14;
  for (int i = 20; i <= 21; ++i)
    c_data2_0[i] = fill_value;
  c_data2_0[22] = 3;
  c_data2_0[23] = 9;
  c_data2_0[24] = 15;
  for (int i = 25; i <= 26; ++i)
    c_data2_0[i] = fill_value;
  c_data2_0[27] = 4;
  c_data2_0[28] = 10;
  c_data2_0[29] = 16;
  for (int i = 30; i <= 31; ++i)
    c_data2_0[i] = fill_value;
  c_data2_0[32] = 5;
  c_data2_0[33] = 11;
  c_data2_0[34] = 17;
  for (int i = 35; i <= 36; ++i)
    c_data2_0[i] = fill_value;
  c_data2_0[37] = 6;
  c_data2_0[38] = 12;
  c_data2_0[39] = 18;
  for (int i = 40; i <= 49; ++i)
    c_data2_0[i] = fill_value;
  CHECK(check_tile<int32_t>(tile2_0.fixed_tile(), c_data2_0));

  // Create subarray (multiple tiles, col-major)
  close_array();
  open_array(array_name, TILEDB_READ);
  int32_t sub3_0[] = {4, 6};
  int32_t sub3_1[] = {18, 22};
  tiledb::sm::Subarray subarray3(
      array_->array_.get(),
      Layout::COL_MAJOR,
      &test::g_helper_stats,
      test::g_helper_logger());
  add_ranges({sub3_0, sub3_1}, sizeof(sub3_0), &subarray3);

  // Create DenseTiler
  buff_a = {1, 6, 11, 2, 7, 12, 3, 8, 13, 4, 9, 14, 5, 10, 15};
  buff_a_size = sizeof(buff_a);
  buffers["a"] = QueryBuffer(&buff_a[0], nullptr, &buff_a_size, nullptr);
  DenseTiler<int32_t> tiler3(&buffers, &subarray3, &test::g_helper_stats);

  // Test get tile 0
  WriterTile tile3_0(
      array_->array_->array_schema_latest(),
      array_->array_->array_schema_latest().domain().cell_num_per_tile(),
      false,
      false,
      sizeof(int32_t),
      Datatype::INT32);
  CHECK(tiler3.get_tile(0, "a", tile3_0).ok());
  std::vector<int32_t> c_data3_0(50);
  for (int i = 0; i <= 37; ++i)
    c_data3_0[i] = fill_value;
  c_data3_0[38] = 1;
  c_data3_0[39] = 6;
  for (int i = 40; i <= 42; ++i)
    c_data3_0[i] = fill_value;
  c_data3_0[43] = 2;
  c_data3_0[44] = 7;
  for (int i = 45; i <= 47; ++i)
    c_data3_0[i] = fill_value;
  c_data3_0[48] = 3;
  c_data3_0[49] = 8;
  CHECK(check_tile<int32_t>(tile3_0.fixed_tile(), c_data3_0));

  // Test get tile 1
  WriterTile tile3_1(
      array_->array_->array_schema_latest(),
      array_->array_->array_schema_latest().domain().cell_num_per_tile(),
      false,
      false,
      sizeof(int32_t),
      Datatype::INT32);
  CHECK(tiler3.get_tile(1, "a", tile3_1).ok());
  std::vector<int32_t> c_data3_1(50);
  for (int i = 0; i <= 34; ++i)
    c_data3_1[i] = fill_value;
  c_data3_1[35] = 11;
  for (int i = 36; i <= 39; ++i)
    c_data3_1[i] = fill_value;
  c_data3_1[40] = 12;
  for (int i = 41; i <= 44; ++i)
    c_data3_1[i] = fill_value;
  c_data3_1[45] = 13;
  for (int i = 46; i <= 49; ++i)
    c_data3_1[i] = fill_value;
  CHECK(check_tile<int32_t>(tile3_1.fixed_tile(), c_data3_1));

  // Test get tile 2
  WriterTile tile3_2(
      array_->array_->array_schema_latest(),
      array_->array_->array_schema_latest().domain().cell_num_per_tile(),
      false,
      false,
      sizeof(int32_t),
      Datatype::INT32);
  CHECK(tiler3.get_tile(2, "a", tile3_2).ok());
  std::vector<int32_t> c_data3_2(50);
  for (int i = 0; i <= 2; ++i)
    c_data3_2[i] = fill_value;
  c_data3_2[3] = 4;
  c_data3_2[4] = 9;
  for (int i = 5; i <= 7; ++i)
    c_data3_2[i] = fill_value;
  c_data3_2[8] = 5;
  c_data3_2[9] = 10;
  for (int i = 10; i <= 49; ++i)
    c_data3_2[i] = fill_value;
  CHECK(check_tile<int32_t>(tile3_2.fixed_tile(), c_data3_2));

  // Test get tile 3
  WriterTile tile3_3(
      array_->array_->array_schema_latest(),
      array_->array_->array_schema_latest().domain().cell_num_per_tile(),
      false,
      false,
      sizeof(int32_t),
      Datatype::INT32);
  CHECK(tiler3.get_tile(3, "a", tile3_3).ok());
  std::vector<int32_t> c_data3_3(50);
  c_data3_3[0] = 14;
  for (int i = 1; i <= 4; ++i)
    c_data3_3[i] = fill_value;
  c_data3_3[5] = 15;
  for (int i = 6; i <= 49; ++i)
    c_data3_3[i] = fill_value;
  CHECK(check_tile<int32_t>(tile3_3.fixed_tile(), c_data3_3));

  // Create subarray (single tile, col-major)
  close_array();
  open_array(array_name, TILEDB_READ);
  int32_t sub4_0[] = {3, 5};
  int32_t sub4_1[] = {13, 18};
  tiledb::sm::Subarray subarray4(
      array_->array_.get(),
      Layout::COL_MAJOR,
      &test::g_helper_stats,
      test::g_helper_logger());
  add_ranges({sub4_0, sub4_1}, sizeof(sub4_0), &subarray4);

  // Create DenseTiler
  buff_a = {1, 7, 13, 2, 8, 14, 3, 9, 15, 4, 10, 16, 5, 11, 17, 6, 12, 18};
  buff_a_size = sizeof(buff_a);
  buffers["a"] = QueryBuffer(&buff_a[0], nullptr, &buff_a_size, nullptr);
  DenseTiler<int32_t> tiler4(&buffers, &subarray4, &test::g_helper_stats);

  // Test get tile 0
  WriterTile tile4_0(
      array_->array_->array_schema_latest(),
      array_->array_->array_schema_latest().domain().cell_num_per_tile(),
      false,
      false,
      sizeof(int32_t),
      Datatype::INT32);
  CHECK(tiler4.get_tile(0, "a", tile4_0).ok());
  std::vector<int32_t> c_data4_0(50);
  for (int i = 0; i <= 11; ++i)
    c_data4_0[i] = fill_value;
  c_data4_0[12] = 1;
  c_data4_0[13] = 7;
  c_data4_0[14] = 13;
  for (int i = 15; i <= 16; ++i)
    c_data4_0[i] = fill_value;
  c_data4_0[17] = 2;
  c_data4_0[18] = 8;
  c_data4_0[19] = 14;
  for (int i = 20; i <= 21; ++i)
    c_data4_0[i] = fill_value;
  c_data4_0[22] = 3;
  c_data4_0[23] = 9;
  c_data4_0[24] = 15;
  for (int i = 25; i <= 26; ++i)
    c_data4_0[i] = fill_value;
  c_data4_0[27] = 4;
  c_data4_0[28] = 10;
  c_data4_0[29] = 16;
  for (int i = 30; i <= 31; ++i)
    c_data4_0[i] = fill_value;
  c_data4_0[32] = 5;
  c_data4_0[33] = 11;
  c_data4_0[34] = 17;
  for (int i = 35; i <= 36; ++i)
    c_data4_0[i] = fill_value;
  c_data4_0[37] = 6;
  c_data4_0[38] = 12;
  c_data4_0[39] = 18;
  for (int i = 40; i <= 49; ++i)
    c_data4_0[i] = fill_value;
  CHECK(check_tile<int32_t>(tile4_0.fixed_tile(), c_data4_0));

  // Clean up
  close_array();
  remove_array(array_name);
}

TEST_CASE_METHOD(
    DenseTilerFx,
    "DenseTiler: Test get tile optimization, 2D, (row, row)",
    "[DenseTiler][get_tile][optimization][2d][row-row]") {
  // Create array
  std::string array_name = "dense_tiler";
  int32_t d_dom_1[] = {1, 10};
  int32_t d_ext_1 = 5;
  int32_t d_dom_2[] = {1, 30};
  int32_t d_ext_2 = 10;
  create_array(
      array_name,
      {{"d1", TILEDB_INT32, d_dom_1, &d_ext_1},
       {"d2", TILEDB_INT32, d_dom_2, &d_ext_2}},
      {{"a", TILEDB_INT32, 1, false}},
      TILEDB_ROW_MAJOR,
      TILEDB_ROW_MAJOR);

  // Create buffers
  std::unordered_map<std::string, QueryBuffer> buffers;
  std::vector<int32_t> buff_a = {
      1,  2,  3,  4,  5,  6,  7,  8,  9,  10, 11, 12, 13, 14, 15,
      16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30,
      31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45,
      46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60};
  uint64_t buff_a_size = sizeof(buff_a);
  buffers["a"] = QueryBuffer(&buff_a[0], nullptr, &buff_a_size, nullptr);

  // Create subarray (multiple tiles)
  open_array(array_name, TILEDB_READ);
  int32_t sub1_0[] = {4, 9};
  int32_t sub1_1[] = {11, 20};
  tiledb::sm::Subarray subarray1(
      array_->array_.get(),
      Layout::ROW_MAJOR,
      &test::g_helper_stats,
      test::g_helper_logger());
  add_ranges({sub1_0, sub1_1}, sizeof(sub1_0), &subarray1);

  // Create DenseTiler
  DenseTiler<int32_t> tiler1(&buffers, &subarray1, &test::g_helper_stats);

  // Test get tile 0
  WriterTile tile1_0(
      array_->array_->array_schema_latest(),
      array_->array_->array_schema_latest().domain().cell_num_per_tile(),
      false,
      false,
      sizeof(int32_t),
      Datatype::INT32);
  CHECK(tiler1.get_tile(0, "a", tile1_0).ok());
  std::vector<int32_t> c_data1_0(50);
  for (int i = 0; i <= 29; ++i)
    c_data1_0[i] = fill_value;
  for (int i = 30; i <= 49; ++i)
    c_data1_0[i] = i - 29;
  CHECK(check_tile<int32_t>(tile1_0.fixed_tile(), c_data1_0));

  // Test get tile 1
  WriterTile tile1_1(
      array_->array_->array_schema_latest(),
      array_->array_->array_schema_latest().domain().cell_num_per_tile(),
      false,
      false,
      sizeof(int32_t),
      Datatype::INT32);
  CHECK(tiler1.get_tile(1, "a", tile1_1).ok());
  std::vector<int32_t> c_data1_1(50);
  for (int i = 0; i <= 39; ++i)
    c_data1_1[i] = i + 21;
  for (int i = 40; i <= 49; ++i)
    c_data1_1[i] = fill_value;
  CHECK(check_tile<int32_t>(tile1_1.fixed_tile(), c_data1_1));

  // Clean up
  close_array();
  remove_array(array_name);
}

TEST_CASE_METHOD(
    DenseTilerFx,
    "DenseTiler: Test get tile optimization, 2D, (col, col)",
    "[DenseTiler][get_tile][optimization][2d][col-col]") {
  // Create array
  std::string array_name = "dense_tiler";
  int32_t d_dom_1[] = {1, 10};
  int32_t d_ext_1 = 5;
  int32_t d_dom_2[] = {1, 30};
  int32_t d_ext_2 = 10;
  create_array(
      array_name,
      {{"d1", TILEDB_INT32, d_dom_1, &d_ext_1},
       {"d2", TILEDB_INT32, d_dom_2, &d_ext_2}},
      {{"a", TILEDB_INT32, 1, false}},
      TILEDB_COL_MAJOR,
      TILEDB_COL_MAJOR);

  // Create buffers
  std::unordered_map<std::string, QueryBuffer> buffers;
  std::vector<int32_t> buff_a = {1,  2,  3,  4,  5,  6,  7,  8,  9,
                                 10, 11, 12, 13, 14, 15, 16, 17, 18,
                                 19, 20, 21, 22, 23, 24, 25};
  uint64_t buff_a_size = sizeof(buff_a);
  buffers["a"] = QueryBuffer(&buff_a[0], nullptr, &buff_a_size, nullptr);

  // Create subarray (multiple tiles)
  open_array(array_name, TILEDB_READ);
  int32_t sub1_0[] = {1, 5};
  int32_t sub1_1[] = {8, 12};
  tiledb::sm::Subarray subarray1(
      array_->array_.get(),
      Layout::COL_MAJOR,
      &test::g_helper_stats,
      test::g_helper_logger());
  add_ranges({sub1_0, sub1_1}, sizeof(sub1_0), &subarray1);

  // Create DenseTiler
  DenseTiler<int32_t> tiler1(&buffers, &subarray1, &test::g_helper_stats);

  // Test get tile 0
  WriterTile tile1_0(
      array_->array_->array_schema_latest(),
      array_->array_->array_schema_latest().domain().cell_num_per_tile(),
      false,
      false,
      sizeof(int32_t),
      Datatype::INT32);
  CHECK(tiler1.get_tile(0, "a", tile1_0).ok());
  std::vector<int32_t> c_data1_0(50);
  for (int i = 0; i <= 34; ++i)
    c_data1_0[i] = fill_value;
  for (int i = 35; i <= 49; ++i)
    c_data1_0[i] = i - 34;
  CHECK(check_tile<int32_t>(tile1_0.fixed_tile(), c_data1_0));

  // Test get tile 1
  WriterTile tile1_1(
      array_->array_->array_schema_latest(),
      array_->array_->array_schema_latest().domain().cell_num_per_tile(),
      false,
      false,
      sizeof(int32_t),
      Datatype::INT32);
  CHECK(tiler1.get_tile(1, "a", tile1_1).ok());
  std::vector<int32_t> c_data1_1(50);
  for (int i = 0; i <= 9; ++i)
    c_data1_1[i] = i + 16;
  for (int i = 10; i <= 49; ++i)
    c_data1_1[i] = fill_value;
  CHECK(check_tile<int32_t>(tile1_1.fixed_tile(), c_data1_1));

  // Clean up
  close_array();
  remove_array(array_name);
}

TEST_CASE_METHOD(
    DenseTilerFx,
    "DenseTiler: Test get tile, cell val num = 2, 1D",
    "[DenseTiler][get_tile][cell_val_num_2][1d]") {
  // Create array
  std::string array_name = "dense_tiler";
  int32_t d_dom[] = {1, 10};
  int32_t d_ext = 5;
  create_array(
      array_name,
      {{"d", TILEDB_INT32, d_dom, &d_ext}},
      {{"a", TILEDB_INT32, 2, false}},
      TILEDB_ROW_MAJOR,
      TILEDB_ROW_MAJOR);

  // Create buffers
  std::unordered_map<std::string, QueryBuffer> buffers;
  std::vector<int32_t> buff_a = {1, 11, 2, 22, 3, 33, 4, 44};
  uint64_t buff_a_size = sizeof(buff_a);
  buffers["a"] = QueryBuffer(&buff_a[0], nullptr, &buff_a_size, nullptr);

  // Create subarray
  open_array(array_name, TILEDB_READ);
  int32_t sub1[] = {3, 6};
  tiledb::sm::Subarray subarray1(
      array_->array_.get(),
      Layout::ROW_MAJOR,
      &test::g_helper_stats,
      test::g_helper_logger());
  add_ranges({sub1}, sizeof(sub1), &subarray1);

  // Create DenseTiler
  DenseTiler<int32_t> tiler1(&buffers, &subarray1, &test::g_helper_stats);

  // Test get tile 0
  WriterTile tile1_0(
      array_->array_->array_schema_latest(),
      array_->array_->array_schema_latest().domain().cell_num_per_tile(),
      false,
      false,
      2 * sizeof(int32_t),
      Datatype::INT32);
  CHECK(tiler1.get_tile(0, "a", tile1_0).ok());
  std::vector<int32_t> c_data1_0 = {
      fill_value, fill_value, fill_value, fill_value, 1, 11, 2, 22, 3, 33};
  CHECK(check_tile<int32_t>(tile1_0.fixed_tile(), c_data1_0));

  // Test get tile 1
  WriterTile tile1_1(
      array_->array_->array_schema_latest(),
      array_->array_->array_schema_latest().domain().cell_num_per_tile(),
      false,
      false,
      2 * sizeof(int32_t),
      Datatype::INT32);
  CHECK(tiler1.get_tile(1, "a", tile1_1).ok());
  std::vector<int32_t> c_data1_1 = {4,
                                    44,
                                    fill_value,
                                    fill_value,
                                    fill_value,
                                    fill_value,
                                    fill_value,
                                    fill_value,
                                    fill_value,
                                    fill_value};
  CHECK(check_tile<int32_t>(tile1_1.fixed_tile(), c_data1_1));

  // Create new subarray
  close_array();
  open_array(array_name, TILEDB_READ);
  int32_t sub2[] = {7, 10};
  tiledb::sm::Subarray subarray2(
      array_->array_.get(),
      Layout::ROW_MAJOR,
      &test::g_helper_stats,
      test::g_helper_logger());
  add_ranges({sub2}, sizeof(sub2), &subarray2);

  // Create DenseTiler
  DenseTiler<int32_t> tiler2(&buffers, &subarray2, &test::g_helper_stats);

  // Test get tile 0
  WriterTile tile2(
      array_->array_->array_schema_latest(),
      array_->array_->array_schema_latest().domain().cell_num_per_tile(),
      false,
      false,
      2 * sizeof(int32_t),
      Datatype::INT32);
  CHECK(tiler2.get_tile(0, "a", tile2).ok());
  std::vector<int32_t> c_data2 = {
      fill_value, fill_value, 1, 11, 2, 22, 3, 33, 4, 44};
  CHECK(check_tile<int32_t>(tile2.fixed_tile(), c_data2));

  // Create new subarray (col-major)
  close_array();
  open_array(array_name, TILEDB_READ);
  int32_t sub3[] = {7, 10};
  tiledb::sm::Subarray subarray3(
      array_->array_.get(),
      Layout::COL_MAJOR,
      &test::g_helper_stats,
      test::g_helper_logger());
  add_ranges({sub3}, sizeof(sub3), &subarray3);

  // Create DenseTiler
  DenseTiler<int32_t> tiler3(&buffers, &subarray3, &test::g_helper_stats);

  // Test get tile 0
  WriterTile tile3(
      array_->array_->array_schema_latest(),
      array_->array_->array_schema_latest().domain().cell_num_per_tile(),
      false,
      false,
      2 * sizeof(int32_t),
      Datatype::INT32);
  CHECK(tiler3.get_tile(0, "a", tile3).ok());
  std::vector<int32_t> c_data3 = {
      fill_value, fill_value, 1, 11, 2, 22, 3, 33, 4, 44};
  CHECK(check_tile<int32_t>(tile3.fixed_tile(), c_data3));

  // Clean up
  close_array();
  remove_array(array_name);
}

TEST_CASE_METHOD(
    DenseTilerFx,
    "DenseTiler: Test get tile, 2 attributes, 1D",
    "[DenseTiler][get_tile][2_attributes][1d]") {
  // Create array
  std::string array_name = "dense_tiler";
  int32_t d_dom[] = {1, 10};
  int32_t d_ext = 5;
  create_array(
      array_name,
      {{"d", TILEDB_INT32, d_dom, &d_ext}},
      {{"a1", TILEDB_INT32, 1, false}, {"a2", TILEDB_FLOAT64, 1, false}},
      TILEDB_ROW_MAJOR,
      TILEDB_ROW_MAJOR);

  // Create buffers
  std::unordered_map<std::string, QueryBuffer> buffers;
  std::vector<int32_t> buff_a1 = {1, 2, 3, 4};
  uint64_t buff_a1_size = sizeof(buff_a1);
  buffers["a1"] = QueryBuffer(&buff_a1[0], nullptr, &buff_a1_size, nullptr);
  std::vector<double> buff_a2 = {1.1, 2.2, 3.3, 4.4};
  uint64_t buff_a2_size = sizeof(buff_a2);
  buffers["a2"] = QueryBuffer(&buff_a2[0], nullptr, &buff_a2_size, nullptr);

  // Create subarray
  open_array(array_name, TILEDB_READ);
  int32_t sub1[] = {3, 6};
  tiledb::sm::Subarray subarray1(
      array_->array_.get(),
      Layout::ROW_MAJOR,
      &test::g_helper_stats,
      test::g_helper_logger());
  add_ranges({sub1}, sizeof(sub1), &subarray1);

  // Create DenseTiler
  DenseTiler<int32_t> tiler1(&buffers, &subarray1, &test::g_helper_stats);

  // Test get tile 0
  WriterTile tile1_0_a1(
      array_->array_->array_schema_latest(),
      array_->array_->array_schema_latest().domain().cell_num_per_tile(),
      false,
      false,
      sizeof(int32_t),
      Datatype::INT32);
  CHECK(tiler1.get_tile(0, "a1", tile1_0_a1).ok());
  std::vector<int32_t> c_data1_0_a1 = {fill_value, fill_value, 1, 2, 3};
  CHECK(check_tile<int32_t>(tile1_0_a1.fixed_tile(), c_data1_0_a1));
  WriterTile tile1_0_a2(
      array_->array_->array_schema_latest(),
      array_->array_->array_schema_latest().domain().cell_num_per_tile(),
      false,
      false,
      sizeof(double),
      Datatype::FLOAT64);
  CHECK(tiler1.get_tile(0, "a2", tile1_0_a2).ok());
  std::vector<double> c_data1_0_a2 = {
      double(fill_value), double(fill_value), 1.1, 2.2, 3.3};
  CHECK(check_tile<double>(tile1_0_a2.fixed_tile(), c_data1_0_a2));

  // Test get tile 1
  WriterTile tile1_1_a1(
      array_->array_->array_schema_latest(),
      array_->array_->array_schema_latest().domain().cell_num_per_tile(),
      false,
      false,
      sizeof(int32_t),
      Datatype::INT32);
  CHECK(tiler1.get_tile(1, "a1", tile1_1_a1).ok());
  std::vector<int32_t> c_data1_1_a1 = {
      4, fill_value, fill_value, fill_value, fill_value};
  CHECK(check_tile<int32_t>(tile1_1_a1.fixed_tile(), c_data1_1_a1));
  WriterTile tile1_1_a2(
      array_->array_->array_schema_latest(),
      array_->array_->array_schema_latest().domain().cell_num_per_tile(),
      false,
      false,
      sizeof(double),
      Datatype::FLOAT64);
  CHECK(tiler1.get_tile(1, "a2", tile1_1_a2).ok());
  std::vector<double> c_data1_1_a2 = {4.4,
                                      double(fill_value),
                                      double(fill_value),
                                      double(fill_value),
                                      double(fill_value)};
  CHECK(check_tile<double>(tile1_1_a2.fixed_tile(), c_data1_1_a2));

  // Create new subarray
  close_array();
  open_array(array_name, TILEDB_READ);
  int32_t sub2[] = {7, 10};
  tiledb::sm::Subarray subarray2(
      array_->array_.get(),
      Layout::ROW_MAJOR,
      &test::g_helper_stats,
      test::g_helper_logger());
  add_ranges({sub2}, sizeof(sub2), &subarray2);

  // Create DenseTiler
  DenseTiler<int32_t> tiler2(&buffers, &subarray2, &test::g_helper_stats);

  // Test get tile 0
  WriterTile tile2_a1(
      array_->array_->array_schema_latest(),
      array_->array_->array_schema_latest().domain().cell_num_per_tile(),
      false,
      false,
      sizeof(int32_t),
      Datatype::INT32);
  CHECK(tiler2.get_tile(0, "a1", tile2_a1).ok());
  std::vector<int32_t> c_data2_a1 = {fill_value, 1, 2, 3, 4};
  CHECK(check_tile<int32_t>(tile2_a1.fixed_tile(), c_data2_a1));
  WriterTile tile2_a2(
      array_->array_->array_schema_latest(),
      array_->array_->array_schema_latest().domain().cell_num_per_tile(),
      false,
      false,
      sizeof(double),
      Datatype::FLOAT64);
  CHECK(tiler2.get_tile(0, "a2", tile2_a2).ok());
  std::vector<double> c_data2_a2 = {double(fill_value), 1.1, 2.2, 3.3, 4.4};
  CHECK(check_tile<double>(tile2_a2.fixed_tile(), c_data2_a2));

  // Create new subarray (col-major)
  close_array();
  open_array(array_name, TILEDB_READ);
  int32_t sub3[] = {7, 10};
  tiledb::sm::Subarray subarray3(
      array_->array_.get(),
      Layout::COL_MAJOR,
      &test::g_helper_stats,
      test::g_helper_logger());
  add_ranges({sub3}, sizeof(sub3), &subarray3);

  // Create DenseTiler
  DenseTiler<int32_t> tiler3(&buffers, &subarray3, &test::g_helper_stats);

  // Test get tile 0
  WriterTile tile3_a1(
      array_->array_->array_schema_latest(),
      array_->array_->array_schema_latest().domain().cell_num_per_tile(),
      false,
      false,
      sizeof(int32_t),
      Datatype::INT32);
  CHECK(tiler3.get_tile(0, "a1", tile3_a1).ok());
  std::vector<int32_t> c_data3_a1 = {fill_value, 1, 2, 3, 4};
  CHECK(check_tile<int32_t>(tile3_a1.fixed_tile(), c_data3_a1));
  WriterTile tile3_a2(
      array_->array_->array_schema_latest(),
      array_->array_->array_schema_latest().domain().cell_num_per_tile(),
      false,
      false,
      sizeof(double),
      Datatype::FLOAT64);
  CHECK(tiler3.get_tile(0, "a2", tile3_a2).ok());
  std::vector<double> c_data3_a2 = {double(fill_value), 1.1, 2.2, 3.3, 4.4};
  CHECK(check_tile<double>(tile3_a2.fixed_tile(), c_data3_a2));

  // Clean up
  close_array();
  remove_array(array_name);
}

TEST_CASE_METHOD(
    DenseTilerFx,
    "DenseTiler: Test get tile, nullable, 2D, (row, row)",
    "[DenseTiler][get_tile][2d][nullable][row-row]") {
  // Create array
  std::string array_name = "dense_tiler";
  int32_t d_dom_1[] = {1, 10};
  int32_t d_ext_1 = 5;
  int32_t d_dom_2[] = {1, 30};
  int32_t d_ext_2 = 10;
  create_array(
      array_name,
      {{"d1", TILEDB_INT32, d_dom_1, &d_ext_1},
       {"d2", TILEDB_INT32, d_dom_2, &d_ext_2}},
      {{"a", TILEDB_INT32, 1, true}},
      TILEDB_ROW_MAJOR,
      TILEDB_ROW_MAJOR);

  // Create buffers
  std::unordered_map<std::string, QueryBuffer> buffers;
  std::vector<int32_t> buff_a = {
      1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15};
  uint64_t buff_a_size = sizeof(buff_a);
  std::vector<uint8_t> buff_a_n = {0, 1, 1, 0, 0, 0, 1, 0, 1, 1, 1, 1, 0, 0, 1};
  uint64_t buff_a_n_size = sizeof(buff_a_n);
  buffers["a"] = QueryBuffer(
      &buff_a[0],
      nullptr,
      &buff_a_size,
      nullptr,
      ValidityVector(&buff_a_n[0], &buff_a_n_size));

  // Create subarray (multiple tiles)
  open_array(array_name, TILEDB_READ);
  int32_t sub1_0[] = {4, 6};
  int32_t sub1_1[] = {18, 22};
  tiledb::sm::Subarray subarray1(
      array_->array_.get(),
      Layout::ROW_MAJOR,
      &test::g_helper_stats,
      test::g_helper_logger());
  add_ranges({sub1_0, sub1_1}, sizeof(sub1_0), &subarray1);

  // Create DenseTiler
  DenseTiler<int32_t> tiler1(&buffers, &subarray1, &test::g_helper_stats);

  // Test get tile 0
  WriterTile tile1_0(
      array_->array_->array_schema_latest(),
      array_->array_->array_schema_latest().domain().cell_num_per_tile(),
      false,
      true,
      sizeof(int32_t),
      Datatype::INT32);
  CHECK(tiler1.get_tile(0, "a", tile1_0).ok());
  std::vector<uint8_t> c_data1_0(50);
  for (int i = 0; i <= 36; ++i)
    c_data1_0[i] = fill_value;
  c_data1_0[37] = 0;
  c_data1_0[38] = 1;
  c_data1_0[39] = 1;
  for (int i = 40; i <= 46; ++i)
    c_data1_0[i] = fill_value;
  c_data1_0[47] = 0;
  c_data1_0[48] = 1;
  c_data1_0[49] = 0;
  CHECK(check_tile<uint8_t>(tile1_0.validity_tile(), c_data1_0));

  // Test get tile 1
  WriterTile tile1_1(
      array_->array_->array_schema_latest(),
      array_->array_->array_schema_latest().domain().cell_num_per_tile(),
      false,
      true,
      sizeof(int32_t),
      Datatype::INT32);
  CHECK(tiler1.get_tile(1, "a", tile1_1).ok());
  std::vector<uint8_t> c_data1_1(50);
  for (int i = 0; i <= 29; ++i)
    c_data1_1[i] = fill_value;
  c_data1_1[30] = 0;
  c_data1_1[31] = 0;
  for (int i = 32; i <= 39; ++i)
    c_data1_1[i] = fill_value;
  c_data1_1[40] = 1;
  c_data1_1[41] = 1;
  for (int i = 42; i <= 49; ++i)
    c_data1_1[i] = fill_value;
  CHECK(check_tile<uint8_t>(tile1_1.validity_tile(), c_data1_1));

  // Test get tile 2
  WriterTile tile1_2(
      array_->array_->array_schema_latest(),
      array_->array_->array_schema_latest().domain().cell_num_per_tile(),
      false,
      true,
      sizeof(int32_t),
      Datatype::INT32);
  CHECK(tiler1.get_tile(2, "a", tile1_2).ok());
  std::vector<uint8_t> c_data1_2(50);
  for (int i = 0; i <= 6; ++i)
    c_data1_2[i] = fill_value;
  c_data1_2[7] = 1;
  c_data1_2[8] = 1;
  c_data1_2[9] = 0;
  for (int i = 10; i <= 49; ++i)
    c_data1_2[i] = fill_value;
  CHECK(check_tile<uint8_t>(tile1_2.validity_tile(), c_data1_2));

  // Test get tile 3
  WriterTile tile1_3(
      array_->array_->array_schema_latest(),
      array_->array_->array_schema_latest().domain().cell_num_per_tile(),
      false,
      true,
      sizeof(int32_t),
      Datatype::INT32);
  CHECK(tiler1.get_tile(3, "a", tile1_3).ok());
  std::vector<uint8_t> c_data1_3(50);
  c_data1_3[0] = 0;
  c_data1_3[1] = 1;
  for (int i = 2; i <= 49; ++i)
    c_data1_3[i] = fill_value;
  CHECK(check_tile<uint8_t>(tile1_3.validity_tile(), c_data1_3));

  // Create subarray (single tile)
  close_array();
  open_array(array_name, TILEDB_READ);
  int32_t sub2_0[] = {3, 5};
  int32_t sub2_1[] = {13, 18};
  tiledb::sm::Subarray subarray2(
      array_->array_.get(),
      Layout::ROW_MAJOR,
      &test::g_helper_stats,
      test::g_helper_logger());
  add_ranges({sub2_0, sub2_1}, sizeof(sub2_0), &subarray2);

  // Create DenseTiler
  buff_a = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18};
  buff_a_size = sizeof(buff_a);
  buff_a_n = {0, 1, 1, 0, 0, 0, 1, 0, 1, 1, 1, 1, 0, 0, 1, 0, 1, 0};
  buff_a_n_size = sizeof(buff_a_n);
  buffers["a"] = QueryBuffer(
      &buff_a[0],
      nullptr,
      &buff_a_size,
      nullptr,
      ValidityVector(&buff_a_n[0], &buff_a_n_size));
  DenseTiler<int32_t> tiler2(&buffers, &subarray2, &test::g_helper_stats);

  // Test get tile 0
  WriterTile tile2_0(
      array_->array_->array_schema_latest(),
      array_->array_->array_schema_latest().domain().cell_num_per_tile(),
      false,
      true,
      sizeof(int32_t),
      Datatype::INT32);
  CHECK(tiler2.get_tile(0, "a", tile2_0).ok());
  std::vector<uint8_t> c_data2_0(50);
  for (int i = 0; i <= 21; ++i)
    c_data2_0[i] = fill_value;
  c_data2_0[22] = 0;
  c_data2_0[23] = 1;
  c_data2_0[24] = 1;
  c_data2_0[25] = 0;
  c_data2_0[26] = 0;
  c_data2_0[27] = 0;
  for (int i = 28; i <= 31; ++i)
    c_data2_0[i] = fill_value;
  c_data2_0[32] = 1;
  c_data2_0[33] = 0;
  c_data2_0[34] = 1;
  c_data2_0[35] = 1;
  c_data2_0[36] = 1;
  c_data2_0[37] = 1;
  for (int i = 38; i <= 41; ++i)
    c_data2_0[i] = fill_value;
  c_data2_0[42] = 0;
  c_data2_0[43] = 0;
  c_data2_0[44] = 1;
  c_data2_0[45] = 0;
  c_data2_0[46] = 1;
  c_data2_0[47] = 0;
  for (int i = 48; i <= 49; ++i)
    c_data2_0[i] = fill_value;
  CHECK(check_tile<uint8_t>(tile2_0.validity_tile(), c_data2_0));

  // Create subarray (multiple tiles, col-major)
  close_array();
  open_array(array_name, TILEDB_READ);
  int32_t sub3_0[] = {4, 6};
  int32_t sub3_1[] = {18, 22};
  tiledb::sm::Subarray subarray3(
      array_->array_.get(),
      Layout::COL_MAJOR,
      &test::g_helper_stats,
      test::g_helper_logger());
  add_ranges({sub3_0, sub3_1}, sizeof(sub3_0), &subarray3);

  // Create DenseTiler
  buff_a = {1, 6, 11, 2, 7, 12, 3, 8, 13, 4, 9, 14, 5, 10, 15};
  buff_a_size = sizeof(buff_a);
  buff_a_n = {0, 1, 1, 0, 0, 0, 1, 0, 1, 1, 1, 1, 0, 0, 1};
  buff_a_n_size = sizeof(buff_a_n);
  buffers["a"] = QueryBuffer(
      &buff_a[0],
      nullptr,
      &buff_a_size,
      nullptr,
      ValidityVector(&buff_a_n[0], &buff_a_n_size));
  DenseTiler<int32_t> tiler3(&buffers, &subarray3, &test::g_helper_stats);

  // Test get tile 0
  WriterTile tile3_0(
      array_->array_->array_schema_latest(),
      array_->array_->array_schema_latest().domain().cell_num_per_tile(),
      false,
      true,
      sizeof(int32_t),
      Datatype::INT32);
  CHECK(tiler3.get_tile(0, "a", tile3_0).ok());
  std::vector<uint8_t> c_data3_0(50);
  for (int i = 0; i <= 36; ++i)
    c_data3_0[i] = fill_value;
  c_data3_0[37] = 0;
  c_data3_0[38] = 0;
  c_data3_0[39] = 1;
  for (int i = 40; i <= 46; ++i)
    c_data3_0[i] = fill_value;
  c_data3_0[47] = 1;
  c_data3_0[48] = 0;
  c_data3_0[49] = 0;
  CHECK(check_tile<uint8_t>(tile3_0.validity_tile(), c_data3_0));

  // Test get tile 1
  WriterTile tile3_1(
      array_->array_->array_schema_latest(),
      array_->array_->array_schema_latest().domain().cell_num_per_tile(),
      false,
      true,
      sizeof(int32_t),
      Datatype::INT32);
  CHECK(tiler3.get_tile(1, "a", tile3_1).ok());
  std::vector<uint8_t> c_data3_1(50);
  for (int i = 0; i <= 29; ++i)
    c_data3_1[i] = fill_value;
  c_data3_1[30] = 1;
  c_data3_1[31] = 0;
  for (int i = 32; i <= 39; ++i)
    c_data3_1[i] = fill_value;
  c_data3_1[40] = 1;
  c_data3_1[41] = 0;
  for (int i = 42; i <= 49; ++i)
    c_data3_1[i] = fill_value;
  CHECK(check_tile<uint8_t>(tile3_1.validity_tile(), c_data3_1));

  // Test get tile 2
  WriterTile tile3_2(
      array_->array_->array_schema_latest(),
      array_->array_->array_schema_latest().domain().cell_num_per_tile(),
      false,
      true,
      sizeof(int32_t),
      Datatype::INT32);
  CHECK(tiler3.get_tile(2, "a", tile3_2).ok());
  std::vector<uint8_t> c_data3_2(50);
  for (int i = 0; i <= 6; ++i)
    c_data3_2[i] = fill_value;
  c_data3_2[7] = 1;
  c_data3_2[8] = 0;
  c_data3_2[9] = 1;
  for (int i = 10; i <= 49; ++i)
    c_data3_2[i] = fill_value;
  CHECK(check_tile<uint8_t>(tile3_2.validity_tile(), c_data3_2));

  // Test get tile 3
  WriterTile tile3_3(
      array_->array_->array_schema_latest(),
      array_->array_->array_schema_latest().domain().cell_num_per_tile(),
      false,
      true,
      sizeof(int32_t),
      Datatype::INT32);
  CHECK(tiler3.get_tile(3, "a", tile3_3).ok());
  std::vector<uint8_t> c_data3_3(50);
  c_data3_3[0] = 1;
  c_data3_3[1] = 1;
  for (int i = 2; i <= 49; ++i)
    c_data3_3[i] = fill_value;
  CHECK(check_tile<uint8_t>(tile3_3.validity_tile(), c_data3_3));

  // Create subarray (single tile, col-major)
  close_array();
  open_array(array_name, TILEDB_READ);
  int32_t sub4_0[] = {3, 5};
  int32_t sub4_1[] = {13, 18};
  tiledb::sm::Subarray subarray4(
      array_->array_.get(),
      Layout::COL_MAJOR,
      &test::g_helper_stats,
      test::g_helper_logger());
  add_ranges({sub4_0, sub4_1}, sizeof(sub4_0), &subarray4);

  // Create DenseTiler
  buff_a = {1, 7, 13, 2, 8, 14, 3, 9, 15, 4, 10, 16, 5, 11, 17, 6, 12, 18};
  buff_a_size = sizeof(buff_a);
  buff_a_n = {0, 1, 1, 0, 0, 0, 1, 0, 1, 1, 1, 1, 0, 0, 1, 0, 1, 0};
  buff_a_n_size = sizeof(buff_a_n);
  buffers["a"] = QueryBuffer(
      &buff_a[0],
      nullptr,
      &buff_a_size,
      nullptr,
      ValidityVector(&buff_a_n[0], &buff_a_n_size));
  DenseTiler<int32_t> tiler4(&buffers, &subarray4, &test::g_helper_stats);

  // Test get tile 0
  WriterTile tile4_0(
      array_->array_->array_schema_latest(),
      array_->array_->array_schema_latest().domain().cell_num_per_tile(),
      false,
      true,
      sizeof(int32_t),
      Datatype::INT32);
  CHECK(tiler4.get_tile(0, "a", tile4_0).ok());
  std::vector<uint8_t> c_data4_0(50);
  for (int i = 0; i <= 21; ++i)
    c_data4_0[i] = fill_value;
  c_data4_0[22] = 0;
  c_data4_0[23] = 0;
  c_data4_0[24] = 1;
  c_data4_0[25] = 1;
  c_data4_0[26] = 0;
  c_data4_0[27] = 0;
  for (int i = 28; i <= 31; ++i)
    c_data4_0[i] = fill_value;
  c_data4_0[32] = 1;
  c_data4_0[33] = 0;
  c_data4_0[34] = 0;
  c_data4_0[35] = 1;
  c_data4_0[36] = 0;
  c_data4_0[37] = 1;
  for (int i = 38; i <= 41; ++i)
    c_data4_0[i] = fill_value;
  c_data4_0[42] = 1;
  c_data4_0[43] = 0;
  c_data4_0[44] = 1;
  c_data4_0[45] = 1;
  c_data4_0[46] = 1;
  c_data4_0[47] = 0;
  for (int i = 48; i <= 49; ++i)
    c_data4_0[i] = fill_value;
  CHECK(check_tile<uint8_t>(tile4_0.validity_tile(), c_data4_0));

  // Clean up
  close_array();
  remove_array(array_name);
}

TEST_CASE_METHOD(
    DenseTilerFx,
    "DenseTiler: Test get tile, var char, 2D, (row, row)",
    "[DenseTiler][get_tile][2d][var][a-char][row-row]") {
  // Create array
  std::string array_name = "dense_tiler";
  int32_t d_dom_1[] = {1, 10};
  int32_t d_ext_1 = 5;
  int32_t d_dom_2[] = {1, 30};
  int32_t d_ext_2 = 10;
  create_array(
      array_name,
      {{"d1", TILEDB_INT32, d_dom_1, &d_ext_1},
       {"d2", TILEDB_INT32, d_dom_2, &d_ext_2}},
      {{"a", TILEDB_CHAR, TILEDB_VAR_NUM, false}},
      TILEDB_ROW_MAJOR,
      TILEDB_ROW_MAJOR);

  // Create buffers
  std::unordered_map<std::string, QueryBuffer> buffers;
  std::vector<int32_t> buff_a = {
      1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15};
  (void)buff_a;  // Used only as a reference
  std::vector<uint64_t> buff_a_off = {
      0, 1, 3, 6, 10, 15, 16, 18, 21, 25, 30, 31, 33, 36, 40};
  uint64_t buff_a_off_size = buff_a_off.size() * sizeof(uint64_t);
  std::string buff_a_val("abbcccddddeeeeefgghhhiiiijjjjjkllmmmnnnnooooo");
  uint64_t buff_a_val_size = buff_a_val.size();
  buffers["a"] = QueryBuffer(
      &buff_a_off[0], &buff_a_val[0], &buff_a_off_size, &buff_a_val_size);

  // Create subarray (multiple tiles)
  open_array(array_name, TILEDB_READ);
  int32_t sub1_0[] = {4, 6};
  int32_t sub1_1[] = {18, 22};
  tiledb::sm::Subarray subarray1(
      array_->array_.get(),
      Layout::ROW_MAJOR,
      &test::g_helper_stats,
      test::g_helper_logger());
  add_ranges({sub1_0, sub1_1}, sizeof(sub1_0), &subarray1);

  // Create DenseTiler
  DenseTiler<int32_t> tiler1(&buffers, &subarray1, &test::g_helper_stats);

  // Test get tile 0
  WriterTile tile1_0(
      array_->array_->array_schema_latest(),
      array_->array_->array_schema_latest().domain().cell_num_per_tile(),
      true,
      false,
      1,
      Datatype::STRING_ASCII);
  CHECK(tiler1.get_tile(0, "a", tile1_0).ok());
  std::vector<uint64_t> c_data1_0_off(50);
  for (int i = 0; i <= 37; ++i)
    c_data1_0_off[i] = i;
  c_data1_0_off[38] = c_data1_0_off[37] + std::string("a").size();
  c_data1_0_off[39] = c_data1_0_off[38] + std::string("bb").size();
  c_data1_0_off[40] = c_data1_0_off[39] + std::string("ccc").size();
  for (int i = 41; i <= 46; ++i)
    c_data1_0_off[i] = c_data1_0_off[i - 1] + 1;
  c_data1_0_off[47] = c_data1_0_off[46] + 1;
  c_data1_0_off[48] = c_data1_0_off[47] + std::string("f").size();
  c_data1_0_off[49] = c_data1_0_off[48] + std::string("gg").size();
  CHECK(check_tile<uint64_t>(tile1_0.offset_tile(), c_data1_0_off));
  std::vector<uint8_t> c_data1_0_val(56);
  for (int i = 0; i <= 36; ++i)
    c_data1_0_val[i] = 0;
  c_data1_0_val[37] = 'a';
  c_data1_0_val[38] = 'b';
  c_data1_0_val[39] = 'b';
  c_data1_0_val[40] = 'c';
  c_data1_0_val[41] = 'c';
  c_data1_0_val[42] = 'c';
  for (int i = 43; i <= 49; ++i)
    c_data1_0_val[i] = 0;
  c_data1_0_val[50] = 'f';
  c_data1_0_val[51] = 'g';
  c_data1_0_val[52] = 'g';
  c_data1_0_val[53] = 'h';
  c_data1_0_val[54] = 'h';
  c_data1_0_val[55] = 'h';
  CHECK(check_tile<uint8_t>(tile1_0.var_tile(), c_data1_0_val));

  // Test get tile 1
  WriterTile tile1_1(
      array_->array_->array_schema_latest(),
      array_->array_->array_schema_latest().domain().cell_num_per_tile(),
      true,
      false,
      1,
      Datatype::STRING_ASCII);
  CHECK(tiler1.get_tile(1, "a", tile1_1).ok());
  std::vector<uint64_t> c_data1_1_off(50);
  for (int i = 0; i <= 30; ++i)
    c_data1_1_off[i] = i;
  c_data1_1_off[31] = c_data1_1_off[30] + std::string("dddd").size();
  c_data1_1_off[32] = c_data1_1_off[31] + std::string("eeeee").size();
  for (int i = 33; i <= 40; ++i)
    c_data1_1_off[i] = c_data1_1_off[i - 1] + 1;
  c_data1_1_off[41] = c_data1_1_off[40] + std::string("iiii").size();
  c_data1_1_off[42] = c_data1_1_off[41] + std::string("ooooo").size();
  for (int i = 43; i <= 49; ++i)
    c_data1_1_off[i] = c_data1_1_off[i - 1] + 1;
  CHECK(check_tile<uint64_t>(tile1_1.offset_tile(), c_data1_1_off));
  std::vector<uint8_t> c_data1_1_val(64);
  for (int i = 0; i <= 29; ++i)
    c_data1_1_val[i] = 0;
  c_data1_1_val[30] = 'd';
  c_data1_1_val[31] = 'd';
  c_data1_1_val[32] = 'd';
  c_data1_1_val[33] = 'd';
  c_data1_1_val[34] = 'e';
  c_data1_1_val[35] = 'e';
  c_data1_1_val[36] = 'e';
  c_data1_1_val[37] = 'e';
  c_data1_1_val[38] = 'e';
  for (int i = 39; i <= 46; ++i)
    c_data1_1_val[i] = 0;
  c_data1_1_val[47] = 'i';
  c_data1_1_val[48] = 'i';
  c_data1_1_val[49] = 'i';
  c_data1_1_val[50] = 'i';
  c_data1_1_val[51] = 'j';
  c_data1_1_val[52] = 'j';
  c_data1_1_val[53] = 'j';
  c_data1_1_val[54] = 'j';
  c_data1_1_val[55] = 'j';
  for (int i = 56; i <= 63; ++i)
    c_data1_1_val[i] = 0;
  CHECK(check_tile<uint8_t>(tile1_1.var_tile(), c_data1_1_val));

  // Test get tile 2
  WriterTile tile1_2(
      array_->array_->array_schema_latest(),
      array_->array_->array_schema_latest().domain().cell_num_per_tile(),
      true,
      false,
      1,
      Datatype::STRING_ASCII);
  CHECK(tiler1.get_tile(2, "a", tile1_2).ok());
  std::vector<uint64_t> c_data1_2_off(50);
  for (int i = 0; i <= 7; ++i)
    c_data1_2_off[i] = i;
  c_data1_2_off[8] = c_data1_2_off[7] + std::string("k").size();
  c_data1_2_off[9] = c_data1_2_off[8] + std::string("ll").size();
  c_data1_2_off[10] = c_data1_2_off[9] + std::string("mmm").size();
  for (int i = 11; i <= 49; ++i)
    c_data1_2_off[i] = c_data1_2_off[i - 1] + 1;
  CHECK(check_tile<uint64_t>(tile1_2.offset_tile(), c_data1_2_off));
  std::vector<uint8_t> c_data1_2_val(53);
  for (int i = 0; i <= 6; ++i)
    c_data1_2_val[i] = 0;
  c_data1_2_val[7] = 'k';
  c_data1_2_val[8] = 'l';
  c_data1_2_val[9] = 'l';
  c_data1_2_val[10] = 'm';
  c_data1_2_val[11] = 'm';
  c_data1_2_val[12] = 'm';
  for (int i = 13; i <= 52; ++i)
    c_data1_2_val[i] = 0;
  CHECK(check_tile<uint8_t>(tile1_2.var_tile(), c_data1_2_val));

  // Test get tile 3
  WriterTile tile1_3(
      array_->array_->array_schema_latest(),
      array_->array_->array_schema_latest().domain().cell_num_per_tile(),
      true,
      false,
      1,
      Datatype::STRING_ASCII);
  CHECK(tiler1.get_tile(3, "a", tile1_3).ok());
  std::vector<uint64_t> c_data1_3_off(50);
  c_data1_3_off[0] = 0;
  c_data1_3_off[1] = 4;
  c_data1_3_off[2] = 9;
  for (int i = 3; i <= 49; ++i)
    c_data1_3_off[i] = c_data1_3_off[i - 1] + 1;
  CHECK(check_tile<uint64_t>(tile1_3.offset_tile(), c_data1_3_off));
  std::vector<uint8_t> c_data1_3_val(57);
  c_data1_3_val[0] = 'n';
  c_data1_3_val[1] = 'n';
  c_data1_3_val[2] = 'n';
  c_data1_3_val[3] = 'n';
  c_data1_3_val[4] = 'o';
  c_data1_3_val[5] = 'o';
  c_data1_3_val[6] = 'o';
  c_data1_3_val[7] = 'o';
  c_data1_3_val[8] = 'o';
  for (int i = 9; i < 57; ++i)
    c_data1_3_val[i] = 0;
  CHECK(check_tile<uint8_t>(tile1_3.var_tile(), c_data1_3_val));

  // Create subarray (single tile)
  close_array();
  open_array(array_name, TILEDB_READ);
  int32_t sub2_0[] = {3, 5};
  int32_t sub2_1[] = {13, 18};
  tiledb::sm::Subarray subarray2(
      array_->array_.get(),
      Layout::ROW_MAJOR,
      &test::g_helper_stats,
      test::g_helper_logger());
  add_ranges({sub2_0, sub2_1}, sizeof(sub2_0), &subarray2);

  // Create DenseTiler
  buff_a = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18};
  buff_a_off = {
      0, 1, 3, 6, 10, 15, 16, 18, 21, 25, 30, 31, 33, 36, 40, 45, 46, 48};
  buff_a_off_size = buff_a_off.size() * sizeof(uint64_t);
  buff_a_val = "abbcccddddeeeeefgghhhiiiijjjjjkllmmmnnnnooooopqqr";
  buff_a_val_size = buff_a_val.size();
  buffers["a"] = QueryBuffer(
      &buff_a_off[0], &buff_a_val[0], &buff_a_off_size, &buff_a_val_size);
  DenseTiler<int32_t> tiler2(&buffers, &subarray2, &test::g_helper_stats);

  // Test get tile 0
  WriterTile tile2_0(
      array_->array_->array_schema_latest(),
      array_->array_->array_schema_latest().domain().cell_num_per_tile(),
      true,
      false,
      1,
      Datatype::STRING_ASCII);
  CHECK(tiler2.get_tile(0, "a", tile2_0).ok());
  std::vector<uint64_t> c_data2_0_off(50);
  for (int i = 0; i <= 22; ++i)
    c_data2_0_off[i] = i;
  c_data2_0_off[23] = c_data2_0_off[22] + std::string("a").size();
  c_data2_0_off[24] = c_data2_0_off[23] + std::string("bb").size();
  c_data2_0_off[25] = c_data2_0_off[24] + std::string("ccc").size();
  c_data2_0_off[26] = c_data2_0_off[25] + std::string("dddd").size();
  c_data2_0_off[27] = c_data2_0_off[26] + std::string("eeeee").size();
  c_data2_0_off[28] = c_data2_0_off[27] + std::string("f").size();
  for (int i = 29; i <= 32; ++i)
    c_data2_0_off[i] = c_data2_0_off[i - 1] + 1;
  c_data2_0_off[33] = c_data2_0_off[32] + std::string("gg").size();
  c_data2_0_off[34] = c_data2_0_off[33] + std::string("hhh").size();
  c_data2_0_off[35] = c_data2_0_off[34] + std::string("iiii").size();
  c_data2_0_off[36] = c_data2_0_off[35] + std::string("jjjjj").size();
  c_data2_0_off[37] = c_data2_0_off[36] + std::string("k").size();
  c_data2_0_off[38] = c_data2_0_off[37] + std::string("ll").size();
  for (int i = 39; i <= 42; ++i)
    c_data2_0_off[i] = c_data2_0_off[i - 1] + 1;
  c_data2_0_off[43] = c_data2_0_off[42] + std::string("mmm").size();
  c_data2_0_off[44] = c_data2_0_off[43] + std::string("nnnn").size();
  c_data2_0_off[45] = c_data2_0_off[44] + std::string("ooooo").size();
  c_data2_0_off[46] = c_data2_0_off[45] + std::string("p").size();
  c_data2_0_off[47] = c_data2_0_off[46] + std::string("qq").size();
  c_data2_0_off[48] = c_data2_0_off[47] + std::string("r").size();
  c_data2_0_off[49] = c_data2_0_off[48] + 1;
  CHECK(check_tile<uint64_t>(tile2_0.offset_tile(), c_data2_0_off));
  std::vector<uint8_t> c_data2_0_val(81);
  for (int i = 0; i <= 21; ++i)
    c_data2_0_val[i] = 0;
  c_data2_0_val[22] = 'a';
  c_data2_0_val[23] = 'b';
  c_data2_0_val[24] = 'b';
  c_data2_0_val[25] = 'c';
  c_data2_0_val[26] = 'c';
  c_data2_0_val[27] = 'c';
  c_data2_0_val[28] = 'd';
  c_data2_0_val[29] = 'd';
  c_data2_0_val[30] = 'd';
  c_data2_0_val[31] = 'd';
  c_data2_0_val[32] = 'e';
  c_data2_0_val[33] = 'e';
  c_data2_0_val[34] = 'e';
  c_data2_0_val[35] = 'e';
  c_data2_0_val[36] = 'e';
  c_data2_0_val[37] = 'f';
  for (int i = 38; i <= 41; ++i)
    c_data2_0_val[i] = 0;
  c_data2_0_val[42] = 'g';
  c_data2_0_val[43] = 'g';
  c_data2_0_val[44] = 'h';
  c_data2_0_val[45] = 'h';
  c_data2_0_val[46] = 'h';
  c_data2_0_val[47] = 'i';
  c_data2_0_val[48] = 'i';
  c_data2_0_val[49] = 'i';
  c_data2_0_val[50] = 'i';
  c_data2_0_val[51] = 'j';
  c_data2_0_val[52] = 'j';
  c_data2_0_val[53] = 'j';
  c_data2_0_val[54] = 'j';
  c_data2_0_val[55] = 'j';
  c_data2_0_val[56] = 'k';
  c_data2_0_val[57] = 'l';
  c_data2_0_val[58] = 'l';
  for (int i = 59; i <= 62; ++i)
    c_data2_0_val[i] = 0;
  c_data2_0_val[63] = 'm';
  c_data2_0_val[64] = 'm';
  c_data2_0_val[65] = 'm';
  c_data2_0_val[66] = 'n';
  c_data2_0_val[67] = 'n';
  c_data2_0_val[68] = 'n';
  c_data2_0_val[69] = 'n';
  c_data2_0_val[70] = 'o';
  c_data2_0_val[71] = 'o';
  c_data2_0_val[72] = 'o';
  c_data2_0_val[73] = 'o';
  c_data2_0_val[74] = 'o';
  c_data2_0_val[75] = 'p';
  c_data2_0_val[76] = 'q';
  c_data2_0_val[77] = 'q';
  c_data2_0_val[78] = 'r';
  for (int i = 79; i <= 80; ++i)
    c_data2_0_val[i] = 0;
  CHECK(check_tile<uint8_t>(tile2_0.var_tile(), c_data2_0_val));

  // Clean up
  close_array();
  remove_array(array_name);
}

TEST_CASE_METHOD(
    DenseTilerFx,
    "DenseTiler: Test get tile, var int32, 2D, (row, row)",
    "[DenseTiler][get_tile][2d][var][a-int32][row-row]") {
  // Create array
  std::string array_name = "dense_tiler";
  int32_t d_dom_1[] = {1, 10};
  int32_t d_ext_1 = 5;
  int32_t d_dom_2[] = {1, 30};
  int32_t d_ext_2 = 10;
  create_array(
      array_name,
      {{"d1", TILEDB_INT32, d_dom_1, &d_ext_1},
       {"d2", TILEDB_INT32, d_dom_2, &d_ext_2}},
      {{"a", TILEDB_INT32, TILEDB_VAR_NUM, false}},
      TILEDB_ROW_MAJOR,
      TILEDB_ROW_MAJOR);

  // Create buffers
  std::unordered_map<std::string, QueryBuffer> buffers;
  std::vector<int32_t> buff_a = {
      1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15};
  (void)buff_a;  // Used only as a reference
  std::vector<uint64_t> buff_a_off = {0,
                                      1 * sizeof(int32_t),
                                      3 * sizeof(int32_t),
                                      6 * sizeof(int32_t),
                                      10 * sizeof(int32_t),
                                      15 * sizeof(int32_t),
                                      16 * sizeof(int32_t),
                                      18 * sizeof(int32_t),
                                      21 * sizeof(int32_t),
                                      25 * sizeof(int32_t),
                                      30 * sizeof(int32_t),
                                      31 * sizeof(int32_t),
                                      33 * sizeof(int32_t),
                                      36 * sizeof(int32_t),
                                      40 * sizeof(int32_t)};
  uint64_t buff_a_off_size = buff_a_off.size() * sizeof(uint64_t);
  std::vector<int32_t> buff_a_val = {
      1,  2,  2,  3,  3,  3,  4,  4,  4,  4,  5,  5,  5,  5,  5,
      6,  7,  7,  8,  8,  8,  9,  9,  9,  9,  10, 10, 10, 10, 10,
      11, 12, 12, 13, 13, 13, 14, 14, 14, 14, 15, 15, 15, 15, 15};
  uint64_t buff_a_val_size = buff_a_val.size() * sizeof(int32_t);
  buffers["a"] = QueryBuffer(
      &buff_a_off[0], &buff_a_val[0], &buff_a_off_size, &buff_a_val_size);

  // Create subarray (multiple tiles)
  open_array(array_name, TILEDB_READ);
  int32_t sub1_0[] = {4, 6};
  int32_t sub1_1[] = {18, 22};
  tiledb::sm::Subarray subarray1(
      array_->array_.get(),
      Layout::ROW_MAJOR,
      &test::g_helper_stats,
      test::g_helper_logger());
  add_ranges({sub1_0, sub1_1}, sizeof(sub1_0), &subarray1);

  // Create DenseTiler
  DenseTiler<int32_t> tiler1(&buffers, &subarray1, &test::g_helper_stats);

  // Test get tile 0
  WriterTile tile1_0(
      array_->array_->array_schema_latest(),
      array_->array_->array_schema_latest().domain().cell_num_per_tile(),
      true,
      false,
      1,
      Datatype::INT32);
  CHECK(tiler1.get_tile(0, "a", tile1_0).ok());
  std::vector<uint64_t> c_data1_0_off(50);
  for (int i = 0; i <= 37; ++i)
    c_data1_0_off[i] = i * sizeof(int32_t);
  c_data1_0_off[38] = c_data1_0_off[37] + sizeof(int32_t);
  c_data1_0_off[39] = c_data1_0_off[38] + 2 * sizeof(int32_t);
  c_data1_0_off[40] = c_data1_0_off[39] + 3 * sizeof(int32_t);
  for (int i = 41; i <= 46; ++i)
    c_data1_0_off[i] = c_data1_0_off[i - 1] + sizeof(int32_t);
  c_data1_0_off[47] = c_data1_0_off[46] + sizeof(int32_t);
  c_data1_0_off[48] = c_data1_0_off[47] + sizeof(int32_t);
  c_data1_0_off[49] = c_data1_0_off[48] + 2 * sizeof(int32_t);
  CHECK(check_tile<uint64_t>(tile1_0.offset_tile(), c_data1_0_off));
  std::vector<int32_t> c_data1_0_val(56);
  for (int i = 0; i <= 36; ++i)
    c_data1_0_val[i] = 0;
  c_data1_0_val[37] = 1;
  c_data1_0_val[38] = 2;
  c_data1_0_val[39] = 2;
  c_data1_0_val[40] = 3;
  c_data1_0_val[41] = 3;
  c_data1_0_val[42] = 3;
  for (int i = 43; i <= 49; ++i)
    c_data1_0_val[i] = 0;
  c_data1_0_val[50] = 6;
  c_data1_0_val[51] = 7;
  c_data1_0_val[52] = 7;
  c_data1_0_val[53] = 8;
  c_data1_0_val[54] = 8;
  c_data1_0_val[55] = 8;
  CHECK(check_tile<int32_t>(tile1_0.var_tile(), c_data1_0_val));

  // Test get tile 1
  WriterTile tile1_1(
      array_->array_->array_schema_latest(),
      array_->array_->array_schema_latest().domain().cell_num_per_tile(),
      true,
      false,
      1,
      Datatype::INT32);
  CHECK(tiler1.get_tile(1, "a", tile1_1).ok());
  std::vector<uint64_t> c_data1_1_off(50);
  for (int i = 0; i <= 30; ++i)
    c_data1_1_off[i] = i * sizeof(int32_t);
  c_data1_1_off[31] = c_data1_1_off[30] + 4 * sizeof(int32_t);
  c_data1_1_off[32] = c_data1_1_off[31] + 5 * sizeof(int32_t);
  for (int i = 33; i <= 40; ++i)
    c_data1_1_off[i] = c_data1_1_off[i - 1] + sizeof(int32_t);
  c_data1_1_off[41] = c_data1_1_off[40] + 4 * sizeof(int32_t);
  c_data1_1_off[42] = c_data1_1_off[41] + 5 * sizeof(int32_t);
  for (int i = 43; i <= 49; ++i)
    c_data1_1_off[i] = c_data1_1_off[i - 1] + sizeof(int32_t);
  CHECK(check_tile<uint64_t>(tile1_1.offset_tile(), c_data1_1_off));
  std::vector<int32_t> c_data1_1_val(64);
  for (int i = 0; i <= 29; ++i)
    c_data1_1_val[i] = 0;
  c_data1_1_val[30] = 4;
  c_data1_1_val[31] = 4;
  c_data1_1_val[32] = 4;
  c_data1_1_val[33] = 4;
  c_data1_1_val[34] = 5;
  c_data1_1_val[35] = 5;
  c_data1_1_val[36] = 5;
  c_data1_1_val[37] = 5;
  c_data1_1_val[38] = 5;
  for (int i = 39; i <= 46; ++i)
    c_data1_1_val[i] = 0;
  c_data1_1_val[47] = 9;
  c_data1_1_val[48] = 9;
  c_data1_1_val[49] = 9;
  c_data1_1_val[50] = 9;
  c_data1_1_val[51] = 10;
  c_data1_1_val[52] = 10;
  c_data1_1_val[53] = 10;
  c_data1_1_val[54] = 10;
  c_data1_1_val[55] = 10;
  for (int i = 56; i <= 63; ++i)
    c_data1_1_val[i] = 0;
  CHECK(check_tile<int32_t>(tile1_1.var_tile(), c_data1_1_val));

  // Test get tile 2
  WriterTile tile1_2(
      array_->array_->array_schema_latest(),
      array_->array_->array_schema_latest().domain().cell_num_per_tile(),
      true,
      false,
      1,
      Datatype::INT32);
  CHECK(tiler1.get_tile(2, "a", tile1_2).ok());
  std::vector<uint64_t> c_data1_2_off(50);
  for (int i = 0; i <= 7; ++i)
    c_data1_2_off[i] = i * sizeof(int32_t);
  c_data1_2_off[8] = c_data1_2_off[7] + sizeof(int32_t);
  c_data1_2_off[9] = c_data1_2_off[8] + 2 * sizeof(int32_t);
  c_data1_2_off[10] = c_data1_2_off[9] + 3 * sizeof(int32_t);
  for (int i = 11; i <= 49; ++i)
    c_data1_2_off[i] = c_data1_2_off[i - 1] + sizeof(int32_t);
  CHECK(check_tile<uint64_t>(tile1_2.offset_tile(), c_data1_2_off));
  std::vector<int32_t> c_data1_2_val(53);
  for (int i = 0; i <= 6; ++i)
    c_data1_2_val[i] = 0;
  c_data1_2_val[7] = 11;
  c_data1_2_val[8] = 12;
  c_data1_2_val[9] = 12;
  c_data1_2_val[10] = 13;
  c_data1_2_val[11] = 13;
  c_data1_2_val[12] = 13;
  for (int i = 13; i <= 52; ++i)
    c_data1_2_val[i] = 0;
  CHECK(check_tile<int32_t>(tile1_2.var_tile(), c_data1_2_val));

  // Test get tile 3
  WriterTile tile1_3(
      array_->array_->array_schema_latest(),
      array_->array_->array_schema_latest().domain().cell_num_per_tile(),
      true,
      false,
      1,
      Datatype::INT32);
  CHECK(tiler1.get_tile(3, "a", tile1_3).ok());
  std::vector<uint64_t> c_data1_3_off(50);
  c_data1_3_off[0] = 0;
  c_data1_3_off[1] = 4 * sizeof(int32_t);
  c_data1_3_off[2] = 9 * sizeof(int32_t);
  for (int i = 3; i <= 49; ++i)
    c_data1_3_off[i] = c_data1_3_off[i - 1] + sizeof(int32_t);
  CHECK(check_tile<uint64_t>(tile1_3.offset_tile(), c_data1_3_off));
  std::vector<int32_t> c_data1_3_val(57);
  c_data1_3_val[0] = 14;
  c_data1_3_val[1] = 14;
  c_data1_3_val[2] = 14;
  c_data1_3_val[3] = 14;
  c_data1_3_val[4] = 15;
  c_data1_3_val[5] = 15;
  c_data1_3_val[6] = 15;
  c_data1_3_val[7] = 15;
  c_data1_3_val[8] = 15;
  for (int i = 9; i < 57; ++i)
    c_data1_3_val[i] = 0;
  CHECK(check_tile<int32_t>(tile1_3.var_tile(), c_data1_3_val));

  // Create subarray (single tile)
  close_array();
  open_array(array_name, TILEDB_READ);
  int32_t sub2_0[] = {3, 5};
  int32_t sub2_1[] = {13, 18};
  tiledb::sm::Subarray subarray2(
      array_->array_.get(),
      Layout::ROW_MAJOR,
      &test::g_helper_stats,
      test::g_helper_logger());
  add_ranges({sub2_0, sub2_1}, sizeof(sub2_0), &subarray2);

  // Create DenseTiler
  buff_a = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18};
  buff_a_off = {0,
                1 * sizeof(int32_t),
                3 * sizeof(int32_t),
                6 * sizeof(int32_t),
                10 * sizeof(int32_t),
                15 * sizeof(int32_t),
                16 * sizeof(int32_t),
                18 * sizeof(int32_t),
                21 * sizeof(int32_t),
                25 * sizeof(int32_t),
                30 * sizeof(int32_t),
                31 * sizeof(int32_t),
                33 * sizeof(int32_t),
                36 * sizeof(int32_t),
                40 * sizeof(int32_t),
                45 * sizeof(int32_t),
                46 * sizeof(int32_t),
                48 * sizeof(int32_t)};
  buff_a_off_size = buff_a_off.size() * sizeof(uint64_t);
  buff_a_val = {1,  2,  2,  3,  3,  3,  4,  4,  4,  4,  5,  5,  5,
                5,  5,  6,  7,  7,  8,  8,  8,  9,  9,  9,  9,  10,
                10, 10, 10, 10, 11, 12, 12, 13, 13, 13, 14, 14, 14,
                14, 15, 15, 15, 15, 15, 16, 17, 17, 18};
  buff_a_val_size = buff_a_val.size() * sizeof(int32_t);
  buffers["a"] = QueryBuffer(
      &buff_a_off[0], &buff_a_val[0], &buff_a_off_size, &buff_a_val_size);
  DenseTiler<int32_t> tiler2(&buffers, &subarray2, &test::g_helper_stats);

  // Test get tile 0
  WriterTile tile2_0(
      array_->array_->array_schema_latest(),
      array_->array_->array_schema_latest().domain().cell_num_per_tile(),
      true,
      false,
      1,
      Datatype::STRING_ASCII);
  CHECK(tiler2.get_tile(0, "a", tile2_0).ok());
  std::vector<uint64_t> c_data2_0_off(50);
  for (int i = 0; i <= 22; ++i)
    c_data2_0_off[i] = i * sizeof(int32_t);
  c_data2_0_off[23] = c_data2_0_off[22] + sizeof(int32_t);
  c_data2_0_off[24] = c_data2_0_off[23] + 2 * sizeof(int32_t);
  c_data2_0_off[25] = c_data2_0_off[24] + 3 * sizeof(int32_t);
  c_data2_0_off[26] = c_data2_0_off[25] + 4 * sizeof(int32_t);
  c_data2_0_off[27] = c_data2_0_off[26] + 5 * sizeof(int32_t);
  c_data2_0_off[28] = c_data2_0_off[27] + sizeof(int32_t);
  for (int i = 29; i <= 32; ++i)
    c_data2_0_off[i] = c_data2_0_off[i - 1] + sizeof(int32_t);
  c_data2_0_off[33] = c_data2_0_off[32] + 2 * sizeof(int32_t);
  c_data2_0_off[34] = c_data2_0_off[33] + 3 * sizeof(int32_t);
  c_data2_0_off[35] = c_data2_0_off[34] + 4 * sizeof(int32_t);
  c_data2_0_off[36] = c_data2_0_off[35] + 5 * sizeof(int32_t);
  c_data2_0_off[37] = c_data2_0_off[36] + sizeof(int32_t);
  c_data2_0_off[38] = c_data2_0_off[37] + 2 * sizeof(int32_t);
  for (int i = 39; i <= 42; ++i)
    c_data2_0_off[i] = c_data2_0_off[i - 1] + sizeof(int32_t);
  c_data2_0_off[43] = c_data2_0_off[42] + 3 * sizeof(int32_t);
  c_data2_0_off[44] = c_data2_0_off[43] + 4 * sizeof(int32_t);
  c_data2_0_off[45] = c_data2_0_off[44] + 5 * sizeof(int32_t);
  c_data2_0_off[46] = c_data2_0_off[45] + sizeof(int32_t);
  c_data2_0_off[47] = c_data2_0_off[46] + 2 * sizeof(int32_t);
  c_data2_0_off[48] = c_data2_0_off[47] + sizeof(int32_t);
  c_data2_0_off[49] = c_data2_0_off[48] + sizeof(int32_t);
  CHECK(check_tile<uint64_t>(tile2_0.offset_tile(), c_data2_0_off));
  std::vector<int32_t> c_data2_0_val(81);
  for (int i = 0; i <= 21; ++i)
    c_data2_0_val[i] = 0;
  c_data2_0_val[22] = 1;
  c_data2_0_val[23] = 2;
  c_data2_0_val[24] = 2;
  c_data2_0_val[25] = 3;
  c_data2_0_val[26] = 3;
  c_data2_0_val[27] = 3;
  c_data2_0_val[28] = 4;
  c_data2_0_val[29] = 4;
  c_data2_0_val[30] = 4;
  c_data2_0_val[31] = 4;
  c_data2_0_val[32] = 5;
  c_data2_0_val[33] = 5;
  c_data2_0_val[34] = 5;
  c_data2_0_val[35] = 5;
  c_data2_0_val[36] = 5;
  c_data2_0_val[37] = 6;
  for (int i = 38; i <= 41; ++i)
    c_data2_0_val[i] = 0;
  c_data2_0_val[42] = 7;
  c_data2_0_val[43] = 7;
  c_data2_0_val[44] = 8;
  c_data2_0_val[45] = 8;
  c_data2_0_val[46] = 8;
  c_data2_0_val[47] = 9;
  c_data2_0_val[48] = 9;
  c_data2_0_val[49] = 9;
  c_data2_0_val[50] = 9;
  c_data2_0_val[51] = 10;
  c_data2_0_val[52] = 10;
  c_data2_0_val[53] = 10;
  c_data2_0_val[54] = 10;
  c_data2_0_val[55] = 10;
  c_data2_0_val[56] = 11;
  c_data2_0_val[57] = 12;
  c_data2_0_val[58] = 12;
  for (int i = 59; i <= 62; ++i)
    c_data2_0_val[i] = 0;
  c_data2_0_val[63] = 13;
  c_data2_0_val[64] = 13;
  c_data2_0_val[65] = 13;
  c_data2_0_val[66] = 14;
  c_data2_0_val[67] = 14;
  c_data2_0_val[68] = 14;
  c_data2_0_val[69] = 14;
  c_data2_0_val[70] = 15;
  c_data2_0_val[71] = 15;
  c_data2_0_val[72] = 15;
  c_data2_0_val[73] = 15;
  c_data2_0_val[74] = 15;
  c_data2_0_val[75] = 16;
  c_data2_0_val[76] = 17;
  c_data2_0_val[77] = 17;
  c_data2_0_val[78] = 18;
  for (int i = 79; i <= 80; ++i)
    c_data2_0_val[i] = 0;
  CHECK(check_tile<int32_t>(tile2_0.var_tile(), c_data2_0_val));

  // Clean up
  close_array();
  remove_array(array_name);
}

TEST_CASE_METHOD(
    DenseTilerFx,
    "DenseTiler: Test get tile, var int32, extra element, 2D, (row, row)",
    "[DenseTiler][get_tile][2d][var][a-int32][extra-element][row-row]") {
  // Create array
  std::string array_name = "dense_tiler";
  int32_t d_dom_1[] = {1, 10};
  int32_t d_ext_1 = 5;
  int32_t d_dom_2[] = {1, 30};
  int32_t d_ext_2 = 10;
  create_array(
      array_name,
      {{"d1", TILEDB_INT32, d_dom_1, &d_ext_1},
       {"d2", TILEDB_INT32, d_dom_2, &d_ext_2}},
      {{"a", TILEDB_INT32, TILEDB_VAR_NUM, false}},
      TILEDB_ROW_MAJOR,
      TILEDB_ROW_MAJOR);

  // Create buffers
  std::unordered_map<std::string, QueryBuffer> buffers;
  std::vector<int32_t> buff_a = {
      1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15};
  (void)buff_a;  // Used only as a reference
  std::vector<uint64_t> buff_a_off = {0,
                                      1 * sizeof(int32_t),
                                      3 * sizeof(int32_t),
                                      6 * sizeof(int32_t),
                                      10 * sizeof(int32_t),
                                      15 * sizeof(int32_t),
                                      16 * sizeof(int32_t),
                                      18 * sizeof(int32_t),
                                      21 * sizeof(int32_t),
                                      25 * sizeof(int32_t),
                                      30 * sizeof(int32_t),
                                      31 * sizeof(int32_t),
                                      33 * sizeof(int32_t),
                                      36 * sizeof(int32_t),
                                      40 * sizeof(int32_t),
                                      45 * sizeof(int32_t)};  // Extra element
  uint64_t buff_a_off_size = buff_a_off.size() * sizeof(uint64_t);
  std::vector<int32_t> buff_a_val = {
      1,  2,  2,  3,  3,  3,  4,  4,  4,  4,  5,  5,  5,  5,  5,
      6,  7,  7,  8,  8,  8,  9,  9,  9,  9,  10, 10, 10, 10, 10,
      11, 12, 12, 13, 13, 13, 14, 14, 14, 14, 15, 15, 15, 15, 15};
  uint64_t buff_a_val_size = buff_a_val.size() * sizeof(int32_t);
  buffers["a"] = QueryBuffer(
      &buff_a_off[0], &buff_a_val[0], &buff_a_off_size, &buff_a_val_size);

  // Create subarray (multiple tiles)
  open_array(array_name, TILEDB_READ);
  int32_t sub1_0[] = {4, 6};
  int32_t sub1_1[] = {18, 22};
  tiledb::sm::Subarray subarray1(
      array_->array_.get(),
      Layout::ROW_MAJOR,
      &test::g_helper_stats,
      test::g_helper_logger());
  add_ranges({sub1_0, sub1_1}, sizeof(sub1_0), &subarray1);

  // Create DenseTiler
  DenseTiler<int32_t> tiler1(
      &buffers, &subarray1, &test::g_helper_stats, "bytes", 64, true);

  // Test get tile 0
  WriterTile tile1_0(
      array_->array_->array_schema_latest(),
      array_->array_->array_schema_latest().domain().cell_num_per_tile(),
      true,
      false,
      1,
      Datatype::INT32);
  CHECK(tiler1.get_tile(0, "a", tile1_0).ok());
  std::vector<uint64_t> c_data1_0_off(50);
  for (int i = 0; i <= 37; ++i)
    c_data1_0_off[i] = i * sizeof(int32_t);
  c_data1_0_off[38] = c_data1_0_off[37] + sizeof(int32_t);
  c_data1_0_off[39] = c_data1_0_off[38] + 2 * sizeof(int32_t);
  c_data1_0_off[40] = c_data1_0_off[39] + 3 * sizeof(int32_t);
  for (int i = 41; i <= 46; ++i)
    c_data1_0_off[i] = c_data1_0_off[i - 1] + sizeof(int32_t);
  c_data1_0_off[47] = c_data1_0_off[46] + sizeof(int32_t);
  c_data1_0_off[48] = c_data1_0_off[47] + sizeof(int32_t);
  c_data1_0_off[49] = c_data1_0_off[48] + 2 * sizeof(int32_t);
  CHECK(check_tile<uint64_t>(tile1_0.offset_tile(), c_data1_0_off));
  std::vector<int32_t> c_data1_0_val(56);
  for (int i = 0; i <= 36; ++i)
    c_data1_0_val[i] = 0;
  c_data1_0_val[37] = 1;
  c_data1_0_val[38] = 2;
  c_data1_0_val[39] = 2;
  c_data1_0_val[40] = 3;
  c_data1_0_val[41] = 3;
  c_data1_0_val[42] = 3;
  for (int i = 43; i <= 49; ++i)
    c_data1_0_val[i] = 0;
  c_data1_0_val[50] = 6;
  c_data1_0_val[51] = 7;
  c_data1_0_val[52] = 7;
  c_data1_0_val[53] = 8;
  c_data1_0_val[54] = 8;
  c_data1_0_val[55] = 8;
  CHECK(check_tile<int32_t>(tile1_0.var_tile(), c_data1_0_val));

  // Test get tile 1
  WriterTile tile1_1(
      array_->array_->array_schema_latest(),
      array_->array_->array_schema_latest().domain().cell_num_per_tile(),
      true,
      false,
      1,
      Datatype::INT32);
  CHECK(tiler1.get_tile(1, "a", tile1_1).ok());
  std::vector<uint64_t> c_data1_1_off(50);
  for (int i = 0; i <= 30; ++i)
    c_data1_1_off[i] = i * sizeof(int32_t);
  c_data1_1_off[31] = c_data1_1_off[30] + 4 * sizeof(int32_t);
  c_data1_1_off[32] = c_data1_1_off[31] + 5 * sizeof(int32_t);
  for (int i = 33; i <= 40; ++i)
    c_data1_1_off[i] = c_data1_1_off[i - 1] + sizeof(int32_t);
  c_data1_1_off[41] = c_data1_1_off[40] + 4 * sizeof(int32_t);
  c_data1_1_off[42] = c_data1_1_off[41] + 5 * sizeof(int32_t);
  for (int i = 43; i <= 49; ++i)
    c_data1_1_off[i] = c_data1_1_off[i - 1] + sizeof(int32_t);
  CHECK(check_tile<uint64_t>(tile1_1.offset_tile(), c_data1_1_off));
  std::vector<int32_t> c_data1_1_val(64);
  for (int i = 0; i <= 29; ++i)
    c_data1_1_val[i] = 0;
  c_data1_1_val[30] = 4;
  c_data1_1_val[31] = 4;
  c_data1_1_val[32] = 4;
  c_data1_1_val[33] = 4;
  c_data1_1_val[34] = 5;
  c_data1_1_val[35] = 5;
  c_data1_1_val[36] = 5;
  c_data1_1_val[37] = 5;
  c_data1_1_val[38] = 5;
  for (int i = 39; i <= 46; ++i)
    c_data1_1_val[i] = 0;
  c_data1_1_val[47] = 9;
  c_data1_1_val[48] = 9;
  c_data1_1_val[49] = 9;
  c_data1_1_val[50] = 9;
  c_data1_1_val[51] = 10;
  c_data1_1_val[52] = 10;
  c_data1_1_val[53] = 10;
  c_data1_1_val[54] = 10;
  c_data1_1_val[55] = 10;
  for (int i = 56; i <= 63; ++i)
    c_data1_1_val[i] = 0;
  CHECK(check_tile<int32_t>(tile1_1.var_tile(), c_data1_1_val));

  // Test get tile 2
  WriterTile tile1_2(
      array_->array_->array_schema_latest(),
      array_->array_->array_schema_latest().domain().cell_num_per_tile(),
      true,
      false,
      1,
      Datatype::INT32);
  CHECK(tiler1.get_tile(2, "a", tile1_2).ok());
  std::vector<uint64_t> c_data1_2_off(50);
  for (int i = 0; i <= 7; ++i)
    c_data1_2_off[i] = i * sizeof(int32_t);
  c_data1_2_off[8] = c_data1_2_off[7] + sizeof(int32_t);
  c_data1_2_off[9] = c_data1_2_off[8] + 2 * sizeof(int32_t);
  c_data1_2_off[10] = c_data1_2_off[9] + 3 * sizeof(int32_t);
  for (int i = 11; i <= 49; ++i)
    c_data1_2_off[i] = c_data1_2_off[i - 1] + sizeof(int32_t);
  CHECK(check_tile<uint64_t>(tile1_2.offset_tile(), c_data1_2_off));
  std::vector<int32_t> c_data1_2_val(53);
  for (int i = 0; i <= 6; ++i)
    c_data1_2_val[i] = 0;
  c_data1_2_val[7] = 11;
  c_data1_2_val[8] = 12;
  c_data1_2_val[9] = 12;
  c_data1_2_val[10] = 13;
  c_data1_2_val[11] = 13;
  c_data1_2_val[12] = 13;
  for (int i = 13; i <= 52; ++i)
    c_data1_2_val[i] = 0;
  CHECK(check_tile<int32_t>(tile1_2.var_tile(), c_data1_2_val));

  // Test get tile 3
  WriterTile tile1_3(
      array_->array_->array_schema_latest(),
      array_->array_->array_schema_latest().domain().cell_num_per_tile(),
      true,
      false,
      1,
      Datatype::INT32);
  CHECK(tiler1.get_tile(3, "a", tile1_3).ok());
  std::vector<uint64_t> c_data1_3_off(50);
  c_data1_3_off[0] = 0;
  c_data1_3_off[1] = 4 * sizeof(int32_t);
  c_data1_3_off[2] = 9 * sizeof(int32_t);
  for (int i = 3; i <= 49; ++i)
    c_data1_3_off[i] = c_data1_3_off[i - 1] + sizeof(int32_t);
  CHECK(check_tile<uint64_t>(tile1_3.offset_tile(), c_data1_3_off));
  std::vector<int32_t> c_data1_3_val(57);
  c_data1_3_val[0] = 14;
  c_data1_3_val[1] = 14;
  c_data1_3_val[2] = 14;
  c_data1_3_val[3] = 14;
  c_data1_3_val[4] = 15;
  c_data1_3_val[5] = 15;
  c_data1_3_val[6] = 15;
  c_data1_3_val[7] = 15;
  c_data1_3_val[8] = 15;
  for (int i = 9; i < 57; ++i)
    c_data1_3_val[i] = 0;
  CHECK(check_tile<int32_t>(tile1_3.var_tile(), c_data1_3_val));

  // Create subarray (single tile)
  close_array();
  open_array(array_name, TILEDB_READ);
  int32_t sub2_0[] = {3, 5};
  int32_t sub2_1[] = {13, 18};
  tiledb::sm::Subarray subarray2(
      array_->array_.get(),
      Layout::ROW_MAJOR,
      &test::g_helper_stats,
      test::g_helper_logger());
  add_ranges({sub2_0, sub2_1}, sizeof(sub2_0), &subarray2);

  // Create DenseTiler
  buff_a = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18};
  buff_a_off = {0,
                1 * sizeof(int32_t),
                3 * sizeof(int32_t),
                6 * sizeof(int32_t),
                10 * sizeof(int32_t),
                15 * sizeof(int32_t),
                16 * sizeof(int32_t),
                18 * sizeof(int32_t),
                21 * sizeof(int32_t),
                25 * sizeof(int32_t),
                30 * sizeof(int32_t),
                31 * sizeof(int32_t),
                33 * sizeof(int32_t),
                36 * sizeof(int32_t),
                40 * sizeof(int32_t),
                45 * sizeof(int32_t),
                46 * sizeof(int32_t),
                48 * sizeof(int32_t),
                49 * sizeof(int32_t)};  // Extra element
  buff_a_off_size = buff_a_off.size() * sizeof(uint64_t);
  buff_a_val = {1,  2,  2,  3,  3,  3,  4,  4,  4,  4,  5,  5,  5,
                5,  5,  6,  7,  7,  8,  8,  8,  9,  9,  9,  9,  10,
                10, 10, 10, 10, 11, 12, 12, 13, 13, 13, 14, 14, 14,
                14, 15, 15, 15, 15, 15, 16, 17, 17, 18};
  buff_a_val_size = buff_a_val.size() * sizeof(int32_t);
  buffers["a"] = QueryBuffer(
      &buff_a_off[0], &buff_a_val[0], &buff_a_off_size, &buff_a_val_size);
  DenseTiler<int32_t> tiler2(
      &buffers, &subarray2, &test::g_helper_stats, "bytes", 64, true);

  // Test get tile 0
  WriterTile tile2_0(
      array_->array_->array_schema_latest(),
      array_->array_->array_schema_latest().domain().cell_num_per_tile(),
      true,
      false,
      1,
      Datatype::INT32);
  CHECK(tiler2.get_tile(0, "a", tile2_0).ok());
  std::vector<uint64_t> c_data2_0_off(50);
  for (int i = 0; i <= 22; ++i)
    c_data2_0_off[i] = i * sizeof(int32_t);
  c_data2_0_off[23] = c_data2_0_off[22] + sizeof(int32_t);
  c_data2_0_off[24] = c_data2_0_off[23] + 2 * sizeof(int32_t);
  c_data2_0_off[25] = c_data2_0_off[24] + 3 * sizeof(int32_t);
  c_data2_0_off[26] = c_data2_0_off[25] + 4 * sizeof(int32_t);
  c_data2_0_off[27] = c_data2_0_off[26] + 5 * sizeof(int32_t);
  c_data2_0_off[28] = c_data2_0_off[27] + sizeof(int32_t);
  for (int i = 29; i <= 32; ++i)
    c_data2_0_off[i] = c_data2_0_off[i - 1] + sizeof(int32_t);
  c_data2_0_off[33] = c_data2_0_off[32] + 2 * sizeof(int32_t);
  c_data2_0_off[34] = c_data2_0_off[33] + 3 * sizeof(int32_t);
  c_data2_0_off[35] = c_data2_0_off[34] + 4 * sizeof(int32_t);
  c_data2_0_off[36] = c_data2_0_off[35] + 5 * sizeof(int32_t);
  c_data2_0_off[37] = c_data2_0_off[36] + sizeof(int32_t);
  c_data2_0_off[38] = c_data2_0_off[37] + 2 * sizeof(int32_t);
  for (int i = 39; i <= 42; ++i)
    c_data2_0_off[i] = c_data2_0_off[i - 1] + sizeof(int32_t);
  c_data2_0_off[43] = c_data2_0_off[42] + 3 * sizeof(int32_t);
  c_data2_0_off[44] = c_data2_0_off[43] + 4 * sizeof(int32_t);
  c_data2_0_off[45] = c_data2_0_off[44] + 5 * sizeof(int32_t);
  c_data2_0_off[46] = c_data2_0_off[45] + sizeof(int32_t);
  c_data2_0_off[47] = c_data2_0_off[46] + 2 * sizeof(int32_t);
  c_data2_0_off[48] = c_data2_0_off[47] + sizeof(int32_t);
  c_data2_0_off[49] = c_data2_0_off[48] + sizeof(int32_t);
  CHECK(check_tile<uint64_t>(tile2_0.offset_tile(), c_data2_0_off));
  std::vector<int32_t> c_data2_0_val(81);
  for (int i = 0; i <= 21; ++i)
    c_data2_0_val[i] = 0;
  c_data2_0_val[22] = 1;
  c_data2_0_val[23] = 2;
  c_data2_0_val[24] = 2;
  c_data2_0_val[25] = 3;
  c_data2_0_val[26] = 3;
  c_data2_0_val[27] = 3;
  c_data2_0_val[28] = 4;
  c_data2_0_val[29] = 4;
  c_data2_0_val[30] = 4;
  c_data2_0_val[31] = 4;
  c_data2_0_val[32] = 5;
  c_data2_0_val[33] = 5;
  c_data2_0_val[34] = 5;
  c_data2_0_val[35] = 5;
  c_data2_0_val[36] = 5;
  c_data2_0_val[37] = 6;
  for (int i = 38; i <= 41; ++i)
    c_data2_0_val[i] = 0;
  c_data2_0_val[42] = 7;
  c_data2_0_val[43] = 7;
  c_data2_0_val[44] = 8;
  c_data2_0_val[45] = 8;
  c_data2_0_val[46] = 8;
  c_data2_0_val[47] = 9;
  c_data2_0_val[48] = 9;
  c_data2_0_val[49] = 9;
  c_data2_0_val[50] = 9;
  c_data2_0_val[51] = 10;
  c_data2_0_val[52] = 10;
  c_data2_0_val[53] = 10;
  c_data2_0_val[54] = 10;
  c_data2_0_val[55] = 10;
  c_data2_0_val[56] = 11;
  c_data2_0_val[57] = 12;
  c_data2_0_val[58] = 12;
  for (int i = 59; i <= 62; ++i)
    c_data2_0_val[i] = 0;
  c_data2_0_val[63] = 13;
  c_data2_0_val[64] = 13;
  c_data2_0_val[65] = 13;
  c_data2_0_val[66] = 14;
  c_data2_0_val[67] = 14;
  c_data2_0_val[68] = 14;
  c_data2_0_val[69] = 14;
  c_data2_0_val[70] = 15;
  c_data2_0_val[71] = 15;
  c_data2_0_val[72] = 15;
  c_data2_0_val[73] = 15;
  c_data2_0_val[74] = 15;
  c_data2_0_val[75] = 16;
  c_data2_0_val[76] = 17;
  c_data2_0_val[77] = 17;
  c_data2_0_val[78] = 18;
  for (int i = 79; i <= 80; ++i)
    c_data2_0_val[i] = 0;
  CHECK(check_tile<int32_t>(tile2_0.var_tile(), c_data2_0_val));

  // Clean up
  close_array();
  remove_array(array_name);
}

TEST_CASE_METHOD(
    DenseTilerFx,
    "DenseTiler: Test get tile, var int32, elements, 2D, (row, row)",
    "[DenseTiler][get_tile][2d][var][a-int32][elements][row-row]") {
  // Create array
  std::string array_name = "dense_tiler";
  int32_t d_dom_1[] = {1, 10};
  int32_t d_ext_1 = 5;
  int32_t d_dom_2[] = {1, 30};
  int32_t d_ext_2 = 10;
  create_array(
      array_name,
      {{"d1", TILEDB_INT32, d_dom_1, &d_ext_1},
       {"d2", TILEDB_INT32, d_dom_2, &d_ext_2}},
      {{"a", TILEDB_INT32, TILEDB_VAR_NUM, false}},
      TILEDB_ROW_MAJOR,
      TILEDB_ROW_MAJOR);

  // Create buffers
  std::unordered_map<std::string, QueryBuffer> buffers;
  std::vector<int32_t> buff_a = {
      1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15};
  (void)buff_a;  // Used only as a reference
  std::vector<uint64_t> buff_a_off = {
      0, 1, 3, 6, 10, 15, 16, 18, 21, 25, 30, 31, 33, 36, 40};
  uint64_t buff_a_off_size = buff_a_off.size() * sizeof(uint64_t);
  std::vector<int32_t> buff_a_val = {
      1,  2,  2,  3,  3,  3,  4,  4,  4,  4,  5,  5,  5,  5,  5,
      6,  7,  7,  8,  8,  8,  9,  9,  9,  9,  10, 10, 10, 10, 10,
      11, 12, 12, 13, 13, 13, 14, 14, 14, 14, 15, 15, 15, 15, 15};
  uint64_t buff_a_val_size = buff_a_val.size() * sizeof(int32_t);
  buffers["a"] = QueryBuffer(
      &buff_a_off[0], &buff_a_val[0], &buff_a_off_size, &buff_a_val_size);

  // Create subarray (multiple tiles)
  open_array(array_name, TILEDB_READ);
  int32_t sub1_0[] = {4, 6};
  int32_t sub1_1[] = {18, 22};
  tiledb::sm::Subarray subarray1(
      array_->array_.get(),
      Layout::ROW_MAJOR,
      &test::g_helper_stats,
      test::g_helper_logger());
  add_ranges({sub1_0, sub1_1}, sizeof(sub1_0), &subarray1);

  // Create DenseTiler
  DenseTiler<int32_t> tiler1(
      &buffers, &subarray1, &test::g_helper_stats, "elements", 64, false);

  // Test get tile 0
  WriterTile tile1_0(
      array_->array_->array_schema_latest(),
      array_->array_->array_schema_latest().domain().cell_num_per_tile(),
      true,
      false,
      1,
      Datatype::INT32);
  CHECK(tiler1.get_tile(0, "a", tile1_0).ok());
  std::vector<uint64_t> c_data1_0_off(50);
  for (int i = 0; i <= 37; ++i)
    c_data1_0_off[i] = i * sizeof(int32_t);
  c_data1_0_off[38] = c_data1_0_off[37] + sizeof(int32_t);
  c_data1_0_off[39] = c_data1_0_off[38] + 2 * sizeof(int32_t);
  c_data1_0_off[40] = c_data1_0_off[39] + 3 * sizeof(int32_t);
  for (int i = 41; i <= 46; ++i)
    c_data1_0_off[i] = c_data1_0_off[i - 1] + sizeof(int32_t);
  c_data1_0_off[47] = c_data1_0_off[46] + sizeof(int32_t);
  c_data1_0_off[48] = c_data1_0_off[47] + sizeof(int32_t);
  c_data1_0_off[49] = c_data1_0_off[48] + 2 * sizeof(int32_t);
  CHECK(check_tile<uint64_t>(tile1_0.offset_tile(), c_data1_0_off));
  std::vector<int32_t> c_data1_0_val(56);
  for (int i = 0; i <= 36; ++i)
    c_data1_0_val[i] = 0;
  c_data1_0_val[37] = 1;
  c_data1_0_val[38] = 2;
  c_data1_0_val[39] = 2;
  c_data1_0_val[40] = 3;
  c_data1_0_val[41] = 3;
  c_data1_0_val[42] = 3;
  for (int i = 43; i <= 49; ++i)
    c_data1_0_val[i] = 0;
  c_data1_0_val[50] = 6;
  c_data1_0_val[51] = 7;
  c_data1_0_val[52] = 7;
  c_data1_0_val[53] = 8;
  c_data1_0_val[54] = 8;
  c_data1_0_val[55] = 8;
  CHECK(check_tile<int32_t>(tile1_0.var_tile(), c_data1_0_val));

  // Test get tile 1
  WriterTile tile1_1(
      array_->array_->array_schema_latest(),
      array_->array_->array_schema_latest().domain().cell_num_per_tile(),
      true,
      false,
      1,
      Datatype::INT32);
  CHECK(tiler1.get_tile(1, "a", tile1_1).ok());
  std::vector<uint64_t> c_data1_1_off(50);
  for (int i = 0; i <= 30; ++i)
    c_data1_1_off[i] = i * sizeof(int32_t);
  c_data1_1_off[31] = c_data1_1_off[30] + 4 * sizeof(int32_t);
  c_data1_1_off[32] = c_data1_1_off[31] + 5 * sizeof(int32_t);
  for (int i = 33; i <= 40; ++i)
    c_data1_1_off[i] = c_data1_1_off[i - 1] + sizeof(int32_t);
  c_data1_1_off[41] = c_data1_1_off[40] + 4 * sizeof(int32_t);
  c_data1_1_off[42] = c_data1_1_off[41] + 5 * sizeof(int32_t);
  for (int i = 43; i <= 49; ++i)
    c_data1_1_off[i] = c_data1_1_off[i - 1] + sizeof(int32_t);
  CHECK(check_tile<uint64_t>(tile1_1.offset_tile(), c_data1_1_off));
  std::vector<int32_t> c_data1_1_val(64);
  for (int i = 0; i <= 29; ++i)
    c_data1_1_val[i] = 0;
  c_data1_1_val[30] = 4;
  c_data1_1_val[31] = 4;
  c_data1_1_val[32] = 4;
  c_data1_1_val[33] = 4;
  c_data1_1_val[34] = 5;
  c_data1_1_val[35] = 5;
  c_data1_1_val[36] = 5;
  c_data1_1_val[37] = 5;
  c_data1_1_val[38] = 5;
  for (int i = 39; i <= 46; ++i)
    c_data1_1_val[i] = 0;
  c_data1_1_val[47] = 9;
  c_data1_1_val[48] = 9;
  c_data1_1_val[49] = 9;
  c_data1_1_val[50] = 9;
  c_data1_1_val[51] = 10;
  c_data1_1_val[52] = 10;
  c_data1_1_val[53] = 10;
  c_data1_1_val[54] = 10;
  c_data1_1_val[55] = 10;
  for (int i = 56; i <= 63; ++i)
    c_data1_1_val[i] = 0;
  CHECK(check_tile<int32_t>(tile1_1.var_tile(), c_data1_1_val));

  // Test get tile 2
  WriterTile tile1_2(
      array_->array_->array_schema_latest(),
      array_->array_->array_schema_latest().domain().cell_num_per_tile(),
      true,
      false,
      1,
      Datatype::INT32);
  CHECK(tiler1.get_tile(2, "a", tile1_2).ok());
  std::vector<uint64_t> c_data1_2_off(50);
  for (int i = 0; i <= 7; ++i)
    c_data1_2_off[i] = i * sizeof(int32_t);
  c_data1_2_off[8] = c_data1_2_off[7] + sizeof(int32_t);
  c_data1_2_off[9] = c_data1_2_off[8] + 2 * sizeof(int32_t);
  c_data1_2_off[10] = c_data1_2_off[9] + 3 * sizeof(int32_t);
  for (int i = 11; i <= 49; ++i)
    c_data1_2_off[i] = c_data1_2_off[i - 1] + sizeof(int32_t);
  CHECK(check_tile<uint64_t>(tile1_2.offset_tile(), c_data1_2_off));
  std::vector<int32_t> c_data1_2_val(53);
  for (int i = 0; i <= 6; ++i)
    c_data1_2_val[i] = 0;
  c_data1_2_val[7] = 11;
  c_data1_2_val[8] = 12;
  c_data1_2_val[9] = 12;
  c_data1_2_val[10] = 13;
  c_data1_2_val[11] = 13;
  c_data1_2_val[12] = 13;
  for (int i = 13; i <= 52; ++i)
    c_data1_2_val[i] = 0;
  CHECK(check_tile<int32_t>(tile1_2.var_tile(), c_data1_2_val));

  // Test get tile 3
  WriterTile tile1_3(
      array_->array_->array_schema_latest(),
      array_->array_->array_schema_latest().domain().cell_num_per_tile(),
      true,
      false,
      1,
      Datatype::INT32);
  CHECK(tiler1.get_tile(3, "a", tile1_3).ok());
  std::vector<uint64_t> c_data1_3_off(50);
  c_data1_3_off[0] = 0;
  c_data1_3_off[1] = 4 * sizeof(int32_t);
  c_data1_3_off[2] = 9 * sizeof(int32_t);
  for (int i = 3; i <= 49; ++i)
    c_data1_3_off[i] = c_data1_3_off[i - 1] + sizeof(int32_t);
  CHECK(check_tile<uint64_t>(tile1_3.offset_tile(), c_data1_3_off));
  std::vector<int32_t> c_data1_3_val(57);
  c_data1_3_val[0] = 14;
  c_data1_3_val[1] = 14;
  c_data1_3_val[2] = 14;
  c_data1_3_val[3] = 14;
  c_data1_3_val[4] = 15;
  c_data1_3_val[5] = 15;
  c_data1_3_val[6] = 15;
  c_data1_3_val[7] = 15;
  c_data1_3_val[8] = 15;
  for (int i = 9; i < 57; ++i)
    c_data1_3_val[i] = 0;
  CHECK(check_tile<int32_t>(tile1_3.var_tile(), c_data1_3_val));

  // Create subarray (single tile)
  close_array();
  open_array(array_name, TILEDB_READ);
  int32_t sub2_0[] = {3, 5};
  int32_t sub2_1[] = {13, 18};
  tiledb::sm::Subarray subarray2(
      array_->array_.get(),
      Layout::ROW_MAJOR,
      &test::g_helper_stats,
      test::g_helper_logger());
  add_ranges({sub2_0, sub2_1}, sizeof(sub2_0), &subarray2);

  // Create DenseTiler
  buff_a = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18};
  buff_a_off = {
      0, 1, 3, 6, 10, 15, 16, 18, 21, 25, 30, 31, 33, 36, 40, 45, 46, 48};
  buff_a_off_size = buff_a_off.size() * sizeof(uint64_t);
  buff_a_val = {1,  2,  2,  3,  3,  3,  4,  4,  4,  4,  5,  5,  5,
                5,  5,  6,  7,  7,  8,  8,  8,  9,  9,  9,  9,  10,
                10, 10, 10, 10, 11, 12, 12, 13, 13, 13, 14, 14, 14,
                14, 15, 15, 15, 15, 15, 16, 17, 17, 18};
  buff_a_val_size = buff_a_val.size() * sizeof(int32_t);
  buffers["a"] = QueryBuffer(
      &buff_a_off[0], &buff_a_val[0], &buff_a_off_size, &buff_a_val_size);
  DenseTiler<int32_t> tiler2(
      &buffers, &subarray2, &test::g_helper_stats, "elements", 64, false);

  // Test get tile 0
  WriterTile tile2_0(
      array_->array_->array_schema_latest(),
      array_->array_->array_schema_latest().domain().cell_num_per_tile(),
      true,
      false,
      1,
      Datatype::INT32);
  CHECK(tiler2.get_tile(0, "a", tile2_0).ok());
  std::vector<uint64_t> c_data2_0_off(50);
  for (int i = 0; i <= 22; ++i)
    c_data2_0_off[i] = i * sizeof(int32_t);
  c_data2_0_off[23] = c_data2_0_off[22] + sizeof(int32_t);
  c_data2_0_off[24] = c_data2_0_off[23] + 2 * sizeof(int32_t);
  c_data2_0_off[25] = c_data2_0_off[24] + 3 * sizeof(int32_t);
  c_data2_0_off[26] = c_data2_0_off[25] + 4 * sizeof(int32_t);
  c_data2_0_off[27] = c_data2_0_off[26] + 5 * sizeof(int32_t);
  c_data2_0_off[28] = c_data2_0_off[27] + sizeof(int32_t);
  for (int i = 29; i <= 32; ++i)
    c_data2_0_off[i] = c_data2_0_off[i - 1] + sizeof(int32_t);
  c_data2_0_off[33] = c_data2_0_off[32] + 2 * sizeof(int32_t);
  c_data2_0_off[34] = c_data2_0_off[33] + 3 * sizeof(int32_t);
  c_data2_0_off[35] = c_data2_0_off[34] + 4 * sizeof(int32_t);
  c_data2_0_off[36] = c_data2_0_off[35] + 5 * sizeof(int32_t);
  c_data2_0_off[37] = c_data2_0_off[36] + sizeof(int32_t);
  c_data2_0_off[38] = c_data2_0_off[37] + 2 * sizeof(int32_t);
  for (int i = 39; i <= 42; ++i)
    c_data2_0_off[i] = c_data2_0_off[i - 1] + sizeof(int32_t);
  c_data2_0_off[43] = c_data2_0_off[42] + 3 * sizeof(int32_t);
  c_data2_0_off[44] = c_data2_0_off[43] + 4 * sizeof(int32_t);
  c_data2_0_off[45] = c_data2_0_off[44] + 5 * sizeof(int32_t);
  c_data2_0_off[46] = c_data2_0_off[45] + sizeof(int32_t);
  c_data2_0_off[47] = c_data2_0_off[46] + 2 * sizeof(int32_t);
  c_data2_0_off[48] = c_data2_0_off[47] + sizeof(int32_t);
  c_data2_0_off[49] = c_data2_0_off[48] + sizeof(int32_t);
  CHECK(check_tile<uint64_t>(tile2_0.offset_tile(), c_data2_0_off));
  std::vector<int32_t> c_data2_0_val(81);
  for (int i = 0; i <= 21; ++i)
    c_data2_0_val[i] = 0;
  c_data2_0_val[22] = 1;
  c_data2_0_val[23] = 2;
  c_data2_0_val[24] = 2;
  c_data2_0_val[25] = 3;
  c_data2_0_val[26] = 3;
  c_data2_0_val[27] = 3;
  c_data2_0_val[28] = 4;
  c_data2_0_val[29] = 4;
  c_data2_0_val[30] = 4;
  c_data2_0_val[31] = 4;
  c_data2_0_val[32] = 5;
  c_data2_0_val[33] = 5;
  c_data2_0_val[34] = 5;
  c_data2_0_val[35] = 5;
  c_data2_0_val[36] = 5;
  c_data2_0_val[37] = 6;
  for (int i = 38; i <= 41; ++i)
    c_data2_0_val[i] = 0;
  c_data2_0_val[42] = 7;
  c_data2_0_val[43] = 7;
  c_data2_0_val[44] = 8;
  c_data2_0_val[45] = 8;
  c_data2_0_val[46] = 8;
  c_data2_0_val[47] = 9;
  c_data2_0_val[48] = 9;
  c_data2_0_val[49] = 9;
  c_data2_0_val[50] = 9;
  c_data2_0_val[51] = 10;
  c_data2_0_val[52] = 10;
  c_data2_0_val[53] = 10;
  c_data2_0_val[54] = 10;
  c_data2_0_val[55] = 10;
  c_data2_0_val[56] = 11;
  c_data2_0_val[57] = 12;
  c_data2_0_val[58] = 12;
  for (int i = 59; i <= 62; ++i)
    c_data2_0_val[i] = 0;
  c_data2_0_val[63] = 13;
  c_data2_0_val[64] = 13;
  c_data2_0_val[65] = 13;
  c_data2_0_val[66] = 14;
  c_data2_0_val[67] = 14;
  c_data2_0_val[68] = 14;
  c_data2_0_val[69] = 14;
  c_data2_0_val[70] = 15;
  c_data2_0_val[71] = 15;
  c_data2_0_val[72] = 15;
  c_data2_0_val[73] = 15;
  c_data2_0_val[74] = 15;
  c_data2_0_val[75] = 16;
  c_data2_0_val[76] = 17;
  c_data2_0_val[77] = 17;
  c_data2_0_val[78] = 18;
  for (int i = 79; i <= 80; ++i)
    c_data2_0_val[i] = 0;
  CHECK(check_tile<int32_t>(tile2_0.var_tile(), c_data2_0_val));

  // Clean up
  close_array();
  remove_array(array_name);
}

TEST_CASE_METHOD(
    DenseTilerFx,
    "DenseTiler: Test get tile, var int32, elements, 32-bit, 2D, (row, row)",
    "[DenseTiler][get_tile][2d][var][a-int32][elements][32-bit][row-row]") {
  // Create array
  std::string array_name = "dense_tiler";
  int32_t d_dom_1[] = {1, 10};
  int32_t d_ext_1 = 5;
  int32_t d_dom_2[] = {1, 30};
  int32_t d_ext_2 = 10;
  create_array(
      array_name,
      {{"d1", TILEDB_INT32, d_dom_1, &d_ext_1},
       {"d2", TILEDB_INT32, d_dom_2, &d_ext_2}},
      {{"a", TILEDB_INT32, TILEDB_VAR_NUM, false}},
      TILEDB_ROW_MAJOR,
      TILEDB_ROW_MAJOR);

  // Create buffers
  std::unordered_map<std::string, QueryBuffer> buffers;
  std::vector<int32_t> buff_a = {
      1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15};
  (void)buff_a;  // Used only as a reference
  std::vector<uint32_t> buff_a_off = {
      0, 1, 3, 6, 10, 15, 16, 18, 21, 25, 30, 31, 33, 36, 40};
  uint64_t buff_a_off_size = buff_a_off.size() * sizeof(uint64_t);
  std::vector<int32_t> buff_a_val = {
      1,  2,  2,  3,  3,  3,  4,  4,  4,  4,  5,  5,  5,  5,  5,
      6,  7,  7,  8,  8,  8,  9,  9,  9,  9,  10, 10, 10, 10, 10,
      11, 12, 12, 13, 13, 13, 14, 14, 14, 14, 15, 15, 15, 15, 15};
  uint64_t buff_a_val_size = buff_a_val.size() * sizeof(int32_t);
  buffers["a"] = QueryBuffer(
      &buff_a_off[0], &buff_a_val[0], &buff_a_off_size, &buff_a_val_size);

  // Create subarray (multiple tiles)
  open_array(array_name, TILEDB_READ);
  int32_t sub1_0[] = {4, 6};
  int32_t sub1_1[] = {18, 22};
  tiledb::sm::Subarray subarray1(
      array_->array_.get(),
      Layout::ROW_MAJOR,
      &test::g_helper_stats,
      test::g_helper_logger());
  add_ranges({sub1_0, sub1_1}, sizeof(sub1_0), &subarray1);

  // Create DenseTiler
  DenseTiler<int32_t> tiler1(
      &buffers, &subarray1, &test::g_helper_stats, "elements", 32, false);

  // Test get tile 0
  WriterTile tile1_0(
      array_->array_->array_schema_latest(),
      array_->array_->array_schema_latest().domain().cell_num_per_tile(),
      true,
      false,
      1,
      Datatype::INT32);
  CHECK(tiler1.get_tile(0, "a", tile1_0).ok());
  std::vector<uint64_t> c_data1_0_off(50);
  for (int i = 0; i <= 37; ++i)
    c_data1_0_off[i] = i * sizeof(int32_t);
  c_data1_0_off[38] = c_data1_0_off[37] + sizeof(int32_t);
  c_data1_0_off[39] = c_data1_0_off[38] + 2 * sizeof(int32_t);
  c_data1_0_off[40] = c_data1_0_off[39] + 3 * sizeof(int32_t);
  for (int i = 41; i <= 46; ++i)
    c_data1_0_off[i] = c_data1_0_off[i - 1] + sizeof(int32_t);
  c_data1_0_off[47] = c_data1_0_off[46] + sizeof(int32_t);
  c_data1_0_off[48] = c_data1_0_off[47] + sizeof(int32_t);
  c_data1_0_off[49] = c_data1_0_off[48] + 2 * sizeof(int32_t);
  CHECK(check_tile<uint64_t>(tile1_0.offset_tile(), c_data1_0_off));
  std::vector<int32_t> c_data1_0_val(56);
  for (int i = 0; i <= 36; ++i)
    c_data1_0_val[i] = 0;
  c_data1_0_val[37] = 1;
  c_data1_0_val[38] = 2;
  c_data1_0_val[39] = 2;
  c_data1_0_val[40] = 3;
  c_data1_0_val[41] = 3;
  c_data1_0_val[42] = 3;
  for (int i = 43; i <= 49; ++i)
    c_data1_0_val[i] = 0;
  c_data1_0_val[50] = 6;
  c_data1_0_val[51] = 7;
  c_data1_0_val[52] = 7;
  c_data1_0_val[53] = 8;
  c_data1_0_val[54] = 8;
  c_data1_0_val[55] = 8;
  CHECK(check_tile<int32_t>(tile1_0.var_tile(), c_data1_0_val));

  // Test get tile 1
  WriterTile tile1_1(
      array_->array_->array_schema_latest(),
      array_->array_->array_schema_latest().domain().cell_num_per_tile(),
      true,
      false,
      1,
      Datatype::INT32);
  CHECK(tiler1.get_tile(1, "a", tile1_1).ok());
  std::vector<uint64_t> c_data1_1_off(50);
  for (int i = 0; i <= 30; ++i)
    c_data1_1_off[i] = i * sizeof(int32_t);
  c_data1_1_off[31] = c_data1_1_off[30] + 4 * sizeof(int32_t);
  c_data1_1_off[32] = c_data1_1_off[31] + 5 * sizeof(int32_t);
  for (int i = 33; i <= 40; ++i)
    c_data1_1_off[i] = c_data1_1_off[i - 1] + sizeof(int32_t);
  c_data1_1_off[41] = c_data1_1_off[40] + 4 * sizeof(int32_t);
  c_data1_1_off[42] = c_data1_1_off[41] + 5 * sizeof(int32_t);
  for (int i = 43; i <= 49; ++i)
    c_data1_1_off[i] = c_data1_1_off[i - 1] + sizeof(int32_t);
  CHECK(check_tile<uint64_t>(tile1_1.offset_tile(), c_data1_1_off));
  std::vector<int32_t> c_data1_1_val(64);
  for (int i = 0; i <= 29; ++i)
    c_data1_1_val[i] = 0;
  c_data1_1_val[30] = 4;
  c_data1_1_val[31] = 4;
  c_data1_1_val[32] = 4;
  c_data1_1_val[33] = 4;
  c_data1_1_val[34] = 5;
  c_data1_1_val[35] = 5;
  c_data1_1_val[36] = 5;
  c_data1_1_val[37] = 5;
  c_data1_1_val[38] = 5;
  for (int i = 39; i <= 46; ++i)
    c_data1_1_val[i] = 0;
  c_data1_1_val[47] = 9;
  c_data1_1_val[48] = 9;
  c_data1_1_val[49] = 9;
  c_data1_1_val[50] = 9;
  c_data1_1_val[51] = 10;
  c_data1_1_val[52] = 10;
  c_data1_1_val[53] = 10;
  c_data1_1_val[54] = 10;
  c_data1_1_val[55] = 10;
  for (int i = 56; i <= 63; ++i)
    c_data1_1_val[i] = 0;
  CHECK(check_tile<int32_t>(tile1_1.var_tile(), c_data1_1_val));

  // Test get tile 2
  WriterTile tile1_2(
      array_->array_->array_schema_latest(),
      array_->array_->array_schema_latest().domain().cell_num_per_tile(),
      true,
      false,
      1,
      Datatype::INT32);
  CHECK(tiler1.get_tile(2, "a", tile1_2).ok());
  std::vector<uint64_t> c_data1_2_off(50);
  for (int i = 0; i <= 7; ++i)
    c_data1_2_off[i] = i * sizeof(int32_t);
  c_data1_2_off[8] = c_data1_2_off[7] + sizeof(int32_t);
  c_data1_2_off[9] = c_data1_2_off[8] + 2 * sizeof(int32_t);
  c_data1_2_off[10] = c_data1_2_off[9] + 3 * sizeof(int32_t);
  for (int i = 11; i <= 49; ++i)
    c_data1_2_off[i] = c_data1_2_off[i - 1] + sizeof(int32_t);
  CHECK(check_tile<uint64_t>(tile1_2.offset_tile(), c_data1_2_off));
  std::vector<int32_t> c_data1_2_val(53);
  for (int i = 0; i <= 6; ++i)
    c_data1_2_val[i] = 0;
  c_data1_2_val[7] = 11;
  c_data1_2_val[8] = 12;
  c_data1_2_val[9] = 12;
  c_data1_2_val[10] = 13;
  c_data1_2_val[11] = 13;
  c_data1_2_val[12] = 13;
  for (int i = 13; i <= 52; ++i)
    c_data1_2_val[i] = 0;
  CHECK(check_tile<int32_t>(tile1_2.var_tile(), c_data1_2_val));

  // Test get tile 3
  WriterTile tile1_3(
      array_->array_->array_schema_latest(),
      array_->array_->array_schema_latest().domain().cell_num_per_tile(),
      true,
      false,
      1,
      Datatype::INT32);
  CHECK(tiler1.get_tile(3, "a", tile1_3).ok());
  std::vector<uint64_t> c_data1_3_off(50);
  c_data1_3_off[0] = 0;
  c_data1_3_off[1] = 4 * sizeof(int32_t);
  c_data1_3_off[2] = 9 * sizeof(int32_t);
  for (int i = 3; i <= 49; ++i)
    c_data1_3_off[i] = c_data1_3_off[i - 1] + sizeof(int32_t);
  CHECK(check_tile<uint64_t>(tile1_3.offset_tile(), c_data1_3_off));
  std::vector<int32_t> c_data1_3_val(57);
  c_data1_3_val[0] = 14;
  c_data1_3_val[1] = 14;
  c_data1_3_val[2] = 14;
  c_data1_3_val[3] = 14;
  c_data1_3_val[4] = 15;
  c_data1_3_val[5] = 15;
  c_data1_3_val[6] = 15;
  c_data1_3_val[7] = 15;
  c_data1_3_val[8] = 15;
  for (int i = 9; i < 57; ++i)
    c_data1_3_val[i] = 0;
  CHECK(check_tile<int32_t>(tile1_3.var_tile(), c_data1_3_val));

  // Create subarray (single tile)
  close_array();
  open_array(array_name, TILEDB_READ);
  int32_t sub2_0[] = {3, 5};
  int32_t sub2_1[] = {13, 18};
  tiledb::sm::Subarray subarray2(
      array_->array_.get(),
      Layout::ROW_MAJOR,
      &test::g_helper_stats,
      test::g_helper_logger());
  add_ranges({sub2_0, sub2_1}, sizeof(sub2_0), &subarray2);

  // Create DenseTiler
  buff_a = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18};
  buff_a_off = {
      0, 1, 3, 6, 10, 15, 16, 18, 21, 25, 30, 31, 33, 36, 40, 45, 46, 48};
  buff_a_off_size = buff_a_off.size() * sizeof(uint64_t);
  buff_a_val = {1,  2,  2,  3,  3,  3,  4,  4,  4,  4,  5,  5,  5,
                5,  5,  6,  7,  7,  8,  8,  8,  9,  9,  9,  9,  10,
                10, 10, 10, 10, 11, 12, 12, 13, 13, 13, 14, 14, 14,
                14, 15, 15, 15, 15, 15, 16, 17, 17, 18};
  buff_a_val_size = buff_a_val.size() * sizeof(int32_t);
  buffers["a"] = QueryBuffer(
      &buff_a_off[0], &buff_a_val[0], &buff_a_off_size, &buff_a_val_size);
  DenseTiler<int32_t> tiler2(
      &buffers, &subarray2, &test::g_helper_stats, "elements", 32, false);

  // Test get tile 0
  WriterTile tile2_0(
      array_->array_->array_schema_latest(),
      array_->array_->array_schema_latest().domain().cell_num_per_tile(),
      true,
      false,
      1,
      Datatype::INT32);
  CHECK(tiler2.get_tile(0, "a", tile2_0).ok());
  std::vector<uint64_t> c_data2_0_off(50);
  for (int i = 0; i <= 22; ++i)
    c_data2_0_off[i] = i * sizeof(int32_t);
  c_data2_0_off[23] = c_data2_0_off[22] + sizeof(int32_t);
  c_data2_0_off[24] = c_data2_0_off[23] + 2 * sizeof(int32_t);
  c_data2_0_off[25] = c_data2_0_off[24] + 3 * sizeof(int32_t);
  c_data2_0_off[26] = c_data2_0_off[25] + 4 * sizeof(int32_t);
  c_data2_0_off[27] = c_data2_0_off[26] + 5 * sizeof(int32_t);
  c_data2_0_off[28] = c_data2_0_off[27] + sizeof(int32_t);
  for (int i = 29; i <= 32; ++i)
    c_data2_0_off[i] = c_data2_0_off[i - 1] + sizeof(int32_t);
  c_data2_0_off[33] = c_data2_0_off[32] + 2 * sizeof(int32_t);
  c_data2_0_off[34] = c_data2_0_off[33] + 3 * sizeof(int32_t);
  c_data2_0_off[35] = c_data2_0_off[34] + 4 * sizeof(int32_t);
  c_data2_0_off[36] = c_data2_0_off[35] + 5 * sizeof(int32_t);
  c_data2_0_off[37] = c_data2_0_off[36] + sizeof(int32_t);
  c_data2_0_off[38] = c_data2_0_off[37] + 2 * sizeof(int32_t);
  for (int i = 39; i <= 42; ++i)
    c_data2_0_off[i] = c_data2_0_off[i - 1] + sizeof(int32_t);
  c_data2_0_off[43] = c_data2_0_off[42] + 3 * sizeof(int32_t);
  c_data2_0_off[44] = c_data2_0_off[43] + 4 * sizeof(int32_t);
  c_data2_0_off[45] = c_data2_0_off[44] + 5 * sizeof(int32_t);
  c_data2_0_off[46] = c_data2_0_off[45] + sizeof(int32_t);
  c_data2_0_off[47] = c_data2_0_off[46] + 2 * sizeof(int32_t);
  c_data2_0_off[48] = c_data2_0_off[47] + sizeof(int32_t);
  c_data2_0_off[49] = c_data2_0_off[48] + sizeof(int32_t);
  CHECK(check_tile<uint64_t>(tile2_0.offset_tile(), c_data2_0_off));
  std::vector<int32_t> c_data2_0_val(81);
  for (int i = 0; i <= 21; ++i)
    c_data2_0_val[i] = 0;
  c_data2_0_val[22] = 1;
  c_data2_0_val[23] = 2;
  c_data2_0_val[24] = 2;
  c_data2_0_val[25] = 3;
  c_data2_0_val[26] = 3;
  c_data2_0_val[27] = 3;
  c_data2_0_val[28] = 4;
  c_data2_0_val[29] = 4;
  c_data2_0_val[30] = 4;
  c_data2_0_val[31] = 4;
  c_data2_0_val[32] = 5;
  c_data2_0_val[33] = 5;
  c_data2_0_val[34] = 5;
  c_data2_0_val[35] = 5;
  c_data2_0_val[36] = 5;
  c_data2_0_val[37] = 6;
  for (int i = 38; i <= 41; ++i)
    c_data2_0_val[i] = 0;
  c_data2_0_val[42] = 7;
  c_data2_0_val[43] = 7;
  c_data2_0_val[44] = 8;
  c_data2_0_val[45] = 8;
  c_data2_0_val[46] = 8;
  c_data2_0_val[47] = 9;
  c_data2_0_val[48] = 9;
  c_data2_0_val[49] = 9;
  c_data2_0_val[50] = 9;
  c_data2_0_val[51] = 10;
  c_data2_0_val[52] = 10;
  c_data2_0_val[53] = 10;
  c_data2_0_val[54] = 10;
  c_data2_0_val[55] = 10;
  c_data2_0_val[56] = 11;
  c_data2_0_val[57] = 12;
  c_data2_0_val[58] = 12;
  for (int i = 59; i <= 62; ++i)
    c_data2_0_val[i] = 0;
  c_data2_0_val[63] = 13;
  c_data2_0_val[64] = 13;
  c_data2_0_val[65] = 13;
  c_data2_0_val[66] = 14;
  c_data2_0_val[67] = 14;
  c_data2_0_val[68] = 14;
  c_data2_0_val[69] = 14;
  c_data2_0_val[70] = 15;
  c_data2_0_val[71] = 15;
  c_data2_0_val[72] = 15;
  c_data2_0_val[73] = 15;
  c_data2_0_val[74] = 15;
  c_data2_0_val[75] = 16;
  c_data2_0_val[76] = 17;
  c_data2_0_val[77] = 17;
  c_data2_0_val[78] = 18;
  for (int i = 79; i <= 80; ++i)
    c_data2_0_val[i] = 0;
  CHECK(check_tile<int32_t>(tile2_0.var_tile(), c_data2_0_val));

  // Clean up
  close_array();
  remove_array(array_name);
}
