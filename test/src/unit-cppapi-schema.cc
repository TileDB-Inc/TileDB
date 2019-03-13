/**
 * @file   unit-cppapi-schema.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017 TileDB Inc.
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
 * Tests the C++ API for schema related functions.
 */

#include "catch.hpp"
#include "tiledb/sm/cpp_api/tiledb"

TEST_CASE("C++ API: Schema", "[cppapi]") {
  using namespace tiledb;
  Context ctx;

  Domain dense_domain(ctx);
  auto id1 = Dimension::create<int>(ctx, "d1", {{-100, 100}}, 10);
  auto id2 = Dimension::create<int>(ctx, "d2", {{0, 100}}, 5);
  dense_domain.add_dimension(id1).add_dimension(id2);

  Domain sparse_domain(ctx);
  auto fd1 = Dimension::create<double>(ctx, "d1", {{-100.0, 100.0}}, 10.0);
  auto fd2 = Dimension::create<double>(ctx, "d2", {{-100.0, 100.0}}, 10.0);
  sparse_domain.add_dimensions(fd1, fd2);

  auto a1 = Attribute::create<int>(ctx, "a1");
  auto a2 = Attribute::create<std::string>(ctx, "a2");
  auto a3 = Attribute::create<std::array<double, 2>>(ctx, "a3");
  auto a4 = Attribute::create<std::vector<uint32_t>>(ctx, "a4");
  FilterList filters(ctx);
  filters.add_filter({ctx, TILEDB_FILTER_LZ4});
  a1.set_filter_list(filters);

  SECTION("Dense Array Schema") {
    ArraySchema schema(ctx, TILEDB_DENSE);
    // cannot have a floating point dense array domain
    CHECK_THROWS(schema.set_domain(sparse_domain));
    schema.set_domain(dense_domain);
    schema.add_attribute(a1);
    schema.add_attribute(a2);
    schema.add_attribute(a3);
    schema.add_attribute(a4);
    schema.set_cell_order(TILEDB_ROW_MAJOR);
    schema.set_tile_order(TILEDB_COL_MAJOR);

    FilterList offsets_filters(ctx);
    offsets_filters.add_filter({ctx, TILEDB_FILTER_DOUBLE_DELTA});
    schema.set_offsets_filter_list(offsets_filters);

    FilterList coords_filters(ctx);
    coords_filters.add_filter({ctx, TILEDB_FILTER_ZSTD});
    schema.set_coords_filter_list(coords_filters);

    auto attrs = schema.attributes();
    CHECK(attrs.count("a1") == 1);
    CHECK(attrs.count("a2") == 1);
    CHECK(attrs.count("a3") == 1);
    REQUIRE(schema.attribute_num() == 4);
    CHECK(schema.attribute(0).name() == "a1");
    CHECK(schema.attribute(1).name() == "a2");
    CHECK(schema.attribute(2).name() == "a3");
    CHECK(
        schema.attribute("a1").filter_list().filter(0).filter_type() ==
        TILEDB_FILTER_LZ4);
    CHECK(schema.attribute("a2").cell_val_num() == TILEDB_VAR_NUM);
    CHECK(schema.attribute("a3").cell_val_num() == 16);
    CHECK(schema.attribute("a4").cell_val_num() == TILEDB_VAR_NUM);
    CHECK(schema.attribute("a4").type() == TILEDB_UINT32);

    auto dims = schema.domain().dimensions();
    REQUIRE(dims.size() == 2);
    CHECK(dims[0].name() == "d1");
    CHECK(dims[1].name() == "d2");
    CHECK_THROWS(dims[0].domain<uint32_t>());
    CHECK(dims[0].domain<int>().first == -100);
    CHECK(dims[0].domain<int>().second == 100);
    CHECK_THROWS(dims[0].tile_extent<unsigned>());
    CHECK(dims[0].tile_extent<int>() == 10);

    CHECK(dense_domain.type() == TILEDB_INT32);
  }

  SECTION("Sparse Array Schema") {
    ArraySchema schema(ctx, TILEDB_SPARSE);
    schema.set_domain(sparse_domain);
    schema.add_attribute(a1);
    schema.add_attribute(a2);
    schema.add_attribute(a3);
    schema.add_attribute(a4);
    schema.set_cell_order(TILEDB_ROW_MAJOR);
    schema.set_tile_order(TILEDB_COL_MAJOR);

    FilterList offsets_filters(ctx);
    offsets_filters.add_filter({ctx, TILEDB_FILTER_DOUBLE_DELTA});
    schema.set_offsets_filter_list(offsets_filters);

    FilterList coords_filters(ctx);
    coords_filters.add_filter({ctx, TILEDB_FILTER_ZSTD});
    schema.set_coords_filter_list(coords_filters);

    auto attrs = schema.attributes();
    CHECK(attrs.count("a1") == 1);
    CHECK(attrs.count("a2") == 1);
    CHECK(attrs.count("a3") == 1);
    REQUIRE(schema.attribute_num() == 4);
    CHECK(schema.attribute(0).name() == "a1");
    CHECK(schema.attribute(1).name() == "a2");
    CHECK(schema.attribute(2).name() == "a3");
    CHECK(
        schema.attribute("a1").filter_list().filter(0).filter_type() ==
        TILEDB_FILTER_LZ4);
    CHECK(schema.attribute("a2").cell_val_num() == TILEDB_VAR_NUM);
    CHECK(schema.attribute("a3").cell_val_num() == 16);
    CHECK(schema.attribute("a4").cell_val_num() == TILEDB_VAR_NUM);
    CHECK(schema.attribute("a4").type() == TILEDB_UINT32);

    auto dims = schema.domain().dimensions();
    REQUIRE(dims.size() == 2);
    CHECK(dims[0].name() == "d1");
    CHECK(dims[1].name() == "d2");
    CHECK_THROWS(dims[0].domain<float>());
    CHECK(dims[0].domain<double>().first == -100.0);
    CHECK(dims[0].domain<double>().second == 100.0);
    CHECK_THROWS(dims[0].tile_extent<unsigned>());
    CHECK(dims[0].tile_extent<double>() == 10.0);

    CHECK(sparse_domain.type() == TILEDB_FLOAT64);
  }

  SECTION("Map Schema") {
    MapSchema schema(ctx);
    schema.add_attribute(a1);
    schema.add_attribute(a2);
    schema.add_attribute(a3);
    schema.add_attribute(a4);

    auto attrs = schema.attributes();
    CHECK(attrs.count("a1") == 1);
    CHECK(attrs.count("a2") == 1);
    CHECK(attrs.count("a3") == 1);
    REQUIRE(schema.attribute_num() == 4);
    CHECK(schema.attribute(0).name() == "a1");
    CHECK(schema.attribute(1).name() == "a2");
    CHECK(schema.attribute(2).name() == "a3");
    CHECK(
        schema.attribute("a1").filter_list().filter(0).filter_type() ==
        TILEDB_FILTER_LZ4);
    CHECK(schema.attribute("a2").cell_val_num() == TILEDB_VAR_NUM);
    CHECK(schema.attribute("a3").cell_val_num() == 16);
    CHECK(schema.attribute("a4").cell_val_num() == TILEDB_VAR_NUM);
    CHECK(schema.attribute("a4").type() == TILEDB_UINT32);
  }

  SECTION("Invalid dimension types") {
    Domain dom(ctx);
    dom.add_dimension(Dimension::create<int>(ctx, "d1", {{0, 10}}));
    CHECK_THROWS_AS(
        dom.add_dimension(Dimension::create<uint64_t>(ctx, "d2", {{0, 10}})),
        TileDBError);
  }

  SECTION("Dimensions without tile extent") {
    Domain dom(ctx);
    dom.add_dimension(Dimension::create<int>(ctx, "d1", {{0, 10}}));
    ArraySchema schema(ctx, TILEDB_DENSE);
    schema.set_domain(dom);

    auto dom_get = schema.domain();
    CHECK(dom_get.dimensions()[0].tile_extent<int>() == 11);
  }
}

TEST_CASE("C++ API: Test schema virtual destructors", "[cppapi]") {
  tiledb::Context ctx;
  // Test that this generates no compiler warnings.
  std::unique_ptr<tiledb::ArraySchema> schema;
  std::unique_ptr<tiledb::MapSchema> map_schema;

  // Just instantiate them, don't care about runtime errors.
  schema.reset(new tiledb::ArraySchema(ctx, TILEDB_SPARSE));
  CHECK_THROWS_AS(
      map_schema.reset(new tiledb::MapSchema(ctx, "")), tiledb::TileDBError);
}
