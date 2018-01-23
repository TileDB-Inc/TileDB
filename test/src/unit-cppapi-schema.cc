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
#include "tiledb"

TEST_CASE("C++ API: Schema", "[cppapi]") {
  using namespace tdb;
  Context ctx;

  Domain domain(ctx);
  auto d1 = Dimension::create<int>(ctx, "d1", {{-100, 100}}, 10);
  auto d2 = Dimension::create<int>(ctx, "d2", {{0, 100}}, 5);
  domain << d1 << d2;

  auto a1 = Attribute::create<int>(ctx, "a1");
  auto a2 = Attribute::create<char>(ctx, "a2");
  auto a3 = Attribute::create<double>(ctx, "a3");
  a1.set_compressor({TILEDB_BLOSC, -1}).set_cell_val_num(1);
  a2.set_cell_val_num(TILEDB_VAR_NUM);
  a3.set_cell_val_num(2);

  SECTION("Array Schema") {
    ArraySchema schema(ctx, TILEDB_DENSE);
    schema << domain;
    schema << a1 << a2 << a3;
    schema.set_cell_order(TILEDB_ROW_MAJOR);
    schema.set_tile_order(TILEDB_COL_MAJOR);
    schema.set_offset_compressor({TILEDB_DOUBLE_DELTA, -1});
    schema.set_coord_compressor({TILEDB_ZSTD, -1});

    auto attrs = schema.attributes();
    CHECK(attrs.count("a1") == 1);
    CHECK(attrs.count("a2") == 1);
    CHECK(attrs.count("a3") == 1);
    REQUIRE(schema.num_attributes() == 3);
    CHECK(schema.attribute(0).name() == "a1");
    CHECK(schema.attribute(1).name() == "a2");
    CHECK(schema.attribute(2).name() == "a3");
    CHECK(schema.attribute("a1").compressor().compressor() == TILEDB_BLOSC);
    CHECK(schema.attribute("a2").cell_val_num() == TILEDB_VAR_NUM);
    CHECK(schema.attribute("a3").cell_val_num() == 2);

    auto dims = schema.domain().dimensions();
    REQUIRE(dims.size() == 2);
    CHECK(dims[0].name() == "d1");
    CHECK(dims[1].name() == "d2");
    CHECK_THROWS(dims[0].domain<uint32_t>());
    CHECK(dims[0].domain<int>().first == -100);
    CHECK(dims[0].domain<int>().second == 100);
    CHECK_THROWS(dims[0].extent<unsigned>());
    CHECK(dims[0].extent<int>() == 10);
  }

  SECTION("Map Schema") {
    MapSchema schema(ctx);
    schema << a1 << a2 << a3;

    auto attrs = schema.attributes();
    CHECK(attrs.count("a1") == 1);
    CHECK(attrs.count("a2") == 1);
    CHECK(attrs.count("a3") == 1);
    REQUIRE(schema.num_attributes() == 3);
    CHECK(schema.attribute(0).name() == "a1");
    CHECK(schema.attribute(1).name() == "a2");
    CHECK(schema.attribute(2).name() == "a3");
    CHECK(schema.attribute("a1").compressor().compressor() == TILEDB_BLOSC);
    CHECK(schema.attribute("a2").cell_val_num() == TILEDB_VAR_NUM);
    CHECK(schema.attribute("a3").cell_val_num() == 2);
  }


  // Types
  CHECK_THROWS(impl::type_check_attr<typename impl::type_from_native<int>::type>(a1, 2));
  CHECK_THROWS(impl::type_check_attr<typename impl::type_from_native<unsigned>::type>(a1, 1));
  CHECK_NOTHROW(impl::type_check_attr<typename impl::type_from_native<int>::type>(a1, 1));
}