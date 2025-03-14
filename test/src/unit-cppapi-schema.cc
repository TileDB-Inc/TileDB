/**
 * @file   unit-cppapi-schema.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2021 TileDB Inc.
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

#include <test/support/tdb_catch.h>
#include "test/support/src/coords_workaround.h"
#include "test/support/src/helpers.h"
#include "tiledb/sm/cpp_api/tiledb"
#include "tiledb/sm/cpp_api/tiledb_experimental"
#include "tiledb/sm/misc/constants.h"

#include <limits>

using namespace tiledb::test;

TEST_CASE("C++ API: Schema", "[cppapi][schema]") {
  using namespace tiledb;
  Context& ctx = vanilla_context_cpp();

  FilterList filters(ctx);
  filters.add_filter({ctx, TILEDB_FILTER_LZ4});

  Domain dense_domain(ctx);
  auto id1 = Dimension::create<int>(ctx, "d1", {{-100, 100}}, 10);
  auto id2 = Dimension::create<int>(ctx, "d2", {{0, 100}}, 5);
  CHECK_THROWS(id1.set_cell_val_num(4));
  CHECK_NOTHROW(id1.set_cell_val_num(1));
  CHECK_NOTHROW(id1.set_filter_list(filters));
  CHECK_NOTHROW(id1.filter_list());
  dense_domain.add_dimension(id1).add_dimension(id2);

  Domain sparse_domain(ctx);
  auto fd1 = Dimension::create<double>(ctx, "d1", {{-100.0, 100.0}}, 10.0);
  auto fd2 = Dimension::create<double>(ctx, "d2", {{-100.0, 100.0}}, 10.0);
  sparse_domain.add_dimensions(fd1, fd2);

  auto a1 = Attribute::create<int>(ctx, "a1");
  auto a2 = Attribute::create<std::string>(ctx, "a2");
  auto a3 = Attribute::create<std::array<double, 2>>(ctx, "a3");
  auto a4 = Attribute::create<std::vector<uint32_t>>(ctx, "a4");
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
    CHECK_THROWS(schema.set_cell_order(TILEDB_UNORDERED));
    CHECK_THROWS(schema.set_tile_order(TILEDB_UNORDERED));
    schema.set_cell_order(TILEDB_ROW_MAJOR);
    schema.set_tile_order(TILEDB_COL_MAJOR);
    CHECK_THROWS(schema.set_allows_dups(1));

    // Offsets filter list set/get
    FilterList offsets_filters(ctx);
    offsets_filters.add_filter({ctx, TILEDB_FILTER_DOUBLE_DELTA});
    schema.set_offsets_filter_list(offsets_filters);

    FilterList offsets_filters_back = schema.offsets_filter_list();
    CHECK(offsets_filters_back.nfilters() == 1);
    CHECK(
        offsets_filters_back.filter(0).filter_type() ==
        TILEDB_FILTER_DOUBLE_DELTA);

    // Validity filter list set/get
    FilterList validity_filters(ctx);
    validity_filters.add_filter({ctx, TILEDB_FILTER_BZIP2});
    schema.set_validity_filter_list(validity_filters);

    FilterList validity_filters_back = schema.validity_filter_list();
    CHECK(validity_filters_back.nfilters() == 1);
    auto validity_filter_back = validity_filters_back.filter(0);
    CHECK(validity_filter_back.filter_type() == TILEDB_FILTER_BZIP2);

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
    CHECK(schema.attribute("a3").cell_val_num() == 2);
    CHECK(schema.attribute("a4").cell_val_num() == TILEDB_VAR_NUM);
    CHECK(schema.attribute("a4").type() == TILEDB_UINT32);
    CHECK(schema.version() == tiledb::sm::constants::format_version);

    auto dims = schema.domain().dimensions();
    REQUIRE(dims.size() == 2);
    CHECK(dims[0].name() == "d1");
    CHECK(dims[1].name() == "d2");
    CHECK_THROWS(dims[0].domain<uint32_t>());
    CHECK(dims[0].domain<int>().first == -100);
    CHECK(dims[0].domain<int>().second == 100);
    CHECK_THROWS(dims[0].tile_extent<unsigned>());
    CHECK(dims[0].tile_extent<int>() == 10);

    auto d1 = schema.domain().dimension(0);
    CHECK(d1.name() == "d1");
    auto d2 = schema.domain().dimension(1);
    CHECK(d2.name() == "d2");
    auto d1_1 = schema.domain().dimension("d1");
    CHECK(d1_1.type() == TILEDB_INT32);
    CHECK(d1_1.name() == "d1");
    auto d2_1 = schema.domain().dimension("d2");
    CHECK(d2_1.type() == TILEDB_INT32);
    CHECK(d2_1.name() == "d2");
    CHECK_THROWS(schema.domain().dimension(2));
    CHECK_THROWS(schema.domain().dimension("foo"));
    CHECK(dense_domain.type() == TILEDB_INT32);
  }

  SECTION("Sparse Array Schema") {
    ArraySchema schema(ctx, TILEDB_SPARSE);
    schema.set_domain(sparse_domain);
    schema.add_attribute(a1);
    schema.add_attribute(a2);
    schema.add_attribute(a3);
    schema.add_attribute(a4);
    CHECK_THROWS(schema.set_cell_order(TILEDB_UNORDERED));
    CHECK_THROWS(schema.set_tile_order(TILEDB_UNORDERED));
    schema.set_cell_order(TILEDB_ROW_MAJOR);
    schema.set_tile_order(TILEDB_COL_MAJOR);
    schema.set_allows_dups(true);

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
    CHECK(schema.attribute("a3").cell_val_num() == 2);
    CHECK(schema.attribute("a4").cell_val_num() == TILEDB_VAR_NUM);
    CHECK(schema.attribute("a4").type() == TILEDB_UINT32);
    CHECK(schema.allows_dups() == true);
    CHECK(schema.version() == tiledb::sm::constants::format_version);

    auto dims = schema.domain().dimensions();
    REQUIRE(dims.size() == 2);
    CHECK(dims[0].name() == "d1");
    CHECK(dims[1].name() == "d2");
    CHECK_THROWS(dims[0].domain<float>());
    CHECK(dims[0].domain<double>().first == -100.0);
    CHECK(dims[0].domain<double>().second == 100.0);
    CHECK_THROWS(dims[0].tile_extent<unsigned>());
    CHECK(dims[0].tile_extent<double>() == 10.0);
    CHECK(dims[0].cell_val_num() == 1);

    CHECK(sparse_domain.type() == TILEDB_FLOAT64);
  }
}

TEST_CASE("C++ API: Test schema virtual destructors", "[cppapi][schema]") {
  tiledb::Context& ctx = vanilla_context_cpp();
  // Test that this generates no compiler warnings.
  std::unique_ptr<tiledb::ArraySchema> schema;

  // Just instantiate them, don't care about runtime errors.
  schema.reset(new tiledb::ArraySchema(ctx, TILEDB_SPARSE));
}

TEST_CASE(
    "C++ API: Test schema, heterogeneous domain, errors",
    "[cppapi][schema][heter][error]") {
  using namespace tiledb;
  tiledb::Context& ctx = vanilla_context_cpp();

  auto d1 = Dimension::create<float>(ctx, "d1", {{1.0f, 2.0f}}, .5f);
  auto d2 = Dimension::create<int32_t>(ctx, "d2", {{1, 2}}, 1);

  // Create domain
  Domain domain(ctx);
  domain.add_dimension(d1).add_dimension(d2);

  // Set domain to a dense array schema should error out
  ArraySchema dense_schema(ctx, TILEDB_DENSE);
  CHECK_THROWS(dense_schema.set_domain(domain));

  // Create sparse array
  ArraySchema sparse_schema(ctx, TILEDB_SPARSE);
  sparse_schema.set_domain(domain);
  auto a = Attribute::create<int32_t>(ctx, "a");
  sparse_schema.add_attribute(a);
  Array::create("sparse_array", sparse_schema);

  // Load array schema and get domain
  auto schema = Array::load_schema(ctx, "sparse_array");
  auto dom = schema.domain();

  // Get domain type should error out
  CHECK_THROWS(dom.type());

  // Check dimension types
  auto r_d1 = domain.dimension("d1");
  auto r_d2 = domain.dimension("d2");
  CHECK(r_d1.type() == TILEDB_FLOAT32);
  CHECK(r_d2.type() == TILEDB_INT32);

  // Open array
  Array array(ctx, "sparse_array", TILEDB_READ);

  // Get non-empty domain should error out
  CHECK_THROWS(array.non_empty_domain<int32_t>());
  std::vector<int32_t> subarray = {1, 2, 1, 3};

  // Query/subarray checks
  Query query(ctx, array, TILEDB_READ);
  Subarray sub(ctx, array);
  CHECK_THROWS(sub.set_subarray(subarray));
  std::vector<int32_t> buff = {1, 2, 4};
  CHECK_THROWS(query.set_data_buffer(tiledb::test::TILEDB_COORDS, buff));

  // Close array
  array.close();

  // Clean up
  VFS vfs(ctx);
  vfs.remove_dir("sparse_array");
}

TEST_CASE(
    "C++ API: Schema, Dimension ranges", "[cppapi][schema][dimension-ranges]") {
  using namespace tiledb;
  tiledb::Context& ctx = vanilla_context_cpp();

  // Test creating dimensions with signed, unsigned, 32-bit, and 64-bit integer
  // domain types.

  SECTION("int32 domain [-10, -5]") {
    const int32_t domain[2] = {-10, -5};
    const int32_t tile_extent = 5;
    CHECK_NOTHROW(tiledb::Dimension::create(
        ctx, "d1", TILEDB_INT32, domain, &tile_extent));
  }

  SECTION("int32 domain [-10, 5]") {
    const int32_t domain[2] = {-10, 5};
    const int32_t tile_extent = 5;
    CHECK_NOTHROW(tiledb::Dimension::create(
        ctx, "d1", TILEDB_INT32, domain, &tile_extent));
  }

  SECTION("int32 domain [5, 10]") {
    const int32_t domain[2] = {5, 10};
    const int32_t tile_extent = 5;
    CHECK_NOTHROW(tiledb::Dimension::create(
        ctx, "d1", TILEDB_INT32, domain, &tile_extent));
  }

  SECTION("int32 domain [min, max]") {
    int32_t domain[2] = {
        std::numeric_limits<int32_t>::lowest(),
        std::numeric_limits<int32_t>::max()};
    const int32_t tile_extent = 5;
    domain[1] -= tile_extent;
    CHECK_NOTHROW(tiledb::Dimension::create(
        ctx, "d1", TILEDB_INT32, domain, &tile_extent));
  }

  SECTION("int64 domain [-10, -5]") {
    const int64_t domain[2] = {-10, -5};
    const int64_t tile_extent = 5;
    CHECK_NOTHROW(tiledb::Dimension::create(
        ctx, "d1", TILEDB_INT64, domain, &tile_extent));
  }

  SECTION("int64 domain [-10, 5]") {
    const int64_t domain[2] = {-10, 5};
    const int64_t tile_extent = 5;
    CHECK_NOTHROW(tiledb::Dimension::create(
        ctx, "d1", TILEDB_INT64, domain, &tile_extent));
  }

  SECTION("int64 domain [5, 10]") {
    const int64_t domain[2] = {5, 10};
    const int64_t tile_extent = 5;
    CHECK_NOTHROW(tiledb::Dimension::create(
        ctx, "d1", TILEDB_INT64, domain, &tile_extent));
  }

  SECTION("int64 domain [min, max]") {
    int64_t domain[2] = {
        std::numeric_limits<int64_t>::lowest(),
        std::numeric_limits<int64_t>::max()};
    const int64_t tile_extent = 5;
    domain[1] -= tile_extent;
    CHECK_NOTHROW(tiledb::Dimension::create(
        ctx, "d1", TILEDB_INT64, domain, &tile_extent));
  }

  SECTION("uint32 domain [5, 10]") {
    const uint32_t domain[2] = {5, 10};
    const uint32_t tile_extent = 5;
    CHECK_NOTHROW(tiledb::Dimension::create(
        ctx, "d1", TILEDB_UINT32, domain, &tile_extent));
  }

  SECTION("uint32 domain [min, max]") {
    uint32_t domain[2] = {
        std::numeric_limits<uint32_t>::lowest(),
        std::numeric_limits<uint32_t>::max()};
    const uint32_t tile_extent = 5;
    domain[1] -= tile_extent;
    CHECK_NOTHROW(tiledb::Dimension::create(
        ctx, "d1", TILEDB_UINT32, domain, &tile_extent));
  }

  SECTION("uint64 domain [5, 10]") {
    const uint64_t domain[2] = {5, 10};
    const uint64_t tile_extent = 5;
    CHECK_NOTHROW(tiledb::Dimension::create(
        ctx, "d1", TILEDB_UINT64, domain, &tile_extent));
  }

  SECTION("uint64 domain [min, max]") {
    uint64_t domain[2] = {
        std::numeric_limits<uint64_t>::lowest(),
        std::numeric_limits<uint64_t>::max()};
    const uint64_t tile_extent = 5;
    domain[1] -= tile_extent;
    CHECK_NOTHROW(tiledb::Dimension::create(
        ctx, "d1", TILEDB_UINT64, domain, &tile_extent));
  }
}
