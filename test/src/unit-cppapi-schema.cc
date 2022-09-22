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

#include "catch.hpp"
#include "tiledb/sm/cpp_api/tiledb"
#include "tiledb/sm/cpp_api/tiledb_experimental"
#include "tiledb/sm/misc/constants.h"

#include <limits>

TEST_CASE("C++ API: Schema", "[cppapi][schema]") {
  using namespace tiledb;
  Context ctx;

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
  tiledb::Context ctx;
  // Test that this generates no compiler warnings.
  std::unique_ptr<tiledb::ArraySchema> schema;

  // Just instantiate them, don't care about runtime errors.
  schema.reset(new tiledb::ArraySchema(ctx, TILEDB_SPARSE));
}

TEST_CASE(
    "C++ API: Test schema, heterogeneous domain, errors",
    "[cppapi][schema][heter][error]") {
  using namespace tiledb;
  tiledb::Context ctx;

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

  // Query checks
  Query query(ctx, array, TILEDB_READ);
  CHECK_THROWS(query.set_subarray(subarray));
  std::vector<int32_t> buff = {1, 2, 4};
  CHECK_THROWS(query.set_data_buffer(TILEDB_COORDS, buff));

  // Close array
  array.close();

  // Clean up
  VFS vfs(ctx);
  vfs.remove_dir("sparse_array");
}

TEST_CASE(
    "C++ API: Schema, Dimension ranges", "[cppapi][schema][dimension-ranges]") {
  using namespace tiledb;
  tiledb::Context ctx;

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
    int32_t domain[2] = {std::numeric_limits<int32_t>::lowest(),
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
    int64_t domain[2] = {std::numeric_limits<int64_t>::lowest(),
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
    uint32_t domain[2] = {std::numeric_limits<uint32_t>::lowest(),
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
    uint64_t domain[2] = {std::numeric_limits<uint64_t>::lowest(),
                          std::numeric_limits<uint64_t>::max()};
    const uint64_t tile_extent = 5;
    domain[1] -= tile_extent;
    CHECK_NOTHROW(tiledb::Dimension::create(
        ctx, "d1", TILEDB_UINT64, domain, &tile_extent));
  }
}

TEST_CASE(
    "C++ API: SchemaEvolution, add and drop attributes",
    "[cppapi][schema][evolution][add][drop]") {
  using namespace tiledb;
  Context ctx;
  VFS vfs(ctx);

  std::string array_uri = "test_schema_evolution_array";

  Domain domain(ctx);
  auto id1 = Dimension::create<int>(ctx, "d1", {{-100, 100}}, 10);
  auto id2 = Dimension::create<int>(ctx, "d2", {{0, 100}}, 5);
  CHECK_THROWS(id1.set_cell_val_num(4));
  CHECK_NOTHROW(id1.set_cell_val_num(1));
  domain.add_dimension(id1).add_dimension(id2);

  auto a1 = Attribute::create<int>(ctx, "a1");
  auto a2 = Attribute::create<int>(ctx, "a2");

  ArraySchema schema(ctx, TILEDB_DENSE);
  schema.set_domain(domain);
  schema.add_attribute(a1);
  schema.add_attribute(a2);
  schema.set_cell_order(TILEDB_ROW_MAJOR);
  schema.set_tile_order(TILEDB_COL_MAJOR);

  if (vfs.is_dir(array_uri)) {
    vfs.remove_dir(array_uri);
  }

  Array::create(array_uri, schema);

  auto evolution = ArraySchemaEvolution(ctx);

  // add a new attribute a3
  auto a3 = Attribute::create<int>(ctx, "a3");
  evolution.add_attribute(a3);

  // drop attribute a1
  evolution.drop_attribute("a1");

  uint64_t now = tiledb_timestamp_now_ms();
  now = now + 1;
  evolution.set_timestamp_range({now, now});

  // evolve array
  evolution.array_evolve(array_uri);

  // read schema
  auto read_schema = Array::load_schema(ctx, array_uri);

  auto attrs = read_schema.attributes();
  CHECK(attrs.count("a1") == 0);
  CHECK(attrs.count("a2") == 1);
  CHECK(attrs.count("a3") == 1);

  // Clean up
  if (vfs.is_dir(array_uri)) {
    vfs.remove_dir(array_uri);
  }
}

TEST_CASE(
    "C++ API: SchemaEvolution, add attributes and read",
    "[cppapi][schema][evolution][add]") {
  using namespace tiledb;
  Context ctx;
  VFS vfs(ctx);
  auto layout = GENERATE(
      TILEDB_ROW_MAJOR,
      TILEDB_COL_MAJOR,
      TILEDB_UNORDERED,
      TILEDB_GLOBAL_ORDER);
  bool duplicates = GENERATE(true, false);

  std::string array_uri = "test_schema_evolution_array_read";

  // Create
  {
    Domain domain(ctx);
    auto id1 = Dimension::create<int>(ctx, "d1", {{-100, 100}}, 10);
    auto id2 = Dimension::create<int>(ctx, "d2", {{0, 100}}, 5);
    domain.add_dimension(id1).add_dimension(id2);

    auto a = Attribute::create<int>(ctx, "a");

    ArraySchema schema(ctx, TILEDB_SPARSE);
    schema.set_domain(domain);
    schema.set_allows_dups(duplicates);
    CHECK(duplicates == schema.allows_dups());
    schema.add_attribute(a);
    schema.set_cell_order(TILEDB_ROW_MAJOR);
    schema.set_tile_order(TILEDB_COL_MAJOR);

    if (vfs.is_dir(array_uri)) {
      vfs.remove_dir(array_uri);
    }

    Array::create(array_uri, schema);
  }

  // Write data
  {
    // Write some simple data to cells (1, 1), (2, 4) and (2, 3).
    std::vector<int> d1_data = {1, 2, 2};
    std::vector<int> d2_data = {1, 4, 3};
    std::vector<int> data = {1, 2, 3};

    // Open the array for writing and create the query.
    Array array(ctx, array_uri, TILEDB_WRITE);
    Query query(ctx, array, TILEDB_WRITE);
    query.set_layout(TILEDB_UNORDERED)
        .set_data_buffer("a", data)
        .set_data_buffer("d1", d1_data)
        .set_data_buffer("d2", d2_data);

    // Perform the write and close the array.
    query.submit();
    array.close();
  }

  // Read
  {
    // Prepare the array for reading
    Array array(ctx, array_uri, TILEDB_READ);

    // Prepare the vector that will hold the result.
    // We take an upper bound on the result size, as we do not
    // know a priori how big it is (since the array is sparse)
    std::vector<int> data(3);
    std::vector<int> d1_data(3);
    std::vector<int> d2_data(3);

    // Prepare the query
    Query query(ctx, array, TILEDB_READ);
    query.add_range(0, 1, 4)
        .add_range(1, 1, 4)
        .set_layout(layout)
        .set_data_buffer("a", data)
        .set_data_buffer("d1", d1_data)
        .set_data_buffer("d2", d2_data);

    // Submit the query and close the array.
    query.submit();
    array.close();

    // Compare the results.
    auto result_num = (int)query.result_buffer_elements()["a"].second;
    CHECK(result_num == 3);
    // Same result buffers for all layouts
    CHECK_THAT(data, Catch::Matchers::Equals(std::vector<int>{1, 3, 2}));
    CHECK_THAT(d1_data, Catch::Matchers::Equals(std::vector<int>{1, 2, 2}));
    CHECK_THAT(d2_data, Catch::Matchers::Equals(std::vector<int>{1, 3, 4}));
  }

  // Evolve
  {
    uint64_t now = tiledb_timestamp_now_ms() + 1;
    ArraySchemaEvolution schemaEvolution = ArraySchemaEvolution(ctx);
    schemaEvolution.set_timestamp_range(std::make_pair(now, now));

    // Add attribute b
    Attribute b = Attribute::create<uint32_t>(ctx, "b");
    uint32_t fill_value = 1;
    b.set_fill_value(&fill_value, sizeof(fill_value));
    schemaEvolution.add_attribute(b);

    Attribute c = Attribute::create<uint32_t>(ctx, "c");
    uint32_t fill_value_c = 2;
    c.set_nullable(true);
    c.set_fill_value(&fill_value_c, sizeof(fill_value_c), false);
    schemaEvolution.add_attribute(c);

    Attribute d = Attribute::create<std::string>(ctx, "d");
    std::string fill_value_d = "test";
    d.set_fill_value(fill_value_d.c_str(), fill_value_d.size());
    schemaEvolution.add_attribute(d);

    Attribute e = Attribute::create<std::string>(ctx, "e");
    std::string fill_value_e = "n";
    e.set_nullable(true);
    e.set_fill_value(fill_value_e.c_str(), fill_value_e.size(), false);
    schemaEvolution.add_attribute(e);

    // evolve array
    schemaEvolution.array_evolve(array_uri);

    // read schema
    auto read_schema = Array::load_schema(ctx, array_uri);

    auto attrs = read_schema.attributes();
    CHECK(attrs.count("a") == 1);
    CHECK(attrs.count("b") == 1);
    CHECK(attrs.count("c") == 1);
    CHECK(attrs.count("d") == 1);
    CHECK(attrs.count("e") == 1);
  }

  // Write again
  {
    // Write some simple data to cell (3, 1)
    std::vector<int> d1_data = {3};
    std::vector<int> d2_data = {1};
    std::vector<int> a_data = {4};
    std::vector<uint32_t> b_data = {4};
    std::vector<uint32_t> c_data = {40};
    std::vector<uint8_t> c_validity = {1};
    std::vector<char> d_data = {'d'};
    std::vector<uint64_t> d_offsets = {0};
    std::vector<char> e_data = {'e'};
    std::vector<uint64_t> e_offsets = {0};
    std::vector<uint8_t> e_validity = {1};

    // Open the array for writing and create the query.
    Array array(ctx, array_uri, TILEDB_WRITE);
    Query query(ctx, array, TILEDB_WRITE);
    query.set_layout(TILEDB_UNORDERED)
        .set_data_buffer("a", a_data)
        .set_data_buffer("b", b_data)
        .set_data_buffer("c", c_data)
        .set_validity_buffer("c", c_validity)
        .set_data_buffer("d", d_data)
        .set_offsets_buffer("d", d_offsets)
        .set_data_buffer("e", e_data)
        .set_validity_buffer("e", e_validity)
        .set_offsets_buffer("e", e_offsets)
        .set_data_buffer("d1", d1_data)
        .set_data_buffer("d2", d2_data);

    // Perform the write and close the array.
    query.submit();
    array.close();
  }

  // Read Again
  {
    // Prepare the array for reading
    Array array(ctx, array_uri, TILEDB_READ);

    // Prepare the vector that will hold the result.
    // We take an upper bound on the result size, as we do not
    // know a priori how big it is (since the array is sparse)
    std::vector<int> a_data(4);
    std::vector<uint32_t> b_data(4);
    std::vector<uint32_t> c_data(4);
    std::vector<uint8_t> c_validity(4);
    std::vector<char> d_data(13);
    std::vector<uint64_t> d_offsets(4);
    std::vector<char> e_data(4);
    std::vector<uint8_t> e_validity(4);
    std::vector<uint64_t> e_offsets(4);
    std::vector<int> d1_data(4);
    std::vector<int> d2_data(4);

    // Prepare the query
    Query query(ctx, array, TILEDB_READ);
    query.add_range(0, 1, 4)
        .add_range(1, 1, 4)
        .set_layout(layout)
        .set_data_buffer("a", a_data)
        .set_data_buffer("b", b_data)
        .set_data_buffer("c", c_data)
        .set_validity_buffer("c", c_validity)
        .set_data_buffer("d", d_data)
        .set_offsets_buffer("d", d_offsets)
        .set_data_buffer("e", e_data)
        .set_offsets_buffer("e", e_offsets)
        .set_validity_buffer("e", e_validity)
        .set_data_buffer("d1", d1_data)
        .set_data_buffer("d2", d2_data);

    // Submit the query and close the array.
    query.submit();
    array.close();

    // Compare the results.
    auto result_num = (int)query.result_buffer_elements()["a"].second;
    CHECK(result_num == 4);
    if (layout == TILEDB_COL_MAJOR) {
      CHECK_THAT(a_data, Catch::Matchers::Equals(std::vector<int>{1, 4, 3, 2}));
      CHECK_THAT(
          b_data, Catch::Matchers::Equals(std::vector<uint32_t>{1, 4, 1, 1}));
      CHECK_THAT(
          c_data, Catch::Matchers::Equals(std::vector<uint32_t>{2, 40, 2, 2}));
      CHECK_THAT(
          c_validity,
          Catch::Matchers::Equals(std::vector<uint8_t>{0, 1, 0, 0}));
      CHECK_THAT(
          d_data,
          Catch::Matchers::Equals(std::vector<char>{'t',
                                                    'e',
                                                    's',
                                                    't',
                                                    'd',
                                                    't',
                                                    'e',
                                                    's',
                                                    't',
                                                    't',
                                                    'e',
                                                    's',
                                                    't'}));
      CHECK_THAT(
          d_offsets,
          Catch::Matchers::Equals(std::vector<uint64_t>{0, 4, 5, 9}));
      CHECK_THAT(
          e_data,
          Catch::Matchers::Equals(std::vector<char>{'n', 'e', 'n', 'n'}));
      CHECK_THAT(
          e_offsets,
          Catch::Matchers::Equals(std::vector<uint64_t>{0, 1, 2, 3}));
      CHECK_THAT(
          e_validity,
          Catch::Matchers::Equals(std::vector<uint8_t>{0, 1, 0, 0}));
      CHECK_THAT(
          d1_data, Catch::Matchers::Equals(std::vector<int>{1, 3, 2, 2}));
      CHECK_THAT(
          d2_data, Catch::Matchers::Equals(std::vector<int>{1, 1, 3, 4}));
    } else {
      // Check values for unordered, global, and row-major
      CHECK_THAT(a_data, Catch::Matchers::Equals(std::vector<int>{1, 3, 2, 4}));
      CHECK_THAT(
          b_data, Catch::Matchers::Equals(std::vector<uint32_t>{1, 1, 1, 4}));
      CHECK_THAT(
          c_data, Catch::Matchers::Equals(std::vector<uint32_t>{2, 2, 2, 40}));
      CHECK_THAT(
          c_validity,
          Catch::Matchers::Equals(std::vector<uint8_t>{0, 0, 0, 1}));
      CHECK_THAT(
          d_data,
          Catch::Matchers::Equals(std::vector<char>{'t',
                                                    'e',
                                                    's',
                                                    't',
                                                    't',
                                                    'e',
                                                    's',
                                                    't',
                                                    't',
                                                    'e',
                                                    's',
                                                    't',
                                                    'd'}));
      CHECK_THAT(
          d_offsets,
          Catch::Matchers::Equals(std::vector<uint64_t>{0, 4, 8, 12}));
      CHECK_THAT(
          e_data,
          Catch::Matchers::Equals(std::vector<char>{'n', 'n', 'n', 'e'}));
      CHECK_THAT(
          e_offsets,
          Catch::Matchers::Equals(std::vector<uint64_t>{0, 1, 2, 3}));
      CHECK_THAT(
          e_validity,
          Catch::Matchers::Equals(std::vector<uint8_t>{0, 0, 0, 1}));
      CHECK_THAT(
          d1_data, Catch::Matchers::Equals(std::vector<int>{1, 2, 2, 3}));
      CHECK_THAT(
          d2_data, Catch::Matchers::Equals(std::vector<int>{1, 3, 4, 1}));
    }
  }

  // Read using overlapping multi-range query
  // + Global order does not support multi-range subarrays
  if (layout != TILEDB_GLOBAL_ORDER) {
    Array array(ctx, array_uri, TILEDB_READ);

    std::vector<int> a_data(8);
    std::vector<uint32_t> b_data(8);
    std::vector<uint32_t> c_data(8);
    std::vector<uint8_t> c_validity(8);
    std::vector<char> d_data(26);
    std::vector<uint64_t> d_offsets(8);
    std::vector<char> e_data(8);
    std::vector<uint8_t> e_validity(8);
    std::vector<uint64_t> e_offsets(8);
    std::vector<int> d1_data(8);
    std::vector<int> d2_data(8);

    // Resize buffers for multi-range unordered read
    if (layout == TILEDB_UNORDERED) {
      a_data.resize(16);
      b_data.resize(16);
      c_data.resize(16);
      c_validity.resize(16);
      d_data.resize(52);
      d_offsets.resize(16);
      e_data.resize(16);
      e_validity.resize(16);
      e_offsets.resize(16);
      d1_data.resize(16);
      d2_data.resize(16);
    }

    // Prepare the query
    Query query(ctx, array, TILEDB_READ);
    query.add_range(0, 1, 4)
        .add_range(0, 1, 4)
        .add_range(1, 1, 4)
        .add_range(1, 1, 4)
        .set_layout(layout)
        .set_data_buffer("a", a_data)
        .set_data_buffer("b", b_data)
        .set_data_buffer("c", c_data)
        .set_validity_buffer("c", c_validity)
        .set_data_buffer("d", d_data)
        .set_offsets_buffer("d", d_offsets)
        .set_data_buffer("e", e_data)
        .set_offsets_buffer("e", e_offsets)
        .set_validity_buffer("e", e_validity)
        .set_data_buffer("d1", d1_data)
        .set_data_buffer("d2", d2_data);

    // Submit the query and close the array.
    query.submit();
    array.close();

    // Compare the results.
    if (layout == TILEDB_COL_MAJOR) {
      auto result_num = (int)query.result_buffer_elements()["a"].second;
      CHECK(result_num == 8);

      CHECK_THAT(
          a_data,
          Catch::Matchers::Equals(std::vector<int>{1, 1, 4, 4, 3, 3, 2, 2}));
      CHECK_THAT(
          b_data,
          Catch::Matchers::Equals(
              std::vector<uint32_t>{1, 1, 4, 4, 1, 1, 1, 1}));
      CHECK_THAT(
          c_data,
          Catch::Matchers::Equals(
              std::vector<uint32_t>{2, 2, 40, 40, 2, 2, 2, 2}));
      CHECK_THAT(
          c_validity,
          Catch::Matchers::Equals(
              std::vector<uint8_t>{0, 0, 1, 1, 0, 0, 0, 0}));
      CHECK_THAT(
          d_data,
          Catch::Matchers::Equals(
              std::vector<char>{'t', 'e', 's', 't', 't', 'e', 's', 't', 'd',
                                'd', 't', 'e', 's', 't', 't', 'e', 's', 't',
                                't', 'e', 's', 't', 't', 'e', 's', 't'}));
      CHECK_THAT(
          d_offsets,
          Catch::Matchers::Equals(
              std::vector<uint64_t>{0, 4, 8, 9, 10, 14, 18, 22}));
      CHECK_THAT(
          e_data,
          Catch::Matchers::Equals(
              std::vector<char>{'n', 'n', 'e', 'e', 'n', 'n', 'n', 'n'}));
      CHECK_THAT(
          e_offsets,
          Catch::Matchers::Equals(
              std::vector<uint64_t>{0, 1, 2, 3, 4, 5, 6, 7}));
      CHECK_THAT(
          e_validity,
          Catch::Matchers::Equals(
              std::vector<uint8_t>{0, 0, 1, 1, 0, 0, 0, 0}));
      CHECK_THAT(
          d1_data,
          Catch::Matchers::Equals(std::vector<int>{1, 1, 3, 3, 2, 2, 2, 2}));
      CHECK_THAT(
          d2_data,
          Catch::Matchers::Equals(std::vector<int>{1, 1, 1, 1, 3, 3, 4, 4}));
    } else if (layout == TILEDB_ROW_MAJOR) {
      auto result_num = (int)query.result_buffer_elements()["a"].second;
      CHECK(result_num == 8);

      CHECK_THAT(
          a_data,
          Catch::Matchers::Equals(std::vector<int>{1, 1, 3, 3, 2, 2, 4, 4}));
      CHECK_THAT(
          b_data,
          Catch::Matchers::Equals(
              std::vector<uint32_t>{1, 1, 1, 1, 1, 1, 4, 4}));
      CHECK_THAT(
          c_data,
          Catch::Matchers::Equals(
              std::vector<uint32_t>{2, 2, 2, 2, 2, 2, 40, 40}));
      CHECK_THAT(
          c_validity,
          Catch::Matchers::Equals(
              std::vector<uint8_t>{0, 0, 0, 0, 0, 0, 1, 1}));
      CHECK_THAT(
          d_data,
          Catch::Matchers::Equals(
              std::vector<char>{'t', 'e', 's', 't', 't', 'e', 's', 't', 't',
                                'e', 's', 't', 't', 'e', 's', 't', 't', 'e',
                                's', 't', 't', 'e', 's', 't', 'd', 'd'}));
      CHECK_THAT(
          d_offsets,
          Catch::Matchers::Equals(
              std::vector<uint64_t>{0, 4, 8, 12, 16, 20, 24, 25}));
      CHECK_THAT(
          e_data,
          Catch::Matchers::Equals(
              std::vector<char>{'n', 'n', 'n', 'n', 'n', 'n', 'e', 'e'}));
      CHECK_THAT(
          e_offsets,
          Catch::Matchers::Equals(
              std::vector<uint64_t>{0, 1, 2, 3, 4, 5, 6, 7}));
      CHECK_THAT(
          e_validity,
          Catch::Matchers::Equals(
              std::vector<uint8_t>{0, 0, 0, 0, 0, 0, 1, 1}));
      CHECK_THAT(
          d1_data,
          Catch::Matchers::Equals(std::vector<int>{1, 1, 2, 2, 2, 2, 3, 3}));
      CHECK_THAT(
          d2_data,
          Catch::Matchers::Equals(std::vector<int>{1, 1, 3, 3, 4, 4, 1, 1}));
    } else if (layout == TILEDB_UNORDERED) {
      auto result_num = (int)query.result_buffer_elements()["a"].second;
      CHECK(result_num == 16);

      CHECK_THAT(
          a_data,
          Catch::Matchers::Equals(std::vector<int>{
              1, 1, 1, 1, 3, 3, 3, 3, 2, 2, 2, 2, 4, 4, 4, 4}));
      CHECK_THAT(
          b_data,
          Catch::Matchers::Equals(std::vector<uint32_t>{
              1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 4, 4, 4, 4}));
      CHECK_THAT(
          c_data,
          Catch::Matchers::Equals(std::vector<uint32_t>{
              2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 40, 40, 40, 40}));
      CHECK_THAT(
          c_validity,
          Catch::Matchers::Equals(std::vector<uint8_t>{
              0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1}));
      CHECK_THAT(
          d_data,
          Catch::Matchers::Equals(std::vector<char>{
              't', 'e', 's', 't', 't', 'e', 's', 't', 't', 'e', 's',
              't', 't', 'e', 's', 't', 't', 'e', 's', 't', 't', 'e',
              's', 't', 't', 'e', 's', 't', 't', 'e', 's', 't', 't',
              'e', 's', 't', 't', 'e', 's', 't', 't', 'e', 's', 't',
              't', 'e', 's', 't', 'd', 'd', 'd', 'd'}));
      CHECK_THAT(
          d_offsets,
          Catch::Matchers::Equals(std::vector<uint64_t>{
              0, 4, 8, 12, 16, 20, 24, 28, 32, 36, 40, 44, 48, 49, 50, 51}));
      CHECK_THAT(
          e_data,
          Catch::Matchers::Equals(std::vector<char>{'n',
                                                    'n',
                                                    'n',
                                                    'n',
                                                    'n',
                                                    'n',
                                                    'n',
                                                    'n',
                                                    'n',
                                                    'n',
                                                    'n',
                                                    'n',
                                                    'e',
                                                    'e',
                                                    'e',
                                                    'e'}));
      CHECK_THAT(
          e_offsets,
          Catch::Matchers::Equals(std::vector<uint64_t>{
              0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}));
      CHECK_THAT(
          e_validity,
          Catch::Matchers::Equals(std::vector<uint8_t>{
              0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1}));
      CHECK_THAT(
          d1_data,
          Catch::Matchers::Equals(std::vector<int>{
              1, 1, 1, 1, 2, 2, 2, 2, 2, 2, 2, 2, 3, 3, 3, 3}));
      CHECK_THAT(
          d2_data,
          Catch::Matchers::Equals(std::vector<int>{
              1, 1, 1, 1, 3, 3, 3, 3, 4, 4, 4, 4, 1, 1, 1, 1}));
    }
  }

  // Clean up
  if (vfs.is_dir(array_uri)) {
    vfs.remove_dir(array_uri);
  }
}