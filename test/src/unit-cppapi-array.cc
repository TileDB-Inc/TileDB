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
 * Tests the C++ API for array related functions.
 */

#include "catch.hpp"
#include "tiledb/sm/cpp_api/tiledb"
#include "tiledb/sm/misc/utils.h"

using namespace tiledb;

struct Point {
  int coords[3];
  double value;
};

struct CPPArrayFx {
  static const unsigned d1_tile = 10;
  static const unsigned d2_tile = 5;

  CPPArrayFx()
      : vfs(ctx) {
    using namespace tiledb;

    if (vfs.is_dir("cpp_unit_array"))
      vfs.remove_dir("cpp_unit_array");

    Domain domain(ctx);
    auto d1 = Dimension::create<int>(ctx, "d1", {{-100, 100}}, d1_tile);
    auto d2 = Dimension::create<int>(ctx, "d2", {{0, 100}}, d2_tile);
    domain.add_dimensions(d1, d2);

    auto a1 = Attribute::create<int>(ctx, "a1");          // (int, 1)
    auto a2 = Attribute::create<std::string>(ctx, "a2");  // (char, VAR_NUM)
    auto a3 = Attribute::create<std::array<double, 2>>(
        ctx, "a3");  // (char, sizeof(std::array<double,2>)
    auto a4 =
        Attribute::create<std::vector<Point>>(ctx, "a4");  // (char, VAR_NUM)
    auto a5 = Attribute::create<Point>(ctx, "a5");  // (char, sizeof(Point))
    FilterList filters(ctx);
    filters.add_filter({ctx, TILEDB_FILTER_LZ4});
    a1.set_filter_list(filters);

    ArraySchema schema(ctx, TILEDB_DENSE);
    schema.set_domain(domain);
    schema.add_attributes(a1, a2, a3, a4, a5);

    Array::create("cpp_unit_array", schema);
  }

  ~CPPArrayFx() {
    if (vfs.is_dir("cpp_unit_array"))
      vfs.remove_dir("cpp_unit_array");
  }

  Context ctx;
  VFS vfs;
};

TEST_CASE("Config", "[cppapi]") {
  // Primarily to instansiate operator[]/= template
  tiledb::Config cfg;
  cfg["vfs.s3.region"] = "us-east-1a";
  cfg["vfs.s3.use_virtual_addressing"] = "true";
  CHECK((std::string)cfg["vfs.s3.region"] == "us-east-1a");
  CHECK((std::string)cfg["vfs.s3.use_virtual_addressing"] == "true");
}

TEST_CASE_METHOD(CPPArrayFx, "C++ API: Arrays", "[cppapi]") {
  SECTION("Dimensions") {
    ArraySchema schema(ctx, "cpp_unit_array");
    CHECK(schema.domain().ndim() == 2);
    auto a = schema.domain().dimensions()[0].domain<int>();
    auto b = schema.domain().dimensions()[1].domain<int>();
    CHECK_THROWS(schema.domain().dimensions()[0].domain<unsigned>());
    CHECK(a.first == -100);
    CHECK(a.second == 100);
    CHECK(b.first == 0);
    CHECK(b.second == 100);
    CHECK_THROWS(schema.domain().dimensions()[0].tile_extent<unsigned>() == 10);
    CHECK(schema.domain().dimensions()[0].tile_extent<int>() == 10);
    CHECK(schema.domain().dimensions()[1].tile_extent<int>() == 5);
    CHECK(schema.domain().cell_num() == 20301);
  }
  SECTION("Make Buffer") {
    Array array(ctx, "cpp_unit_array", TILEDB_WRITE);
    Query query(ctx, array, TILEDB_WRITE);
    CHECK_THROWS(query.set_subarray<unsigned>({1, 2}));  // Wrong type
    CHECK_THROWS(query.set_subarray<int>({1, 2}));       // Wrong num
    array.close();
  }

  SECTION("Read/Write") {
    std::vector<int> a1 = {1, 2};
    std::vector<std::string> a2 = {"abc", "defg"};
    std::vector<std::array<double, 2>> a3 = {{{1.0, 2.0}}, {{3.0, 4.0}}};
    std::vector<std::vector<Point>> a4 = {
        {{{{1, 2, 3}, 4.1}, {{2, 3, 4}, 5.2}}}, {{{{5, 6, 7}, 8.3}}}};
    std::vector<Point> a5 = {{{5, 6, 7}, 8.3}, {{5, 6, 7}, 8.3}};

    auto a2buf = ungroup_var_buffer(a2);
    auto a4buf = ungroup_var_buffer(a4);

    std::vector<int> subarray = {0, 1, 0, 0};

    try {
      Array array(ctx, "cpp_unit_array", TILEDB_READ);
      CHECK(array.query_type() == TILEDB_READ);
      CHECK(array.is_open());

      // Close and reopen
      array.close();
      CHECK(!array.is_open());
      array.open(TILEDB_WRITE);
      CHECK(array.is_open());
      CHECK(array.query_type() == TILEDB_WRITE);

      Query query(ctx, array, TILEDB_WRITE);
      CHECK(query.query_type() == TILEDB_WRITE);
      query.set_subarray(subarray);
      query.set_buffer("a1", a1);
      query.set_buffer("a2", a2buf);
      query.set_buffer("a3", a3);
      query.set_buffer("a4", a4buf);
      query.set_buffer("a5", a5);
      query.set_layout(TILEDB_ROW_MAJOR);

      REQUIRE(query.submit() == Query::Status::COMPLETE);

      CHECK(!query.has_results());

      query.finalize();
      array.close();

      tiledb::Array::consolidate(ctx, "cpp_unit_array");
    } catch (std::exception& e) {
      std::cout << e.what() << std::endl;
    }

    {
      std::fill(std::begin(a1), std::end(a1), 0);
      std::fill(std::begin(a2buf.first), std::end(a2buf.first), 0);
      std::fill(std::begin(a2buf.second), std::end(a2buf.second), 0);
      std::fill(std::begin(a3), std::end(a3), std::array<double, 2>({{0, 0}}));
      std::fill(std::begin(a4buf.first), std::end(a4buf.first), 0);
      std::fill(
          std::begin(a4buf.second),
          std::end(a4buf.second),
          Point{{0, 0, 0}, 0});
      std::fill(std::begin(a5), std::end(a5), Point{{0, 0, 0}, 0});

      Array array(ctx, "cpp_unit_array", TILEDB_READ);

      auto buff_el = array.max_buffer_elements(subarray);

      CHECK(buff_el.count("a1"));
      CHECK(buff_el.count("a2"));
      CHECK(buff_el.count("a3"));
      CHECK(buff_el.count("a4"));
      CHECK(buff_el.count("a5"));
      CHECK(buff_el["a1"].first == 0);
      CHECK(buff_el["a1"].second >= 2);
      CHECK(buff_el["a2"].first == 2);
      CHECK(buff_el["a2"].second >= 7);
      CHECK(buff_el["a3"].first == 0);
      CHECK(buff_el["a3"].second >= 4);
      CHECK(buff_el["a4"].first == 2);
      CHECK(buff_el["a4"].second >= 3);
      CHECK(buff_el["a5"].first == 0);
      CHECK(buff_el["a5"].second >= 2);

      a1.resize(buff_el["a1"].second);
      a2buf.first.resize(buff_el["a2"].first);
      a2buf.second.resize(buff_el["a2"].second);
      a1.resize(buff_el["a3"].second);
      a4buf.first.resize(buff_el["a4"].first);
      a4buf.second.resize(buff_el["a4"].second);
      a1.resize(buff_el["a5"].second);

      Query query(ctx, array);

      CHECK(!query.has_results());

      query.set_buffer("a1", a1);
      query.set_buffer("a2", a2buf);
      query.set_buffer("a3", a3);
      query.set_buffer("a4", a4buf);
      query.set_buffer("a5", a5);
      query.set_layout(TILEDB_ROW_MAJOR);
      query.set_subarray(subarray);

      // Make sure no segfault when called before submit
      query.result_buffer_elements();

      REQUIRE(query.submit() == Query::Status::COMPLETE);

      CHECK(query.has_results());

      query.finalize();
      array.close();

      auto ret = query.result_buffer_elements();
      REQUIRE(ret.size() == 5);
      CHECK(ret["a1"].first == 0);
      CHECK(ret["a1"].second == 2);
      CHECK(ret["a2"].first == 2);
      CHECK(ret["a2"].second == 7);
      CHECK(ret["a3"].first == 0);
      CHECK(ret["a3"].second == 2);
      CHECK(ret["a4"].first == 2);
      CHECK(ret["a4"].second == 3);
      CHECK(ret["a5"].first == 0);
      CHECK(ret["a5"].second == 2);

      CHECK(a1[0] == 1);
      CHECK(a1[1] == 2);

      auto reada2 = group_by_cell<char, std::string>(a2buf, 2, 7);
      CHECK(reada2[0] == "abc");
      CHECK(reada2[1] == "defg");

      REQUIRE(a3.size() == 2);
      CHECK(a3[0][0] == 1.0);
      CHECK(a3[0][1] == 2.0);
      CHECK(a3[1][0] == 3.0);
      CHECK(a3[1][1] == 4.0);

      auto reada4 = group_by_cell<Point>(a4buf, 2, 3);
      REQUIRE(reada4.size() == 2);
      REQUIRE(reada4[0].size() == 2);
      REQUIRE(reada4[1].size() == 1);
      CHECK(reada4[0][0].coords[0] == 1);
      CHECK(reada4[0][0].coords[1] == 2);
      CHECK(reada4[0][0].coords[2] == 3);
      CHECK(reada4[0][0].value == 4.1);
      CHECK(reada4[0][1].coords[0] == 2);
      CHECK(reada4[0][1].coords[1] == 3);
      CHECK(reada4[0][1].coords[2] == 4);
      CHECK(reada4[0][1].value == 5.2);
      CHECK(reada4[1][0].coords[0] == 5);
      CHECK(reada4[1][0].coords[1] == 6);
      CHECK(reada4[1][0].coords[2] == 7);
      CHECK(reada4[1][0].value == 8.3);
    }
  }

  SECTION("Global order write") {
    std::vector<int> subarray = {0, d1_tile - 1, 0, d2_tile - 1};
    std::vector<int> a1 = {1, 2};
    std::vector<std::string> a2 = {"abc", "defg"};
    std::vector<std::array<double, 2>> a3 = {{{1.0, 2.0}}, {{3.0, 4.0}}};
    std::vector<std::vector<Point>> a4 = {
        {{{{1, 2, 3}, 4.1}, {{2, 3, 4}, 5.2}}}, {{{{5, 6, 7}, 8.3}}}};
    std::vector<Point> a5 = {{{5, 6, 7}, 8.3}, {{5, 6, 7}, 8.3}};

    // Pad out to tile multiple
    size_t num_dummies = d1_tile * d2_tile - a1.size();
    for (size_t i = 0; i < num_dummies; i++) {
      a1.push_back(0);
      a2.push_back("-");
      a3.push_back({{0.0, 0.0}});
      a4.push_back({{{0, 0, 0}, 0.0}});
      a5.push_back({{0, 0, 0}, 0.0});
    }

    auto a2buf = ungroup_var_buffer(a2);
    auto a4buf = ungroup_var_buffer(a4);

    Array array(ctx, "cpp_unit_array", TILEDB_WRITE);
    Query query(ctx, array, TILEDB_WRITE);
    query.set_subarray(subarray);
    query.set_buffer("a1", a1);
    query.set_buffer("a2", a2buf);
    query.set_buffer("a3", a3);
    query.set_buffer("a4", a4buf);
    query.set_buffer("a5", a5);

    query.set_layout(TILEDB_GLOBAL_ORDER);
    CHECK(query.submit() == tiledb::Query::Status::COMPLETE);
    REQUIRE_NOTHROW(query.finalize());
    array.close();

    array.open(TILEDB_READ);
    auto non_empty = array.non_empty_domain<int>();
    REQUIRE(non_empty.size() == 2);
    CHECK(non_empty[0].second.first == 0);
    CHECK(non_empty[0].second.second == d1_tile - 1);
    CHECK(non_empty[1].second.first == 0);
    CHECK(non_empty[1].second.second == d2_tile - 1);
    array.close();
  }

  SECTION("Global order write - no dummy values") {
    std::vector<int> a1 = {1, 2};
    std::vector<int> subarray = {0, 1, 0, 0};
    Array array(ctx, "cpp_unit_array", TILEDB_WRITE);
    Query query(ctx, array, TILEDB_WRITE);
    query.set_subarray(subarray);
    query.set_buffer("a1", a1);
    query.set_layout(TILEDB_GLOBAL_ORDER);
    // Incorrect subarray for global order
    REQUIRE_THROWS(query.submit());
    query.finalize();
    array.close();
  }
}

TEST_CASE(
    "C++ API: Incorrect buffer size and offsets",
    "[cppapi], [invalid-offsets]") {
  const std::string array_name_1d = "cpp_unit_array_1d";
  Context ctx;
  VFS vfs(ctx);

  if (vfs.is_dir(array_name_1d))
    vfs.remove_dir(array_name_1d);

  ArraySchema schema(ctx, TILEDB_SPARSE);
  Domain domain(ctx);
  domain.add_dimension(Dimension::create<int32_t>(ctx, "d", {{0, 1000}}));
  schema.set_domain(domain);
  schema.add_attribute(Attribute::create<std::vector<int32_t>>(ctx, "a"));
  Array::create(array_name_1d, schema);
  Array array(ctx, array_name_1d, TILEDB_WRITE);

  std::vector<int32_t> a, coord = {10, 20, 30};
  std::vector<uint64_t> a_offset = {0, 0, 0};

  // Test case where backing buffer for vector is non-null, but size is 0.
  // For bug https://github.com/TileDB-Inc/TileDB/issues/590
  {
    a.reserve(10);
    a = {};
    Query q(ctx, array, TILEDB_WRITE);
    q.set_layout(TILEDB_GLOBAL_ORDER);
    q.set_coordinates(coord);
    REQUIRE_THROWS(q.set_buffer("a", a_offset, a));
  }

  // Test case of non-ascending offsets
  {
    a = {0, 1, 2};
    a_offset = {0, 2, 1};
    Query q(ctx, array, TILEDB_WRITE);
    q.set_layout(TILEDB_GLOBAL_ORDER);
    q.set_coordinates(coord);
    REQUIRE_THROWS(q.set_buffer("a", a_offset, a));

    a = {0, 1, 2};
    a_offset = {0, 1, 1};
    REQUIRE_THROWS(q.set_buffer("a", a_offset, a));
  }

  array.close();

  if (vfs.is_dir(array_name_1d))
    vfs.remove_dir(array_name_1d);
}

TEST_CASE("C++ API: Read subarray with expanded domain", "[cppapi], [dense]") {
  const std::vector<tiledb_layout_t> tile_layouts = {TILEDB_ROW_MAJOR,
                                                     TILEDB_COL_MAJOR},
                                     cell_layouts = {TILEDB_ROW_MAJOR,
                                                     TILEDB_COL_MAJOR};
  const std::vector<int> tile_extents = {1, 2, 3, 4};

  for (auto tile_layout : tile_layouts) {
    for (auto cell_layout : cell_layouts) {
      for (int tile_extent : tile_extents) {
        const std::string array_name = "cpp_unit_array";
        Context ctx;
        VFS vfs(ctx);

        if (vfs.is_dir(array_name))
          vfs.remove_dir(array_name);

        // Create
        Domain domain(ctx);
        domain
            .add_dimension(
                Dimension::create<int>(ctx, "rows", {{0, 3}}, tile_extent))
            .add_dimension(
                Dimension::create<int>(ctx, "cols", {{0, 3}}, tile_extent));
        ArraySchema schema(ctx, TILEDB_DENSE);
        schema.set_domain(domain).set_order({{tile_layout, cell_layout}});
        schema.add_attribute(Attribute::create<int>(ctx, "a"));
        Array::create(array_name, schema);

        // Write
        std::vector<int> data_w = {
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};
        Array array_w(ctx, array_name, TILEDB_WRITE);
        Query query_w(ctx, array_w);
        query_w.set_subarray({0, 3, 0, 3})
            .set_layout(TILEDB_ROW_MAJOR)
            .set_buffer("a", data_w);
        query_w.submit();
        array_w.close();

        // Read
        Array array(ctx, array_name, TILEDB_READ);
        Query query(ctx, array);
        const std::vector<int> subarray = {0, 3, 0, 3};
        std::vector<int> data(16);
        query.set_subarray(subarray)
            .set_layout(TILEDB_ROW_MAJOR)
            .set_buffer("a", data);
        query.submit();
        array.close();

        INFO(
            "Tile layout " << ArraySchema::to_str(tile_layout)
                           << ", cell layout "
                           << ArraySchema::to_str(cell_layout)
                           << ", tile extent " << tile_extent);
        for (int i = 0; i < 16; i++) {
          REQUIRE(data[i] == i + 1);
        }

        if (vfs.is_dir(array_name))
          vfs.remove_dir(array_name);
      }
    }
  }
}

TEST_CASE(
    "C++ API: Consolidation of empty arrays", "[cppapi], [consolidation]") {
  Context ctx;
  VFS vfs(ctx);
  const std::string array_name = "cpp_unit_array";

  if (vfs.is_dir(array_name))
    vfs.remove_dir(array_name);

  REQUIRE_THROWS_AS(Array::consolidate(ctx, array_name), tiledb::TileDBError);

  Domain domain(ctx);
  domain.add_dimension(Dimension::create<int>(ctx, "d", {{0, 3}}, 1));
  ArraySchema schema(ctx, TILEDB_DENSE);
  schema.set_domain(domain);
  schema.add_attribute(Attribute::create<int>(ctx, "a"));
  Array::create(array_name, schema);

  REQUIRE_NOTHROW(Array::consolidate(ctx, array_name));

  if (vfs.is_dir(array_name))
    vfs.remove_dir(array_name);
}

TEST_CASE("C++ API: Encrypted array", "[cppapi], [encryption]") {
  Context ctx;
  VFS vfs(ctx);
  const std::string array_name = "cpp_unit_array";

  if (vfs.is_dir(array_name))
    vfs.remove_dir(array_name);

  const char key[] = "0123456789abcdeF0123456789abcdeF";
  uint32_t key_len = (uint32_t)strlen(key);

  Domain domain(ctx);
  domain.add_dimension(Dimension::create<int>(ctx, "d", {{0, 3}}, 1));
  ArraySchema schema(ctx, TILEDB_DENSE);
  schema.set_domain(domain);
  schema.add_attribute(Attribute::create<int>(ctx, "a"));
  Array::create(array_name, schema, TILEDB_AES_256_GCM, key, key_len);

  REQUIRE_THROWS_AS(
      [&]() { ArraySchema schema_read(ctx, array_name); }(),
      tiledb::TileDBError);
  REQUIRE_NOTHROW([&]() {
    ArraySchema schema_read(ctx, array_name, TILEDB_AES_256_GCM, key, key_len);
  }());

  REQUIRE_THROWS_AS(
      [&]() { Array array(ctx, array_name, TILEDB_WRITE); }(),
      tiledb::TileDBError);

  Array array(ctx, array_name, TILEDB_WRITE, TILEDB_AES_256_GCM, key, key_len);
  array.close();
  REQUIRE_THROWS_AS(array.open(TILEDB_WRITE), tiledb::TileDBError);
  array.open(TILEDB_WRITE, TILEDB_AES_256_GCM, key, key_len);

  REQUIRE_THROWS_AS(
      [&]() { Array array2(ctx, array_name, TILEDB_WRITE); }(),
      tiledb::TileDBError);

  Query query(ctx, array);
  query.set_layout(TILEDB_ROW_MAJOR);
  std::vector<int> a_values = {1, 2, 3, 4};
  query.set_buffer("a", a_values);
  query.submit();
  array.close();

  // Write a second time, as consolidation needs at least two fragments
  // to trigger an error with encryption (consolidation is a noop for
  // single-fragment arrays and, thus, always succeeds)
  Array array_2(
      ctx, array_name, TILEDB_WRITE, TILEDB_AES_256_GCM, key, key_len);
  Query query_2(ctx, array_2);
  query_2.set_layout(TILEDB_ROW_MAJOR);
  query_2.set_buffer("a", a_values);
  query_2.submit();
  array_2.close();

  REQUIRE_THROWS_AS(Array::consolidate(ctx, array_name), tiledb::TileDBError);
  REQUIRE_NOTHROW(
      Array::consolidate(ctx, array_name, TILEDB_AES_256_GCM, key, key_len));

  array.open(TILEDB_READ, TILEDB_AES_256_GCM, key, key_len);
  array.reopen();

  std::vector<int> subarray = {0, 3};
  std::vector<int> a_read(4);
  Query query_r(ctx, array);
  query_r.set_subarray(subarray)
      .set_layout(TILEDB_ROW_MAJOR)
      .set_buffer("a", a_read);
  query_r.submit();
  array.close();

  for (unsigned i = 0; i < 4; i++)
    REQUIRE(a_values[i] == a_read[i]);

  if (vfs.is_dir(array_name))
    vfs.remove_dir(array_name);
}

TEST_CASE("C++ API: Open array at", "[cppapi], [cppapi-open-array-at]") {
  Context ctx;
  VFS vfs(ctx);
  const std::string array_name = "cppapi_open_array_at";
  if (vfs.is_dir(array_name))
    vfs.remove_dir(array_name);

  // Create array
  Domain domain(ctx);
  domain.add_dimension(Dimension::create<int>(ctx, "d", {{1, 4}}, 4));
  ArraySchema schema(ctx, TILEDB_DENSE);
  schema.set_domain(domain);
  schema.add_attribute(Attribute::create<int>(ctx, "a"));
  Array::create(array_name, schema);

  REQUIRE_THROWS_AS(
      Array(ctx, array_name, TILEDB_WRITE, 0), tiledb::TileDBError);

  // Write array
  Array array_w(ctx, array_name, TILEDB_WRITE);
  Query query_w(ctx, array_w);
  query_w.set_layout(TILEDB_ROW_MAJOR);
  std::vector<int> a_w = {1, 2, 3, 4};
  query_w.set_buffer("a", a_w);
  query_w.submit();
  array_w.close();

  // Get timestamp after write
  auto first_timestamp = TILEDB_TIMESTAMP_NOW_MS;

  // Normal read
  Array array_r(ctx, array_name, TILEDB_READ);
  std::vector<int> subarray = {1, 4};
  std::vector<int> a_r(4);
  Query query_r(ctx, array_r);
  query_r.set_subarray(subarray)
      .set_layout(TILEDB_ROW_MAJOR)
      .set_buffer("a", a_r);
  query_r.submit();
  array_r.close();
  CHECK(std::equal(a_r.begin(), a_r.end(), a_w.begin()));

  // Read from 0 timestamp
  Array array_r_at_0(ctx, array_name, TILEDB_READ, 0);
  CHECK(array_r_at_0.timestamp() == 0);

  SECTION("Testing Array::Array") {
    // Nothing to do - just for clarity
  }

  SECTION("Testing Array::open") {
    array_r_at_0.close();
    array_r_at_0.open(TILEDB_READ, 0);
  }

  std::vector<int> a_r_at_0(4);
  Query query_r_at_0(ctx, array_r_at_0);
  query_r_at_0.set_subarray(subarray)
      .set_layout(TILEDB_ROW_MAJOR)
      .set_buffer("a", a_r_at_0);
  query_r_at_0.submit();
  array_r_at_0.close();
  auto result = query_r_at_0.result_buffer_elements();
  CHECK(result["a"].second == 0);
  CHECK(!std::equal(a_r_at_0.begin(), a_r_at_0.end(), a_w.begin()));

  // Read from later timestamp
  auto timestamp = TILEDB_TIMESTAMP_NOW_MS;
  Array array_r_at(ctx, array_name, TILEDB_READ, timestamp);

  SECTION("Testing Array::Array") {
    // Nothing to do - just for clarity
  }

  SECTION("Testing Array::open") {
    array_r_at.close();
    array_r_at.open(TILEDB_READ, timestamp);
  }

  std::vector<int> a_r_at(4);
  Query query_r_at(ctx, array_r_at);
  query_r_at.set_subarray(subarray)
      .set_layout(TILEDB_ROW_MAJOR)
      .set_buffer("a", a_r_at_0);
  query_r_at.submit();
  CHECK(!std::equal(a_r_at.begin(), a_r_at.end(), a_w.begin()));

  // Reopen at first timestamp.
  array_r_at.reopen_at(first_timestamp);
  CHECK(array_r_at.timestamp() == first_timestamp);
  std::vector<int> a_r_reopen_at(4);
  Query query_r_reopen_at(ctx, array_r_at);
  query_r_reopen_at.set_subarray(subarray)
      .set_layout(TILEDB_ROW_MAJOR)
      .set_buffer("a", a_r_reopen_at);
  query_r_reopen_at.submit();
  CHECK(std::equal(a_r_reopen_at.begin(), a_r_reopen_at.end(), a_w.begin()));
  array_r_at.close();

  if (vfs.is_dir(array_name))
    vfs.remove_dir(array_name);
}

TEST_CASE(
    "C++ API: Open encrypted array at",
    "[cppapi], [cppapi-open-encrypted-array-at]") {
  const char key[] = "0123456789abcdeF0123456789abcdeF";
  uint32_t key_len = (uint32_t)strlen(key);

  Context ctx;
  VFS vfs(ctx);
  const std::string array_name = "cppapi_open_encrypted_array_at";
  if (vfs.is_dir(array_name))
    vfs.remove_dir(array_name);

  // Create array
  Domain domain(ctx);
  domain.add_dimension(Dimension::create<int>(ctx, "d", {{1, 4}}, 4));
  ArraySchema schema(ctx, TILEDB_DENSE);
  schema.set_domain(domain);
  schema.add_attribute(Attribute::create<int>(ctx, "a"));
  Array::create(array_name, schema, TILEDB_AES_256_GCM, key, key_len);

  REQUIRE_THROWS_AS(
      Array(ctx, array_name, TILEDB_WRITE, TILEDB_AES_256_GCM, key, key_len, 0),
      tiledb::TileDBError);

  // Write array
  Array array_w(
      ctx, array_name, TILEDB_WRITE, TILEDB_AES_256_GCM, key, key_len);
  Query query_w(ctx, array_w);
  query_w.set_layout(TILEDB_ROW_MAJOR);
  std::vector<int> a_w = {1, 2, 3, 4};
  query_w.set_buffer("a", a_w);
  query_w.submit();
  array_w.close();

  // Normal read
  Array array_r(ctx, array_name, TILEDB_READ, TILEDB_AES_256_GCM, key, key_len);
  std::vector<int> subarray = {1, 4};
  std::vector<int> a_r(4);
  Query query_r(ctx, array_r);
  query_r.set_subarray(subarray)
      .set_layout(TILEDB_ROW_MAJOR)
      .set_buffer("a", a_r);
  query_r.submit();
  array_r.close();
  CHECK(std::equal(a_r.begin(), a_r.end(), a_w.begin()));

  // Read from 0 timestamp
  Array array_r_at_0(
      ctx, array_name, TILEDB_READ, TILEDB_AES_256_GCM, key, key_len, 0);

  SECTION("Testing Array::Array") {
    // Nothing to do - just for clarity
  }

  SECTION("Testing Array::open") {
    array_r_at_0.close();
    array_r_at_0.open(TILEDB_READ, TILEDB_AES_256_GCM, key, key_len, 0);
  }

  std::vector<int> a_r_at_0(4);
  Query query_r_at_0(ctx, array_r_at_0);
  query_r_at_0.set_subarray(subarray)
      .set_layout(TILEDB_ROW_MAJOR)
      .set_buffer("a", a_r_at_0);
  query_r_at_0.submit();
  array_r_at_0.close();
  auto result = query_r_at_0.result_buffer_elements();
  CHECK(result["a"].second == 0);
  CHECK(!std::equal(a_r_at_0.begin(), a_r_at_0.end(), a_w.begin()));

  // Read from later timestamp
  auto timestamp = TILEDB_TIMESTAMP_NOW_MS;
  Array array_r_at(
      ctx,
      array_name,
      TILEDB_READ,
      TILEDB_AES_256_GCM,
      key,
      key_len,
      timestamp);

  SECTION("Testing Array::Array") {
    // Nothing to do - just for clarity
  }

  SECTION("Testing Array::open") {
    array_r_at.close();
    array_r_at.open(TILEDB_READ, TILEDB_AES_256_GCM, key, key_len, timestamp);
  }

  std::vector<int> a_r_at(4);
  Query query_r_at(ctx, array_r_at);
  query_r_at.set_subarray(subarray)
      .set_layout(TILEDB_ROW_MAJOR)
      .set_buffer("a", a_r_at_0);
  query_r_at.submit();
  array_r_at.close();
  CHECK(!std::equal(a_r_at.begin(), a_r_at.end(), a_w.begin()));

  if (vfs.is_dir(array_name))
    vfs.remove_dir(array_name);
}
