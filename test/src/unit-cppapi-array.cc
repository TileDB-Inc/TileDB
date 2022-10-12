/**
 * @file   unit-cppapi-array.cc
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
 * Tests the C++ API for array related functions.
 */

#include <test/support/tdb_catch.h>
#include "helpers.h"
#include "test/src/serialization_wrappers.h"
#include "tiledb/common/stdx_string.h"
#include "tiledb/sm/cpp_api/tiledb"
#include "tiledb/sm/filesystem/uri.h"
#include "tiledb/sm/misc/constants.h"
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
    auto a3 =
        Attribute::create<std::array<double, 2>>(ctx, "a3");  // (double, 2)
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

TEST_CASE("Config", "[cppapi][config]") {
  // Primarily to instansiate operator[]/= template
  tiledb::Config cfg;
  cfg["vfs.s3.region"] = "us-east-1a";
  cfg["vfs.s3.use_virtual_addressing"] = "true";
  CHECK((std::string)cfg["vfs.s3.region"] == "us-east-1a");
  CHECK((std::string)cfg["vfs.s3.use_virtual_addressing"] == "true");
}

TEST_CASE_METHOD(CPPArrayFx, "C++ API: Arrays", "[cppapi][basic]") {
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

  SECTION("Set Config") {
    // Create a config for the context
    tiledb::Config cfg;
    cfg["a"] = "1";
    cfg["b"] = "10";
    Context ctx1(cfg);

    // Create an array with ctx
    Array array(ctx1, "cpp_unit_array", TILEDB_READ);

    // Check that the config values are correct
    CHECK((std::string)array.config()["a"] == "1");
    CHECK((std::string)array.config()["b"] == "10");

    // Create a config for the array
    tiledb::Config cfg2;
    cfg2["b"] = "5";
    array.set_config(cfg2);

    // Check that the config values are correct
    CHECK((std::string)array.config()["a"] == "1");
    CHECK((std::string)array.config()["b"] == "5");

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

    uint64_t buf_back_elem_size;
    void* buf_back;
    uint64_t buf_back_nelem = 0;
    uint64_t* offsets_back;
    uint64_t offsets_back_nelem = 0;

    REQUIRE(
        Array::encryption_type(ctx, "cpp_unit_array") == TILEDB_NO_ENCRYPTION);

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
      query.set_data_buffer("a1", a1);
      query.set_data_buffer("a2", a2buf.second);
      query.set_offsets_buffer("a2", a2buf.first);
      query.set_data_buffer("a3", a3);
      query.set_data_buffer("a4", a4buf.second);
      query.set_offsets_buffer("a4", a4buf.first);
      query.set_data_buffer("a5", a5);
      query.set_layout(TILEDB_ROW_MAJOR);
      REQUIRE(query.submit() == Query::Status::COMPLETE);

      // check a1 buffers
      query.get_data_buffer(
          "a1", &buf_back, &buf_back_nelem, &buf_back_elem_size);
      REQUIRE(buf_back == a1.data());
      REQUIRE(buf_back_nelem == 2);
      REQUIRE(buf_back_elem_size == sizeof(int));

      // check a2 buffers
      query.get_data_buffer(
          "a2", &buf_back, &buf_back_nelem, &buf_back_elem_size);
      query.get_offsets_buffer("a2", &offsets_back, &offsets_back_nelem);
      REQUIRE(buf_back == a2buf.second.data());
      REQUIRE(buf_back_nelem == 7);
      REQUIRE(buf_back_elem_size == sizeof(char));
      REQUIRE(offsets_back == a2buf.first.data());
      REQUIRE(offsets_back_nelem == 2);
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

      a1.resize(2);
      a2buf.first.resize(2);
      a2buf.second.resize(57);
      a1.resize(32);
      a4buf.first.resize(2);
      a4buf.second.resize(122);
      a1.resize(48);

      Query query(ctx, array);

      CHECK(!query.has_results());

      query.set_data_buffer("a1", a1);
      query.set_data_buffer("a2", a2buf.second);
      query.set_offsets_buffer("a2", a2buf.first);
      query.set_data_buffer("a3", a3);
      query.set_data_buffer("a4", a4buf.second);
      query.set_offsets_buffer("a4", a4buf.first);
      query.set_data_buffer("a5", a5);
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
    query.set_data_buffer("a1", a1);
    query.set_data_buffer("a2", a2buf.second);
    query.set_offsets_buffer("a2", a2buf.first);
    query.set_data_buffer("a3", a3);
    query.set_data_buffer("a4", a4buf.second);
    query.set_offsets_buffer("a4", a4buf.first);
    query.set_data_buffer("a5", a5);

    query.set_layout(TILEDB_GLOBAL_ORDER);

    bool serialized_writes = false;
    SECTION("no serialization") {
      serialized_writes = false;
    }
    SECTION("serialization enabled global order write") {
#ifdef TILEDB_SERIALIZATION
      serialized_writes = true;
#endif
    }
    if (!serialized_writes) {
      CHECK(query.submit() == tiledb::Query::Status::COMPLETE);
      REQUIRE_NOTHROW(query.finalize());
    } else {
      test::submit_and_finalize_serialized_query(ctx, query);
    }

    // Check non-empty domain while array open in write mode
    CHECK_THROWS(array.non_empty_domain<int>(1));
    CHECK_THROWS(array.non_empty_domain<int>("d1"));

    array.close();

    // Check non-empty domain before open from read
    CHECK_THROWS(array.non_empty_domain<int>(1));
    CHECK_THROWS(array.non_empty_domain<int>("d1"));

    array.open(TILEDB_READ);

    // Check non-empty domain
    auto non_empty = array.non_empty_domain<int>();
    REQUIRE(non_empty.size() == 2);
    CHECK(non_empty[0].second.first == 0);
    CHECK(non_empty[0].second.second == d1_tile - 1);
    CHECK(non_empty[1].second.first == 0);
    CHECK(non_empty[1].second.second == d2_tile - 1);

    // Check non-empty domain from index
    CHECK_THROWS(array.non_empty_domain<int>(5));
    auto non_empty_0 = array.non_empty_domain<int>(0);
    auto non_empty_1 = array.non_empty_domain<int>(1);
    CHECK(non_empty_0.first == 0);
    CHECK(non_empty_0.second == d1_tile - 1);
    CHECK(non_empty_1.first == 0);
    CHECK(non_empty_1.second == d2_tile - 1);

    // Check non-empty domain from name
    CHECK_THROWS(array.non_empty_domain<int>("foo"));
    non_empty_0 = array.non_empty_domain<int>("d1");
    non_empty_1 = array.non_empty_domain<int>("d2");
    CHECK(non_empty_0.first == 0);
    CHECK(non_empty_0.second == d1_tile - 1);
    CHECK(non_empty_1.first == 0);
    CHECK(non_empty_1.second == d2_tile - 1);

    array.close();
  }

  SECTION("Global order write - no dummy values") {
    std::vector<int> a1 = {1, 2};
    std::vector<int> subarray = {0, 1, 0, 0};
    Array array(ctx, "cpp_unit_array", TILEDB_WRITE);
    Query query(ctx, array, TILEDB_WRITE);
    query.set_subarray(subarray);
    query.set_data_buffer("a1", a1);
    query.set_layout(TILEDB_GLOBAL_ORDER);
    // Incorrect subarray for global order
    REQUIRE_THROWS(query.submit());
    query.finalize();
    array.close();
  }
}

TEST_CASE("C++ API: Zero length buffer", "[cppapi][zero-length]") {
  const std::string array_name_1d = "cpp_unit_array_1d";
  Context ctx;
  VFS vfs(ctx);

  tiledb_layout_t write_layout = TILEDB_GLOBAL_ORDER;
  tiledb_array_type_t array_type = TILEDB_DENSE;
  bool null_pointer = true;

  bool serialized_writes = false;
  SECTION("SPARSE") {
    array_type = TILEDB_SPARSE;
    SECTION("GLOBAL_ORDER") {
      write_layout = TILEDB_GLOBAL_ORDER;

      SECTION("NULL_PTR") {
        null_pointer = true;
      }

      SECTION("NON_NULL_PTR") {
        null_pointer = false;
      }
      SECTION("no serialization") {
        serialized_writes = false;
      }
      SECTION("serialization enabled global order write") {
#ifdef TILEDB_SERIALIZATION
        serialized_writes = true;
#endif
      }
    }

    SECTION("UNORDERED") {
      write_layout = TILEDB_UNORDERED;
    }
  }

  SECTION("DENSE") {
    array_type = TILEDB_DENSE;
    SECTION("GLOBAL_ORDER") {
      write_layout = TILEDB_GLOBAL_ORDER;

      SECTION("NULL_PTR") {
        null_pointer = true;
      }

      SECTION("NON_NULL_PTR") {
        null_pointer = false;
      }
    }
  }

  if (vfs.is_dir(array_name_1d))
    vfs.remove_dir(array_name_1d);

  ArraySchema schema(ctx, array_type);
  Domain domain(ctx);
  domain.add_dimension(Dimension::create<int32_t>(ctx, "d", {{0, 2}}, 3));
  schema.set_domain(domain);
  schema.add_attribute(Attribute::create<std::vector<int32_t>>(ctx, "a"));
  schema.add_attribute(Attribute::create<uint64_t>(ctx, "b"));
  Array::create(array_name_1d, schema);

  {
    Array array(ctx, array_name_1d, TILEDB_WRITE);

    std::vector<int32_t> a, coord = {0, 1, 2};
    std::vector<uint64_t> a_offset = {0, 0, 0};
    std::vector<uint64_t> b = {1, 2, 3};

    if (!null_pointer) {
      a.reserve(10);
    }

    a = {};
    Query q(ctx, array, TILEDB_WRITE);
    q.set_layout(write_layout);
    if (array_type == TILEDB_SPARSE)
      q.set_data_buffer("d", coord);
    q.set_data_buffer("a", a);
    q.set_offsets_buffer("a", a_offset);
    q.set_data_buffer("b", b);
    if (!serialized_writes || write_layout != TILEDB_GLOBAL_ORDER) {
      q.submit();
      q.finalize();
    } else {
      test::submit_and_finalize_serialized_query(ctx, q);
    }

    array.close();
  }

  {
    Array array(ctx, array_name_1d, TILEDB_READ);

    std::vector<int32_t> a(3);
    std::vector<uint64_t> a_offset = {1, 1, 1};
    std::vector<uint64_t> b(3);

    a.reserve(10);
    a = {};
    const std::vector<int> subarray = {0, 2};
    Query q(ctx, array, TILEDB_READ);
    q.set_layout(TILEDB_GLOBAL_ORDER);
    q.set_subarray(subarray);
    q.set_data_buffer("a", a);
    q.set_offsets_buffer("a", a_offset);
    q.set_data_buffer("b", b);
    REQUIRE(q.submit() == Query::Status::COMPLETE);
    CHECK((int)q.result_buffer_elements()["a"].first == 3);
    CHECK((int)q.result_buffer_elements()["a"].second == 0);
    CHECK((int)q.result_buffer_elements()["b"].second == 3);

    array.close();

    for (size_t i = 0; i < 3; i++) {
      CHECK(a_offset[i] == 0);
      CHECK(b[i] == i + 1);
    }
  }

  if (vfs.is_dir(array_name_1d))
    vfs.remove_dir(array_name_1d);
}

TEST_CASE("C++ API: Incorrect offsets", "[cppapi][invalid-offsets]") {
  const std::string array_name_1d = "cpp_unit_array_1d";
  Context ctx;
  VFS vfs(ctx);

  if (vfs.is_dir(array_name_1d))
    vfs.remove_dir(array_name_1d);

  ArraySchema schema(ctx, TILEDB_SPARSE);
  Domain domain(ctx);
  domain.add_dimension(Dimension::create<int32_t>(ctx, "d", {{0, 1000}}, 1001));
  schema.set_domain(domain);
  schema.add_attribute(Attribute::create<std::vector<int32_t>>(ctx, "a"));
  Array::create(array_name_1d, schema);
  Array array(ctx, array_name_1d, TILEDB_WRITE);

  std::vector<int32_t> a, coord = {10, 20, 30};
  std::vector<uint64_t> a_offset = {0, 0, 0};

  // Test case of non-ascending offsets
  {
    a = {0, 1, 2};
    a_offset = {0, 2, 1};
    Query q(ctx, array, TILEDB_WRITE);
    q.set_layout(TILEDB_GLOBAL_ORDER);
    q.set_coordinates(coord);
    q.set_data_buffer("a", a);
    q.set_offsets_buffer("a", a_offset);
    REQUIRE_THROWS(q.submit());
  }

  array.close();

  if (vfs.is_dir(array_name_1d))
    vfs.remove_dir(array_name_1d);
}

TEST_CASE("C++ API: Read subarray with expanded domain", "[cppapi][dense]") {
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
            .set_data_buffer("a", data_w);
        query_w.submit();
        array_w.close();

        // Read
        Array array(ctx, array_name, TILEDB_READ);
        Query query(ctx, array);
        const std::vector<int> subarray = {0, 3, 0, 3};
        std::vector<int> data(16);
        query.set_subarray(subarray)
            .set_layout(TILEDB_ROW_MAJOR)
            .set_data_buffer("a", data);
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

TEST_CASE("C++ API: Consolidation of empty arrays", "[cppapi][consolidation]") {
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

TEST_CASE(
    "C++ API: Consolidation of sequential fragment writes",
    "[cppapi][consolidation][sequential]") {
  Context ctx;
  VFS vfs(ctx);
  const std::string array_name = "cpp_unit_array";

  if (vfs.is_dir(array_name))
    vfs.remove_dir(array_name);

  Domain domain(ctx);
  domain.add_dimension(Dimension::create<int>(ctx, "d", {{0, 11}}, 12));

  ArraySchema schema(ctx, TILEDB_DENSE);
  schema.set_domain(domain).set_order({{TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR}});
  schema.add_attribute(Attribute::create<int>(ctx, "a"));

  tiledb::Array::create(array_name, schema);
  auto array_w = tiledb::Array(ctx, array_name, TILEDB_WRITE);
  auto query_w = tiledb::Query(ctx, array_w, TILEDB_WRITE);
  std::vector<int> data = {0, 1};

  query_w.set_data_buffer("a", data).set_subarray({0, 1}).submit();
  query_w.set_data_buffer("a", data).set_subarray({2, 3}).submit();
  // this fragment write caused crash during consolidation
  //   https://github.com/TileDB-Inc/TileDB/issues/1205
  //   https://github.com/TileDB-Inc/TileDB/issues/1212
  query_w.set_data_buffer("a", data).set_subarray({4, 5}).submit();
  query_w.finalize();
  array_w.close();
  CHECK(tiledb::test::num_fragments(array_name) == 3);
  Array::consolidate(ctx, array_name);
  CHECK(tiledb::test::num_fragments(array_name) == 4);
  Array::vacuum(ctx, array_name);
  CHECK(tiledb::test::num_fragments(array_name) == 1);

  if (vfs.is_dir(array_name))
    vfs.remove_dir(array_name);
}

TEST_CASE("C++ API: Encrypted array", "[cppapi][encryption]") {
  const char key[] = "0123456789abcdeF0123456789abcdeF";
  tiledb::Config cfg;
  cfg["sm.encryption_type"] = "AES_256_GCM";
  cfg["sm.encryption_key"] = key;
  Context ctx(cfg);
  VFS vfs(ctx);
  const std::string array_name = "cpp_unit_array";

  if (vfs.is_dir(array_name))
    vfs.remove_dir(array_name);

  Domain domain(ctx);
  domain.add_dimension(Dimension::create<int>(ctx, "d", {{0, 3}}, 1));
  ArraySchema schema(ctx, TILEDB_DENSE);
  schema.set_domain(domain);
  schema.add_attribute(Attribute::create<int>(ctx, "a"));

  REQUIRE_THROWS_AS(
      Array::encryption_type(ctx, array_name), tiledb::TileDBError);
  Array::create(array_name, schema);
  REQUIRE(Array::encryption_type(ctx, array_name) == TILEDB_AES_256_GCM);

  ArraySchema schema_read(ctx, array_name);

  Array array(ctx, array_name, TILEDB_WRITE);
  REQUIRE(Array::encryption_type(ctx, array_name) == TILEDB_AES_256_GCM);
  array.close();

  array.open(TILEDB_WRITE);
  REQUIRE(Array::encryption_type(ctx, array_name) == TILEDB_AES_256_GCM);

  Query query(ctx, array);
  query.set_layout(TILEDB_ROW_MAJOR);
  std::vector<int> a_values = {1, 2, 3, 4};
  query.set_data_buffer("a", a_values);
  query.submit();
  array.close();

  // Write a second time, as consolidation needs at least two fragments
  // to trigger an error with encryption (consolidation is a noop for
  // single-fragment arrays and, thus, always succeeds)
  Array array_2(ctx, array_name, TILEDB_WRITE);
  Query query_2(ctx, array_2);
  query_2.set_layout(TILEDB_ROW_MAJOR);
  query_2.set_data_buffer("a", a_values);
  query_2.submit();
  array_2.close();

  Array::consolidate(ctx, array_name);

  array.open(TILEDB_READ);
  array.reopen();

  std::vector<int> subarray = {0, 3};
  std::vector<int> a_read(4);
  Query query_r(ctx, array);
  query_r.set_subarray(subarray)
      .set_layout(TILEDB_ROW_MAJOR)
      .set_data_buffer("a", a_read);
  query_r.submit();
  array.close();

  for (unsigned i = 0; i < 4; i++)
    REQUIRE(a_values[i] == a_read[i]);

  Array array_3(ctx, array_name, TILEDB_READ);
  a_read = std::vector<int>(4, 0);
  Query query_r2(ctx, array_3);
  query_r2.set_subarray(subarray)
      .set_layout(TILEDB_ROW_MAJOR)
      .set_data_buffer("a", a_read);
  query_r2.submit();
  array_3.close();

  for (unsigned i = 0; i < 4; i++)
    REQUIRE(a_values[i] == a_read[i]);

  if (vfs.is_dir(array_name))
    vfs.remove_dir(array_name);
}

TEST_CASE("C++ API: Encrypted array, std::string key", "[cppapi][encryption]") {
  const std::string key = "0123456789abcdeF0123456789abcdeF";
  tiledb::Config cfg;
  cfg["sm.encryption_type"] = "AES_256_GCM";
  cfg["sm.encryption_key"] = key.c_str();
  Context ctx(cfg);
  VFS vfs(ctx);
  const std::string array_name = "cpp_unit_array";

  if (vfs.is_dir(array_name))
    vfs.remove_dir(array_name);

  Domain domain(ctx);
  domain.add_dimension(Dimension::create<int>(ctx, "d", {{0, 3}}, 1));
  ArraySchema schema(ctx, TILEDB_DENSE);
  schema.set_domain(domain);
  schema.add_attribute(Attribute::create<int>(ctx, "a"));

  REQUIRE_THROWS_AS(
      Array::encryption_type(ctx, array_name), tiledb::TileDBError);
  Array::create(array_name, schema);
  REQUIRE(Array::encryption_type(ctx, array_name) == TILEDB_AES_256_GCM);

  ArraySchema schema_read(ctx, array_name);

  Array array(ctx, array_name, TILEDB_WRITE);
  REQUIRE(Array::encryption_type(ctx, array_name) == TILEDB_AES_256_GCM);
  array.close();
  array.open(TILEDB_WRITE);
  REQUIRE(Array::encryption_type(ctx, array_name) == TILEDB_AES_256_GCM);

  Array array2(ctx, array_name, TILEDB_WRITE);

  Query query(ctx, array);
  query.set_layout(TILEDB_ROW_MAJOR);
  std::vector<int> a_values = {1, 2, 3, 4};
  query.set_data_buffer("a", a_values);
  query.submit();
  array.close();

  // Write a second time, as consolidation needs at least two fragments
  // to trigger an error with encryption (consolidation is a noop for
  // single-fragment arrays and, thus, always succeeds)
  Array array_2(ctx, array_name, TILEDB_WRITE);
  Query query_2(ctx, array_2);
  query_2.set_layout(TILEDB_ROW_MAJOR);
  query_2.set_data_buffer("a", a_values);
  query_2.submit();
  array_2.close();

  Array::consolidate(ctx, array_name);

  array.open(TILEDB_READ);
  array.reopen();

  std::vector<int> subarray = {0, 3};
  std::vector<int> a_read(4);
  Query query_r(ctx, array);
  query_r.set_subarray(subarray)
      .set_layout(TILEDB_ROW_MAJOR)
      .set_data_buffer("a", a_read);
  query_r.submit();
  array.close();

  for (unsigned i = 0; i < 4; i++)
    REQUIRE(a_values[i] == a_read[i]);

  if (vfs.is_dir(array_name))
    vfs.remove_dir(array_name);
}

TEST_CASE(
    "C++ API: Open array with anonymous attribute",
    "[cppapi][open-array-anon-attr]") {
  Context ctx;
  VFS vfs(ctx);
  const std::string array_name = "cppapi_open_array_anon_attr";
  if (vfs.is_dir(array_name))
    vfs.remove_dir(array_name);

  // Create array
  Domain domain(ctx);
  domain.add_dimension(Dimension::create<int>(ctx, "d", {{1, 4}}, 4));
  ArraySchema schema(ctx, TILEDB_DENSE);
  schema.set_domain(domain);
  schema.add_attribute(Attribute::create<int>(ctx, ""));
  Array::create(array_name, schema);

  Array array(ctx, array_name, TILEDB_READ);
  auto reloaded_schema = array.schema();

  REQUIRE(reloaded_schema.attribute_num() == 1);

  array.close();
  if (vfs.is_dir(array_name))
    vfs.remove_dir(array_name);
}

TEST_CASE("C++ API: Open array at", "[cppapi][open-array-at]") {
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

  // Write array
  Array array_w(ctx, array_name, TILEDB_WRITE);
  Query query_w(ctx, array_w);
  query_w.set_layout(TILEDB_ROW_MAJOR);
  std::vector<int> a_w = {1, 2, 3, 4};
  query_w.set_data_buffer("a", a_w);
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
      .set_data_buffer("a", a_r);
  query_r.submit();
  array_r.close();
  CHECK(std::equal(a_r.begin(), a_r.end(), a_w.begin()));

  // Read from 0 timestamp
  Array array_r_at_0(ctx, array_name, TILEDB_READ);
  array_r_at_0.close();
  array_r_at_0.set_open_timestamp_end(0);
  array_r_at_0.open(TILEDB_READ);
  CHECK(array_r_at_0.open_timestamp_end() == 0);

  SECTION("Testing Array::Array") {
    // Nothing to do - just for clarity
  }

  SECTION("Testing Array::open") {
    array_r_at_0.close();
    array_r_at_0.open(TILEDB_READ);
  }

  std::vector<int> a_r_at_0(4);
  Query query_r_at_0(ctx, array_r_at_0);
  query_r_at_0.set_subarray(subarray)
      .set_layout(TILEDB_ROW_MAJOR)
      .set_data_buffer("a", a_r_at_0);
  query_r_at_0.submit();
  array_r_at_0.close();
  auto result = query_r_at_0.result_buffer_elements();
  CHECK(result["a"].second == 4);  // Empty arrays return fill values
  CHECK(!std::equal(a_r_at_0.begin(), a_r_at_0.end(), a_w.begin()));

  // Read from later timestamp
  auto timestamp = TILEDB_TIMESTAMP_NOW_MS;
  Array array_r_at(ctx, array_name, TILEDB_READ);
  array_r_at.close();
  array_r_at.set_open_timestamp_end(timestamp);
  array_r_at.open(TILEDB_READ);
  CHECK(array_r_at.open_timestamp_end() == timestamp);

  SECTION("Testing Array::Array") {
    // Nothing to do - just for clarity
  }

  SECTION("Testing Array::open") {
    array_r_at.close();
    array_r_at.open(TILEDB_READ);
  }

  std::vector<int> a_r_at(4);
  Query query_r_at(ctx, array_r_at);
  query_r_at.set_subarray(subarray)
      .set_layout(TILEDB_ROW_MAJOR)
      .set_data_buffer("a", a_r_at_0);
  query_r_at.submit();
  CHECK(!std::equal(a_r_at.begin(), a_r_at.end(), a_w.begin()));
  array_r_at.close();

  // Reopen at first timestamp.
  Array array_reopen_at(ctx, array_name, TILEDB_READ);
  array_reopen_at.set_open_timestamp_end(first_timestamp);
  array_reopen_at.reopen();
  CHECK(array_reopen_at.open_timestamp_end() == first_timestamp);
  std::vector<int> a_r_reopen_at(4);
  Query query_r_reopen_at(ctx, array_reopen_at);
  query_r_reopen_at.set_subarray(subarray)
      .set_layout(TILEDB_ROW_MAJOR)
      .set_data_buffer("a", a_r_reopen_at);
  query_r_reopen_at.submit();
  CHECK(std::equal(a_r_reopen_at.begin(), a_r_reopen_at.end(), a_w.begin()));
  array_reopen_at.close();

  if (vfs.is_dir(array_name))
    vfs.remove_dir(array_name);
}

TEST_CASE(
    "C++ API: Open encrypted array at", "[cppapi][open-encrypted-array-at]") {
  const char key[] = "0123456789abcdeF0123456789abcdeF";
  tiledb::Config cfg;
  cfg["sm.encryption_type"] = "AES_256_GCM";
  cfg["sm.encryption_key"] = key;
  Context ctx(cfg);
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
  Array::create(array_name, schema);

  // Write array
  Array array_w(ctx, array_name, TILEDB_WRITE);
  Query query_w(ctx, array_w);
  query_w.set_layout(TILEDB_ROW_MAJOR);
  std::vector<int> a_w = {1, 2, 3, 4};
  query_w.set_data_buffer("a", a_w);
  query_w.submit();
  array_w.close();

  // Normal read
  Array array_r(ctx, array_name, TILEDB_READ);
  std::vector<int> subarray = {1, 4};
  std::vector<int> a_r(4);
  Query query_r(ctx, array_r);
  query_r.set_subarray(subarray)
      .set_layout(TILEDB_ROW_MAJOR)
      .set_data_buffer("a", a_r);
  query_r.submit();
  array_r.close();
  CHECK(std::equal(a_r.begin(), a_r.end(), a_w.begin()));

  // Read from 0 timestamp
  Array array_r_at_0(ctx, array_name, TILEDB_READ);
  array_r_at_0.close();
  array_r_at_0.set_open_timestamp_end(0);
  array_r_at_0.open(TILEDB_READ);

  SECTION("Testing Array::Array") {
    // Nothing to do - just for clarity
  }

  SECTION("Testing Array::open") {
    array_r_at_0.close();
    array_r_at_0.open(TILEDB_READ);
  }

  std::vector<int> a_r_at_0(4);
  Query query_r_at_0(ctx, array_r_at_0);
  query_r_at_0.set_subarray(subarray)
      .set_layout(TILEDB_ROW_MAJOR)
      .set_data_buffer("a", a_r_at_0);
  query_r_at_0.submit();
  array_r_at_0.close();
  auto result = query_r_at_0.result_buffer_elements();
  CHECK(result["a"].second == 4);  // Empty arrays return fill values
  CHECK(!std::equal(a_r_at_0.begin(), a_r_at_0.end(), a_w.begin()));

  // Read from later timestamp
  auto timestamp = TILEDB_TIMESTAMP_NOW_MS;
  Array array_r_at(ctx, array_name, TILEDB_READ);
  array_r_at.close();
  array_r_at.set_open_timestamp_end(timestamp);
  array_r_at.open(TILEDB_READ);

  SECTION("Testing Array::Array") {
    // Nothing to do - just for clarity
  }

  SECTION("Testing Array::open") {
    array_r_at.close();
    array_r_at.open(TILEDB_READ);
  }

  std::vector<int> a_r_at(4);
  Query query_r_at(ctx, array_r_at);
  query_r_at.set_subarray(subarray)
      .set_layout(TILEDB_ROW_MAJOR)
      .set_data_buffer("a", a_r_at_0);
  query_r_at.submit();
  array_r_at.close();
  CHECK(!std::equal(a_r_at.begin(), a_r_at.end(), a_w.begin()));

  if (vfs.is_dir(array_name))
    vfs.remove_dir(array_name);
}

TEST_CASE(
    "C++ API: Writing single cell with global order",
    "[cppapi][sparse][global]") {
  const std::string array_name = "cpp_unit_array";
  Context ctx;
  VFS vfs(ctx);

  if (vfs.is_dir(array_name))
    vfs.remove_dir(array_name);

  // Create
  Domain domain(ctx);
  domain.add_dimension(Dimension::create<int>(ctx, "rows", {{0, 3}}, 4))
      .add_dimension(Dimension::create<int>(ctx, "cols", {{0, 3}}, 4));
  ArraySchema schema(ctx, TILEDB_SPARSE);
  schema.set_domain(domain).set_order({{TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR}});
  schema.add_attribute(Attribute::create<int>(ctx, "a"));
  Array::create(array_name, schema);

  // Write
  std::vector<int> data_w = {1};
  std::vector<int> coords_w = {0, 0};
  Array array_w(ctx, array_name, TILEDB_WRITE);
  Query query_w(ctx, array_w);
  query_w.set_coordinates(coords_w)
      .set_layout(TILEDB_GLOBAL_ORDER)
      .set_data_buffer("a", data_w);
  bool serialized_writes = false;
  SECTION("no serialization") {
    serialized_writes = false;
  }
  SECTION("serialization enabled global order write") {
#ifdef TILEDB_SERIALIZATION
    serialized_writes = true;
#endif
  }
  if (!serialized_writes) {
    query_w.submit();
    query_w.finalize();
  } else {
    test::submit_and_finalize_serialized_query(ctx, query_w);
  }
  array_w.close();

  // Read
  Array array(ctx, array_name, TILEDB_READ);
  Query query(ctx, array);
  const std::vector<int> subarray = {0, 0, 0, 0};
  std::vector<int> data(1);
  query.set_subarray(subarray)
      .set_layout(TILEDB_ROW_MAJOR)
      .set_data_buffer("a", data);
  query.submit();
  array.close();

  REQUIRE(data[0] == 1);

  if (vfs.is_dir(array_name))
    vfs.remove_dir(array_name);
}

TEST_CASE("C++ API: Write cell with large cell val num", "[cppapi][sparse]") {
  const std::string array_name = "cpp_unit_array";
  Context ctx;
  VFS vfs(ctx);

  if (vfs.is_dir(array_name))
    vfs.remove_dir(array_name);

  // Create array with a large fixed-length attribute
  const size_t cell_val_num = 70000;
  auto attr = Attribute::create<int>(ctx, "a")
                  .set_cell_val_num(cell_val_num)
                  .set_filter_list(FilterList(ctx).add_filter(
                      Filter(ctx, TILEDB_FILTER_BZIP2)));
  Array::create(
      array_name,
      ArraySchema(ctx, TILEDB_SPARSE)
          .set_domain(Domain(ctx).add_dimension(
              Dimension::create<uint32_t>(ctx, "cols", {{0, 9}}, 5)))
          .set_order({{TILEDB_COL_MAJOR, TILEDB_COL_MAJOR}})
          .add_attribute(attr));

  // Write a single cell (this used to crash because of
  // https://github.com/TileDB-Inc/TileDB/issues/1155)
  std::vector<int> data_w(cell_val_num);
  std::vector<uint32_t> coords_w = {4};
  for (auto i = 0u; i < cell_val_num; i++)
    data_w[i] = 2 * i;

  Array array_w(ctx, array_name, TILEDB_WRITE);
  Query query_w(ctx, array_w);
  query_w.set_layout(TILEDB_UNORDERED)
      .set_data_buffer("a", data_w)
      .set_coordinates(coords_w)
      .submit();
  query_w.finalize();
  array_w.close();

  // Read and check results
  std::vector<int> data_r(cell_val_num, -1);
  std::vector<uint32_t> coords_r = {4};
  Array array_r(ctx, array_name, TILEDB_READ);
  Query query_r(ctx, array_r);
  query_r.set_layout(TILEDB_ROW_MAJOR)
      .set_data_buffer("a", data_r)
      .set_coordinates(coords_r);
  REQUIRE(query_r.submit() == Query::Status::COMPLETE);

  auto result_num = query_r.result_buffer_elements()["a"].second;
  REQUIRE(result_num == cell_val_num);
  for (int i = 0; i < (int)cell_val_num; i++)
    REQUIRE(data_r[i] == 2 * i);

  array_r.close();

  if (vfs.is_dir(array_name))
    vfs.remove_dir(array_name);
}

using namespace tiledb::test;

TEST_CASE("C++ API: Test heterogeneous dimensions", "[cppapi][sparse][heter]") {
  const std::string array_name = "cpp_unit_array";
  Context ctx;
  VFS vfs(ctx);

  if (vfs.is_dir(array_name))
    vfs.remove_dir(array_name);

  // Create array
  auto d1 = Dimension::create<float>(ctx, "d1", {1.0f, 20.0f}, 5.0f);
  auto d2 = Dimension::create<int64_t>(ctx, "d2", {1, 30}, 5);
  auto a = Attribute::create<int32_t>(ctx, "a");
  Domain dom(ctx);
  dom.add_dimensions(d1, d2);
  ArraySchema schema(ctx, TILEDB_SPARSE);
  schema.add_attribute(a);
  schema.set_domain(dom);
  Array::create(array_name, schema);

  // Write
  Array array(ctx, array_name, TILEDB_WRITE);
  std::vector<float> buff_d1 = {1.1f, 1.2f, 1.3f, 1.4f};
  std::vector<int64_t> buff_d2 = {1, 2, 3, 4};
  std::vector<int32_t> buff_a = {1, 2, 3, 4};
  Query query(ctx, array, TILEDB_WRITE);
  query.set_data_buffer("d1", buff_d1);
  query.set_data_buffer("d2", buff_d2);
  query.set_data_buffer("a", buff_a);
  query.set_layout(TILEDB_UNORDERED);
  query.submit();
  array.close();

  // Read
  Array array_r(ctx, array_name, TILEDB_READ);
  std::vector<float> buff_d1_r(4);
  std::vector<int64_t> buff_d2_r(4);
  std::vector<int32_t> buff_a_r(4);
  Query query_r(ctx, array_r, TILEDB_READ);
  query_r.set_data_buffer("d1", buff_d1_r);
  query_r.set_data_buffer("d2", buff_d2_r);
  query_r.set_data_buffer("a", buff_a_r);
  query_r.set_layout(TILEDB_UNORDERED);
  query_r.add_range(0, 1.0f, 20.0f);
  query_r.add_range(1, (int64_t)1, (int64_t)30);
  query_r.submit();

  auto ret = query.result_buffer_elements();
  REQUIRE(ret.size() == 3);
  CHECK(ret["a"].first == 0);
  CHECK(ret["a"].second == 4);
  CHECK(ret["d1"].first == 0);
  CHECK(ret["d1"].second == 4);
  CHECK(ret["d2"].first == 0);
  CHECK(ret["d2"].second == 4);

  array_r.close();

  CHECK(buff_d1 == buff_d1_r);
  CHECK(buff_d2 == buff_d2_r);
  CHECK(buff_a == buff_a_r);

  if (vfs.is_dir(array_name))
    vfs.remove_dir(array_name);
}

TEST_CASE(
    "C++ API: Test string dimensions, 1d",
    "[cppapi][sparse][string-dims][1d]") {
  const std::string array_name = "cpp_unit_array";
  Context ctx;
  VFS vfs(ctx);

  if (vfs.is_dir(array_name))
    vfs.remove_dir(array_name);

  // Create array
  auto d = Dimension::create(ctx, "d", TILEDB_STRING_ASCII, nullptr, nullptr);
  Domain dom(ctx);
  dom.add_dimension(d);
  ArraySchema schema(ctx, TILEDB_SPARSE);
  auto a = Attribute::create<int32_t>(ctx, "a");
  schema.add_attribute(a);
  schema.set_domain(dom);
  Array::create(array_name, schema);

  // Write
  Array array(ctx, array_name, TILEDB_WRITE);
  std::vector<int32_t> buff_a = {3, 2, 1, 4};
  std::string d_data("ccbbddddaa");
  uint64_t d_off[] = {0, 2, 4, 8};
  uint64_t d_off_size = 4;
  Query query(ctx, array, TILEDB_WRITE);
  CHECK_NOTHROW(
      query.set_data_buffer("d", (void*)d_data.c_str(), d_data.size()));
  CHECK_NOTHROW(query.set_offsets_buffer("d", d_off, d_off_size));
  query.set_data_buffer("a", buff_a);
  query.set_layout(TILEDB_UNORDERED);
  CHECK_NOTHROW(query.submit());
  array.close();

  // Non-empty domain
  Array array_r(ctx, array_name, TILEDB_READ);
  auto non_empty_domain = array_r.non_empty_domain_var("d");
  CHECK(non_empty_domain.first == std::string("aa"));
  CHECK(non_empty_domain.second == std::string("dddd"));
  non_empty_domain = array_r.non_empty_domain_var(0);
  CHECK(non_empty_domain.first == std::string("aa"));
  CHECK(non_empty_domain.second == std::string("dddd"));
  CHECK_THROWS(array_r.non_empty_domain_var(1));
  CHECK_THROWS(array_r.non_empty_domain_var("foo"));

  // Read
  bool empty_results = false;
  std::string s1("a", 1);
  std::string s2("ee", 2);
  Query query_r(ctx, array_r, TILEDB_READ);

  SECTION("Non empty range") {
    query_r.add_range(0, s1, s2);
    CHECK_THROWS(query_r.add_range(1, s1, s2));

    // Check range
    CHECK_THROWS(query_r.range(1, 1));
    std::array<std::string, 2> range = query_r.range(0, 0);
    CHECK(range[0] == s1);
    CHECK(range[1] == s2);
  }

  SECTION("Empty first range") {
    query_r.add_range(0, "", s2);
    CHECK_THROWS(query_r.add_range(1, "", s2));

    // Check range
    CHECK_THROWS(query_r.range(1, 1));
    std::array<std::string, 2> range = query_r.range(0, 0);
    CHECK(range[0] == "");
    CHECK(range[1] == s2);
  }

  SECTION("Empty second range") {
    query_r.add_range(0, s1, "");
    CHECK_THROWS(query_r.add_range(1, s1, ""));

    // Check range
    CHECK_THROWS(query_r.range(1, 1));
    std::array<std::string, 2> range = query_r.range(0, 0);
    CHECK(range[0] == s1);
    CHECK(range[1] == "");
    empty_results = true;
  }

  SECTION("Empty ranges") {
    query_r.add_range(0, std::string(""), std::string(""));
    CHECK_THROWS(query_r.add_range(1, std::string(""), std::string("")));

    // Check range
    CHECK_THROWS(query_r.range(1, 1));
    std::array<std::string, 2> range = query_r.range(0, 0);
    CHECK(range[0] == "");
    CHECK(range[1] == "");
    empty_results = true;
  }

  std::string data;
  data.resize(10);
  std::vector<uint64_t> offsets(4);
  query_r.set_data_buffer("d", data);
  query_r.set_offsets_buffer("d", offsets);
  query_r.submit();
  if (empty_results) {
    auto data_str = data.c_str();
    for (uint64_t i = 0; i < 10; i++) {
      CHECK(data_str[i] == 0);
    }
  } else {
    CHECK(data == "aabbccdddd");
  }
  std::vector<uint64_t> c_offsets(4, 0);
  if (!empty_results)
    c_offsets = {0, 2, 4, 6};
  CHECK(offsets == c_offsets);
  auto ret = query.result_buffer_elements();
  REQUIRE(ret.size() == 2);
  CHECK(ret["a"].first == 0);
  CHECK(ret["a"].second == 4);
  CHECK(ret["d"].first == 4);
  CHECK(ret["d"].second == 10);

  // Close array
  array_r.close();

  if (vfs.is_dir(array_name))
    vfs.remove_dir(array_name);
}

TEST_CASE(
    "C++ API: Test string dimensions, 1d, col-major",
    "[cppapi][sparse][string-dims][1d][col-major]") {
  Context ctx;
  VFS vfs(ctx);

  std::string array_name = "tes_string_dims";
  if (vfs.is_dir(array_name))
    vfs.remove_dir(array_name);

  // Create array
  auto d = Dimension::create(ctx, "d", TILEDB_STRING_ASCII, nullptr, nullptr);
  Domain dom(ctx);
  dom.add_dimension(d);
  ArraySchema schema(ctx, TILEDB_SPARSE);
  auto a = Attribute::create<int32_t>(ctx, "a");
  schema.add_attribute(a);
  schema.set_tile_order(TILEDB_COL_MAJOR);
  schema.set_cell_order(TILEDB_COL_MAJOR);
  schema.set_domain(dom);
  Array::create(array_name, schema);

  // Write
  Array array(ctx, array_name, TILEDB_WRITE);
  std::vector<int32_t> buff_a = {3, 2, 1, 4};
  std::string d_data("ccbbddddaa");
  uint64_t d_off[] = {0, 2, 4, 8};
  uint64_t d_off_size = 4;
  Query query(ctx, array, TILEDB_WRITE);
  CHECK_NOTHROW(
      query.set_data_buffer("d", (void*)d_data.c_str(), d_data.size()));
  CHECK_NOTHROW(query.set_offsets_buffer("d", d_off, d_off_size));
  query.set_data_buffer("a", buff_a);
  query.set_layout(TILEDB_UNORDERED);
  CHECK_NOTHROW(query.submit());
  array.close();

  // Non-empty domain
  Array array_r(ctx, array_name, TILEDB_READ);
  auto non_empty_domain = array_r.non_empty_domain_var("d");
  CHECK(non_empty_domain.first == std::string("aa"));
  CHECK(non_empty_domain.second == std::string("dddd"));
  non_empty_domain = array_r.non_empty_domain_var(0);
  CHECK(non_empty_domain.first == std::string("aa"));
  CHECK(non_empty_domain.second == std::string("dddd"));

  // Read
  std::string s1("a", 1);
  std::string s2("ee", 2);
  Query query_r(ctx, array_r, TILEDB_READ);
  query_r.add_range(0, s1, s2);
  std::string data;
  data.resize(10);
  std::vector<uint64_t> offsets(4);
  query_r.set_data_buffer("d", data);
  query_r.set_offsets_buffer("d", offsets);
  query_r.submit();
  CHECK(data == "aabbccdddd");
  std::vector<uint64_t> c_offsets = {0, 2, 4, 6};
  CHECK(offsets == c_offsets);

  // Close array
  array_r.close();

  if (vfs.is_dir(array_name))
    vfs.remove_dir(array_name);
}

TEST_CASE(
    "C++ API: Sparse global order, dimension only read",
    "[cppapi][sparse][global][read][dimension-only]") {
  const std::string array_name = "cpp_unit_array";
  Context ctx;
  VFS vfs(ctx);

  if (vfs.is_dir(array_name))
    vfs.remove_dir(array_name);

  // Create
  Domain domain(ctx);
  domain.add_dimension(Dimension::create<int>(ctx, "rows", {{0, 3}}, 4))
      .add_dimension(Dimension::create<int>(ctx, "cols", {{0, 3}}, 4));
  ArraySchema schema(ctx, TILEDB_SPARSE);
  schema.set_domain(domain).set_order({{TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR}});
  schema.add_attribute(Attribute::create<int>(ctx, "a"));
  Array::create(array_name, schema);

  // Write
  std::vector<int> data_w = {1};
  std::vector<int> coords_w = {0, 0};
  Array array_w(ctx, array_name, TILEDB_WRITE);
  Query query_w(ctx, array_w);
  query_w.set_coordinates(coords_w)
      .set_layout(TILEDB_GLOBAL_ORDER)
      .set_data_buffer("a", data_w);
  bool serialized_writes = false;
  SECTION("no serialization") {
    serialized_writes = false;
  }
  SECTION("serialization enabled global order write") {
#ifdef TILEDB_SERIALIZATION
    serialized_writes = true;
#endif
  }
  if (!serialized_writes) {
    query_w.submit();
    query_w.finalize();
  } else {
    test::submit_and_finalize_serialized_query(ctx, query_w);
  }
  array_w.close();

  // Read
  Array array(ctx, array_name, TILEDB_READ);
  Query query(ctx, array);
  const std::vector<int> subarray = {0, 0, 0, 0};
  std::vector<int> rows(1);
  query.set_subarray(subarray)
      .set_layout(TILEDB_GLOBAL_ORDER)
      .set_data_buffer("rows", rows);
  query.submit();
  array.close();

  REQUIRE(rows[0] == 0);

  if (vfs.is_dir(array_name))
    vfs.remove_dir(array_name);
}

TEST_CASE(
    "C++ API: Unordered with dups, dimension only read",
    "[cppapi][sparse][unordered][dups][read][dimension-only]") {
  const std::string array_name = "cpp_unit_array";
  Context ctx;
  VFS vfs(ctx);

  if (vfs.is_dir(array_name))
    vfs.remove_dir(array_name);

  // Create
  Domain domain(ctx);
  domain.add_dimension(Dimension::create<int>(ctx, "rows", {{0, 3}}, 4))
      .add_dimension(Dimension::create<int>(ctx, "cols", {{0, 3}}, 4));
  ArraySchema schema(ctx, TILEDB_SPARSE);
  schema.set_domain(domain).set_order({{TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR}});
  schema.add_attribute(Attribute::create<int>(ctx, "a"));
  schema.set_allows_dups(true);
  Array::create(array_name, schema);

  // Write
  std::vector<int> data_w = {1};
  std::vector<int> coords_w = {0, 0};
  Array array_w(ctx, array_name, TILEDB_WRITE);
  Query query_w(ctx, array_w);
  query_w.set_coordinates(coords_w)
      .set_layout(TILEDB_GLOBAL_ORDER)
      .set_data_buffer("a", data_w);
  bool serialized_writes = false;
  SECTION("no serialization") {
    serialized_writes = false;
  }
  SECTION("serialization enabled global order write") {
#ifdef TILEDB_SERIALIZATION
    serialized_writes = true;
#endif
  }
  if (!serialized_writes) {
    query_w.submit();
    query_w.finalize();
  } else {
    test::submit_and_finalize_serialized_query(ctx, query_w);
  }
  array_w.close();

  // Read
  Array array(ctx, array_name, TILEDB_READ);
  Query query(ctx, array);
  const std::vector<int> subarray = {0, 0, 0, 0};
  std::vector<int> rows(1);
  query.set_subarray(subarray)
      .set_layout(TILEDB_UNORDERED)
      .set_data_buffer("rows", rows);
  query.submit();
  array.close();

  REQUIRE(rows[0] == 0);

  if (vfs.is_dir(array_name))
    vfs.remove_dir(array_name);
}

TEST_CASE(
    "C++ API: Read subarray with multiple ranges",
    "[cppapi][dense][multi-range]") {
  const std::string array_name = "cpp_unit_array";
  Context ctx;
  VFS vfs(ctx);

  if (vfs.is_dir(array_name))
    vfs.remove_dir(array_name);

  // Create
  Domain domain(ctx);
  domain.add_dimension(Dimension::create<int>(ctx, "rows", {{0, 3}}, 4))
      .add_dimension(Dimension::create<int>(ctx, "cols", {{0, 3}}, 4));
  ArraySchema schema(ctx, TILEDB_DENSE);
  schema.set_domain(domain).set_order({{TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR}});
  schema.add_attribute(Attribute::create<int>(ctx, "a"));
  Array::create(array_name, schema);

  // Write
  std::vector<int> data_w = {
      1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};
  Array array_w(ctx, array_name, TILEDB_WRITE);
  Query query_w(ctx, array_w);
  query_w.set_subarray({0, 3, 0, 3})
      .set_layout(TILEDB_ROW_MAJOR)
      .set_data_buffer("a", data_w);
  query_w.submit();
  array_w.close();

  // Read
  Array array(ctx, array_name, TILEDB_READ);
  Query query(ctx, array);
  std::vector<int> data(12);
  query.add_range(0, 0, 1)
      .add_range(0, 3, 3)
      .add_range(1, 0, 3)
      .set_layout(TILEDB_ROW_MAJOR)
      .set_data_buffer("a", data);
  query.submit();
  array.close();

  for (int i = 0; i < 8; i++) {
    REQUIRE(data[i] == i + 1);
  }

  for (int i = 8; i < 12; i++) {
    REQUIRE(data[i] == i + 5);
  }

  if (vfs.is_dir(array_name))
    vfs.remove_dir(array_name);
}

TEST_CASE(
    "C++ API: Array open VFS calls, dense", "[cppapi][dense][vfs-calls]") {
  const std::string array_name = "cpp_unit_array";
  Context ctx;
  VFS vfs(ctx);

  if (vfs.is_dir(array_name))
    vfs.remove_dir(array_name);

  // Create
  Domain domain(ctx);
  domain.add_dimension(Dimension::create<int>(ctx, "rows", {{0, 3}}, 4))
      .add_dimension(Dimension::create<int>(ctx, "cols", {{0, 3}}, 4));
  ArraySchema schema(ctx, TILEDB_DENSE);
  schema.set_domain(domain).set_order({{TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR}});
  schema.add_attribute(Attribute::create<int>(ctx, "a"));
  Array::create(array_name, schema);

  // Write
  std::vector<int> data_w = {
      1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};
  Array array_w(ctx, array_name, TILEDB_WRITE);
  Query query_w(ctx, array_w);
  query_w.set_subarray({0, 3, 0, 3})
      .set_layout(TILEDB_ROW_MAJOR)
      .set_data_buffer("a", data_w);
  query_w.submit();
  array_w.close();

  // Open for read.
  Stats::reset();
  Stats::enable();
  Array array(ctx, array_name, TILEDB_READ);
  array.close();
  Stats::disable();

  // Check stats.
  std::string stats;
  Stats::dump(&stats);

  // Expect ls on:
  // cpp_unit_array/
  // cpp_unit_array/__commits
  // cpp_unit_array/__schema
  // cpp_unit_array/__meta
  // cpp_unit_array/__fragment_meta
  CHECK(
      stats.find("\"Context.StorageManager.VFS.ls_num\": 5") !=
      std::string::npos);

  // Expect file_size on the fragment.
  CHECK(
      stats.find("\"Context.StorageManager.VFS.file_size_num\": 1") !=
      std::string::npos);
}

TEST_CASE(
    "C++ API: Array open VFS calls, sparse", "[cppapi][sparse][vfs-calls]") {
  const std::string array_name = "cpp_unit_array";
  Context ctx;
  VFS vfs(ctx);

  if (vfs.is_dir(array_name))
    vfs.remove_dir(array_name);

  // Create
  Domain domain(ctx);
  domain.add_dimension(Dimension::create<int>(ctx, "rows", {{0, 3}}, 4))
      .add_dimension(Dimension::create<int>(ctx, "cols", {{0, 3}}, 4));
  ArraySchema schema(ctx, TILEDB_SPARSE);
  schema.set_domain(domain).set_order({{TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR}});
  schema.add_attribute(Attribute::create<int>(ctx, "a"));
  Array::create(array_name, schema);

  // Write
  std::vector<int> data_w = {1};
  std::vector<int> coords_w = {0, 0};
  Array array_w(ctx, array_name, TILEDB_WRITE);
  Query query_w(ctx, array_w);
  query_w.set_coordinates(coords_w)
      .set_layout(TILEDB_GLOBAL_ORDER)
      .set_data_buffer("a", data_w);
  query_w.submit();
  query_w.finalize();
  array_w.close();

  // Open for read.
  Stats::reset();
  Stats::enable();
  Array array(ctx, array_name, TILEDB_READ);
  array.close();
  Stats::disable();

  // Check stats.
  std::string stats;
  Stats::dump(&stats);

  // Expect ls on:
  // cpp_unit_array/
  // cpp_unit_array/__commits
  // cpp_unit_array/__schema
  // cpp_unit_array/__meta
  // cpp_unit_array/__fragment_meta
  CHECK(
      stats.find("\"Context.StorageManager.VFS.ls_num\": 5") !=
      std::string::npos);

  // Expect file_size on the fragment.
  CHECK(
      stats.find("\"Context.StorageManager.VFS.file_size_num\": 1") !=
      std::string::npos);
}

TEST_CASE(
    "C++ API: Write and read to an array with experimental build enabled",
    "[cppapi][array][experimental]") {
  if constexpr (!is_experimental_build) {
    return;
  }

  static const std::string arrays_dir =
      std::string(TILEDB_TEST_INPUTS_DIR) + "/arrays";
  std::string old_array_name(arrays_dir + "/non_split_coords_v1_4_0");
  std::string new_array_name(arrays_dir + "/experimental_array_vUINT32_MAX");
  Context ctx;

  // Try writing to an older-versioned array
  REQUIRE_THROWS_WITH(
      Array(ctx, old_array_name, TILEDB_WRITE),
      Catch::Contains("Array format version") &&
          Catch::Contains("is not the library format version"));

  // Read from an older-versioned array
  Array array(ctx, old_array_name, TILEDB_READ);

  // Upgrade version
  Array::upgrade_version(ctx, old_array_name);

  // Write to upgraded (current) version
  Array array_write(ctx, old_array_name, TILEDB_WRITE);
  std::vector<int> subarray_write = {1, 2, 10, 10};
  std::vector<int> a_write = {11, 12};
  std::vector<int> d1_write = {1, 2};
  std::vector<int> d2_write = {10, 10};

  Query query_write(ctx, array_write, TILEDB_WRITE);

  query_write.set_layout(TILEDB_GLOBAL_ORDER);
  query_write.set_data_buffer("a", a_write);
  query_write.set_data_buffer("d1", d1_write);
  query_write.set_data_buffer("d2", d2_write);

  query_write.submit();
  query_write.finalize();
  array_write.close();

  FragmentInfo fragment_info(ctx, old_array_name);
  fragment_info.load();

  bool serialized_load = false;
  SECTION("no serialization") {
    serialized_load = false;
  }
#ifdef TILEDB_SERIALIZATION
  SECTION("serialization enabled fragment info load") {
    serialized_load = true;
  }
#endif

  if (serialized_load) {
    FragmentInfo deserialized_fragment_info(ctx, old_array_name);
    tiledb_fragment_info_serialize(
        ctx.ptr().get(),
        old_array_name.c_str(),
        fragment_info.ptr().get(),
        deserialized_fragment_info.ptr().get(),
        tiledb_serialization_type_t(0));
    fragment_info = deserialized_fragment_info;
  }

  std::string fragment_uri = fragment_info.fragment_uri(1);

  // old version fragment
  CHECK(fragment_info.version(0) == 1);
  // new version fragment
  CHECK(fragment_info.version(1) == tiledb::sm::constants::format_version);

  // Read from upgraded version
  Array array_read(ctx, old_array_name, TILEDB_READ);
  std::vector<int> subarray_read = {1, 4, 10, 10};
  std::vector<int> a_read;
  a_read.resize(4);
  std::vector<int> d1_read;
  d1_read.resize(4);
  std::vector<int> d2_read;
  d2_read.resize(4);

  Query query_read(ctx, array_read);
  query_read.set_subarray(subarray_read)
      .set_layout(TILEDB_ROW_MAJOR)
      .set_data_buffer("a", a_read)
      .set_data_buffer("d1", d1_read)
      .set_data_buffer("d2", d2_read);

  query_read.submit();
  array_read.close();

  for (int i = 0; i < 2; i++) {
    REQUIRE(a_read[i] == i + 11);
  }
  for (int i = 2; i < 3; i++) {
    REQUIRE(a_read[i] == i + 1);
  }

  // Try writing to a newer-versioned (UINT32_MAX) array
  REQUIRE_THROWS_WITH(
      Array(ctx, new_array_name, TILEDB_WRITE),
      Catch::Contains("Incompatible format version."));

  // Try reading from a newer-versioned (UINT32_MAX) array
  REQUIRE_THROWS_WITH(
      Array(ctx, new_array_name, TILEDB_READ),
      Catch::Contains("Incompatible format version."));

  // Clean up
  VFS vfs(ctx);
  vfs.remove_dir(get_fragment_dir(array_read.uri()));
  vfs.remove_dir(get_commit_dir(array_read.uri()));
  vfs.remove_dir(array_read.uri() + "/__schema");
}
