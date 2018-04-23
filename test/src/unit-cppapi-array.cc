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
    auto a5 = Attribute::create<Point>(ctx, "a5");  // (char, sizeof(Point)
    a1.set_compressor({TILEDB_BLOSC_LZ, -1});

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

TEST_CASE_METHOD(CPPArrayFx, "C++ API: Arrays", "[cppapi]") {
  SECTION("Make Buffer") {
    Query query(ctx, "cpp_unit_array", TILEDB_WRITE);
    CHECK_THROWS(query.set_subarray<unsigned>({1, 2}));  // Wrong type
    CHECK_THROWS(query.set_subarray<int>({1, 2}));       // Wrong num
    std::vector<int> subarray = {0, 5, 0, 5};
    query.set_subarray(subarray);
    // TODO: This is only required because the array is currently locked
    // on query creation. If we don't finalize, the file won't be unlocked
    // and we won't be able to delete the array in between tests on Windows
    // (which has stricter file removal rules).
    query.finalize();
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
      Query query(ctx, "cpp_unit_array", TILEDB_WRITE);
      query.set_subarray(subarray);
      query.set_buffer("a1", a1);
      query.set_buffer("a2", a2buf);
      query.set_buffer("a3", a3);
      query.set_buffer("a4", a4buf);
      query.set_buffer("a5", a5);
      query.set_layout(TILEDB_ROW_MAJOR);

      REQUIRE(query.submit() == Query::Status::COMPLETE);

      query.finalize();
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

      auto buff_el =
          Array::max_buffer_elements(ctx, "cpp_unit_array", subarray);
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

      Query query(ctx, "cpp_unit_array", TILEDB_READ);
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

      query.finalize();

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
    // Pad out to tile multiple
    size_t num_dummies = d1_tile * d2_tile - a1.size();
    for (size_t i = 0; i < num_dummies; i++) {
      a1.push_back(0);
    }

    Query query(ctx, "cpp_unit_array", TILEDB_WRITE);
    query.set_subarray(subarray);
    query.set_buffer("a1", a1);
    query.set_layout(TILEDB_GLOBAL_ORDER);
    CHECK(query.submit() == tiledb::Query::Status::COMPLETE);
    REQUIRE_NOTHROW(query.finalize());

    auto non_empty =
        tiledb::Array::non_empty_domain<int>(ctx, "cpp_unit_array");
    CHECK(non_empty[0].second.first == 0);
    CHECK(non_empty[0].second.second == d1_tile - 1);
    CHECK(non_empty[1].second.first == 0);
    CHECK(non_empty[1].second.second == d2_tile - 1);
  }

  SECTION("Global order write - no dummy values") {
    std::vector<int> a1 = {1, 2};
    std::vector<int> subarray = {0, 1, 0, 0};
    Query query(ctx, "cpp_unit_array", TILEDB_WRITE);
    query.set_subarray(subarray);
    query.set_buffer("a1", a1);
    query.set_layout(TILEDB_GLOBAL_ORDER);
    // Incorrect subarray for global order
    REQUIRE_THROWS(query.submit());
    query.finalize();
  }
}
