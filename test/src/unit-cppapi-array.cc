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
#include "tiledb"

using namespace tdb;

struct CPPArrayFx {

  CPPArrayFx() : vfs(ctx) {
    using namespace tdb;

    if (vfs.is_dir("cpp_unit_array")) vfs.remove_dir("cpp_unit_array");

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

    ArraySchema schema(ctx);
    schema << domain << a1 << a2 << a3;

    create_array("cpp_unit_array", schema);
  }

  Context ctx;
  VFS vfs;
};


TEST_CASE_METHOD(CPPArrayFx, "C++ API: Arrays", "[cppapi]") {

  SECTION("Make Buffer") {
    Query query(ctx, "cpp_unit_array", TILEDB_WRITE);
    CHECK_THROWS(query.set_subarray<unsigned>({1,2})); // Wrong type
    CHECK_THROWS(query.set_subarray<int>({1,2})); // Wrong num

    query.set_subarray<int>({{0,5}, {0,5}});

    CHECK_THROWS(query.make_var_buffers<int>("a1")); // Not var attr
    CHECK_THROWS(query.make_buffer<char>("a1")); // Wrong type

    CHECK(query.make_buffer<int>("a1").size() == 36);
    CHECK(query.make_buffer<int>("a1", 5).size() == 5);
  }

  SECTION("Read/Write") {

    std::vector<int> a1 = {1, 2};
    std::vector<std::string> a2 = {"abc", "defg"};
    std::vector<std::array<double, 2>> a3 = {{1.0,2.0}, {3.0,4.0}};

    auto a2buf = make_var_buffers(a2);
    auto a3buf = flatten(a3);

    {
      Query query(ctx, "cpp_unit_array", TILEDB_WRITE);
      query.set_subarray<int>({{0, 1}, {0, 0}});
      query.set_buffer("a1", a1);
      query.set_buffer("a2", a2buf);
      query.set_buffer("a3", a3buf);
      query.set_layout(TILEDB_ROW_MAJOR);

      REQUIRE(query.submit() == Query::Status::COMPLETE);
    }

    {
      std::fill(std::begin(a1), std::end(a1), 0);
      std::fill(std::begin(a2buf.first), std::end(a2buf.first), 0);
      std::fill(std::begin(a2buf.second), std::end(a2buf.second), 0);
      std::fill(std::begin(a3buf), std::end(a3buf), 0);

      Query query(ctx, "cpp_unit_array", TILEDB_READ);
      query.set_buffer("a1", a1);
      query.set_buffer("a2", a2buf);
      query.set_buffer("a3", a3buf);
      query.set_layout(TILEDB_ROW_MAJOR);
      query.set_subarray<int>({{0, 1}, {0, 0}});

      REQUIRE(query.submit() == Query::Status::COMPLETE);
      auto ret = query.returned_buff_sizes();
      REQUIRE(ret.size() == 4);
      CHECK(ret[0] == 2);
      CHECK(ret[1] == 2);
      CHECK(ret[2] == 7);
      CHECK(ret[3] == 4);

      CHECK(a1[0] == 1);
      CHECK(a1[1] == 2);

      auto reada2 = group_by_cell<char, std::string>(a2buf, 2, 7);
      CHECK(reada2[0] == "abc");
      CHECK(reada2[1] == "defg");

      auto reada3 = group_by_cell<2>(a3buf, 4);
      REQUIRE(reada3.size() == 2);
      CHECK(reada3[0][0] == 1.0);
      CHECK(reada3[0][1] == 2.0);
      CHECK(reada3[1][0] == 3.0);
      CHECK(reada3[1][1] == 4.0);
    }

  }

}
