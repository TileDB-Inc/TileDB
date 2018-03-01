/**
 * @file   unit-cppapi-map.cc
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
 * Tests the C++ API for map related functions.
 */

#include "catch.hpp"
#include "tiledb/sm/cpp_api/tiledb"

using namespace tiledb;

struct CPPMapFx {
  CPPMapFx()
      : vfs(ctx) {
    using namespace tiledb;

    if (vfs.is_dir("cpp_unit_map"))
      vfs.remove_dir("cpp_unit_map");

    auto a1 = Attribute::create<int>(ctx, "a1");
    auto a2 = Attribute::create<std::string>(ctx, "a2");
    auto a3 = Attribute::create<std::array<double, 2>>(ctx, "a3");
    a1.set_compressor({TILEDB_BLOSC_LZ, -1});

    MapSchema schema(ctx);
    schema.add_attribute(a1).add_attribute(a2).add_attribute(a3);
    Map::create("cpp_unit_map", schema);
  }

  ~CPPMapFx() {
    if (vfs.is_dir("cpp_unit_map"))
      vfs.remove_dir("cpp_unit_map");
  }

  Context ctx;
  VFS vfs;
};

TEST_CASE_METHOD(CPPMapFx, "C++ API: Map", "[cppapi]") {
  Map map(ctx, "cpp_unit_map");

  int simple_key = 10;
  std::vector<double> compound_key = {2.43, 214};

  // Via independent item
  auto i1 = Map::create_item(ctx, simple_key);
  i1.set("a1", 1.234);
  i1["a2"] = std::string("someval");
  i1["a3"] = std::array<double, 2>({{3, 2.4}});

  CHECK_THROWS(map.add_item(i1));
  i1["a1"] = 1;
  map.add_item(i1);
  map.flush();

  using my_cell_t = std::tuple<int, std::string, std::array<double, 2>>;

  // write via tuple
  my_cell_t ret = map[simple_key][{"a1", "a2", "a3"}];

  CHECK(std::get<0>(ret) == 1);
  CHECK(std::get<1>(ret) == "someval");
  CHECK(std::get<2>(ret).size() == 2);
  CHECK(std::get<2>(ret)[0] == 3);

  map[compound_key][{"a1", "a2", "a3"}] = my_cell_t(2, "aaa", {{4.2, 1}});

  map.flush();

  CHECK((int)map[simple_key]["a1"] == 1);
  CHECK(map.get_item(simple_key).get<std::string>("a2") == "someval");
  CHECK((map[simple_key].get<std::array<double, 2>>("a3").size()) == 2);

  ret = map[compound_key][{"a1", "a2", "a3"}];
  CHECK(std::get<0>(ret) == 2);
  CHECK(std::get<1>(ret) == "aaa");
  CHECK(std::get<2>(ret).size() == 2);
  CHECK(std::get<2>(ret)[0] == 4.2);
}

struct CPPMapFromMapFx {
  CPPMapFromMapFx()
      : vfs(ctx) {
    using namespace tiledb;

    if (vfs.is_dir("cpp_unit_map"))
      vfs.remove_dir("cpp_unit_map");

    std::map<int, std::string> map;
    map[0] = "0";
    map[1] = "12";
    map[2] = "123";

    Map::create(ctx, "cpp_unit_map", map);
  }

  ~CPPMapFromMapFx() {
    if (vfs.is_dir("cpp_unit_map"))
      vfs.remove_dir("cpp_unit_map");
  }

  Context ctx;
  VFS vfs;
};

TEST_CASE_METHOD(CPPMapFromMapFx, "C++ API: Map from std::map", "[cppapi]") {
  Map map(ctx, "cpp_unit_map");
  CHECK(map[0].get<std::string>() == "0");
  CHECK(map[1].get<std::string>() == "12");
  CHECK(map[2].get<std::string>() == "123");
}