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
#include "tiledb"

using namespace tdb;


struct CPPMapFx {

  CPPMapFx() : vfs(ctx) {
    using namespace tdb;

    if (vfs.is_dir("cpp_unit_map")) vfs.remove_dir("cpp_unit_map");

    auto a1 = Attribute::create<int>(ctx, "a1");
    auto a2 = Attribute::create<char>(ctx, "a2");
    auto a3 = Attribute::create<double>(ctx, "a3");
    a1.set_compressor({TILEDB_BLOSC, -1}).set_cell_val_num(1);
    a2.set_cell_val_num(TILEDB_VAR_NUM);
    a3.set_cell_val_num(2);

    MapSchema schema(ctx);
    schema  << a1 << a2 << a3;
    Map::create("cpp_unit_map", schema);

  }

  Context ctx;
  VFS vfs;
};

TEST_CASE_METHOD(CPPMapFx, "C++ API: Map", "[cppapi]") {

    // TODO add invalid item value-type check once implemented in C API
    Map map(ctx, "cpp_unit_map");

    int simple_key = 10;
    std::vector<float> compound_key = {2.43, 214};

    // Via independent item
    auto i1 = Map::create_item(ctx, simple_key);
    i1.set("a1", 1);
    i1["a2"] = "someval";
    i1["a3"] = std::vector<double>{3, 2.4};

    map << i1;
    map.flush();

    // write via tuple
    std::tuple<int, std::string, std::vector<double>> ret = map[simple_key][{"a1", "a2", "a3"}];

    CHECK(std::get<0>(ret) == 1);
    CHECK(std::get<1>(ret) == "someval");
    CHECK(std::get<2>(ret).size() == 2);
    CHECK(std::get<2>(ret)[0] == 3);

    map[compound_key][{"a1", "a2", "a3"}] = std::tuple<int, std::string, std::vector<double>>(2, "aaa", {4.2,1});

    map.flush();

    CHECK((int) map[simple_key]["a1"] == 1);
    CHECK(map.get_item(simple_key).get<std::string>("a2") == "someval");
    CHECK(map[simple_key].get<std::vector<double>>("a3").size() == 2);

    ret = map[compound_key][{"a1", "a2", "a3"}];
    CHECK(std::get<0>(ret) == 2);
    CHECK(std::get<1>(ret) == "aaa");
    CHECK(std::get<2>(ret).size() == 2);
    CHECK(std::get<2>(ret)[0] == 4.2);

}