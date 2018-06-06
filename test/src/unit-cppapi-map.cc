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
    schema.set_capacity(10);
    Map::create("cpp_unit_map", schema);
  }

  ~CPPMapFx() {
    if (vfs.is_dir("cpp_unit_map"))
      vfs.remove_dir("cpp_unit_map");
  }

  Context ctx;
  VFS vfs;
};

TEST_CASE_METHOD(CPPMapFx, "C++ API: Map", "[cppapi], [cppapi-map]") {
  Map map(ctx, "cpp_unit_map");
  CHECK(!map.is_dirty());

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

  CHECK(map.is_dirty());

  map.flush();

  CHECK(!map.is_dirty());

  map.close();
  map.open();

  auto schema = map.schema();
  CHECK(schema.capacity() == 10);

  using my_cell_t = std::tuple<int, std::string, std::array<double, 2>>;

  // write via tuple
  my_cell_t ret = map[simple_key][{"a1", "a2", "a3"}];

  CHECK(std::get<0>(ret) == 1);
  CHECK(std::get<1>(ret) == "someval");
  CHECK(std::get<2>(ret).size() == 2);
  CHECK(std::get<2>(ret)[0] == 3);

  map[compound_key][{"a1", "a2", "a3"}] = my_cell_t(2, "aaa", {{4.2, 1}});

  map.flush();
  map.close();
  map.open();

  CHECK(map.has_key(simple_key));
  CHECK(map.has_key(compound_key));
  CHECK(!map.has_key(3453463));

  CHECK((int)map[simple_key]["a1"] == 1);
  CHECK(map.get_item(simple_key).get<std::string>("a2") == "someval");
  CHECK((map[simple_key].get<std::array<double, 2>>("a3").size()) == 2);

  ret = map[compound_key][{"a1", "a2", "a3"}];
  CHECK(std::get<0>(ret) == 2);
  CHECK(std::get<1>(ret) == "aaa");
  CHECK(std::get<2>(ret).size() == 2);
  CHECK(std::get<2>(ret)[0] == 4.2);

  map.close();
}

/**
 * Test to catch segfault in empty map due to nullptr for
 * attr_buffer.second.buffer_var_size_ in Reader::zero_out_buffer_sizes()
 *
 * Issue #606 https://github.com/TileDB-Inc/TileDB/issues/606
 *
 */
TEST_CASE_METHOD(
    CPPMapFx,
    "C++ API: Map Issue 606 segault in Reader::zero_out_buffer_sizes()",
    "[cppapi], [cppapi-map]") {
  Map map(ctx, "cpp_unit_map");

  int simple_key = 1;

  // Create item to add to map
  auto i1 = Map::create_item(ctx, simple_key);
  i1["a1"] = 1;
  i1["a2"] = std::string("someval");
  i1["a3"] = std::array<double, 2>({{3, 2.4}});

  // Get item from map, at this point the map is empty, item should != good()
  // This is where segfault was happening for issue #606
  tiledb::MapItem test_get_item_by_key = map.get_item(simple_key);
  CHECK(!test_get_item_by_key.good());

  // Add the item
  map.add_item(i1);

  // Flush and reopen
  map.flush();
  map.close();
  map.open();

  CHECK(map.has_key(simple_key));

  // Validate item is now on map
  test_get_item_by_key = map.get_item(simple_key);
  CHECK(test_get_item_by_key.good());
  CHECK(test_get_item_by_key.get<int>("a1") == 1);
  CHECK(test_get_item_by_key.get<std::string>("a2") == "someval");
  CHECK(test_get_item_by_key.get<std::array<double, 2>>("a3")[0] == 3);

  map.close();
}

struct CPPMapFx1A {
  CPPMapFx1A()
      : vfs(ctx) {
    using namespace tiledb;

    if (vfs.is_dir("cpp_unit_map"))
      vfs.remove_dir("cpp_unit_map");

    auto a1 = Attribute::create<int>(ctx, "a1");
    a1.set_compressor({TILEDB_BLOSC_LZ, -1});

    MapSchema schema(ctx);
    schema.add_attribute(a1);
    Map::create("cpp_unit_map", schema);
  }

  ~CPPMapFx1A() {
    if (vfs.is_dir("cpp_unit_map"))
      vfs.remove_dir("cpp_unit_map");
  }

  Context ctx;
  VFS vfs;
};

TEST_CASE_METHOD(
    CPPMapFx1A, "C++ API: Map, implicit attribute", "[cppapi], [cppapi-map]") {
  Map map(ctx, "cpp_unit_map");
  map[10] = 1;

  // Flush and reopen
  map.flush();
  map.close();
  map.open();

  assert(int(map[10]) == 1);

  map.close();
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

    Map::create(ctx, "cpp_unit_map", map, "val");
  }

  ~CPPMapFromMapFx() {
    if (vfs.is_dir("cpp_unit_map"))
      vfs.remove_dir("cpp_unit_map");
  }

  Context ctx;
  VFS vfs;
};

TEST_CASE_METHOD(
    CPPMapFromMapFx, "C++ API: Map from std::map", "[cppapi], [cppapi-map]") {
  Map map(ctx, "cpp_unit_map");
  CHECK(map[0]["val"].get<std::string>() == "0");
  CHECK(map[1]["val"].get<std::string>() == "12");
  CHECK(map[2].get<std::string>() == "123");  // implicit
  map.close();

  // Check reopening
  map.open();
  CHECK(map[0]["val"].get<std::string>() == "0");
  CHECK(map[1]["val"].get<std::string>() == "12");
  CHECK(map[2].get<std::string>() == "123");  // implicit

  // Check opening without closing
  REQUIRE_THROWS(map.open());
  CHECK(map[0]["val"].get<std::string>() == "0");
  CHECK(map[1]["val"].get<std::string>() == "12");
  CHECK(map[2].get<std::string>() == "123");  // implicit
  map.close();
}

TEST_CASE_METHOD(
    CPPMapFromMapFx,
    "C++ API: Map with explicit attributes",
    "[cppapi], [cppapi-map]") {
  std::vector<std::string> attributes = {std::string("val")};
  Map map(ctx, "cpp_unit_map", attributes);
  CHECK(map[0]["val"].get<std::string>() == "0");
  CHECK(map[1]["val"].get<std::string>() == "12");
  CHECK(map[2].get<std::string>() == "123");  // implicit
  map.close();

  // Check reopening
  map.open(attributes);
  map.reopen();
  CHECK(map[0]["val"].get<std::string>() == "0");
  CHECK(map[1]["val"].get<std::string>() == "12");
  CHECK(map[2].get<std::string>() == "123");  // implicit

  // Check opening without closing
  REQUIRE_THROWS(map.open());
  CHECK(map[0]["val"].get<std::string>() == "0");
  CHECK(map[1]["val"].get<std::string>() == "12");
  CHECK(map[2].get<std::string>() == "123");  // implicit
  map.close();
}

TEST_CASE_METHOD(
    CPPMapFromMapFx, "C++ API: Map iter", "[cppapi], [cppapi-map]") {
  Map map(ctx, "cpp_unit_map");

  // Test closing and reopening
  map.close();
  map.open();

  MapIter iter(map), end(map, true);

  std::vector<std::string> vals;
  for (; iter != end; ++iter) {
    vals.push_back(iter->get<std::string>());
  }

  REQUIRE(vals.size() == 3);
  CHECK(std::count(vals.begin(), vals.end(), "0") == 1);
  CHECK(std::count(vals.begin(), vals.end(), "12") == 1);
  CHECK(std::count(vals.begin(), vals.end(), "123") == 1);

  // Reset the iterator and check again
  iter.reset();
  vals.clear();
  for (; iter != end; ++iter) {
    vals.push_back(iter->get<std::string>());
  }

  REQUIRE(vals.size() == 3);
  CHECK(std::count(vals.begin(), vals.end(), "0") == 1);
  CHECK(std::count(vals.begin(), vals.end(), "12") == 1);
  CHECK(std::count(vals.begin(), vals.end(), "123") == 1);

  map.close();
}
