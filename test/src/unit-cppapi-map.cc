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
#include "tiledb/sm/misc/utils.h"

using namespace tiledb;

struct CPPMapFx {
  tiledb_encryption_type_t encryption_type = TILEDB_NO_ENCRYPTION;
  const char* encryption_key = nullptr;

  CPPMapFx()
      : vfs(ctx) {
  }

  ~CPPMapFx() {
    if (vfs.is_dir("cpp_unit_map"))
      vfs.remove_dir("cpp_unit_map");
  }

  Context ctx;
  VFS vfs;

  void create_map() {
    using namespace tiledb;

    if (vfs.is_dir("cpp_unit_map"))
      vfs.remove_dir("cpp_unit_map");

    auto a1 = Attribute::create<int>(ctx, "a1");
    auto a2 = Attribute::create<std::string>(ctx, "a2");
    auto a3 = Attribute::create<std::array<double, 2>>(ctx, "a3");
    FilterList filters(ctx);
    filters.add_filter({ctx, TILEDB_FILTER_LZ4});
    a1.set_filter_list(filters);

    MapSchema schema(ctx);
    schema.add_attribute(a1).add_attribute(a2).add_attribute(a3);
    schema.set_capacity(10);
    if (encryption_type == TILEDB_NO_ENCRYPTION) {
      Map::create("cpp_unit_map", schema);
    } else {
      auto key_len = (uint32_t)strlen(encryption_key);
      Map::create(
          "cpp_unit_map", schema, encryption_type, encryption_key, key_len);
    }
  }
};

TEST_CASE_METHOD(CPPMapFx, "C++ API: Map", "[cppapi], [cppapi-map]") {
  create_map();

  REQUIRE(Map::encryption_type(ctx, "cpp_unit_map") == TILEDB_NO_ENCRYPTION);

  Map map(ctx, "cpp_unit_map", TILEDB_WRITE);
  CHECK(map.is_open());
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
  CHECK(!map.is_open());

  map.open(TILEDB_READ);
  CHECK(map.is_open());

  auto schema = map.schema();
  CHECK(schema.capacity() == 10);

  using my_cell_t = std::tuple<int, std::string, std::array<double, 2>>;

  // write via tuple
  my_cell_t ret = map[simple_key][{"a1", "a2", "a3"}];

  CHECK(std::get<0>(ret) == 1);
  CHECK(std::get<1>(ret) == "someval");
  CHECK(std::get<2>(ret).size() == 2);
  CHECK(std::get<2>(ret)[0] == 3);

  map.close();
  CHECK(!map.is_open());

  map.open(TILEDB_WRITE);
  CHECK(map.is_open());

  map[compound_key][{"a1", "a2", "a3"}] = my_cell_t(2, "aaa", {{4.2, 1}});

  map.flush();
  map.close();
  CHECK(!map.is_open());

  map.open(TILEDB_READ);
  CHECK(map.is_open());
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
  CHECK(!map.is_open());
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
  create_map();

  Map map(ctx, "cpp_unit_map", TILEDB_WRITE);

  int simple_key = 1;

  // Create item to add to map
  auto i1 = Map::create_item(ctx, simple_key);
  i1["a1"] = 1;
  i1["a2"] = std::string("someval");
  i1["a3"] = std::array<double, 2>({{3, 2.4}});

  // Add the item
  map.add_item(i1);

  // Flush and reopen
  map.flush();
  map.close();

  map.open(TILEDB_READ);

  CHECK(map.has_key(simple_key));

  // Validate item is now on map
  tiledb::MapItem test_get_item_by_key = map.get_item(simple_key);
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
    FilterList filters(ctx);
    filters.add_filter({ctx, TILEDB_FILTER_LZ4});
    a1.set_filter_list(filters);

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
  Map map(ctx, "cpp_unit_map", TILEDB_WRITE);
  map[10] = 1;

  // Flush and reopen
  map.flush();
  map.close();
  map.open(TILEDB_READ);

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
  Map map(ctx, "cpp_unit_map", TILEDB_READ);
  CHECK(map[0]["val"].get<std::string>() == "0");
  CHECK(map[1]["val"].get<std::string>() == "12");
  CHECK(map[2].get<std::string>() == "123");  // implicit
  map.close();

  // Check reopening
  map.open(TILEDB_READ);
  CHECK(map[0]["val"].get<std::string>() == "0");
  CHECK(map[1]["val"].get<std::string>() == "12");
  CHECK(map[2].get<std::string>() == "123");  // implicit

  // Check opening without closing
  REQUIRE_THROWS(map.open(TILEDB_READ));
  CHECK(map[0]["val"].get<std::string>() == "0");
  CHECK(map[1]["val"].get<std::string>() == "12");
  CHECK(map[2].get<std::string>() == "123");  // implicit
  map.close();
}

TEST_CASE_METHOD(
    CPPMapFromMapFx, "C++ API: Map iter", "[cppapi], [cppapi-map]") {
  Map map(ctx, "cpp_unit_map", TILEDB_READ);

  // Test closing and reopening
  map.close();
  map.open(TILEDB_READ);

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

TEST_CASE_METHOD(
    CPPMapFx,
    "C++ API: Encrypted map",
    "[cppapi], [cppapi-map], [encryption]") {
  encryption_type = TILEDB_AES_256_GCM;
  encryption_key = "0123456789abcdeF0123456789abcdeF";
  auto key_len = (uint32_t)strlen(encryption_key);
  create_map();

  REQUIRE_THROWS_AS(
      [&]() { Map map(ctx, "cpp_unit_map", TILEDB_WRITE); }(),
      tiledb::TileDBError);

  Map map(
      ctx,
      "cpp_unit_map",
      TILEDB_WRITE,
      TILEDB_AES_256_GCM,
      encryption_key,
      key_len);
  CHECK(map.is_open());
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
  CHECK(!map.is_open());

  REQUIRE(Map::encryption_type(ctx, "cpp_unit_map") == TILEDB_AES_256_GCM);

  REQUIRE_THROWS_AS(map.open(TILEDB_READ), tiledb::TileDBError);
  map.open(TILEDB_READ, TILEDB_AES_256_GCM, encryption_key, key_len);
  CHECK(map.is_open());

  auto schema = map.schema();
  CHECK(schema.capacity() == 10);

  using my_cell_t = std::tuple<int, std::string, std::array<double, 2>>;

  // write via tuple
  my_cell_t ret = map[simple_key][{"a1", "a2", "a3"}];

  CHECK(std::get<0>(ret) == 1);
  CHECK(std::get<1>(ret) == "someval");
  CHECK(std::get<2>(ret).size() == 2);
  CHECK(std::get<2>(ret)[0] == 3);

  map.close();
  CHECK(!map.is_open());

  map.open(TILEDB_WRITE, TILEDB_AES_256_GCM, encryption_key, key_len);
  CHECK(map.is_open());

  map[compound_key][{"a1", "a2", "a3"}] = my_cell_t(2, "aaa", {{4.2, 1}});

  map.flush();
  map.close();
  CHECK(!map.is_open());

  map.open(TILEDB_READ, TILEDB_AES_256_GCM, encryption_key, key_len);
  CHECK(map.is_open());
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
  CHECK(!map.is_open());
}

TEST_CASE("C++ API: Open map at", "[cppapi], [cppapi-open-map-at]") {
  Context ctx;
  VFS vfs(ctx);
  const std::string map_name = "cppapi_open_map_at";
  if (vfs.is_dir(map_name))
    vfs.remove_dir(map_name);

  // Create map
  auto a = Attribute::create<int>(ctx, "a");
  MapSchema schema(ctx);
  schema.add_attribute(a);
  Map::create(map_name, schema);

  REQUIRE_THROWS_AS(Map(ctx, map_name, TILEDB_WRITE, 0), tiledb::TileDBError);

  // Write map
  Map map_w(ctx, map_name, TILEDB_WRITE);
  map_w["key"] = 10;
  map_w.flush();
  map_w.close();

  // Get timestamp after write
  auto first_timestamp = TILEDB_TIMESTAMP_NOW_MS;

  // Normal read
  Map map_r(ctx, map_name, TILEDB_READ);
  CHECK(map_r.has_key("key"));
  CHECK((int)map_r["key"]["a"] == 10);
  map_r.close();

  // Read from 0 timestamp
  Map map_r_at_0(ctx, map_name, TILEDB_READ, 0);
  CHECK(map_r_at_0.timestamp() == 0);

  SECTION("Testing Map::Map") {
    // Nothing to do - just for clarity
  }

  SECTION("Testing Map::open") {
    map_r_at_0.close();
    map_r_at_0.open(TILEDB_READ, 0);
  }

  CHECK(!map_r_at_0.has_key("key"));
  map_r_at_0.close();

  // Read from later timestamp
  auto timestamp = TILEDB_TIMESTAMP_NOW_MS;
  Map map_r_at(ctx, map_name, TILEDB_READ, timestamp);

  SECTION("Testing Map::Map") {
    // Nothing to do - just for clarity
  }

  SECTION("Testing Map::open") {
    map_r_at.close();
    map_r_at.open(TILEDB_READ, timestamp);
  }

  CHECK(map_r_at.has_key("key"));
  CHECK((int)map_r_at["key"]["a"] == 10);

  // Reopen at first timestamp.
  map_r_at.reopen_at(first_timestamp);
  CHECK(map_r_at.timestamp() == first_timestamp);
  CHECK(map_r_at.has_key("key"));
  CHECK((int)map_r_at["key"]["a"] == 10);
  map_r_at.close();

  if (vfs.is_dir(map_name))
    vfs.remove_dir(map_name);
}

TEST_CASE(
    "C++ API: Open encrypted map at",
    "[cppapi], [cppapi-open-encrypted-map-at]") {
  const char key[] = "0123456789abcdeF0123456789abcdeF";
  auto key_len = (uint32_t)strlen(key);

  Context ctx;
  VFS vfs(ctx);
  const std::string map_name = "cppapi_open_encrypted_map_at";
  if (vfs.is_dir(map_name))
    vfs.remove_dir(map_name);

  // Create map
  auto a = Attribute::create<int>(ctx, "a");
  MapSchema schema(ctx);
  schema.add_attribute(a);
  Map::create(map_name, schema, TILEDB_AES_256_GCM, key, key_len);

  REQUIRE_THROWS_AS(
      Map(ctx, map_name, TILEDB_WRITE, TILEDB_AES_256_GCM, key, key_len, 0),
      tiledb::TileDBError);

  // Write map
  Map map_w(ctx, map_name, TILEDB_WRITE, TILEDB_AES_256_GCM, key, key_len);
  map_w["key"] = 10;
  map_w.flush();
  map_w.close();

  // Normal read
  Map map_r(ctx, map_name, TILEDB_READ, TILEDB_AES_256_GCM, key, key_len);
  CHECK(map_r.has_key("key"));
  CHECK((int)map_r["key"]["a"] == 10);
  map_r.close();

  // Read from 0 timestamp
  Map map_r_at_0(
      ctx, map_name, TILEDB_READ, TILEDB_AES_256_GCM, key, key_len, 0);

  SECTION("Testing Map::Map") {
    // Nothing to do - just for clarity
  }

  SECTION("Testing Map::open") {
    map_r_at_0.close();
    map_r_at_0.open(TILEDB_READ, TILEDB_AES_256_GCM, key, key_len, 0);
  }

  CHECK(!map_r_at_0.has_key("key"));
  map_r_at_0.close();

  // Read from later timestamp
  auto timestamp = TILEDB_TIMESTAMP_NOW_MS;
  Map map_r_at(
      ctx, map_name, TILEDB_READ, TILEDB_AES_256_GCM, key, key_len, timestamp);

  SECTION("Testing Map::Map") {
    // Nothing to do - just for clarity
  }

  SECTION("Testing Map::open") {
    map_r_at.close();
    map_r_at.open(TILEDB_READ, TILEDB_AES_256_GCM, key, key_len, timestamp);
  }

  CHECK(map_r_at.has_key("key"));
  CHECK((int)map_r_at["key"]["a"] == 10);
  map_r_at.close();

  if (vfs.is_dir(map_name))
    vfs.remove_dir(map_name);
}
