/**
 * @file   map.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2018 TileDB, Inc.
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
 * This is a part of the TileDB tutorial:
 *   https://docs.tiledb.io/en/latest/tutorials/kv.html
 *
 * This program shows the various ways you can use a TileDB map (key-value
 * store).
 *
 */

#include <iostream>
#include <tiledb/tiledb>

using namespace tiledb;

// Name of map.
std::string map_name("map_array");

void create_map() {
  // Create TileDB context
  tiledb::Context ctx;

  // Create a map with two attributes attribute
  tiledb::MapSchema schema(ctx);
  auto a1 = tiledb::Attribute::create<int>(ctx, "a1");
  auto a2 = tiledb::Attribute::create<float>(ctx, "a2");
  schema.add_attributes(a1, a2);
  tiledb::Map::create(map_name, schema);
}

void write_map() {
  tiledb::Context ctx;

  // Open the map
  tiledb::Map map(ctx, map_name, TILEDB_WRITE);

  std::vector<std::string> attrs = {"a1", "a2"};

  // Add map items with [] operator
  map["key_1"][attrs] = std::tuple<int, float>(1, 1.1f);
  map["key_2"][attrs] = std::tuple<int, float>(2, 2.1f);
  map.flush();

  // Add map items through functions
  auto key3_item = Map::create_item(ctx, "key_3");
  key3_item.set("a1", 3);
  key3_item["a2"] = 3.1f;
  map.add_item(key3_item);
  map.flush();

  // Close the map
  map.close();
}

void read_map() {
  Context ctx;

  // Open the map
  tiledb::Map map(ctx, map_name, TILEDB_READ);

  // Read the keys
  int key1_a1 = map["key_1"]["a1"];
  float key1_a2 = map["key_1"]["a2"];
  auto key2_item = map["key_2"];
  int key2_a1 = key2_item["a1"];
  auto key3_item = map["key_3"];
  float key3_a2 = key3_item["a2"];

  // Print
  std::cout << "Simple read\n";
  std::cout << "key_1, a1: " << key1_a1 << "\n";
  std::cout << "key_1, a2: " << key1_a2 << "\n";
  std::cout << "key_2: a1: " << key2_a1 << "\n";
  std::cout << "key_3: a2: " << key3_a2 << "\n";

  // Close the map
  map.close();
}

void iter_map() {
  Context ctx;

  // Open the map
  tiledb::Map map(ctx, map_name, TILEDB_READ);

  std::cout << "\nIterating over map items\n";
  MapIter iter(map), end(map, true);
  for (; iter != end; ++iter) {
    auto key = iter->key<std::string>();
    int a1 = (*iter)["a1"];
    float a2 = (*iter)["a2"];
    std::cout << "key: " << key << ", a1: " << a1 << ", a2: " << a2 << "\n";
  }
}

int main() {
  Context ctx;

  // Create and write the map only if it does not exist
  if (Object::object(ctx, map_name).type() != Object::Type::KeyValue) {
    create_map();
    write_map();
  }

  read_map();
  iter_map();

  return 0;
}