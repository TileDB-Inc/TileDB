/**
 * @file   quickstart_map.cc
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
 * This is a part of the TileDB quickstart tutorial:
 *   https://docs.tiledb.io/en/latest/tutorials/quickstart.html
 *
 * When run, this program will create a simple key-value store (i.e., a map),
 * write some data to it, and read data based on keys.
 *
 */

#include <iostream>
#include <tiledb/tiledb>

using namespace tiledb;

// Name of map.
std::string map_name("quickstart_map_array");

void create_map() {
  // Create TileDB context
  tiledb::Context ctx;

  // Create a map with a single integer attribute
  tiledb::MapSchema schema(ctx);
  tiledb::Attribute a = tiledb::Attribute::create<int>(ctx, "a");
  schema.add_attribute(a);
  tiledb::Map::create(map_name, schema);
}

void write_map() {
  tiledb::Context ctx;

  // Open the map
  tiledb::Map map(ctx, map_name, TILEDB_WRITE);

  map["key_1"]["a"] = 1;
  map["key_2"] = 2;  // Implicit "a" since there is 1 attr
  map["key_3"] = 3;
  map.flush();

  // Close the map
  map.close();
}

void read_map() {
  Context ctx;

  // Open the map
  tiledb::Map map(ctx, map_name, TILEDB_READ);

  // Read the keys
  int a1 = map["key_1"];
  int a2 = map["key_2"];
  int a3 = map["key_3"];

  // Print
  std::cout << "key_1: " << a1 << "\n";
  std::cout << "key_2: " << a2 << "\n";
  std::cout << "key_3: " << a3 << "\n";

  // Close the map
  map.close();
}

int main() {
  Context ctx;

  if (Object::object(ctx, map_name).type() != Object::Type::KeyValue) {
    create_map();
    write_map();
  }

  read_map();

  return 0;
}