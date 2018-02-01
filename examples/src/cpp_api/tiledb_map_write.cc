/**
 * @file   tiledb_map_write.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017 TileDB, Inc.
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
 * Write to a Map. Run map_create before this.
 */

#include <tiledb>

int main() {
  tiledb::Context ctx;

  {
    // Create map items unassociated with any map
    auto item1 = tiledb::Map::create_item(ctx, 100);
    item1["a1"] = 1;
    item1["a2"] = std::string("a");
    item1["a3"] = std::vector<float>{1.1, 1.2};

    auto item2 = tiledb::Map::create_item(ctx, 200.0);
    item2["a1"] = 2;
    item2["a2"] = std::string("bb");
    item2["a3"] = std::vector<float>{2.1, 2.2};

    auto item4 = tiledb::Map::create_item(ctx, std::string("key_4"));
    item4["a1"] = 4;
    item4["a2"] = std::string("dddd");
    item4["a3"] = std::vector<float>{4.1, 4.2};

    // Create a map
    tiledb::Map map(ctx, "my_map");
    map.set_max_buffered_items(100); // Flush every 100 items

    // Add items to map
    map << item1 << item2;

    // Force write to storage backend
    map.flush();

    // Write another item. These will be flushed on map destruction.
    map << item4;

    // Update an existing key. Old values for a2, a3 will be carried over.
    // Each time the map is keyed it is reloaded.
    map[100]["a1"] = 3;

    // Create an item, assign values, and add to the map in a single op
    // This only loads the item once when updating values.
    int t1 = 3;
    std::string t2{"CCC"};
    std::vector<float> t3{3.1, 3.2};
    map[std::vector<double>{300, 300.1}][{"a1", "a2", "a3"}] = std::make_tuple(t1, t2, t3);

    // Error; key doesn't exist yet so can't just assign
    // a single attribute since a2 and a3 are undefined.
    try {
      map[143523]["a1"] = 3;
    } catch (tiledb::TileDBError &e) {
      std::cout << "Item expects all 3 attributes to be assigned, only a1 set.\n";
    }

  } // Map goes out of scope, so it will be flushed.

  // Consolidate fragments (optional)
  tiledb::consolidate_map(ctx, "my_map");

  return 0;
}