/**
 * @file   tiledb_map_read.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2018 TileDB, Inc.
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
 * It shows how to read from a TileDB map.
 *
 * You need to run the following to make it work:
 *
 * $ ./tiledb_map_create_cpp
 * $ ./tiledb_map_write_cpp
 * $ ./tiledb_map_read_cpp
 */

#include <tiledb/tiledb>

int main() {
  // Create TileDB context
  tiledb::Context ctx;

  // Create TileDB map
  tiledb::Map map(ctx, "my_map");

  using my_cell_t = std::tuple<int, std::string, std::array<float, 2>>;

  // Get item with key 100
  auto item1 = map.get_item(100);

  // Get value by implicit cast - you need to be sure that the item exists
  int a1 = map[100]["a1"];
  std::string a2 = map[100]["a2"];
  std::array<float, 2> a3 = map[100]["a3"];

  // Get item with explicit type
  a1 = item1.get<int>("a1");
  a2 = item1.get<std::string>("a2");
  a3 = item1.get<std::array<float, 2>>("a3");

  // Get values into a tuple
  my_cell_t vals = map[100][{"a1", "a2", "a3"}];

  // Get pointer to data with no C++ API copies
  auto a2_data = item1.get_ptr<char>("a2");

  // Print item
  std::cout << "a1\ta2\t(a3[0], a3[1])\n"
            << "-----------------------------\n"
            << a1 << "\t" << std::string(a2_data.first, a2_data.second) << "\t";
  std::cout << "(" << std::get<2>(vals)[0] << ", " << a3[1] << ")\n";

  // Try to get item that does not exist
  auto item2 = map.get_item(12345);
  if (!item2.good())
    std::cout << "\nItem with key '" << 12345 << "' does not exist\n";
  try {
    int err = map[12345]["a1"];
    (void)err;
  } catch (tiledb::TileDBError& e) {
    std::cout << e.what() << "\n";
  }

  // Nothing to clean up - all C++ objects are deleted when exiting scope

  return 0;
}
