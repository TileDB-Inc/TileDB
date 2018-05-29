/**
 * @file   tiledb_map_iter.cc
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
 * It shows how to read all items from a TileDB map using an iterator.
 *
 * You need to run the following to make it work:
 *
 * $ ./tiledb_map_create_cpp
 * $ ./tiledb_map_write_cpp
 * $ ./tiledb_map_iter_cpp
 */

#include <tiledb/map.h>

int main() {
  // Create TileDB context
  tiledb::Context ctx;

  // Create TileDB map and map iterator
  tiledb::Map map(ctx, "my_map");
  tiledb::MapIter iter(map), end(map, true);

  using my_cell_t = std::tuple<int, std::string, std::array<float, 2>>;

  // Read using iterator
  std::cout << "Iterating over all keys:\n";
  while (iter != end) {
    my_cell_t vals = (*iter)[{"a1", "a2", "a3"}];
    std::cout << "a1: " << std::get<0>(vals) << "\n"
              << "a2: " << std::get<1>(vals) << "\n"
              << "a3: " << std::get<2>(vals)[0] << " " << std::get<2>(vals)[1]
              << "\n";
    std::cout << "-----\n";
    ++iter;
  }

  // Nothing to clean up - all C++ objects are deleted when exiting scope

  return 0;
}
