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

#include <tiledb/tiledb>

int main() {
  // Create TileDB context
  tiledb::Context ctx;

  // Create TileDB map
  tiledb::Map map(ctx, "my_map");

  using my_cell_t = std::tuple<int, std::string, std::array<float, 2>>;

  // Read using iterator
  std::cout << "Iterating over all keys:\n";
  for (auto& item : map) {
    my_cell_t vals = item[{"a1", "a2", "a3"}];
    std::cout << "a1: " << std::get<0>(vals) << "\n"
              << "a2: " << std::get<1>(vals) << "\n"
              << "a3: " << std::get<2>(vals)[0] << " " << std::get<2>(vals)[1]
              << "\n";
    std::cout << "-----\n";
  }

  // Read using iterator, only int keys
  std::cout << "\nOnly iterating over int keys:\n";
  for (auto item = map.begin<int>(); item != map.end(); ++item) {
    auto key = item->key<int>();
    my_cell_t vals = (*item)[{"a1", "a2", "a3"}];
    std::cout << "key: " << key << "\n"
              << "a1: " << std::get<0>(vals) << "\n"
              << "a2: " << std::get<1>(vals) << "\n"
              << "a3: " << std::get<2>(vals)[0] << " " << std::get<2>(vals)[1]
              << "\n";
    std::cout << "-----\n";
  }

  // Read using iterator, only string keys
  std::cout << "\nOnly iterating over string keys:\n";
  for (auto item = map.begin<std::string>(); item != map.end(); ++item) {
    auto key = item->key<std::string>();
    my_cell_t vals = (*item)[{"a1", "a2", "a3"}];
    std::cout << "key: " << key << "\n"
              << "a1: " << std::get<0>(vals) << "\n"
              << "a2: " << std::get<1>(vals) << "\n"
              << "a3: " << std::get<2>(vals)[0] << " " << std::get<2>(vals)[1]
              << "\n";
    std::cout << "-----\n";
  }

  // Read using iterator, only double vector keys
  std::cout << "\nOnly iterating over double vector keys:\n";
  for (auto item = map.begin<std::vector<double>>(); item != map.end();
       ++item) {
    auto key = item->key<std::vector<double>>();
    auto key_type = item->key_info();
    my_cell_t vals = (*item)[{"a1", "a2", "a3"}];
    std::cout << "key: ";
    for (uint64_t i = 0; i < key_type.second / sizeof(double); ++i)
      std::cout << key[i] << " ";
    std::cout << "\na1: " << std::get<0>(vals) << "\n"
              << "a2: " << std::get<1>(vals) << "\n"
              << "a3: " << std::get<2>(vals)[0] << " " << std::get<2>(vals)[1]
              << "\n";
    std::cout << "-----\n";
  }

  // Nothing to clean up - all C++ objects are deleted when exiting scope

  return 0;
}
