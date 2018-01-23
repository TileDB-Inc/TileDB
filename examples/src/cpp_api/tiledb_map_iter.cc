/**
 * @file   tiledb_map_read.cc
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
 * Read a Map. Run map_write before this.
 */

#include <tiledb>

int main() {
  tiledb::Context ctx;
  tiledb::Map map(ctx, "my_map");

  std::cout << "\nWriting by iterating over all keys.\n";
  int i = 0;
  for (auto &item : map) {
    item["a1"] = ++i << 2;
    item["a2"] = std::string((unsigned) i*2, 'x');
    item["a3"] = std::vector<float>{(float)(i/.15), (float)(i/.05)};
  }

  // After iteration, map is flushed.

  std::cout << "\nIterating over all keys:\n";
  // Read using iterator
  for (auto &item : map) {
    std::tuple<int, std::string, std::vector<float>> vals = item[{"a1", "a2", "a3"}];
    std::cout << std::get<0>(vals) << ", " << std::get<1>(vals)
              << ", (" << std::get<2>(vals)[0] << ", " << std::get<2>(vals)[1] << ")\n";
  }

  std::cout << "\nOnly iterating over int keys:\n";
  // Read using iterator, only int keys
  for (auto item = map.begin<int>(); item != map.end(); ++item) {
    auto key = item->key<int>();
    std::tuple<int, std::string, std::vector<float>> vals = (*item)[{"a1", "a2", "a3"}];
    std::cout << key << ": " << std::get<0>(vals) << ", " << std::get<1>(vals)
              << ", (" << std::get<2>(vals)[0] << ", " << std::get<2>(vals)[1] << ")\n";
  }

  std::cout << "\nOnly iterating over str keys:\n";
  // Read using iterator, only int keys
  for (auto item = map.begin<std::string>(); item != map.end(); ++item) {
    auto key = item->key<std::string>();
    std::tuple<int, std::string, std::vector<float>> vals = (*item)[{"a1", "a2", "a3"}];
    std::cout << key << ": " << std::get<0>(vals) << ", " << std::get<1>(vals)
              << ", (" << std::get<2>(vals)[0] << ", " << std::get<2>(vals)[1] << ")\n";
  }
}