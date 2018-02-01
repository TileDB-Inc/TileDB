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

  // Get item with key 100
  auto item1 = map.get_item(100);

  std::cout << "a1, a2, (a3.first, a3.second)\n"
            << "-----------------------------\n"
            // Get a attr known to be a primitive
            << item1.get<int>("a1") << ", "
            // Get a >1 element attr with a std::string container.
            // Default container is std::vector<T>.
            << item1.get<std::string>("a2") << ", ";


  // Get value by implicit cast
  int a1 = map[100]["a1"];
  std::string a2 = map[100]["a2"];
  std::vector<float> a3 = map[100]["a3"];

  std::cout << "(" << a3[0] << ", " << a3[1] << ")\n"
            << "-----------------------------\n";

  // Get values into a tuple
  std::tuple<int, std::string, std::vector<float>> vals =
      map[std::vector<double>{300, 300.1}][{"a1", "a2", "a3"}];

  std::cout << "\nGet with tuple:\n"
            << "-----------------------------\n"
            << std::get<0>(vals) << ", " << std::get<1>(vals)
            << ", (" << std::get<2>(vals)[0] << ", " << std::get<2>(vals)[1] << ")\n"
            << "-----------------------------\n";


  // Error, can't read when key already set
  try {
    int err = map[12341]["a1"];
    (void) err;
  } catch (tiledb::TileDBError &e) {
    std::cout << "Error: key doesn't exist.\n";
  }

  // Get pointer to data with no C++ API copies.
  auto data = item1.get_ptr<char>("a2");
  std::cout << "\nNo copy get of attribute 2: " << std::string(data.first, data.second) << '\n'
            << "Implicit casts: " << a1 << ", " << a2;

  return 0;
}

