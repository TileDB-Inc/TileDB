/**
 * @file   tiledb_3d_create_csv.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2016 MIT and Intel Corporation
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
 * Creates a CSV file FILENAME, where each line in the CSV file must be of
 * the form:
 *
 * <coord_1> <coord_2> <coord_3> <attribute_value>
 *
 * CELL_NUM random tuples with the above format are generated.
 */

#include <fstream>
#include <random>

#define CELL_NUM 10
#define DIM_NUM 3
#define FILENAME "file.csv"
#define COORD_MAX 10000
#define ATTR_MAX 10000

int main() {
  std::random_device rd;
  std::default_random_engine gen(rd());
  std::uniform_int_distribution<long long unsigned> dist(1, 10000000000);

  std::ofstream file(FILENAME);
  for (uint64_t i = 0; i < CELL_NUM; ++i) {
    for (int i = 0; i < DIM_NUM; ++i)
      file << (dist(gen) % COORD_MAX) << " ";
    file << (dist(gen) % ATTR_MAX) << "\n";
  }

  file.close();

  return 0;
}
