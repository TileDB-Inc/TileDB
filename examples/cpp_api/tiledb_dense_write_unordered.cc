/**
 * @file   tiledb_dense_write_unordered.cc
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
 * It shows how to write random (unordered) cells to a dense array.
 *
 * Make sure that there is no directory named `my_dense_array` in your
 * current working directory.
 *
 * You need to run the following to make this work:
 *
 * ./tiledb_dense_create_cpp
 * ./tiledb_dense_write_unordered_cpp
 */

#include <tiledb/tiledb>

int main() {
  // Create TileDB context
  tiledb::Context ctx;

  // Prepare cell buffers
  std::vector<int> a1_data = {211, 213, 212, 208};
  std::vector<std::string> a2 = {"wwww", "yy", "x", "u"};
  std::vector<float> a3_data = {
      211.1f, 211.2f, 213.1f, 213.2f, 212.1f, 212.2f, 208.1f, 208.2f};
  std::vector<uint64_t> coords = {4, 2, 3, 4, 3, 3, 3, 1};

  // Creates two buffers: first is the starting offsets, second is the values
  auto a2_buff = tiledb::ungroup_var_buffer(a2);

  // Create query
  tiledb::Query query(ctx, "my_dense_array", TILEDB_WRITE);
  query.set_layout(TILEDB_UNORDERED);
  query.set_subarray<uint64_t>({{{3, 4}}, {{3, 4}}});
  query.set_buffer("a1", a1_data);
  query.set_buffer("a2", a2_buff);
  query.set_buffer("a3", a3_data);
  query.set_coordinates(coords);

  // Submit query
  query.submit();
  // Finalize query
  query.finalize();

  // Nothing to clean up - all C++ objects are deleted when exiting scope

  return 0;
}
