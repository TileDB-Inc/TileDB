/**
 * @file   tiledb_sparse_write_unordered_1_again.cc
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
 * It shows how to write unordered cells to a sparse array in a single write.
 * This time we write 4 cells.
 *
 * You need to run the following to make this work:
 *
 * ./tiledb_sparse_create_cpp
 * ./tiledb_sparse_write_unordered_1_again_cpp
 */

#include <tiledb/query.h>

int main() {
  // Create TileDB context
  tiledb::Context ctx;

  // Open array
  tiledb::Array array(ctx, "my_sparse_array");

  // Prepare cell buffers
  std::vector<int> a1_buff = {107, 104, 106, 105};
  std::vector<std::string> a2_str = {"yyy", "u", "w", "vvvv"};
  auto a2_buff = tiledb::ungroup_var_buffer<std::string>(a2_str);
  std::vector<float> a3_buff = {
      107.1f, 107.2f, 104.1f, 104.2f, 106.1f, 106.2f, 105.1f, 105.2f};
  std::vector<uint64_t> coords_buff = {3, 4, 3, 2, 3, 3, 4, 1};

  // Create query
  tiledb::Query query(ctx, array, TILEDB_WRITE);
  query.set_layout(TILEDB_UNORDERED);
  query.set_buffer("a1", a1_buff);
  query.set_buffer("a2", a2_buff);
  query.set_buffer("a3", a3_buff);
  query.set_coordinates(coords_buff);

  // Submit query
  query.submit();

  // Finalize query
  query.finalize();

  // Close array
  array.close();

  // Nothing to clean up - all C++ objects are deleted when exiting scope

  return 0;
}
