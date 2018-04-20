/**
 * @file   tiledb_sparse_write_unordered_2.cc
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
 * It shows how to write unordered cells to a sparse array with two write
 * queries.
 *
 * You need to run the following to make this work:
 *
 * ./tiledb_sparse_create
 * ./tiledb_sparse_write_unordered_2
 */

#include <tiledb/tiledb>

int main() {
  // Create TileDB context
  tiledb::Context ctx;

  // Prepare cell buffers - #1
  std::vector<int> a1_buff = {7, 5, 0};
  auto a2_buff = tiledb::ungroup_var_buffer<std::string>({"hhhh", "ff", "a"});
  std::vector<float> a3_buff = {7.1f, 7.2f, 5.1f, 5.2f, 0.1f, 0.2f};
  std::vector<uint64_t> coords_buff = {3, 4, 4, 2, 1, 1};

  // Create query
  tiledb::Query query(ctx, "my_sparse_array", TILEDB_WRITE);
  query.set_layout(TILEDB_UNORDERED);
  query.set_buffer("a1", a1_buff);
  query.set_buffer("a2", a2_buff);
  query.set_buffer("a3", a3_buff);
  query.set_coordinates(coords_buff);

  // Submit query - #1
  query.submit();
  // Don't finalize the query yet -- we wish to append to the same fragment.

  // Prepare cell buffers - #2
  a1_buff = {6, 4, 3, 1, 2};
  std::vector<std::string> a2_str = {"ggg", "e", "dddd", "bb", "ccc"};
  auto a2_buff_2 = tiledb::ungroup_var_buffer<std::string>(a2_str);
  a3_buff = {6.1f, 6.2f, 4.1f, 4.2f, 3.1f, 3.2f, 1.1f, 1.2f, 2.1f, 2.2f};
  coords_buff = {3, 3, 3, 1, 2, 3, 1, 2, 1, 4};

  // Reset buffers
  query.reset_buffers();
  query.set_buffer("a1", a1_buff);
  query.set_buffer("a2", a2_buff_2);
  query.set_buffer("a3", a3_buff);
  query.set_coordinates(coords_buff);

  // Submit query - #2
  query.submit();
  // Finalize query only after the second write.
  query.finalize();

  // Nothing to clean up - all C++ objects are deleted when exiting scope

  return 0;
}
