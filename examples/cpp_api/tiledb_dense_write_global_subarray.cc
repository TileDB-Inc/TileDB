/**
 * @file   tiledb_dense_write_global_subarray.cc
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
 * It shows how to write a dense subarray in the global cell order.
 * Make sure that there is not directory named `my_dense_array` in your current
 * working directory.
 *
 * You need to run the following to make this work:
 *
 * ./tiledb_dense_create_cpp
 * ./tiledb_dense_write_global_subarray_cpp
 */

#include <tiledb/tiledb>

int main() {
  // Create TileDB context
  tiledb::Context ctx;

  // Prepare cell buffers
  std::vector<int> a1_data = {112, 113, 114, 115};
  std::vector<std::string> a2 = {"M", "NN", "OOO", "PPPP"};
  std::vector<float> a3_data = {
      112.1f, 112.2f, 113.1f, 113.2f, 114.1f, 114.2f, 115.1f, 115.2f};

  // Creates two buffers: first is the starting offsets, second is the values
  auto a2_buff = tiledb::ungroup_var_buffer(a2);

  // Create query
  tiledb::Query query(ctx, "my_dense_array", TILEDB_WRITE);
  query.set_layout(TILEDB_GLOBAL_ORDER);
  query.set_subarray<uint64_t>({3, 4, 3, 4});
  query.set_buffer("a1", a1_data);
  query.set_buffer("a2", a2_buff);
  query.set_buffer("a3", a3_data);

  // Submit query
  query.submit();
  // Finalize query
  query.finalize();

  // Nothing to clean up - all C++ objects are deleted when exiting scope

  return 0;
}
