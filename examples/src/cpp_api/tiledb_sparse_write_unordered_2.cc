/**
 * @file   tdbpp_sparse_write_unordered_2.cc
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
 * It shows how to write unordered cells to a sparse array with two write
 * queries.
 *
 * You need to run the following to make this work:
 *
 * ./tiledb_sparse_create
 * ./tiledb_sparse_write_unordered_2
 */

#include <tiledb>

int main() {
  tiledb::Context ctx;
  tiledb::Query query(ctx, "my_sparse_array", TILEDB_WRITE);

  // clang-format off
  std::vector<int> a1_buff = {7, 5, 0};
  auto a2_buff = tiledb::make_var_buffers<std::string>({"hhhh", "ff", "a"});
  std::vector<float> a3_buff = {7.1, 7.2, 5.1, 5.2, 0.1, 0.2};
  std::vector<uint64_t> coords_buff = {3, 4, 4, 2, 1, 1};

  query.set_buffer("a1", a1_buff);
  query.set_buffer("a2", a2_buff);
  query.set_buffer("a3", a3_buff);
  query.set_buffer(TILEDB_COORDS, coords_buff);
  query.set_layout(TILEDB_UNORDERED);

  query.submit();

  a1_buff = {6, 4, 3, 1, 2};
  auto a2_2_buff = tiledb::make_var_buffers<std::string>({"ggg", "e", "dddd", "bb", "ccc"});
  a3_buff = {6.1, 6.2, 4.1, 4.2, 3.1, 3.2, 1.1, 1.2, 2.1, 2.2};
  coords_buff = {3, 3, 3, 1, 2, 3, 1, 2, 1, 4};

  // Reset buffers. This is needed in case vectors reallocate during reassignment.
  query.reset_buffers();
  query.set_buffer("a1", a1_buff);
  query.set_buffer("a2", a2_2_buff);
  query.set_buffer("a3", a3_buff);
  query.set_buffer(TILEDB_COORDS, coords_buff);

  query.submit();

return 0;
}
