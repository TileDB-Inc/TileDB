/**
 * @file   tdbpp_sparse_write_global_2.cc
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
 * It shows how to write to a sparse array with two write queries, assuming
 * that the user provides the cells ordered in the array global cell order.
 *
 * You need to run the following to make this work:
 *
 * ./tiledb_sparse_create
 * ./tiledb_sparse_write_global_2
 */

#include <tiledb>

int main() {
  tiledb::Context ctx;
  tiledb::Query query(ctx, "my_sparse_array", TILEDB_WRITE);

  std::vector<int> a1_buff = {0, 1, 2};
  auto a2_buff = tiledb::make_var_buffers<std::string>(
      {"a", "bb", "ccc", "dddd", "e", "ff", "ggg", "hhhh"});
  std::vector<float> a3_buff = {0.1,
                                0.2,
                                1.1,
                                1.2,
                                2.1,
                                2.2,
                                3.1,
                                3.2,
                                4.1,
                                4.2,
                                5.1,
                                5.2,
                                6.1,
                                6.2,
                                7.1,
                                7.2};
  std::vector<uint64_t> coords_buff = {1, 1, 1, 2};

  query.set_buffer("a1", a1_buff);
  query.set_buffer("a2", a2_buff);
  query.set_buffer("a3", a3_buff);
  query.set_buffer(TILEDB_COORDS, coords_buff);
  query.set_layout(TILEDB_GLOBAL_ORDER);

  query.submit();

  a1_buff = {3, 4, 5, 6, 7};
  a2_buff.first.clear();
  a2_buff.second.clear();
  a3_buff.clear();
  coords_buff = {1, 4, 2, 3, 3, 1, 4, 2, 3, 3, 3, 4};

  // Reset buffers. This is needed in case vectors reallocate during
  // reassignment.
  query.reset_buffers();
  query.set_buffer("a1", a1_buff);
  query.set_buffer("a2", a2_buff);
  query.set_buffer("a3", a3_buff);
  query.set_buffer(TILEDB_COORDS, coords_buff);

  query.submit();

  return 0;
}