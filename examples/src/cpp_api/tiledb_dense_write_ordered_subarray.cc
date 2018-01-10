/**
 * @file   tdbpp_dense_read_ordered_subarray.cc
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
 * It shows how to read from a dense array, constraining the read
 * to a specific subarray. The cells are copied to the
 * input buffers sorted in row-major order within the selected subarray.
 *
 * You need to run the following to make it work:
 *
 * $ ./tiledb_dense_create
 * $ ./tiledb_dense_write_global_1
 * $ ./tiledb_dense_read_ordered_subarray
 */

#include <tiledb>

int main() {
  tdb::Context ctx;
  tdb::Query query(ctx, "my_dense_array", TILEDB_WRITE);

  query.set_subarray<uint64_t>({3, 4, 2, 4}).set_layout(TILEDB_ROW_MAJOR);

  std::vector<int> a1_data = {9, 12, 13, 11, 14, 15};
  std::vector<uint64_t> a2_offsets = {0, 2, 3, 5, 9, 12};
  const std::string a2str = "jjmnnllllooopppp";
  std::vector<char> a2_data{a2str.begin(), a2str.end()};
  std::vector<float> a3_data = {
      9.1, 9.2, 12.1, 12.2, 13.1, 13.2, 11.1, 11.2, 14.1, 14.2, 15.1, 15.2};

  query.set_buffer("a1", a1_data);
  query.set_buffer("a2", a2_offsets, a2_data);
  query.set_buffer("a3", a3_data);

  query.submit();
  return 0;
}