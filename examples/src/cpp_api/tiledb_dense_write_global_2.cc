/**
 * @file   tdbpp_dense_write_global_2.cc
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
 * It shows how to write to a dense array invoking the write function
 * twice. This will have the same effect as program
 * tiledb_dense_write_entire_1.cc.
 *
 * You need to run the following to make this work:
 * ./tdbpp_dense_create
 * ./tdbpp_dense_write_global_2
 */

#include <tiledb>

int main() {
  tdb::Context ctx;

  // Buffers
  std::vector<int> a1_data = {0, 1, 2, 3, 4, 5};
  std::string a2str = "abbcccddddeffggghhhh";
  std::vector<char> a2_data(a2str.begin(), a2str.end());
  std::vector<uint64_t> a2_offsets = {0, 1, 3, 6, 10, 11, 13, 16};
  std::vector<float> a3_data = {};

  // Init the array & query for the array
  tdb::Query query(ctx, "my_dense_array", TILEDB_WRITE);

  query.set_layout(TILEDB_GLOBAL_ORDER);
  query.set_buffer("a1", a1_data);
  query.set_buffer("a2", a2_offsets, a2_data);
  query.set_buffer("a3", a3_data);

  query.submit();

  a1_data = {6, 7, 8, 9, 10, 11, 12, 13, 14, 15};
  a2_offsets = {0, 1, 3, 6, 10, 11, 13, 16};
  a2str = "ijjkkkllllmnnooopppp";
  a2_data = std::vector<char>(a2str.begin(), a2str.end());
  a3_data = {
      0.1,  0.2,  1.1,  1.2,  2.1,  2.2,  3.1,  3.2,   // Upper left tile
      4.1,  4.2,  5.1,  5.2,  6.1,  6.2,  7.1,  7.2,   // Upper right tile
      8.1,  8.2,  9.1,  9.2,  10.1, 10.2, 11.1, 11.2,  // Lower left tile
      12.1, 12.2, 13.1, 13.2, 14.1, 14.2, 15.1, 15.2,  // Lower right tile
  };

  query.reset_buffers();
  query.set_buffer("a1", a1_data);
  query.set_buffer("a2", a2_offsets, a2_data);
  query.set_buffer("a3", a3_data);
  query.submit();

  return 0;
}
