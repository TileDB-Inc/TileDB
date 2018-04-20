/**
 * @file   tiledb_dense_write_global_2.cc
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
 * It shows how to write to a dense array invoking the write function
 * twice. This will have the same effect as program
 * `tiledb_dense_write_global_1.cc`.
 *
 * You need to run the following to make this work:
 *
 * ./tiledb_dense_create_cpp
 * ./tiledb_dense_write_global_2_cpp
 */

#include <tiledb/tiledb>

int main() {
  // Create TileDB context
  tiledb::Context ctx;

  // Prepare cell buffers - #1
  std::vector<int> a1_data = {0, 1, 2, 3, 4, 5};
  std::string a2str = "abbcccddddeffggghhhh";
  std::vector<uint64_t> a2_offsets = {0, 1, 3, 6, 10, 11, 13, 16};
  std::vector<float> a3_data = {};

  // Create query
  tiledb::Query query(ctx, "my_dense_array", TILEDB_WRITE);
  query.set_layout(TILEDB_GLOBAL_ORDER);
  query.set_buffer("a1", a1_data);
  query.set_buffer("a2", a2_offsets, a2str);
  query.set_buffer("a3", a3_data);

  // Submit query - #1
  query.submit();
  // Don't finalize the query yet -- we wish to append to the same fragment.

  // Prepare cell buffers - #2
  // clang-format off
  a1_data = {6, 7, 8, 9, 10, 11, 12, 13, 14, 15};
  a2_offsets = {0, 1, 3, 6, 10, 11, 13, 16};
  a2str = "ijjkkkllllmnnooopppp";
  a3_data = {
      0.1f,  0.2f,  1.1f,  1.2f, 2.1f,  2.2f,  3.1f,  3.2f,  // Upper left tile
      4.1f,  4.2f,  5.1f,  5.2f, 6.1f,  6.2f,  7.1f,  7.2f,  // Upper right tile
      8.1f,  8.2f,  9.1f,  9.2f, 10.1f, 10.2f, 11.1f, 11.2f,  // Lower left tile
      12.1f, 12.2f, 13.1f, 13.2f, 14.1f, 14.2f, 15.1f, 15.2f,  // Lower right tile
  };
  // clang-format on

  // Reset buffers
  query.reset_buffers();
  query.set_buffer("a1", a1_data);
  query.set_buffer("a2", a2_offsets, a2str);
  query.set_buffer("a3", a3_data);

  // Submit query - #2
  query.submit();

  // Finalize query only after the second write.
  query.finalize();

  // Nothing to clean up - all C++ objects are deleted when exiting scope

  return 0;
}
