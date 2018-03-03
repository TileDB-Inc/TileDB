/**
 * @file   tiledb_dense_write_async.cc
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
 * It shows how to write asynchronoulsy to a dense array. The case of sparse
 * arrays is similar.
 *
 * You need to run the following to make this work:
 *
 * $ ./tiledb_dense_create_cpp
 * # ./tiledb_dense_write_async_cpp
 */

#include <tiledb/tiledb>

int main() {
  // Create TileDB context
  tiledb::Context ctx;

  // Prepare cell buffers
  // clang-format off
  std::vector<int> a1_data = {
      0, 1, 2, 3, 4, 5, 6, 7,
      8, 9, 10, 11, 12, 13, 14, 15
  };
  std::string a2str =
      "abbcccdddd"
      "effggghhhh"
      "ijjkkkllll"
      "mnnooopppp";
  std::vector<char> a2_data(a2str.begin(), a2str.end());
  std::vector<uint64_t> a2_offsets = {
      0, 1, 3, 6, 10, 11, 13, 16,
      20, 21, 23, 26, 30, 31, 33, 36
  };
  std::vector<float> a3_data = {
      0.1f,  0.2f,  1.1f,  1.2f,  2.1f,  2.2f,  3.1f,  3.2f,
      4.1f,  4.2f,  5.1f,  5.2f,  6.1f,  6.2f,  7.1f,  7.2f,
      8.1f,  8.2f,  9.1f,  9.2f,  10.1f, 10.2f, 11.1f, 11.2f,
      12.1f, 12.2f, 13.1f, 13.2f, 14.1f, 14.2f, 15.1f, 15.2f,
  };
  // clang-format on

  // Create query
  tiledb::Query query(ctx, "my_dense_array", TILEDB_WRITE);
  query.set_layout(TILEDB_GLOBAL_ORDER);
  query.set_buffer("a1", a1_data);
  query.set_buffer("a2", a2_offsets, a2_data);
  query.set_buffer("a3", a3_data);

  // Submit query with callback
  query.submit_async([]() { std::cout << "Callback: Query completed\n"; });

  // Wait for query to complete
  std::cout << "Query in progress\n";
  tiledb::Query::Status status;
  do {
    status = query.query_status();
  } while (status == tiledb::Query::Status::INPROGRESS);

  // Nothing to clean up - all C++ objects are deleted when exiting scope

  return 0;
}
