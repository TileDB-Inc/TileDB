/**
 * @file   tdbpp_dense_write_async.cc
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
 * It shows how to write asynchronoulsy to a dense array. The case of sparse
 * arrays is similar.
 *
 * You need to run the following to make this work:
 *
 * $ ./tiledb_dense_create
 * # ./tiledb_dense_write_async
 */

#include <tiledb>

int main() {
  tiledb::Context ctx;

  // Buffers
  std::vector<int> a1_data = {
      0,
      1,
      2,
      3,  // Upper left tile
      4,
      5,
      6,
      7,  // Upper right tile
      8,
      9,
      10,
      11,  // Lower left tile
      12,
      13,
      14,
      15  // Lower right tile
  };
  std::string a2str =
      "abbcccdddd"   // Upper left tile
      "effggghhhh"   // Upper right tile
      "ijjkkkllll"   // Lower left tile
      "mnnooopppp";  // Lower right tile
  std::vector<char> a2_data(a2str.begin(), a2str.end());
  std::vector<uint64_t> a2_offsets = {
      0,
      1,
      3,
      6,  // Upper left tile
      10,
      11,
      13,
      16,  // Upper right tile
      20,
      21,
      23,
      26,  // Lower left tile
      30,
      31,
      33,
      36  // Lower right tile
  };
  std::vector<float> a3_data = {
      0.1,  0.2,  1.1,  1.2,  2.1,  2.2,  3.1,  3.2,   // Upper left tile
      4.1,  4.2,  5.1,  5.2,  6.1,  6.2,  7.1,  7.2,   // Upper right tile
      8.1,  8.2,  9.1,  9.2,  10.1, 10.2, 11.1, 11.2,  // Lower left tile
      12.1, 12.2, 13.1, 13.2, 14.1, 14.2, 15.1, 15.2,  // Lower right tile
  };

  // Init the array & query for the array
  tiledb::Query query(ctx, "my_dense_array", TILEDB_WRITE);

  query.set_layout(TILEDB_GLOBAL_ORDER);
  query.set_buffer("a1", a1_data);
  query.set_buffer("a2", a2_offsets, a2_data);
  query.set_buffer("a3", a3_data);

  // Submit query with callback
  std::function<void(void*)> callback = [](void*){std::cout << "Callback: query completed.\n";};
  query.submit_async(callback);

  std::cout << "Query in progress\n";
  tiledb::Query::Status status;
  do {
    // Wait till query is done
    status = query.query_status();
  } while (status == tiledb::Query::Status::INPROGRESS);

  return 0;
}