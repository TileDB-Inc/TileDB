/**
 * @file   tiledb_sparse_read_subset.cc
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
 * It shows how to read from a sparse array, constraining the read
 * to a specific subarray and a subset of attributes. Moreover, the
 * program shows how to handle incomplete queries that did not complete
 * because the input buffers were not big enough to hold the entire
 * result.
 *
 * You need to run the following to make it work:
 *
 * $ ./tiledb_sparse_create_cpp
 * $ ./tiledb_sparse_write_global_1_cpp
 * $ ./tiledb_sparse_read_subset_cpp
 */

#include <tiledb/tiledb>

int main() {
  // Create TileDB Context
  tiledb::Context ctx;

  // Prepare cell buffers
  std::vector<int> a1_data(3);

  // Create query
  tiledb::Query query(ctx, "my_sparse_array", TILEDB_READ);
  query.set_layout(TILEDB_COL_MAJOR);
  query.set_subarray<uint64_t>({3, 4, 2, 4});
  query.set_buffer("a1", a1_data);
  query.submit();
  query.finalize();

  // Print the results
  std::cout << "a1\n---\n";
  auto result_el = query.result_buffer_elements();
  for (unsigned i = 0; i < result_el["a1"].second; ++i)
    std::cout << a1_data[i] << "\n";

  // Nothing to clean up - all C++ objects are deleted when exiting scope

  return 0;
}