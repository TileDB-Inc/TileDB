/**
 * @file   tiledb_dense_read_subset_incomplete.cc
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
 * It shows how to read from a dense array, constraining the read
 * to a specific subarray and a subset of attributes. Moreover, the
 * program shows how to handle incomplete queries that did not complete
 * because the input buffers were not big enough to hold the entire
 * result.
 *
 * You need to run the following to make it work:
 *
 * $ ./tiledb_dense_create_cpp
 * $ ./tiledb_dense_write_global_1_cpp
 * $ ./tiledb_dense_read_subset_incomplete_cpp
 */

#include <tiledb/query.h>
#include <iomanip>

int main() {
  // Create TileDB Context
  tiledb::Context ctx;

  // Open array
  tiledb::Array array(ctx, "my_dense_array");

  // Prepare cell buffers
  std::vector<int> a1_data(2);

  // Create query
  tiledb::Query query(ctx, array, TILEDB_READ);
  query.set_layout(TILEDB_COL_MAJOR);
  query.set_subarray<uint64_t>({3, 4, 2, 4});
  query.set_buffer("a1", a1_data);

  // Loop until the query is completed
  std::cout << "a1\n---\n";
  do {
    std::cout << "Reading cells...\n";
    query.submit();
    auto result_el = query.result_buffer_elements();

    for (unsigned i = 0; i < result_el["a1"].second; ++i) {
      std::cout << a1_data[i] << "\n";
    }
  } while (query.query_status() == tiledb::Query::Status::INCOMPLETE);

  // Finalize after we're done re-submitting the query.
  query.finalize();

  // Close array
  array.close();

  // Nothing to clean up - all C++ objects are deleted when exiting scope

  return 0;
}