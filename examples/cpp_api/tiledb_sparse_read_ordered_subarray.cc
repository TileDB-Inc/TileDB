/**
 * @file   tiledb_sparse_read_ordered_subarray.cc
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
 * to a specific subarray. This time the cells are returned in row-major order
 * within the specified subarray.
 *
 * You need to run the following to make it work:
 *
 * $ ./tiledb_sparse_create_cpp
 * $ ./tiledb_sparse_write_global_1_cpp
 * $ ./tiledb_sparse_read_ordered_subarray_cpp
 */

#include <tiledb/array.h>
#include <tiledb/query.h>
#include <iomanip>

int main() {
  // Create TileDB context
  tiledb::Context ctx;

  // Open array
  tiledb::Array array(ctx, "my_sparse_array");

  // Calculate maximum buffer elements for the query results per attribute
  const std::vector<uint64_t> subarray = {3, 4, 2, 4};
  auto max_sizes = array.max_buffer_elements(subarray);

  // Prepare cell buffers
  std::vector<int> a1_buff(max_sizes["a1"].second);
  std::vector<uint64_t> a2_offsets(max_sizes["a2"].first);
  std::vector<char> a2_data(max_sizes["a2"].second);
  std::vector<float> a3_buff(max_sizes["a3"].second);
  std::vector<uint64_t> coords_buff(max_sizes[TILEDB_COORDS].second);

  // Create query
  tiledb::Query query(ctx, array, TILEDB_READ);
  query.set_layout(TILEDB_ROW_MAJOR).set_subarray(subarray);
  query.set_buffer("a1", a1_buff);
  query.set_buffer("a2", a2_offsets, a2_data);
  query.set_buffer("a3", a3_buff);
  query.set_coordinates(coords_buff);

  // Submit query
  query.submit();
  // Finalize query
  query.finalize();

  // Print cell values (assumes all attributes are read)
  auto result_el = query.result_buffer_elements();
  auto a2_buff =
      std::pair<std::vector<uint64_t>, std::vector<char>>(a2_offsets, a2_data);
  auto a2 = tiledb::group_by_cell(
      a2_buff, result_el["a2"].first, result_el["a2"].second);
  auto a3 = tiledb::group_by_cell<2>(a3_buff);
  auto coords =
      tiledb::group_by_cell<2>(coords_buff, result_el[TILEDB_COORDS].second);

  std::cout << "Result num: " << result_el["a1"].second << "\n\n";
  std::cout << std::setw(8) << TILEDB_COORDS << std::setw(9) << "a1"
            << std::setw(9) << "a2" << std::setw(11) << "a3[0]" << std::setw(10)
            << "a3[1]\n";
  std::cout << "------------------------------------------------\n";
  for (unsigned i = 0; i < result_el["a1"].second; ++i) {
    std::cout << "(" << coords[i][0] << ", " << coords[i][1] << ")"
              << std::setw(10) << a1_buff[i] << std::setw(10)
              << std::string(a2[i].data(), a2[i].size()) << std::setw(10)
              << a3[i][0] << std::setw(10) << a3[i][1] << '\n';
  }

  // Close array
  array.close();

  // Nothing to clean up - all C++ objects are deleted when exiting scope

  return 0;
}