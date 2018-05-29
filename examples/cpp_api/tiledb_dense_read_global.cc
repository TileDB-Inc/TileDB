/**
 * @file   tiledb_dense_read_global.cc
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
 * It shows how to read a complete dense array in the global cell order.
 *
 * You need to run the following to make it work:
 *
 * $ ./tiledb_dense_create_cpp
 * $ ./tiledb_dense_write_global_1_cpp
 * $ ./tiledb_dense_read_global_cpp
 */

#include <tiledb/array.h>
#include <tiledb/query.h>
#include <iomanip>

int main() {
  // Create TileDB context
  tiledb::Context ctx;

  // Open array
  tiledb::Array array(ctx, "my_dense_array");

  // Print non-empty domain
  auto domain = array.non_empty_domain<uint64_t>();
  std::cout << "Non empty domain:\n";
  for (const auto& d : domain) {
    std::cout << d.first << ": (" << d.second.first << ", " << d.second.second
              << ")\n";
  }

  // Print maximum buffer elements for the query results per attribute
  const std::vector<uint64_t> subarray = {1, 4, 1, 4};
  auto max_sizes = array.max_buffer_elements(subarray);
  std::cout << "\nMaximum buffer elements:\n";
  for (const auto& e : max_sizes) {
    std::cout << e.first << ": (" << e.second.first << ", " << e.second.second
              << ")\n";
  }

  // Prepare cell buffers
  std::vector<int> a1_buff(max_sizes["a1"].second);
  std::vector<uint64_t> a2_offsets(max_sizes["a2"].first);
  std::vector<char> a2_data(max_sizes["a2"].second);
  std::vector<float> a3_buff(max_sizes["a3"].second);

  // Create query
  tiledb::Query query(ctx, array, TILEDB_READ);
  query.set_layout(TILEDB_GLOBAL_ORDER);
  query.set_subarray(subarray);
  query.set_buffer("a1", a1_buff);
  query.set_buffer("a2", a2_offsets, a2_data);
  query.set_buffer("a3", a3_buff);

  // Submit query
  std::cout << "\nQuery submitted: " << query.submit();

  // Finalize query
  query.finalize();

  // Print cell values (assumes all attributes are read)
  auto result_el = query.result_buffer_elements();
  auto a2_buff =
      std::pair<std::vector<uint64_t>, std::vector<char>>(a2_offsets, a2_data);
  auto a2 = tiledb::group_by_cell(
      a2_buff, result_el["a2"].first, result_el["a2"].second);
  auto a3 = tiledb::group_by_cell<2>(a3_buff);

  std::cout << "\n\nResult num: " << result_el["a1"].second << "\n\n";
  std::cout << std::setw(5) << "a1" << std::setw(10) << "a2" << std::setw(10)
            << "a3[0]" << std::setw(11) << "a3[1]\n";
  std::cout << "-------------------------------------\n";
  for (unsigned i = 0; i < result_el["a1"].second; ++i) {
    std::cout << std::setw(5) << a1_buff[i] << std::setw(10)
              << std::string(a2[i].data(), a2[i].size()) << std::setw(10)
              << a3[i][0] << std::setw(10) << a3[i][1] << '\n';
  }

  // Close array
  array.close();

  // Nothing to clean up - all C++ objects are deleted when exiting scope

  return 0;
}