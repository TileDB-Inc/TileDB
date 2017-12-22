/**
 * @file   tdbpp_dense_read_global.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017 TileDB, Inc.
 * @copyright Copyright (c) 2016 MIT and Intel Corporation
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
 * $ ./tiledb_dense_create
 * $ ./tiledb_dense_write_global_1
 * $ ./tiledb_dense_read_global
 */

#include <tdbpp>
#include <iomanip>

int main() {
  using std::setw;
  tdb::Context ctx;

  // Buffers
  std::vector<int> a1_data;
  std::vector<char> a2_data;
  std::vector<uint64_t> a2_offsets;
  std::vector<float> a3_data;

  // Init the array & query for the array
  tdb::Array array = ctx.array_get("my_dense_array");
  tdb::Query query = array.read();

  // Set the layout of output, desired attributes, and determine buff sizes
  query.layout(TILEDB_GLOBAL_ORDER);
  query.attributes({"a1", "a2", "a3"});
  query.resize_buffer<tdb::type::INT32>("a1", a1_data); // For fixed size, compute the size we need.
  query.resize_var_buffer<tdb::type::CHAR>("a2", a2_offsets, a2_data, 3); // For var size, use expected num per cell = 3
  query.resize_buffer<tdb::type::FLOAT32>("a3", a3_data, 1000); // Bound the max buff size to N elements

  std::cout << "Query submitted: " << query.submit() << "\n'n";

  // Get the number of elements filled in by the query
  // Order is by attribute. For variable size attrs, the offset_buff comes first.
  const auto &buff_sizes = query.buff_sizes();

  // chunk the continous buffer by cell
  auto a2 = tdb::group_by_cell(a2_offsets, a2_data, buff_sizes[1], buff_sizes[2]); // For var size: use offset buff
  auto a3 = tdb::group_by_cell(a3_data, array.meta().attributes().at("a3").num());

  std::cout << "Result num: " << buff_sizes[0] << '\n'; // This assumes all attributes were fully read.
  std::cout << "a1" << setw(10) << "a2" << setw(10) << "a3[0]" << setw(10) << "a3[1]\n";
  for (unsigned i = 0; i < buff_sizes[0]; ++i) {
    std::cout << a1_data[i] << setw(10)
              << std::string(a2[i].data(), a2[i].size()) << setw(10)
              << a3[i][0] << setw(10)
              << a3[i][1] << '\n';
  }

  return 0;
}