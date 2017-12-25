/**
 * @file   tdbpp_sparse_read_ordered_subarray.cc
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
 * It shows how to read from a sparse array, constraining the read
 * to a specific subarray. This time the cells are returned in row-major order
 * within the specified subarray.
 *
 * You need to run the following to make it work:
 *
 * $ ./tiledb_sparse_create
 * $ ./tiledb_sparse_write_global_1
 * $ ./tiledb_sparse_read_ordered_subarray
 */

#include <tdbpp>
#include <iomanip>

int main() {
  using std::setw;
  tdb::Context ctx;

  // Init the array & query for the array
  tdb::Array array = ctx.array_get("my_sparse_array");
  tdb::Query query = array.read();

  // Set the layout of output, desired attributes, and determine buff sizes
  query.layout(TILEDB_GLOBAL_ORDER).subarray<tdb::type::UINT64>({3, 4, 2, 4});
  query.buffer_list({"a1", "a2", "a3", TILEDB_COORDS});
  auto a1_buff = query.make_buffer<tdb::type::INT32>("a1", 64);
  auto a2_buff = query.make_var_buffers<tdb::type::CHAR>("a2", 3, 64); // variable sized attr makes a pair of buffs
  auto a3_buff = query.make_fixed_buffer<tdb::type::FLOAT32,2>("a3", 64); // 2 floats per cell
  auto coord_buff = query.make_fixed_buffer<tdb::type::UINT64,2>(TILEDB_COORDS);
  query.set_buffer<tdb::type::INT32>("a1", a1_buff);
  query.set_buffer<tdb::type::CHAR>("a2", a2_buff);
  query.set_buffer<tdb::type::FLOAT32>("a3", a3_buff);
  query.set_buffer<tdb::type::UINT64>(TILEDB_COORDS, coord_buff);

  std::cout << "Query submitted: " << query.submit() << "\n";

  // Get the number of elements filled in by the query
  // Order is by attribute. For variable size attrs, the offset_buff comes first.
  const auto buff_sizes = query.returned_buff_sizes();

  auto a2 = tdb::group_by_cell(a2_buff, buff_sizes[1], buff_sizes[2]);

  std::cout << "Result num: " << buff_sizes[0] << '\n'; // This assumes all attributes were fully read.
  std::cout << "coords" << setw(10) << "a1" << setw(10) << "a2" << setw(10) << "a3[0]" << setw(8) << "a3[1]\n";
  for (unsigned i = 0; i < buff_sizes[0]; ++i) {
    std::cout << '(' << coord_buff[i][0] << ',' << coord_buff[i][1] << ')' << setw(10)
              << a1_buff[i] << setw(10)
              << std::string(a2[i].data(), a2[i].size()) << setw(8)
              << '(' << a3_buff[i][0] << ',' << setw(5) << a3_buff[i][1] << ')' << '\n';
  }

  return 0;
}