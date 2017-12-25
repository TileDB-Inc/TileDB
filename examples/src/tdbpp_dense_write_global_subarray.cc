/**
 * @file   tdbpp_dense_write_global_subarray.cc
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
 * It shows how to write a dense subarray in the global cell order.
 * Make sure that there is not directory named "my_dense_array" in your current
 * working directory.
 *
 * You need to run the following to make this work:
 *
 * ./tsbpp_dense_create
 * ./tdbpp_dense_write_global_subarray
 */

#include <tdbpp>

int main() {
  tdb::Context ctx;
  tdb::Array array = ctx.array_get("my_dense_array");
  tdb::Query query = array.write();

  query.buffer_list({"a1", "a2", "a3"}).subarray<tdb::type::UINT64>({3, 4, 3, 4}).layout(TILEDB_GLOBAL_ORDER);

  std::vector<int> a1_data = {112, 113, 114, 115};
  std::vector<uint64_t> a2_offsets = {0, 1, 3, 6};
  const std::string a2str = "MNNOOOPPPP";
  std::vector<char> a2_data{a2str.begin(), a2str.end()};
  std::vector<std::array<float,2>> a3_data = {{112.1, 112.2}, {113.1, 113.2},
                                              {114.1, 114.2}, {115.1, 115.2}};

  query.set_buffer<tdb::type::INT32>("a1", a1_data);
  query.set_buffer<tdb::type::CHAR>("a2", a2_offsets, a2_data);
  query.set_buffer<tdb::type::FLOAT32>("a3", a3_data);

  query.submit();
  return 0;
}