/**
 * @file   tdbpp_dense_write_unordered.cc
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
 * It shows how to write random (unordered) cells to a dense array.
 *
 * Make sure that there is no directory named "my_dense_array" in your
 * current working directory.
 *
 * You need to run the following to make this work:
 *
 * ./tdbpp_dense_create
 * ./tdbpp_dense_write_unordered
 */

#include <tiledb>
#include <tuple>

int main() {
  tdb::Context ctx;
  tdb::Query query(ctx, "my_dense_array", TILEDB_WRITE);

  query.set_layout(TILEDB_UNORDERED);
  query.set_subarray<uint64_t>({{{3, 4}}, {{3, 4}}});

  std::vector<int> a1_data = {211, 213, 212, 208};

  // Make buffers for var size attr
  std::vector<std::string> a2 = {"wwww", "yy", "x", "u"};
  auto a2buff = tdb::make_var_buffers(a2);

  std::vector<float> a3_data = {
      211.1, 211.2, 213.1, 213.2, 212.1, 212.2, 208.1, 208.2};
  std::vector<uint64_t> coords = {4, 2, 3, 4, 3, 3, 3, 1};

  query.set_buffer<tdb::type::INT32>("a1", a1_data);
  query.set_buffer<tdb::type::CHAR>("a2", a2buff);
  query.set_buffer<tdb::type::FLOAT32>("a3", a3_data);
  query.set_buffer<tdb::type::UINT64>(TILEDB_COORDS, coords);

  query.submit();
  return 0;
}