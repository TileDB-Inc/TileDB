/**
 * @file   tdbpp_sparse_create.cc
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
 * It shows how to create a dense array. Make sure that no directory exists
 * with the name "my_sparse_array" in the current working directory.
 */

#include <tdbpp>

int main() {
  tdb::Context ctx;

  // Can also do: domain.create<tdb::type::UINT64>();
  tdb::Domain domain(ctx, TILEDB_UINT64);
  tdb::Dimension d1(ctx), d2(ctx);
  d1.create<tdb::type::UINT64>("d1", {1,4}, 2);
  d2.create<tdb::type::UINT64>("d2", {1,4}, 2);
  domain << d1 << d2; // Add dims to domain

  // Can also do: a1.create<tdb::type::INT32>("a1")
  tdb::Attribute a1(ctx, "a1", TILEDB_INT32);
  tdb::Attribute a2(ctx, "a2", TILEDB_CHAR);
  tdb::Attribute a3(ctx, "a3", TILEDB_FLOAT32);

  a1.set_compressor({TILEDB_BLOSC, -1}).set_num(1);
  a2.set_compressor({TILEDB_GZIP, -1}).set_num(TILEDB_VAR_NUM);
  a3.set_compressor({TILEDB_ZSTD, -1}).set_num(2);

  tdb::ArrayMetadata meta(ctx);
  meta.create("my_sparse_array");
  meta << TILEDB_SPARSE << domain << a1 << a2 << a3; // Add attributes to array
  meta.set_capacity(2);
  meta.set_tile_order(TILEDB_ROW_MAJOR).set_cell_order(TILEDB_ROW_MAJOR);

  // Check the metadata, and make the array.
  tdb::Array array(ctx);
  array.create(meta);

  std::cout << array << std::endl;

  return 0;
}