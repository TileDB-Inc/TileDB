/**
 * @file   tdbpp_sparse_create.cc
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
 * It shows how to create a dense array. Make sure that no directory exists
 * with the name "my_sparse_array" in the current working directory.
 */

#include <array>
#include <tiledb>

int main() {
  tiledb::Context ctx;

  tiledb::Domain domain(ctx);
  tiledb::Dimension d1 = tiledb::Dimension::create<uint64_t>(ctx, "d1", {{1, 4}}, 2);
  tiledb::Dimension d2 = tiledb::Dimension::create<uint64_t>(ctx, "d2", {{1, 4}}, 2);
  domain << d1 << d2;  // Add dims to domain

  tiledb::Attribute a1 = tiledb::Attribute::create<int>(ctx, "a1");
  tiledb::Attribute a2 = tiledb::Attribute::create<char>(ctx, "a2");
  tiledb::Attribute a3 = tiledb::Attribute::create<float>(ctx, "a3");

  // Set attr compressors and number
  a1.set_compressor(tiledb::Compressor{TILEDB_BLOSC, -1}).set_cell_val_num(1);
  a2.set_compressor(tiledb::Compressor{TILEDB_GZIP, -1})
      .set_cell_val_num(TILEDB_VAR_NUM);
  a3.set_compressor(tiledb::Compressor{TILEDB_ZSTD, -1}).set_cell_val_num(2);

  tiledb::ArraySchema schema(ctx, TILEDB_SPARSE);
  schema.set_order({{TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR}});
  schema << 2                // set capacity
         << domain           // Set domain
         << a1 << a2 << a3;  // set attributes

  // Check the schema, and make the array.
  tiledb::create_array("my_sparse_array", schema);

  std::cout << "Array created with schema: " << schema << std::endl;

  return 0;
}