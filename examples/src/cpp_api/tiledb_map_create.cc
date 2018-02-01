/**
 * @file   tiledb_map_create.cc
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
 * Create a Map.
 */

#include <tiledb>

int main() {
  tiledb::Context ctx;
  tiledb::MapSchema schema(ctx);

  tiledb::Attribute a1 = tiledb::Attribute::create<int>(ctx, "a1");
  tiledb::Attribute a2 = tiledb::Attribute::create<char>(ctx, "a2");
  tiledb::Attribute a3 = tiledb::Attribute::create<float>(ctx, "a3");

  a1.set_compressor({TILEDB_BLOSC, -1}).set_cell_val_num(1);
  a2.set_compressor({TILEDB_GZIP, -1}).set_cell_val_num(TILEDB_VAR_NUM);
  a3.set_compressor({TILEDB_ZSTD, -1}).set_cell_val_num(2);

  schema << a1 << a2 << a3;

  schema.dump(stdout);

  tiledb::create_map("my_map", schema);
  
  return 0;
}