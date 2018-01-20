/**
 * @file   tiledb_array_schema.cc
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
 * This example explores the C++ API for the array schema.
 *
 * Simply run the following to make it work.
 *
 * $ ./tiledb_array_schema
 */

#include <tiledb>

int main() {
  tiledb::Context ctx;
  tiledb::ArraySchema schema(ctx);

  std::cout << "\nFirst dump:\n";
  schema.dump(stdout);

  // sparse array with tile capacity 10
  schema << TILEDB_SPARSE << 10;
  schema.set_tile_order(TILEDB_ROW_MAJOR);
  schema.set_cell_order(TILEDB_COL_MAJOR);
  schema.set_coord_compressor({TILEDB_ZSTD, 4});
  schema.set_offset_compressor({TILEDB_BLOSC, 5});

  std::cout << "Second dump:\n";
  schema.dump(stdout);

  tiledb::Domain domain(ctx);
  tiledb::Dimension d1 =
      tiledb::Dimension::create<uint64_t>(ctx, "d1", {{0, 1000}}, 10);
  tiledb::Dimension d2 =
      tiledb::Dimension::create<uint64_t>(ctx, "d2", {{100, 10000}}, 100);
  domain << d1 << d2;
  schema << domain;

  tiledb::Attribute a1 = tiledb::Attribute::create<int>(ctx, "");
  tiledb::Attribute a2 = tiledb::Attribute::create<float>(ctx, "a2");
  a1.set_cell_val_num(3);
  a2.set_compressor({TILEDB_GZIP, -1});
  schema << a1 << a2;

  std::cout << "Third dump:\n";
  schema.dump(stdout);

  std::cout << "\nFrom getters:"
            << "\n- Array type: " << schema.array_type()
            << "\n- Cell order: " << schema.cell_order()
            << "\n- Tile order: " << schema.tile_order()
            << "\n- Capacity: " << schema.capacity()
            << "\n- Coordinate compressor: " << schema.coord_compressor()
            << "\n- Offsets compressor: " << schema.offset_compressor();

  std::cout << "\nAttribute names:";
  for (const auto &a : schema.attributes())
    std::cout << "\n* " << a.first;

  std::cout << "\nDimension names:";
  for (const auto &d : schema.domain().dimensions())
    std::cout << "\n* " << d.name();

  return 0;
}