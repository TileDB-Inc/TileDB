/**
 * @file   tiledb_array_schema.cc
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
 * This example explores the C++ API for the array schema.
 *
 * Simply run the following to make it work.
 *
 * $ ./tiledb_array_schema_cpp
 */

#include <tiledb/tiledb>

int main() {
  // Create TileDB context
  tiledb::Context ctx;

  // Create array schema
  tiledb::ArraySchema schema(ctx, TILEDB_SPARSE);

  // Print array schema contents
  std::cout << "First dump:\n";
  schema.dump(stdout);

  // Set some values
  schema.set_capacity(10);
  schema.set_tile_order(TILEDB_ROW_MAJOR);
  schema.set_cell_order(TILEDB_COL_MAJOR);
  schema.set_coords_compressor({TILEDB_ZSTD, 4});
  schema.set_offsets_compressor({TILEDB_BLOSC_LZ, 5});

  // Print array schema contents again
  std::cout << "Second dump:\n";
  schema.dump(stdout);

  // Create dimensions
  auto d1 = tiledb::Dimension::create<uint64_t>(ctx, "", {{1, 1000}}, 10);
  auto d2 = tiledb::Dimension::create<uint64_t>(ctx, "d2", {{101, 10000}}, 100);

  // Create and set domain
  tiledb::Domain domain(ctx);
  domain.add_dimension(d1).add_dimension(d2);
  schema.set_domain(domain);

  // Create and add attributes
  tiledb::Attribute a1 = tiledb::Attribute::create<std::array<int, 3>>(ctx, "");
  tiledb::Attribute a2 = tiledb::Attribute::create<float>(ctx, "a2");
  a2.set_compressor({TILEDB_GZIP, -1});
  schema.add_attribute(a1).add_attribute(a2);

  // Print array schema contents again
  std::cout << "Third dump:\n";
  schema.dump(stdout);

  // Print from getters
  std::cout << "\nFrom getters:"
            << "\n- Array type: "
            << ((schema.array_type() == TILEDB_DENSE) ? "dense" : "sparse")
            << "\n- Cell order: "
            << (schema.cell_order() == TILEDB_ROW_MAJOR ? "row-major" :
                                                          "col-major")
            << "\n- Tile order: "
            << (schema.tile_order() == TILEDB_ROW_MAJOR ? "row-major" :
                                                          "col-major")
            << "\n- Capacity: " << schema.capacity()
            << "\n- Coordinates compressor: " << schema.coords_compressor()
            << "\n- Offsets compressor: " << schema.offsets_compressor();

  // Print the attribute names
  std::cout << "\n\nArray schema attribute names: \n";
  for (const auto& a : schema.attributes())
    std::cout << "* " << a.first << "\n";
  std::cout << "\n";

  // Print domain
  schema.domain().dump(stdout);

  // Print the dimension names using iterators
  std::cout << "\nArray schema dimension names: \n";
  for (const auto& d : schema.domain().dimensions())
    std::cout << "* " << d.name() << "\n";

  // Nothing to clean up - all C++ objects are deleted when exiting scope

  return 0;
}