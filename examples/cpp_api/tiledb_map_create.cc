/**
 * @file   tiledb_map_create.cc
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
 * It shows how to create a TileDB map (key-value store).
 *
 * Simply run:
 *
 * $ ./tiledb_map_create_cpp
 */

#include <tiledb/tiledb>

int main() {
  // Create TileDB context
  tiledb::Context ctx;

  // Create attributes
  tiledb::Attribute a1 = tiledb::Attribute::create<int>(ctx, "a1");
  tiledb::Attribute a2 = tiledb::Attribute::create<std::string>(ctx, "a2");
  tiledb::Attribute a3 =
      tiledb::Attribute::create<std::array<float, 2>>(ctx, "a3");
  a1.set_compressor({TILEDB_BLOSC_LZ, -1});
  a2.set_compressor({TILEDB_GZIP, -1});
  a3.set_compressor({TILEDB_ZSTD, -1});

  // Create map schema
  tiledb::MapSchema schema(ctx);
  schema.add_attributes(a1, a2, a3);

  // Check array schema
  try {
    schema.check();
  } catch (tiledb::TileDBError& e) {
    std::cout << e.what() << "\n";
    return -1;
  }

  // Print the map schema
  schema.dump(stdout);

  // Create the map on storage
  tiledb::Map::create("my_map", schema);

  // Nothing to clean up - all C++ objects are deleted when exiting scope

  return 0;
}