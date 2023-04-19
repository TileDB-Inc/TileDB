/**
 * @file   using_tiledb_stats.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2018-2022 TileDB, Inc.
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
 * When run, this program will create a 0.5GB dense array, and enable the
 * TileDB statistics surrounding reads from the array.
 */

#include <iostream>
#include <tiledb/tiledb>

using namespace tiledb;

// Name of array.
std::string array_name("stats_array");

void create_array(uint32_t row_tile_extent, uint32_t col_tile_extent) {
  Context ctx;
  VFS vfs(ctx);
  if (vfs.is_dir(array_name))
    vfs.remove_dir(array_name);

  ArraySchema schema(ctx, TILEDB_DENSE);

  Domain dom(ctx);
  dom.add_dimension(
         Dimension::create<uint32_t>(ctx, "row", {{1, 12000}}, row_tile_extent))
      .add_dimension(Dimension::create<uint32_t>(
          ctx, "col", {{1, 12000}}, col_tile_extent));

  schema.set_domain(dom);
  schema.add_attribute(Attribute::create<int32_t>(ctx, "a"));

  Array::create(array_name, schema);
}

void write_array() {
  Context ctx;
  Array array(ctx, array_name, TILEDB_WRITE);
  Query query(ctx, array);

  std::vector<int32_t> values(12000 * 12000);
  for (unsigned i = 0; i < values.size(); i++) {
    values[i] = i;
  }

  query.set_layout(TILEDB_ROW_MAJOR).set_data_buffer("a", values);
  query.submit();
}

void read_array() {
  Context ctx;
  Array array(ctx, array_name, TILEDB_READ);
  Query query(ctx, array);

  // Read a slice of 3,000 rows.
  Subarray subarray(ctx, array);
  subarray.add_range<uint32_t>(0, 1, 3000).add_range<uint32_t>(1, 1, 12000);

  std::vector<int32_t> values(3000 * 12000);
  query.set_subarray(subarray).set_data_buffer("a", values);

  // Enable the stats for the read query, and print the report.
  Stats::enable();
  query.submit();
  Stats::dump(stdout);

  // Check stats.
  std::string stats;
  Stats::dump(&stats);
  if (stats.find("\"Context.StorageManager.subSubarray.add_range\": 2") ==
      std::string::npos) {
    throw std::logic_error("Invalid counter for add_range");
  }

  // Ensure additional calls to Query::submit have no effect on the stats
  query.submit();
  query.submit();
  Stats::dump(&stats);
  if (stats.find("\"Context.StorageManager.subSubarray.add_range\": 2") ==
      std::string::npos) {
    throw std::logic_error("Invalid counter for add_range");
  }

  // Invoke add_range and check the stats again.
  // #TODO Update After removal of deprecated Query::add_range
  query.add_range(0, (uint32_t)0, (uint32_t)3);
  query.add_range(1, (uint32_t)0, (uint32_t)3);
  query.submit();
  Stats::dump(&stats);
  if (stats.find("\"Context.StorageManager.subSubarray.add_range\": 4") ==
      std::string::npos) {
    throw std::logic_error("Invalid counter for add_range");
  }
  Stats::disable();
}

int main() {
  // Create array with each row as a tile.
  create_array(1, 12000);
  write_array();
  read_array();
  return 0;
}
