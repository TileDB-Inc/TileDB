/**
 * @file   quickstart.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2018 TileDB, Inc.
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
 * This is a part of the TileDB quickstart tutorial:
 *   https://docs.tiledb.io/en/latest/quickstart.html
 *   https://github.com/TileDB-Inc/TileDB/blob/dev/README.md
 *
 * When run, this program will create a sample 2D sparse array, write some data
 * to it, and read the data back.
 *
 */

#include <iostream>
#include <tiledb/tiledb>

using namespace tiledb;

// Name of array.
std::string array_name("my_array");

void create_array() {
  Context ctx;
  // If the array already exists on disk, return immediately.
  if (Object::object(ctx, array_name).type() == Object::Type::Array)
    return;

  // The array will be 2D with dimensions "x" and "y", with domain [0,4].
  Domain domain(ctx);
  domain.add_dimension(Dimension::create<int>(ctx, "x", {{0, 4}}, 2))
      .add_dimension(Dimension::create<int>(ctx, "y", {{0, 4}}, 2));

  // The array will be sparse.
  ArraySchema schema(ctx, TILEDB_SPARSE);
  schema.set_order({{TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR}})
      .set_capacity(4)
      .set_domain(domain);

  // Add a single attribute "a" so each (x,y) cell can store a character.
  schema.add_attribute(Attribute::create<char>(ctx, "a"));

  // Create the (empty) array on disk.
  Array::create(array_name, schema);
}

void write_array() {
  Context ctx;
  // Write some simple data to cells (0, 0), (1, 1) and (2, 3).
  std::vector<int> coords = {0, 0, 1, 1, 2, 3};
  std::vector<char> data = {'a', 'b', 'c'};

  // Open the array for writing and create the query.
  Array array(ctx, array_name, TILEDB_WRITE);
  Query query(ctx, array);
  // "Unordered" means we provide the coordinates for each cell being written.
  query.set_layout(TILEDB_UNORDERED)
      .set_buffer("a", data)
      .set_coordinates(coords);
  // Perform the write and close the array.
  query.submit();
  query.finalize();
  array.close();
}

void read_array() {
  Context ctx;
  Array array(ctx, array_name, TILEDB_READ);

  // Read using a spatial query with bounding box from (0, 0) to (3, 3).
  const std::vector<int> subarray = {0, 3, 0, 3};
  // Figure out how big our buffers need to be to hold the query result.
  auto max_sizes = array.max_buffer_elements(subarray);
  std::vector<char> data(max_sizes["a"].second);
  std::vector<int> coords(max_sizes[TILEDB_COORDS].second);

  Query query(ctx, array);
  // "Global order" read means TileDB won't sort the cells before returning.
  query.set_subarray(subarray)
      .set_layout(TILEDB_GLOBAL_ORDER)
      .set_buffer("a", data)
      .set_coordinates(coords);
  // Submit the query and close the array.
  query.submit();
  query.finalize();
  array.close();

  // Print out the results.
  int num_cells_read = query.result_buffer_elements()["a"].second;
  for (int i = 0; i < num_cells_read; i++) {
    int x = coords[2 * i], y = coords[2 * i + 1];
    char a = data[i];
    std::cout << "Cell (" << x << "," << y << ") has data '" << a << "'"
              << std::endl;
  }
}

int main() {
  create_array();
  write_array();
  read_array();
  return 0;
}