/**
 * @file   reading_dense_layouts.cc
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
 * This is a part of the TileDB tutorial:
 *   https://docs.tiledb.io/en/latest/tutorials/reading.html
 *
 * When run, this program will create a simple 2D dense array, write some data
 * to it, and read a slice of the data back in the layout of the user's choice
 * (passed as an argument to the program: "row", "col", or "global").
 *
 */

#include <iostream>
#include <tiledb/tiledb>

using namespace tiledb;

// Name of array.
std::string array_name("reading_dense_layouts");

void create_array() {
  // Create a TileDB context.
  Context ctx;

  // Create domain.
  Domain domain(ctx);
  domain.add_dimension(Dimension::create<int>(ctx, "rows", {{1, 4}}, 2))
      .add_dimension(Dimension::create<int>(ctx, "cols", {{1, 4}}, 2));

  // The array will be dense.
  ArraySchema schema(ctx, TILEDB_DENSE);
  schema.set_domain(domain).set_order({{TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR}});

  // Add a single attribute "a" so each (i,j) cell can store an integer.
  schema.add_attribute(Attribute::create<int>(ctx, "a"));

  // Create the (empty) array on disk.
  Array::create(array_name, schema);
}

void write_array() {
  Context ctx;

  // Prepare some data for the array
  std::vector<int> data = {
      1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};

  // Open the array for writing and create the query.
  Array array(ctx, array_name, TILEDB_WRITE);
  Query query(ctx, array);
  query.set_layout(TILEDB_GLOBAL_ORDER).set_buffer("a", data);

  // Perform the write and close the array.
  query.submit();
  query.finalize();
  array.close();
}

void read_array(tiledb_layout_t layout) {
  Context ctx;

  // Prepare the array for reading
  Array array(ctx, array_name, TILEDB_READ);

  // Print non-empty domain
  auto non_empty_domain = array.non_empty_domain<int>();
  std::cout << "Non-empty domain: ";
  std::cout << "[" << non_empty_domain[0].second.first << ","
            << non_empty_domain[0].second.second << "], ["
            << non_empty_domain[1].second.first << ","
            << non_empty_domain[1].second.second << "]\n";

  // Slice only rows 1, 2 and cols 2, 3, 4
  const std::vector<int> subarray = {1, 2, 2, 4};

  // Prepare the vectors that will hold the results
  std::vector<int> data(6);
  std::vector<int> coords(12);

  // Prepare the query
  Query query(ctx, array);
  query.set_subarray(subarray)
      .set_layout(layout)
      .set_buffer("a", data)
      .set_coordinates(coords);

  // Submit the query and close the array.
  query.submit();
  array.close();

  // Print out the results.
  for (int r = 0; r < 6; r++) {
    int i = coords[2 * r], j = coords[2 * r + 1];
    int a = data[r];
    std::cout << "Cell (" << i << ", " << j << ") has data " << a << "\n";
  }
}

int main(int argc, char* argv[]) {
  Context ctx;

  // Create and write the array only if it does not exist
  if (Object::object(ctx, array_name).type() != Object::Type::Array) {
    create_array();
    write_array();
  }

  // Choose a layout (default is row-major)
  tiledb_layout_t layout = TILEDB_ROW_MAJOR;
  if (argc > 1) {
    if (argv[1] == std::string("row"))
      layout = TILEDB_ROW_MAJOR;
    else if (argv[1] == std::string("col"))
      layout = TILEDB_COL_MAJOR;
    else if (argv[1] == std::string("global"))
      layout = TILEDB_GLOBAL_ORDER;
  }

  read_array(layout);
  return 0;
}