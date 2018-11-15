/**
 * @file   writing_sparse_global.cc
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
 *   https://docs.tiledb.io/en/latest/tutorials/writing-sparse.html
 *
 * When run, this program will create a simple 2D sparse array, write some data
 * to it in global order, and read the data back.
 *
 */

#include <iostream>
#include <tiledb/tiledb>

using namespace tiledb;

// Name of array.
std::string array_name("global_order_sparse_array");

void create_array() {
  // Create a TileDB context.
  Context ctx;

  // If the array already exists on disk, return immediately.
  if (Object::object(ctx, array_name).type() == Object::Type::Array)
    return;

  // The array will be 4x4 with dimensions "rows" and "cols", with domain [1,4].
  Domain domain(ctx);
  domain.add_dimension(Dimension::create<int>(ctx, "rows", {{1, 4}}, 4))
      .add_dimension(Dimension::create<int>(ctx, "cols", {{1, 4}}, 4));

  // The array will be sparse.
  ArraySchema schema(ctx, TILEDB_SPARSE);
  schema.set_domain(domain).set_order({{TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR}});

  // Add a single attribute "a" so each (i,j) cell can store an integer.
  schema.add_attribute(Attribute::create<int>(ctx, "a"));

  // Create the (empty) array on disk.
  Array::create(array_name, schema);
}

void write_array() {
  // Open the array for writing and create the query.
  Context ctx;
  Array array(ctx, array_name, TILEDB_WRITE);
  Query query(ctx, array);

  // Set layout to global
  query.set_layout(TILEDB_GLOBAL_ORDER);

  // Submit first query
  std::vector<int> coords_1 = {1, 1, 2, 4};
  std::vector<int> data_1 = {1, 2};
  query.set_buffer("a", data_1).set_coordinates(coords_1);
  query.submit();

  // Submit second query
  std::vector<int> coords_2 = {2, 3};
  std::vector<int> data_2 = {3};
  query.set_buffer("a", data_2).set_coordinates(coords_2);
  query.submit();

  // Finalize - IMPORTANT!
  query.finalize();

  // Close the array
  array.close();
}

void read_array() {
  Context ctx;

  // Prepare the array for reading
  Array array(ctx, array_name, TILEDB_READ);

  // Read the whole array
  const std::vector<int> subarray = {1, 4, 1, 4};

  // Prepare the vector that will hold the result.
  // We take an upper bound on the result size, as we do not
  // know a priori how big it is (since the array is sparse)
  auto max_el = array.max_buffer_elements(subarray);
  std::vector<int> data(max_el["a"].second);
  std::vector<int> coords(max_el[TILEDB_COORDS].second);

  // Prepare the query
  Query query(ctx, array);
  query.set_subarray(subarray)
      .set_layout(TILEDB_ROW_MAJOR)
      .set_buffer("a", data)
      .set_coordinates(coords);

  // Submit the query and close the array.
  query.submit();
  array.close();

  // Print out the results.
  auto result_num = (int)query.result_buffer_elements()["a"].second;
  for (int r = 0; r < result_num; r++) {
    int i = coords[2 * r], j = coords[2 * r + 1];
    int a = data[r];
    std::cout << "Cell (" << i << ", " << j << ") has data " << a << "\n";
  }
}

int main() {
  create_array();
  write_array();
  read_array();
  return 0;
}