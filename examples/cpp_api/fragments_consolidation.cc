/**
 * @file   fragments_consolidation.cc
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
 *   https://docs.tiledb.io/en/latest/tutorials/fragments-consolidation.html
 *
 * When run, this program will create a simple 2D dense array, write some data
 * with three queries (creating three fragments), optionally consolidate
 * and read the entire array data back.
 */

#include <iostream>
#include <tiledb/tiledb>

using namespace tiledb;

// Name of array.
std::string array_name("fragments_consolidation_array");

void create_array() {
  // Create a TileDB context.
  Context ctx;

  // The array will be 4x4 with dimensions "rows" and "cols", with domain [1,4]
  // and space tiles 2x2
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

void write_array_1() {
  Context ctx;

  // Prepare some data for the array
  std::vector<int> data = {1, 2, 3, 4, 5, 6, 7, 8};
  std::vector<int> subarray = {1, 2, 1, 4};

  // Open the array for writing and create the query.
  Array array(ctx, array_name, TILEDB_WRITE);
  Query query(ctx, array);
  query.set_layout(TILEDB_ROW_MAJOR)
      .set_buffer("a", data)
      .set_subarray(subarray);

  // Perform the write and close the array.
  query.submit();
  array.close();
}

void write_array_2() {
  Context ctx;

  // Prepare some data for the array
  std::vector<int> data = {101, 102, 103, 104};
  std::vector<int> subarray = {2, 3, 2, 3};

  // Open the array for writing and create the query.
  Array array(ctx, array_name, TILEDB_WRITE);
  Query query(ctx, array);
  query.set_layout(TILEDB_ROW_MAJOR)
      .set_buffer("a", data)
      .set_subarray(subarray);

  // Perform the write and close the array.
  query.submit();
  array.close();
}

void write_array_3() {
  Context ctx;

  // Prepare some data for the array
  std::vector<int> data = {201, 202};
  std::vector<int> coords = {1, 1, 3, 4};

  // Open the array for writing and create the query.
  Array array(ctx, array_name, TILEDB_WRITE);
  Query query(ctx, array);
  query.set_layout(TILEDB_UNORDERED)
      .set_buffer("a", data)
      .set_coordinates(coords);

  // Perform the write and close the array.
  query.submit();
  array.close();
}

void read_array() {
  Context ctx;

  // Prepare the array for reading
  Array array(ctx, array_name, TILEDB_READ);

  // Read the entire array
  const std::vector<int> subarray = {1, 4, 1, 4};

  // Prepare the vector that will hold the result
  std::vector<int> data(16);
  std::vector<int> coords(32);

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
  for (int r = 0; r < 16; r++) {
    int i = coords[2 * r], j = coords[2 * r + 1];
    int a = data[r];
    std::cout << "Cell (" << i << ", " << j << ") has data " << a << "\n";
  }
}

int main(int argc, char* argv[]) {
  Context ctx;

  // Create and write array only if it does not exist
  if (Object::object(ctx, array_name).type() == Object::Type::Invalid) {
    create_array();
    write_array_1();
    write_array_2();
    write_array_3();
  }

  // Optionally consolidate
  if (argc > 1 && argv[1] == std::string("consolidate"))
    Array::consolidate(ctx, array_name);

  read_array();

  return 0;
}