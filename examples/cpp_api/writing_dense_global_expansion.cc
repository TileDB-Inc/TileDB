/**
 * @file   writing_dense_global_expansion.cc
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
 *   https://docs.tiledb.io/en/latest/tutorials/writing-dense.html
 *
 * When run, this program will create a simple 2D dense array, write some data
 * to it in global layout, and read the entire array data back. Here we show
 * how to handle the case where some tile extent does not divide the respective
 * dimension domain (and, hence, internal domain expansion occurs).
 */

#include <iostream>
#include <tiledb/tiledb>

using namespace tiledb;

// Name of array.
std::string array_name("writing_dense_global_expansion_array");

void create_array() {
  // Create a TileDB context.
  Context ctx;

  // The array will be 4x3 with dimensions "rows" and "cols",
  // and space tiles 2x2
  Domain domain(ctx);
  domain.add_dimension(Dimension::create<int>(ctx, "rows", {{1, 4}}, 2))
      .add_dimension(Dimension::create<int>(ctx, "cols", {{1, 3}}, 2));

  // The array will be dense.
  ArraySchema schema(ctx, TILEDB_DENSE);
  schema.set_domain(domain).set_order({{TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR}});

  // Add a single attribute "a" so each (i,j) cell can store an integer.
  schema.add_attribute(Attribute::create<int>(ctx, "a"));

  // Create the (empty) array on disk.
  Array::create(array_name, schema);
}

void write_array_global() {
  std::vector<int> subarray = {1, 4, 1, 2};
  Context ctx;
  Array array(ctx, array_name, TILEDB_WRITE);
  Query query(ctx, array);
  std::vector<int> data = {1, 2, 3, 4, 5, 6, 7, 8};
  query.set_layout(TILEDB_GLOBAL_ORDER)
      .set_buffer("a", data)
      .set_subarray(subarray);
  query.submit();
  query.finalize();
  array.close();
}

void write_array_row_major() {
  std::vector<int> subarray = {1, 4, 3, 3};
  Context ctx;
  Array array(ctx, array_name, TILEDB_WRITE);
  Query query(ctx, array);
  std::vector<int> data = {9, 10, 11, 12};
  query.set_layout(TILEDB_ROW_MAJOR)
      .set_buffer("a", data)
      .set_subarray(subarray);
  query.submit();
  array.close();
}

void read_array() {
  Context ctx;

  // Prepare the array for reading
  Array array(ctx, array_name, TILEDB_READ);

  // Read the entire array
  const std::vector<int> subarray = {1, 4, 1, 3};

  // Prepare the vector that will hold the result (of size 12 elements)
  std::vector<int> data(12);

  // Prepare the query
  Query query(ctx, array);
  query.set_subarray(subarray)
      .set_layout(TILEDB_ROW_MAJOR)
      .set_buffer("a", data);

  // Submit the query and close the array.
  query.submit();
  array.close();

  // Print out the results.
  for (auto d : data)
    std::cout << d << "\n";
}

int main() {
  Context ctx;
  if (Object::object(ctx, array_name).type() != Object::Type::Array) {
    create_array();
    write_array_global();
    write_array_row_major();
  }

  read_array();

  return 0;
}