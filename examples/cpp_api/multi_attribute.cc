/**
 * @file   multi_attribute.cc
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
 * This is a part of the TileDB "Multi-attribute Arrays" tutorial:
 *   https://docs.tiledb.io/en/latest/tutorials/multi-attribute-arrays.html
 *
 * When run, this program will create a simple 2D dense array with two
 * attributes, write some data to it, and read a slice of the data back on
 * (i) both attributes, and (ii) subselecting on only one of the attributes.
 *
 */

#include <iostream>
#include <tiledb/tiledb>

using namespace tiledb;

// Name of array.
std::string array_name("multi_attribute_array");

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

  // The array will be dense.
  ArraySchema schema(ctx, TILEDB_DENSE);
  schema.set_domain(domain).set_order({{TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR}});

  // Add two attributes "a1" and "a2", so each (i,j) cell can store
  // a character on "a1" and a vector of two floats on "a2".
  schema.add_attribute(Attribute::create<char>(ctx, "a1"));
  schema.add_attribute(Attribute::create<float[2]>(ctx, "a2"));

  // Create the (empty) array on disk.
  Array::create(array_name, schema);
}

void write_array() {
  Context ctx;

  // Prepare some data for the array
  std::vector<char> a1 = {'a',
                          'b',
                          'c',
                          'd',
                          'e',
                          'f',
                          'g',
                          'h',
                          'i',
                          'j',
                          'k',
                          'l',
                          'm',
                          'n',
                          'o',
                          'p'};
  std::vector<float> a2 = {1.1f,  1.2f,  2.1f,  2.2f,  3.1f,  3.2f,  4.1f,
                           4.2f,  5.1f,  5.2f,  6.1f,  6.2f,  7.1f,  7.2f,
                           8.1f,  8.2f,  9.1f,  9.2f,  10.1f, 10.2f, 11.1f,
                           11.2f, 12.1f, 12.2f, 13.1f, 13.2f, 14.1f, 14.2f,
                           15.1f, 15.2f, 16.1f, 16.2f};

  // Open the array for writing and create the query.
  Array array(ctx, array_name, TILEDB_WRITE);
  Query query(ctx, array);
  query.set_layout(TILEDB_ROW_MAJOR).set_buffer("a1", a1).set_buffer("a2", a2);

  // Perform the write and close the array.
  query.submit();
  array.close();
}

void read_array() {
  Context ctx;

  // Prepare the array for reading
  Array array(ctx, array_name, TILEDB_READ);

  // Slice only rows 1, 2 and cols 2, 3, 4
  const std::vector<int> subarray = {1, 2, 2, 4};

  // Prepare the vector that will hold the result
  // (of size 6 elements for "a1" and 12 elements for "a2" since
  // it stores two floats per cell)
  std::vector<char> data_a1(6);
  std::vector<float> data_a2(12);

  // Prepare the query
  Query query(ctx, array);
  query.set_subarray(subarray)
      .set_layout(TILEDB_ROW_MAJOR)
      .set_buffer("a1", data_a1)
      .set_buffer("a2", data_a2);

  // Submit the query and close the array.
  query.submit();
  array.close();

  // Print out the results.
  std::cout << "Reading both attributes a1 and a2:\n";
  for (int i = 0; i < 6; ++i)
    std::cout << "a1: " << data_a1[i] << ", a2: (" << data_a2[2 * i] << ","
              << data_a2[2 * i + 1] << ")\n";
  std::cout << "\n";
}

void read_array_subselect() {
  Context ctx;

  // Prepare the array for reading
  Array array(ctx, array_name, TILEDB_READ);

  // Slice only rows 1, 2 and cols 2, 3, 4
  const std::vector<int> subarray = {1, 2, 2, 4};

  // Prepare the vector that will hold the result
  // (of size 6 elements for "a1")
  std::vector<char> data_a1(6);

  // Prepare the query - subselect over "a1" only
  Query query(ctx, array);
  query.set_subarray(subarray)
      .set_layout(TILEDB_ROW_MAJOR)
      .set_buffer("a1", data_a1);

  // Submit the query and close the array.
  query.submit();
  array.close();

  // Print out the results.
  std::cout << "Subselecting on attribute a1:\n";
  for (int i = 0; i < 6; ++i)
    std::cout << "a1: " << data_a1[i] << "\n";
}

int main() {
  create_array();
  write_array();
  read_array();
  read_array_subselect();
  return 0;
}