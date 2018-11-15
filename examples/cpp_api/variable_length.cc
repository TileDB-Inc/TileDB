/**
 * @file   variable_length.cc
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
 *   https://docs.tiledb.io/en/latest/tutorials/variable-length-attributes.html
 *
 * When run, this program will create a simple 2D dense array with two
 * variable-length attributes, write some data to it, and read a slice of the
 * data back on both attributes.
 *
 */

#include <iostream>
#include <tiledb/tiledb>

using namespace tiledb;

// Name of array.
std::string array_name("variable_length_array");

void create_array() {
  // Create a TileDB context
  Context ctx;

  // The array will be 4x4 with dimensions "rows" and "cols", with domain [1,4]
  Domain domain(ctx);
  domain.add_dimension(Dimension::create<int>(ctx, "rows", {{1, 4}}, 4))
      .add_dimension(Dimension::create<int>(ctx, "cols", {{1, 4}}, 4));

  // The array will be dense
  ArraySchema schema(ctx, TILEDB_DENSE);
  schema.set_domain(domain).set_order({{TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR}});

  // Add two variable-length attributes "a1" and "a2", the first storing
  // strings and the second storing a variable number of integers.
  schema.add_attribute(Attribute::create<std::string>(ctx, "a1"));
  schema.add_attribute(Attribute::create<std::vector<int>>(ctx, "a2"));

  // Create the (empty) array on disk.
  Array::create(array_name, schema);
}

void write_array() {
  Context ctx;

  // Prepare some data for the array
  std::string a1_data = "abbcccddeeefghhhijjjkklmnoop";
  std::vector<uint64_t> a1_off = {
      0, 1, 3, 6, 8, 11, 12, 13, 16, 17, 20, 22, 23, 24, 25, 27};
  std::vector<int> a2_data = {1, 1, 2, 2,  3,  4,  5,  6,  6,  7,  7,  8,  8,
                              8, 9, 9, 10, 11, 12, 12, 13, 14, 14, 14, 15, 16};
  std::vector<uint64_t> a2_el_off = {
      0, 2, 4, 5, 6, 7, 9, 11, 14, 16, 17, 18, 20, 21, 24, 25};
  std::vector<uint64_t> a2_off;
  for (auto e : a2_el_off)
    a2_off.push_back(e * sizeof(int));

  // Open the array for writing and create the query
  Array array(ctx, array_name, TILEDB_WRITE);
  Query query(ctx, array);
  query.set_layout(TILEDB_ROW_MAJOR)
      .set_buffer("a1", a1_off, a1_data)
      .set_buffer("a2", a2_off, a2_data);

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

  // Prepare the vectors that will hold the result
  auto max_el_map = array.max_buffer_elements(subarray);
  std::vector<uint64_t> a1_off(max_el_map["a1"].first);
  std::string a1_data;
  a1_data.resize(max_el_map["a1"].second);
  std::vector<uint64_t> a2_off(max_el_map["a2"].first);
  std::vector<int> a2_data(max_el_map["a2"].second);

  // Prepare and submit the query, and close the array
  Query query(ctx, array);
  query.set_subarray(subarray)
      .set_layout(TILEDB_ROW_MAJOR)
      .set_buffer("a1", a1_off, a1_data)
      .set_buffer("a2", a2_off, a2_data);
  query.submit();
  array.close();

  // Get the string sizes
  auto result_el_map = query.result_buffer_elements();
  auto result_el_a1_off = result_el_map["a1"].first;
  std::vector<uint64_t> a1_str_sizes;
  for (size_t i = 0; i < result_el_a1_off - 1; ++i)
    a1_str_sizes.push_back(a1_off[i + 1] - a1_off[i]);
  auto result_a1_data_size = result_el_map["a1"].second * sizeof(char);
  a1_str_sizes.push_back(result_a1_data_size - a1_off[result_el_a1_off - 1]);

  // Get the strings
  std::vector<std::string> a1_str;
  for (size_t i = 0; i < result_el_a1_off; ++i)
    a1_str.push_back(std::string(&a1_data[a1_off[i]], a1_str_sizes[i]));

  // Get the element offsets
  std::vector<uint64_t> a2_el_off;
  auto result_el_a2_off = result_el_map["a2"].first;
  for (size_t i = 0; i < result_el_a2_off; ++i)
    a2_el_off.push_back(a2_off[i] / sizeof(int));

  // Get the number of elements per cell value
  std::vector<uint64_t> a2_cell_el;
  for (size_t i = 0; i < result_el_a2_off - 1; ++i)
    a2_cell_el.push_back(a2_el_off[i + 1] - a2_el_off[i]);
  auto result_el_a2_data = result_el_map["a2"].second;
  a2_cell_el.push_back(result_el_a2_data - a2_el_off.back());

  // Print the results
  for (size_t i = 0; i < result_el_a1_off; ++i) {
    std::cout << "a1: " << a1_str[i] << ", a2: ";
    for (size_t j = 0; j < a2_cell_el[i]; ++j)
      std::cout << a2_data[a2_el_off[i] + j] << " ";
    std::cout << "\n";
  }
}

int main() {
  Context ctx;
  if (Object::object(ctx, array_name).type() != Object::Type::Array) {
    create_array();
    write_array();
  }

  read_array();

  return 0;
}