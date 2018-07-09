/**
 * @file   reading_incomplete.cc
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
 *   https://docs.tiledb.io/en/latest/tutorials/reading.html
 *
 * This example demonstrates the concept of incomplete read queries
 * for a sparse array with two attributes.
 */

#include <iostream>
#include <tiledb/tiledb>

using namespace tiledb;

// Name of array.
std::string array_name("reading_incomplete");

void create_array() {
  // Create a TileDB context.
  Context ctx;

  // Create domain.
  Domain domain(ctx);
  domain.add_dimension(Dimension::create<int>(ctx, "rows", {{1, 4}}, 2))
      .add_dimension(Dimension::create<int>(ctx, "cols", {{1, 4}}, 2));

  // The array will be sparse.
  ArraySchema schema(ctx, TILEDB_SPARSE);
  schema.set_domain(domain).set_order({{TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR}});

  // Add two attributes "a1" and "a2", the first integer and the second string
  schema.add_attribute(Attribute::create<int>(ctx, "a1"));
  schema.add_attribute(Attribute::create<std::string>(ctx, "a2"));

  // Create the (empty) array on disk.
  Array::create(array_name, schema);
}

void write_array() {
  Context ctx;

  // Prepare some data for the array
  std::vector<int> coords = {1, 1, 2, 1, 2, 2};
  std::vector<int> a1_data = {1, 2, 3};
  std::string a2_data = "abbccc";
  std::vector<uint64_t> a2_off = {0, 1, 3};

  // Open the array for writing and create the query.
  Array array(ctx, array_name, TILEDB_WRITE);
  Query query(ctx, array);
  query.set_layout(TILEDB_GLOBAL_ORDER)
      .set_buffer("a1", a1_data)
      .set_buffer("a2", a2_off, a2_data)
      .set_coordinates(coords);

  // Perform the write and close the array.
  query.submit();
  query.finalize();
  array.close();
}

void reallocate_buffers(
    std::vector<int>* coords,
    std::vector<int>* a1_data,
    std::vector<uint64_t>* a2_off,
    std::string* a2_data) {
  std::cout << "Reallocating...\n";

  // Note: this is a naive reallocation - you should handle
  // reallocation properly depending on your application
  coords->resize(2 * coords->size());
  a1_data->resize(2 * a1_data->size());
  a2_off->resize(2 * a2_off->size());
  a2_data->resize(2 * a2_data->size());
}

void print_results(
    const std::vector<int>& coords,
    const std::vector<int>& a1_data,
    const std::vector<uint64_t>& a2_off,
    const std::string& a2_data,
    const std::unordered_map<std::string, std::pair<uint64_t, uint64_t>>&
        result_el_map) {
  std::cout << "Printing results...\n";

  // Get the string sizes
  auto result_el_a2_off = result_el_map.find("a2")->second.first;
  std::vector<uint64_t> a2_str_sizes;
  for (size_t i = 0; i < result_el_a2_off - 1; ++i)
    a2_str_sizes.push_back(a2_off[i + 1] - a2_off[i]);
  auto result_a2_data_size =
      result_el_map.find("a2")->second.second * sizeof(char);
  a2_str_sizes.push_back(result_a2_data_size - a2_off[result_el_a2_off - 1]);

  // Get the strings
  std::vector<std::string> a2_str;
  for (size_t i = 0; i < result_el_a2_off; ++i)
    a2_str.push_back(std::string(&a2_data[a2_off[i]], a2_str_sizes[i]));

  // Print the results
  auto result_num = result_el_a2_off;  // For clarity
  for (size_t r = 0; r < result_num; ++r) {
    int i = coords[2 * r], j = coords[2 * r + 1];
    int a1 = a1_data[r];
    std::cout << "Cell (" << i << ", " << j << "), a1: " << a1
              << ", a2: " << a2_str[r] << "\n";
  }
}

void read_array() {
  Context ctx;

  // Prepare the array for reading
  Array array(ctx, array_name, TILEDB_READ);

  // Read entire array
  const std::vector<int> subarray = {1, 4, 1, 4};

  // Prepare buffers such that the results **cannot** fit
  std::vector<int> coords(2);
  std::vector<int> a1_data(1);
  std::vector<uint64_t> a2_off(1);
  std::string a2_data;
  a2_data.resize(1);

  // Prepare the query
  Query query(ctx, array);
  query.set_subarray(subarray)
      .set_layout(TILEDB_ROW_MAJOR)
      .set_buffer("a1", a1_data)
      .set_buffer("a2", a2_off, a2_data)
      .set_coordinates(coords);

  // Create a loop
  Query::Status status;
  do {
    // Submit query and get status
    query.submit();
    status = query.query_status();

    // If any results were retrieved, parse and print them
    auto result_num = (int)query.result_buffer_elements()["a1"].second;
    if (status == Query::Status::INCOMPLETE &&
        result_num == 0) {  // VERY IMPORTANT!!
      reallocate_buffers(&coords, &a1_data, &a2_off, &a2_data);
      query.set_buffer("a1", a1_data)
          .set_buffer("a2", a2_off, a2_data)
          .set_coordinates(coords);
    } else {
      print_results(
          coords, a1_data, a2_off, a2_data, query.result_buffer_elements());
    }
  } while (status == Query::Status::INCOMPLETE);

  // Handle error
  if (status == Query::Status::FAILED) {
    std::cout << "Error in reading\n";
    return;
  }

  // Close the array
  array.close();
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