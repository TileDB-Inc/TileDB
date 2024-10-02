/**
 * @file   nullable_attribute.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2020-2022 TileDB, Inc.
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
 * When run, this program will create a simple 2D dense array with one fixed
 * nullable attribute and one var-sized nullable attribute, write some data
 * to it, and read the data back on both attributes.
 */

#include <iostream>
#include <tiledb/tiledb>

using namespace tiledb;

// Name of array.
std::string array_name("nullable_attributes_array");

void create_array() {
  // Create a TileDB context
  Context ctx;

  // The array will be 2x2 with dimensions "rows" and "cols", with domain [1,2]
  Domain domain(ctx);
  domain.add_dimension(Dimension::create<int>(ctx, "rows", {{1, 2}}, 2))
      .add_dimension(Dimension::create<int>(ctx, "cols", {{1, 2}}, 2));

  // The array will be dense
  ArraySchema schema(ctx, TILEDB_DENSE);
  schema.set_domain(domain).set_order({{TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR}});

  // Create three attributes "a1", "a2" and "a3", the first fixed, the second
  // variable-sized and the last one a variable-sized UTF8 string
  Attribute a1 = Attribute::create<int>(ctx, "a1");
  Attribute a2 = Attribute::create<std::vector<int>>(ctx, "a2");
  auto a3 = Attribute(ctx, "a3", TILEDB_STRING_UTF8);
  a3.set_cell_val_num(TILEDB_VAR_NUM);

  // Set all attributes as nullable
  a1.set_nullable(true);
  a2.set_nullable(true);
  a3.set_nullable(true);

  schema.add_attribute(a1);
  schema.add_attribute(a2);
  schema.add_attribute(a3);

  // Create the (empty) array on disk.
  Array::create(ctx, array_name, schema);
}

void write_array() {
  Context ctx;

  // Prepare some data for the array
  std::vector<int> a1_data = {100, 200, 300, 400};
  std::vector<int> a2_data = {10, 10, 20, 30, 30, 30, 40, 40};
  std::vector<uint64_t> a2_el_off = {0, 2, 3, 6};
  std::vector<uint64_t> a2_off;
  for (auto e : a2_el_off)
    a2_off.push_back(e * sizeof(int));

  const char a3_data_char[] = "abcdewxyz";
  std::vector<char> a3_data(a3_data_char, a3_data_char + 9);
  std::vector<uint64_t> a3_el_off = {0, 3, 4, 5};
  std::vector<uint64_t> a3_off;
  for (auto e : a3_el_off)
    a3_off.push_back(e * sizeof(char));

  // Open the array for writing and create the query
  Array array(ctx, array_name, TILEDB_WRITE);
  Query query(ctx, array);
  query.set_layout(TILEDB_ROW_MAJOR);

  // Specify the validity buffer for each attribute
  std::vector<uint8_t> a1_validity_buf = {1, 0, 0, 1};
  std::vector<uint8_t> a2_validity_buf = {0, 1, 1, 0};
  std::vector<uint8_t> a3_validity_buf = {1, 0, 0, 1};

  // Set the query buffers specifying the validity for each data
  query.set_data_buffer("a1", a1_data)
      .set_validity_buffer("a1", a1_validity_buf)
      .set_data_buffer("a2", a2_data)
      .set_offsets_buffer("a2", a2_off)
      .set_validity_buffer("a2", a2_validity_buf)
      .set_data_buffer("a3", a3_data)
      .set_offsets_buffer("a3", a3_off)
      .set_validity_buffer("a3", a3_validity_buf);

  // Perform the write and close the array.
  query.submit();
  array.close();
}

void read_array() {
  Context ctx;

  // Prepare the array for reading
  Array array(ctx, array_name, TILEDB_READ);

  // Prepare the vectors that will hold the results
  std::vector<int> a1_data(4);
  std::vector<uint8_t> a1_validity_buf(a1_data.size());

  std::vector<int> a2_data(8);
  std::vector<uint64_t> a2_off(4);
  std::vector<uint8_t> a2_validity_buf(a2_off.size());

  std::vector<char> a3_data(1000);
  std::vector<uint64_t> a3_off(10);
  std::vector<uint8_t> a3_validity_buf(a3_off.size());

  // Prepare and submit the query, and close the array
  Query query(ctx, array);
  query.set_layout(TILEDB_ROW_MAJOR);

  // Read the full array
  Subarray subarray_full(ctx, array);
  subarray_full.add_range(0, 1, 2).add_range(1, 1, 2);
  query.set_subarray(subarray_full);

  // Set the query buffers specifying the validity for each data
  query.set_data_buffer("a1", a1_data)
      .set_validity_buffer("a1", a1_validity_buf)
      .set_data_buffer("a2", a2_data)
      .set_offsets_buffer("a2", a2_off)
      .set_validity_buffer("a2", a2_validity_buf)
      .set_data_buffer("a3", a3_data)
      .set_offsets_buffer("a3", a3_off)
      .set_validity_buffer("a3", a3_validity_buf);
  query.submit();

  if (query.query_status() == tiledb::Query::Status::INCOMPLETE) {
    std::cerr << "** Query did not complete! **" << std::endl;
  }

  auto result_elements = query.result_buffer_elements();
  array.close();

  // Unpack a3 result to a vector of strings
  std::vector<std::string> a3_results;
  auto a3_result_elements = result_elements["a3"];
  uint64_t a3_offsets_num = a3_result_elements.first;
  uint64_t a3_data_num = a3_result_elements.second;

  uint64_t start = 0;
  uint64_t end = 0;

  for (size_t i = 0; i < a3_offsets_num; i++) {
    start = a3_off[i];
    end = (i == a3_offsets_num - 1) ? a3_data_num : a3_off[i + 1];
    a3_results.push_back(
        std::string(a3_data.data() + start, a3_data.data() + end));
  }

  // Print out the data we read for each nullable atttribute
  unsigned long i = 0;
  std::cout << "a1: " << std::endl;
  for (i = 0; i < 4; ++i) {
    std::cout << (a1_validity_buf[i] > 0 ? std::to_string(a1_data[i]) : "NULL");
    std::cout << " ";
  }
  std::cout << std::endl;

  std::cout << "a2: " << std::endl;
  for (i = 0; i < 4; ++i) {
    if (a2_validity_buf[i] > 0) {
      std::cout << "{ ";
      std::cout << std::to_string(a2_data[i * 2]);
      std::cout << ", ";
      std::cout << std::to_string(a2_data[i * 2 + 1]);
      std::cout << " }";
    } else {
      std::cout << "{ NULL }";
    }
    std::cout << " ";
  }

  std::cout << std::endl << "a3: " << std::endl;
  for (i = 0; i < 4; ++i) {
    if (a3_validity_buf[i] > 0)
      std::cout << "  " << a3_results[i] << std::endl;
    else
      std::cout << "  "
                << "NULL" << std::endl;
  }

  std::cout << std::endl;
}

int main() {
  Context ctx;
  if (Object::object(ctx, array_name).type() == Object::Type::Array) {
    tiledb::Object::remove(ctx, array_name);
  }
  create_array();
  write_array();
  read_array();

  return 0;
}
