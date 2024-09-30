/**
 * @file   query_condition_dense.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022 TileDB, Inc.
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
 * When run, this program will create a dense 1D array with 4 attributes. It
 * will then run queries with different query conditions to demonstrate how
 * query conditions can be used to filter out results in TileDB arrays.
 */

#include <iostream>
#include <optional>
#include <tiledb/tiledb>
#include <vector>

using namespace tiledb;

// Name of array.
std::string array_name("query_condition_dense_array");
int32_t num_elems = 10;

// Fill values.
int c_fill_value = -1;
float d_fill_value = 0.0;

/**
 * @brief Function to print the values of all the attributes for one
 * index of this array.
 *
 * @param a Attribute a's value.
 * @param b Attribute b's value.
 * @param c Attribute c's value.
 * @param d Attribute d's value.
 */
void print_elem(std::optional<int> a, std::string b, int32_t c, float d) {
  if (a == std::nullopt) {
    std::cout << "{null, " << b << ", " << c << ", " << d << "}\n";
  } else {
    std::cout << "{" << a.value() << ", " << b << ", " << c << ", " << d
              << "}\n";
  }
}

/**
 * @brief Function to create the TileDB array used in this example.
 * The array will be 1D with size 1 with dimension "index".
 * The bounds on the index will be 0 through 9, inclusive.
 *
 * The array has four attributes. The four attributes are
 *  - "a" (type int)
 *  - "b" (type std::string)
 *  - "c" (type int32_t)
 *  - "d" (type float)
 *
 * @param ctx The context.
 */
void create_array(Context& ctx) {
  // Creating the domain and the dimensions.
  Domain domain(ctx);
  domain.add_dimension(
      Dimension::create<int32_t>(ctx, "index", {{0, num_elems - 1}}));

  // The array will be dense.
  ArraySchema schema(ctx, TILEDB_DENSE);
  schema.set_domain(domain).set_order({{TILEDB_ROW_MAJOR}});

  // Adding the attributes of the array to the array schema.
  Attribute a = Attribute::create<int>(ctx, "a").set_nullable(true);
  Attribute b = Attribute::create<std::string>(ctx, "b");
  Attribute c = Attribute::create<int32_t>(ctx, "c").set_fill_value(
      &c_fill_value, sizeof(int32_t));
  Attribute d = Attribute::create<float>(ctx, "d").set_fill_value(
      &d_fill_value, sizeof(float));
  schema.add_attribute(a).add_attribute(b).add_attribute(c).add_attribute(d);

  // Create the (empty) array.
  Array::create(ctx, array_name, schema);
}

/**
 * @brief Execute a write on array query_condition_dense array
 * which then stores the following data in the array. The table
 * is organized by dimension/attribute.
 *
 * index |  a   |   b   | c |  d
 * -------------------------------
 *   0   | null | alice | 0 | 4.1
 *   1   | 2    | bob   | 0 | 3.4
 *   2   | null | craig | 0 | 5.6
 *   3   | 4    | dave  | 0 | 3.7
 *   4   | null | erin  | 0 | 2.3
 *   5   | 6    | frank | 0 | 1.7
 *   6   | null | grace | 1 | 3.8
 *   7   | 8    | heidi | 2 | 4.9
 *   8   | null | ivan  | 3 | 3.2
 *   9   | 10   | judy  | 4 | 3.1
 *
 * @param ctx The context.
 */
void write_array(Context& ctx) {
  // Create data buffers that store the values to be written in.
  std::vector<int32_t> a_data = {0, 2, 0, 4, 0, 6, 0, 8, 0, 10};
  std::vector<uint8_t> a_data_validity = {0, 1, 0, 1, 0, 1, 0, 1, 0, 1};
  std::vector<std::string> b_strs = {
      "alice",
      "bob",
      "craig",
      "dave",
      "erin",
      "frank",
      "grace",
      "heidi",
      "ivan",
      "judy"};
  std::string b_data = "";
  std::vector<uint64_t> b_data_offsets;
  for (const auto& elem : b_strs) {
    b_data_offsets.push_back(b_data.size());
    b_data += elem;
  }
  std::vector<int32_t> c_data = {0, 0, 0, 0, 0, 0, 1, 2, 3, 4};
  std::vector<float> d_data = {
      4.1, 3.4, 5.6, 3.7, 2.3, 1.7, 3.8, 4.9, 3.2, 3.1};

  // Execute the write query.
  Array array_w(ctx, array_name, TILEDB_WRITE);
  Query query_w(ctx, array_w);
  query_w.set_layout(TILEDB_ROW_MAJOR)
      .set_data_buffer("a", a_data)
      .set_validity_buffer("a", a_data_validity)
      .set_data_buffer("b", b_data)
      .set_offsets_buffer("b", b_data_offsets)
      .set_data_buffer("c", c_data)
      .set_data_buffer("d", d_data);

  query_w.submit();
  query_w.finalize();
  array_w.close();
}

/**
 * @brief Executes the read query for the array created in write_array.
 *
 * @param ctx The context.
 * @param qc The query condition to execute the query with.
 */
void read_array_with_qc(Context& ctx, std::optional<QueryCondition> qc) {
  // Create data buffers to read the values into.
  std::vector<int> a_data(num_elems);
  std::vector<uint8_t> a_data_validity(num_elems);

  // We initialize the string b_data to contain 44 characters because
  // that is the combined size of all strings in attribute b.
  std::string b_data;
  b_data.resize(44);

  std::vector<uint64_t> b_data_offsets(num_elems);
  std::vector<int32_t> c_data(num_elems);
  std::vector<float> d_data(num_elems);

  // Execute the read query.
  Array array(ctx, array_name, TILEDB_READ);
  Subarray subarray(ctx, array);
  subarray.add_range("index", 0, num_elems - 1);
  Query query(ctx, array);
  query.set_layout(TILEDB_ROW_MAJOR)
      .set_data_buffer("a", a_data)
      .set_validity_buffer("a", a_data_validity)
      .set_data_buffer("b", b_data)
      .set_offsets_buffer("b", b_data_offsets)
      .set_data_buffer("c", c_data)
      .set_data_buffer("d", d_data)
      .set_subarray(subarray);
  if (qc) {
    query.set_condition(qc.value());
  }

  query.submit();

  // Collect the results of the read query. The number of elements
  // the filtered array contains is in num_elements_result.
  // The length of the filtered substring of all the data is in
  // b_data, and all the offsets for filtered individual elements
  // are in  b_data_offsets.
  auto table = query.result_buffer_elements_nullable();
  size_t num_elements_result = std::get<1>(table["c"]);
  uint64_t b_str_length = std::get<1>(table["b"]);
  b_data_offsets.push_back(b_str_length);

  // Here we print all the elements that are returned by the query.
  for (size_t i = 0; i < num_elements_result; ++i) {
    // We can filter results based on whether the fill value is used in the
    // result buffers.
    if (c_data[i] != c_fill_value) {
      // We pass in nullopt if the data is invalid, per the validity buffer.
      std::optional<int> a_val;
      if (a_data_validity[i]) {
        a_val = a_data[i];
      } else {
        a_val = std::nullopt;
      }
      print_elem(
          a_val,
          b_data.substr(
              b_data_offsets[i], b_data_offsets[i + 1] - b_data_offsets[i]),
          c_data[i],
          d_data[i]);
    }
  }

  query.finalize();
  array.close();
}

int main() {
  // Create the context.
  Context ctx;
  VFS vfs(ctx);
  if (vfs.is_dir(array_name))
    vfs.remove_dir(array_name);

  // Create and write data to the array.
  create_array(ctx);
  write_array(ctx);

  // Printing the entire array.
  std::cout << "Printing the entire array...\n";
  read_array_with_qc(ctx, std::nullopt);
  std::cout << "\n";

  // Execute a read query with query condition `a = null`.
  std::cout << "Running read query with query condition `a = null`...\n";
  QueryCondition qc(ctx);
  qc.init("a", nullptr, 0, TILEDB_EQ);
  read_array_with_qc(ctx, qc);
  std::cout << "\n";

  // Execute a read query with query condition `b < "eve"`.
  std::cout << "Running read query with query condition `b < \"eve\"`...\n";
  QueryCondition qc1(ctx);
  qc1.init("b", "eve", TILEDB_LT);
  read_array_with_qc(ctx, qc1);
  std::cout << "\n";

  // Execute a read query with query condition `c >= 1`.
  std::cout << "Running read query with query condition `c >= 1`...\n";
  QueryCondition qc2(ctx);
  int val = 1;
  qc2.init("c", &val, sizeof(int), TILEDB_GE);
  read_array_with_qc(ctx, qc2);
  std::cout << "\n";

  // Execute a read query with query condition `3.0f <= d AND d <= 4.0f`.
  std::cout << "Running read query with query condition `3.0f <= d AND d <= "
               "4.0f`...\n";
  QueryCondition qc3(ctx);
  float arr[] = {3.0f, 4.0f};
  qc3.init("d", &arr[0], sizeof(float), TILEDB_GE);
  QueryCondition qc4(ctx);
  qc4.init("d", &arr[1], sizeof(float), TILEDB_LE);
  QueryCondition qc5 = qc3.combine(qc4, TILEDB_AND);
  read_array_with_qc(ctx, qc5);
  std::cout << "\n";

  // Execute a read query with query condition `3.0f <= d AND d <= 4.0f AND a !=
  // null AND b < \"eve\"`.
  std::cout << "Running read query with query condition `3.0f <= d AND d <= "
               "4.0f AND a != null AND b < \"eve\"`...\n";
  QueryCondition qc6(ctx);
  qc6.init("a", nullptr, 0, TILEDB_NE);
  qc = qc5.combine(qc6, TILEDB_AND);
  qc = qc.combine(qc1, TILEDB_AND);
  read_array_with_qc(ctx, qc);
  std::cout << "\n";

  if (vfs.is_dir(array_name))
    vfs.remove_dir(array_name);

  return 0;
}
