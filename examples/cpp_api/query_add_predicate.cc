/**
 * @file   query_add_predicate.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2025 TileDB, Inc.
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
 * This example demonstrates using the `QueryExperimental::add_predicate`
 * API to add one or more text predicates to a query. This API parses a SQL
 * predicate and uses it to filter results inside of the storage engine
 * before returning them to the user.
 *
 * The array used in this example is identical to that of the
 * `query_condition_sparse` example. The first group of predicates which
 * run are text equivalents of the predicates in that example, and produce
 * the same results.
 *
 * This example also has additional queries which use predicates which
 * combine dimensions and attributes, highlighting a capability which
 * cannot be replicated by just subarrays and query conditions.
 */

#include <iostream>
#include <optional>
#include <tiledb/tiledb>
#include <tiledb/tiledb_experimental>
#include <vector>

using namespace tiledb;

// Name of array.
std::string array_name("array_query_add_predicate");

// Enumeration variants
const std::vector<std::string> us_states = {
    "alabama",
    "alaska",
    "arizona",
    "arkansas",
    "california",
    "colorado",
    "connecticut",
    "etc"};

/**
 * @brief Function to print the values of all the attributes for one
 * index of this array.
 *
 * @param a Attribute a's value.
 * @param b Attribute b's value.
 * @param c Attribute c's value.
 * @param d Attribute d's value.
 */
void print_elem(
    std::optional<int> a,
    std::string b,
    int32_t c,
    float d,
    std::optional<uint8_t> e) {
  std::cout << "{" << (a.has_value() ? std::to_string(a.value()) : "null")
            << ", " << b << ", " << c << ", " << d << ", "
            << (e.has_value() ?
                    (e.value() < us_states.size() ?
                         us_states[e.value()] :
                         "(invalid key " + std::to_string(e.value()) + ")") :
                    "null")
            << "}" << std::endl;
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
  domain.add_dimension(Dimension::create<int32_t>(ctx, "index", {{0, 9}}));

  // The array will be sparse.
  ArraySchema schema(ctx, TILEDB_SPARSE);
  schema.set_domain(domain).set_order({{TILEDB_ROW_MAJOR}});

  // Adding the attributes of the array to the array schema.
  Attribute a = Attribute::create<int>(ctx, "a").set_nullable(true);
  schema.add_attribute(a)
      .add_attribute(Attribute::create<std::string>(ctx, "b"))
      .add_attribute(Attribute::create<int32_t>(ctx, "c"))
      .add_attribute(Attribute::create<float>(ctx, "d"));

  // Create enumeration and an attribute using it
  ArraySchemaExperimental::add_enumeration(
      ctx,
      schema,
      Enumeration::create(ctx, std::string("us_states"), us_states));

  {
    auto e = Attribute::create<uint8_t>(ctx, "e").set_nullable(true);
    AttributeExperimental::set_enumeration_name(ctx, e, "us_states");
    schema.add_attribute(e);
  }

  // Create the (empty) array.
  Array::create(ctx, array_name, schema);
}

/**
 * @brief Execute a write on array query_condition_sparse array
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
  std::vector<int32_t> dim_data = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
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

  std::vector<uint8_t> e_keys = {2, 7, 5, 6, 100, 3, 7, 7, 5, 4};
  std::vector<uint8_t> e_validity = {1, 1, 1, 1, 0, 1, 1, 1, 1, 1};

  // Execute the write query.
  Array array_w(ctx, array_name, TILEDB_WRITE);
  Query query_w(ctx, array_w);
  query_w.set_layout(TILEDB_UNORDERED)
      .set_data_buffer("index", dim_data)
      .set_data_buffer("a", a_data)
      .set_validity_buffer("a", a_data_validity)
      .set_data_buffer("b", b_data)
      .set_offsets_buffer("b", b_data_offsets)
      .set_data_buffer("c", c_data)
      .set_data_buffer("d", d_data)
      .set_data_buffer("e", e_keys)
      .set_validity_buffer("e", e_validity);

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
void read_array_with_predicates(
    Context& ctx, std::vector<std::string> predicates) {
  const unsigned reserve_cells = 16;

  // Create data buffers to read the values into.
  std::vector<int> a_data(reserve_cells);
  std::vector<uint8_t> a_data_validity(reserve_cells);

  // We initialize the string b_data to have enough space to
  // contain the total length of all of the strings written
  // into attribute b
  std::string b_data;
  b_data.resize(256);

  std::vector<uint64_t> b_data_offsets(reserve_cells);
  std::vector<int32_t> c_data(reserve_cells);
  std::vector<float> d_data(reserve_cells);
  std::vector<uint8_t> e_keys(reserve_cells);
  std::vector<uint8_t> e_validity(reserve_cells);

  // Execute the read query.
  Array array(ctx, array_name, TILEDB_READ);
  Query query(ctx, array);
  query.set_layout(TILEDB_GLOBAL_ORDER)
      .set_data_buffer("a", a_data)
      .set_validity_buffer("a", a_data_validity)
      .set_data_buffer("b", b_data)
      .set_offsets_buffer("b", b_data_offsets)
      .set_data_buffer("c", c_data)
      .set_data_buffer("d", d_data)
      .set_data_buffer("e", e_keys)
      .set_validity_buffer("e", e_validity);

  for (const auto& predicate : predicates) {
    QueryExperimental::add_predicate(ctx, query, predicate.c_str());
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
    // We pass in nullopt if the data is invalid, per the validity buffer.
    print_elem(
        (a_data_validity[i] ? std::optional{a_data[i]} : std::nullopt),
        b_data.substr(
            b_data_offsets[i], b_data_offsets[i + 1] - b_data_offsets[i]),
        c_data[i],
        d_data[i],
        (e_validity[i] ? std::optional{e_keys[i]} : std::nullopt));
  }

  query.finalize();
  array.close();
}

int main() {
  // Create the context.
  Context ctx;
  VFS vfs(ctx);
  if (!vfs.is_dir(array_name)) {
    // Create and write data to the array.
    create_array(ctx);
    write_array(ctx);
  }

  // EXAMPLES FROM query_condition_sparse.cc EXAMPLE

  // Printing the entire array.
  std::cout << "WHERE TRUE" << std::endl;
  read_array_with_predicates(ctx, {});
  std::cout << std::endl;

  // Execute a read query with query condition `a = null`.
  std::cout << "WHERE a IS NULL" << std::endl;
  read_array_with_predicates(ctx, {"a IS NULL"});
  std::cout << std::endl;

  // Execute a read query with query condition `b < "eve"`.
  std::cout << "WHERE b < 'eve'" << std::endl;
  read_array_with_predicates(ctx, {"b < 'eve'"});
  std::cout << std::endl;

  // Execute a read query with query condition `c >= 1`.
  std::cout << "WHERE c >= 1" << std::endl;
  read_array_with_predicates(ctx, {"c >= 1"});
  std::cout << std::endl;

  // Execute a read query with query condition `3.0f <= d AND d <= 4.0f`.
  std::cout << "WHERE d BETWEEN 3.0 AND 4.0" << std::endl;
  QueryCondition qc3(ctx);
  read_array_with_predicates(ctx, {"d BETWEEN 3.0 AND 4.0"});
  std::cout << std::endl;

  // Execute a read query with query condition `3.0f <= d AND d <= 4.0f AND a !=
  // null AND b < \"eve\"`.
  std::cout << "WHERE d BETWEEN 3.0 AND 4.0 AND a IS NOT NULL AND b < 'eve'"
            << std::endl;
  read_array_with_predicates(
      ctx, {"d BETWEEN 3.0 AND 4.0", "a IS NOT NULL", "b < 'eve'"});
  std::cout << std::endl;

  // BEGIN EXAMPLES WITH ENUMERATIONS
  // error is expected as enumerations are not supported yet
  std::cout << "WHERE e = 'california'" << std::endl;
  try {
    read_array_with_predicates(ctx, {"e = 'california'"});
    // should not get here
    return TILEDB_ERR;
  } catch (const std::exception& e) {
    std::cout << e.what() << std::endl;
  }
  std::cout << std::endl;

  // BEGIN EXAMPLES WITH NO EQUIVALENT
  // these examples cannot be expressed using subarray + query condition

  // query condition does not have functions, here we use coalesce
  std::cout << "WHERE coalesce(a, 2) + c < index" << std::endl;
  read_array_with_predicates(ctx, {"coalesce(a, 2) + c < index"});
  std::cout << std::endl;

  // FIXME: this is query-condition-able, use arithmetic
  std::cout << "WHERE a > 6 OR a IS NULL" << std::endl;
  read_array_with_predicates(ctx, {"a > 6 OR a IS NULL"});
  std::cout << std::endl;

  return 0;
}
