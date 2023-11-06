/**
 * @file   aggregates.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2018-2021 TileDB, Inc.
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
 * When run, this program will create a simple 2D sparse array, write some data
 * to it in global order, and read the data back with aggregates.
 */

#include <iostream>
#include <tiledb/tiledb>
#include <tiledb/tiledb_experimental>

using namespace tiledb;

// Name of array
std::string array_name("aggregates_array");

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
  std::vector<int> coords_rows_1 = {1, 2};
  std::vector<int> coords_cols_1 = {1, 4};
  std::vector<int> data_1 = {1, 2};
  query.set_data_buffer("a", data_1)
      .set_data_buffer("rows", coords_rows_1)
      .set_data_buffer("cols", coords_cols_1);
  query.submit();

  // Submit second query
  std::vector<int> coords_rows_2 = {3};
  std::vector<int> coords_cols_2 = {3};
  std::vector<int> data_2 = {3};
  query.set_data_buffer("a", data_2)
      .set_data_buffer("rows", coords_rows_2)
      .set_data_buffer("cols", coords_cols_2);
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
  Subarray subarray(ctx, array);
  subarray.add_range(0, 1, 4).add_range(1, 1, 4);

  std::vector<uint64_t> count(1);
  std::vector<int64_t> sum(1);

  // Create a  query
  Query query(ctx, array);

  // Add aggregates for sum and count on the default channel.
  QueryChannel default_channel = QueryExperimental::get_default_channel(query);
  default_channel.apply_aggregate("Count", CountOperation());
  ChannelOperation operation =
      QueryExperimental::create_unary_aggregate<SumOperator>(query, "a");
  default_channel.apply_aggregate("Sum", operation);

  // Set layout and buffers.
  query.set_layout(TILEDB_UNORDERED)
      .set_data_buffer("Count", count)
      .set_data_buffer("Sum", sum);

  // Submit the query and close the array.
  query.submit();
  array.close();

  // Print out the results.
  std::cout << "Count: " << count[0] << std::endl;
  std::cout << "Sum: " << sum[0] << std::endl;
}

int main() {
  create_array();
  write_array();
  read_array();
  return 0;
}
