/**
 * @file   array_schema_evolution.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2018-2022 TileDB, Inc.
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
 * to it, and read a slice of the data back, then show adding a column, writing
 * a second time and getting results back
 */

#include <iostream>
#include <tiledb/tiledb>
#include <tiledb/tiledb_experimental>

using namespace tiledb;

// Name of array.
std::string array_uri("array_schema_evolution_array");

void create_array(const Context& ctx) {
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
  Array::create(ctx, array_uri, schema);
}

void write_array(const Context& ctx) {
  // Write some simple data to cells (1, 1), (2, 4) and (2, 3).
  std::vector<int> coords_rows = {1, 2, 2};
  std::vector<int> coords_cols = {1, 4, 3};
  std::vector<int> data = {1, 2, 3};

  // Open the array for writing and create the query.
  Array array(ctx, array_uri, TILEDB_WRITE);
  Query query(ctx, array, TILEDB_WRITE);
  query.set_layout(TILEDB_UNORDERED)
      .set_data_buffer("a", data)
      .set_data_buffer("rows", coords_rows)
      .set_data_buffer("cols", coords_cols);

  // Perform the write and close the array.
  query.submit();
  array.close();
}

void write_array2(const Context& ctx) {
  // Write some simple data to cells (1, 1), (2, 4) and (2, 3).
  std::vector<int> coords_rows = {3};
  std::vector<int> coords_cols = {1};
  std::vector<int> a_data = {4};
  std::vector<uint32_t> b_data = {4};

  // Open the array for writing and create the query.
  Array array(ctx, array_uri, TILEDB_WRITE);
  Query query(ctx, array, TILEDB_WRITE);
  query.set_layout(TILEDB_UNORDERED)
      .set_data_buffer("a", a_data)
      .set_data_buffer("b", b_data)
      .set_data_buffer("rows", coords_rows)
      .set_data_buffer("cols", coords_cols);

  // Perform the write and close the array.
  query.submit();
  array.close();
}

void read_array(const Context& ctx) {
  // Prepare the array for reading
  Array array(ctx, array_uri, TILEDB_READ);

  // Slice only rows 1, 2 and cols 2, 3, 4
  Subarray subarray(ctx, array);
  subarray.add_range(0, 1, 2).add_range(1, 2, 4);

  // Prepare the vector that will hold the result.
  // We take an upper bound on the result size, as we do not
  // know a priori how big it is (since the array is sparse)
  std::vector<int> data(3);
  std::vector<int> coords_rows(3);
  std::vector<int> coords_cols(3);

  // Prepare the query
  Query query(ctx, array, TILEDB_READ);
  query.set_subarray(subarray)
      .set_layout(TILEDB_ROW_MAJOR)
      .set_data_buffer("a", data)
      .set_data_buffer("rows", coords_rows)
      .set_data_buffer("cols", coords_cols);

  // Submit the query and close the array.
  query.submit();
  array.close();

  // Print out the results.
  auto result_num = (int)query.result_buffer_elements()["a"].second;
  for (int r = 0; r < result_num; r++) {
    int i = coords_rows[r];
    int j = coords_cols[r];
    int a = data[r];
    std::cout << "Cell (" << i << ", " << j << ") has data " << a << "\n";
  }
}

void read_array2(const Context& ctx) {
  // Prepare the array for reading
  Array array(ctx, array_uri, TILEDB_READ);

  // Prepare the vector that will hold the result.
  // We take an upper bound on the result size, as we do not
  // know a priori how big it is (since the array is sparse)
  std::vector<int> a_data(4);
  std::vector<uint32_t> b_data(4);
  std::vector<int> coords_rows(4);
  std::vector<int> coords_cols(4);

  // Prepare the query
  Subarray subarray(ctx, array);
  subarray.add_range(0, 1, 4).add_range(1, 1, 4);
  Query query(ctx, array, TILEDB_READ);
  query.set_subarray(subarray)
      .set_layout(TILEDB_ROW_MAJOR)
      .set_data_buffer("a", a_data)
      .set_data_buffer("b", b_data)
      .set_data_buffer("rows", coords_rows)
      .set_data_buffer("cols", coords_cols);

  // Submit the query and close the array.
  query.submit();
  array.close();

  // Print out the results.
  auto result_num = (int)query.result_buffer_elements()["a"].second;
  for (int r = 0; r < result_num; r++) {
    int i = coords_rows[r];
    int j = coords_cols[r];
    int a = a_data[r];
    int b = b_data[r];
    std::cout << "Cell (" << i << ", " << j << ") has data " << a << ", " << b
              << "\n";
  }
}

void array_schema_evolve(const Context& ctx) {
  ArraySchemaEvolution schemaEvolution = ArraySchemaEvolution(ctx);

  // Add attribute b
  Attribute b = Attribute::create<uint32_t>(ctx, "b");
  uint32_t fill_value = 1;
  b.set_fill_value(&fill_value, sizeof(fill_value));
  schemaEvolution.add_attribute(b);

  // evolve array
  schemaEvolution.array_evolve(array_uri);
}

int main() {
  Config cfg;
  Context ctx(cfg);

  if (Object::object(ctx, array_uri).type() != Object::Type::Array) {
    create_array(ctx);
    write_array(ctx);
    read_array(ctx);
    array_schema_evolve(ctx);
    write_array2(ctx);
  }

  read_array2(ctx);
  return 0;
}
