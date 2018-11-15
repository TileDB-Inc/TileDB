/**
 * @file   async.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2018 TileDB, Inc.
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
 *   https://docs.tiledb.io/en/latest/tutorials/async.html
 *
 * This program creates a simple 2D sparse array and shows how to write and
 * read asynchronously.
 */

#include <iostream>
#include <tiledb/tiledb>

using namespace tiledb;

// Name of array.
std::string array_name("async_array");

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

  // Add a single attribute
  schema.add_attribute(Attribute::create<int>(ctx, "a"));

  // Create the (empty) array on disk.
  Array::create(array_name, schema);
}

void write_array() {
  Context ctx;

  // Prepare some data for the array
  std::vector<int> coords = {1, 1, 2, 1, 2, 2, 4, 3};
  std::vector<int> data = {1, 2, 3, 4};

  // Open the array for writing and create the query.
  Array array(ctx, array_name, TILEDB_WRITE);
  Query query(ctx, array);
  query.set_layout(TILEDB_GLOBAL_ORDER)
      .set_buffer("a", data)
      .set_coordinates(coords);

  // Submit query asynchronously with callback
  query.submit_async(
      []() { std::cout << "Callback: Write query completed\n"; });

  // Wait for query to complete
  std::cout << "Write query in progress\n";
  tiledb::Query::Status status;
  do {
    status = query.query_status();
  } while (status == tiledb::Query::Status::INPROGRESS);

  // Finalize query and close the array.
  query.finalize();
  array.close();
}

void read_array() {
  Context ctx;
  Array array(ctx, array_name, TILEDB_READ);
  const std::vector<int> subarray = {1, 4, 1, 4};
  auto max_el = array.max_buffer_elements(subarray);
  std::vector<int> data(max_el["a"].second);
  std::vector<int> coords(max_el[TILEDB_COORDS].second);

  // Prepare the query
  Query query(ctx, array, TILEDB_READ);
  query.set_subarray(subarray)
      .set_layout(TILEDB_ROW_MAJOR)
      .set_buffer("a", data)
      .set_coordinates(coords);

  // Submit query asynchronously with callback
  query.submit_async([]() { std::cout << "Callback: Read query completed\n"; });

  // Wait for query to complete
  std::cout << "Read query in progress\n";
  tiledb::Query::Status status;
  do {
    status = query.query_status();
  } while (status == tiledb::Query::Status::INPROGRESS);

  // Print out the results.
  auto result_num = (int)query.result_buffer_elements()["a"].second;
  for (int r = 0; r < result_num; r++) {
    int i = coords[2 * r], j = coords[2 * r + 1];
    int a = data[r];
    std::cout << "Cell (" << i << ", " << j << ") has data " << a << "\n";
  }

  // Submit the query and close the array.
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
