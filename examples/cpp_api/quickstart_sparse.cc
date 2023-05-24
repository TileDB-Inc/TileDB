/**
 * @file   quickstart_sparse.cc
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
 * to it, and read a slice of the data back.
 */

#include <iostream>
#include <tiledb/tiledb>

using namespace tiledb;

// Name of array.
//std::string array_name("quickstart_sparse_array");
std::string array_name(
    "tiledb://demo/s3://tiledb-shaun/arrays/quickstart_sparse_array");

Context ctx;

void create_array() {
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
  // Write some simple data to cells (1, 1), (2, 4) and (2, 3).
  std::vector<int> coords_rows = {1, 2, 2};
  std::vector<int> coords_cols = {1, 4, 3};
  std::vector<int> data = {1, 2, 3};

  // Open the array for writing and create the query.
  Array array(ctx, array_name, TILEDB_WRITE);
  Query query(ctx, array, TILEDB_WRITE);
  query.set_layout(TILEDB_UNORDERED)
      .set_data_buffer("a", data)
      .set_data_buffer("rows", coords_rows)
      .set_data_buffer("cols", coords_cols);

  // Perform the write and close the array.
  query.submit();
  // A second REST request segfaults during fragment metadata serialization.
  // We can use finalize as the second request to test with any example using
  // UnorderedWriter.
  query.finalize();
  array.close();
}

void write_array_separate() {
  Array array(ctx, array_name, TILEDB_WRITE);
  Config config = ctx.config();

  // Write dimensions separate from attributes.
  std::vector<int> coords_rows = {1, 2, 2};
  std::vector<int> coords_cols = {1, 4, 3};
  Query query(ctx, array, TILEDB_WRITE);
  query.set_config(config);
  query.set_layout(TILEDB_UNORDERED)
      .set_data_buffer("rows", coords_rows)
      .set_data_buffer("cols", coords_cols);
  query.submit();

  // Write attributes.
  std::vector<int> data = {1, 2, 3};
  query.set_data_buffer("a", data);
  query.submit();
  query.finalize();

  array.close();
}

void read_array() {
  // Prepare the array for reading
  Array array(ctx, array_name, TILEDB_READ);

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

int main() {
  Config config;
  config["sm.allow_separate_attribute_writes"] = "true";
  config["rest.server_address"] = "127.0.0.1:9191";
  config["rest.token"] = "YOUR_TOKEN";
  ctx = Context(config);

  if (array_name.find("tiledb://") != std::string::npos) {
    if (Object::object(ctx, array_name).type() == Object::Type::Array) {
      Array::delete_array(ctx, array_name);
    }
  } else {
    VFS vfs(ctx);
    if (vfs.is_dir(array_name)) {
      vfs.remove_dir(array_name);
    }
  }

  create_array();
//  write_array();
  write_array_separate();
  read_array();
}
