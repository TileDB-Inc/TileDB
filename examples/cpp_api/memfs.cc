/**
 * @file   memfs.cc
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
 * When run, this program will create a simple 2D dense array on memfs,
 * write some data to it, and read a slice of the data back.
 *
 * Note: memfs requires a global Context object
 */

#include <iostream>
#include <tiledb/tiledb>

using namespace tiledb;

// Name of array.
std::string array_name_("mem://quickstart_dense_array");

// Example-global Context object.
Context ctx_;

void create_array(std::string array_name) {
  // Create an empty dense 4x4 array with "rows" and "cols" on memfs
  Domain domain(ctx_);
  domain.add_dimension(Dimension::create<int>(ctx_, "rows", {{1, 4}}, 4))
      .add_dimension(Dimension::create<int>(ctx_, "cols", {{1, 4}}, 4));
  ArraySchema schema(ctx_, TILEDB_DENSE);
  schema.set_domain(domain)
      .set_order({{TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR}})
      .add_attribute(Attribute::create<int>(ctx_, "a"));
  Array::create(array_name, schema);
}

void write_array(std::string array_name) {
  // Write data to the array
  std::vector<int> data = {
      1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};

  // Try writing with non-global context
  try {
    Context ctx_non_global;
    Array array(ctx_non_global, array_name, TILEDB_WRITE);
  } catch (std::exception& e) {
    std::cout << "Error: Must use process global Context on memfs.\n";
  }

  Array array(ctx_, array_name, TILEDB_WRITE);
  Query query(ctx_, array, TILEDB_WRITE);
  query.set_layout(TILEDB_ROW_MAJOR).set_data_buffer("a", data).submit();
  query.submit();
  array.close();
}

void read_array(std::string array_name) {
  // Read the data
  Array array(ctx_, array_name, TILEDB_READ);
  std::vector<int> data(6);
  Query query(ctx_, array, TILEDB_READ);
  Subarray subarray(ctx_, array);
  subarray.add_range(0, 1, 2).add_range(1, 2, 4);
  query.set_subarray(subarray)
      .set_layout(TILEDB_ROW_MAJOR)
      .set_data_buffer("a", data);
  query.submit();
  array.close();
  for (auto d : data)
    std::cout << d << " ";
  std::cout << "\n";
}

void remove_array(std::string array_name) {
  // Initialize a VFS object
  VFS vfs(ctx_);

  // Ensure the memfs directory has been initialized
  std::vector<std::string> uris = vfs.ls(array_name);
  for (auto uri : uris) {
    std::cout << uri << std::endl;
  }

  // Clean up
  vfs.remove_dir(array_name);

  // Ensure memfs has been cleaned up
  std::vector<std::string> uris2 = vfs.ls(array_name);
  if (uris2.size() != 0) {
    std::cout << "Error: MemFS directory has not been fully deleted.";
  }
}

int main() {
  if (Object::object(ctx_, array_name_).type() != Object::Type::Array) {
    create_array(array_name_);
    write_array(array_name_);
  }
  read_array(array_name_);
  remove_array(array_name_);

  return 0;
}
