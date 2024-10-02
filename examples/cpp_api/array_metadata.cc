/**
 * @file   array_metadata.cc
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
 * This program shows how to write, read and consolidate array metadata.
 */

#include <iostream>
#include <tiledb/tiledb>

using namespace tiledb;

// Name of array
std::string array_name("array_metadata_array");

void create_array() {
  // Create a TileDB context.
  Context ctx;

  // Create some array (it can be dense or sparse, with
  // any number of dimensions and attributes).
  Domain domain(ctx);
  domain.add_dimension(Dimension::create<int>(ctx, "rows", {{1, 4}}, 4))
      .add_dimension(Dimension::create<int>(ctx, "cols", {{1, 4}}, 4));

  // The array will be sparse.
  ArraySchema schema(ctx, TILEDB_SPARSE);
  schema.set_domain(domain).set_order({{TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR}});

  // Add a single attribute "a" so each (i,j) cell can store an integer.
  schema.add_attribute(Attribute::create<int>(ctx, "a"));

  // Create the (empty) array on disk.
  Array::create(ctx, array_name, schema);
}

void write_array_metadata() {
  // Create TileDB context
  Context ctx;

  // Open array for writing
  Array array(ctx, array_name, TILEDB_WRITE);

  // Write some metadata
  int v = 100;
  array.put_metadata("aaa", TILEDB_INT32, 1, &v);
  float f[] = {1.1f, 1.2f};
  array.put_metadata("bb", TILEDB_FLOAT32, 2, f);

  // Close array - Important so that the metadata get flushed
  array.close();
}

void read_array_metadata() {
  // Create TileDB context
  Context ctx;

  // Open array for reading
  Array array(ctx, array_name, TILEDB_READ);

  // Read with key
  tiledb_datatype_t v_type;
  uint32_t v_num;
  const void* v;
  array.get_metadata("aaa", &v_type, &v_num, &v);
  std::cout << "Details of item with key: aaa\n";
  std::cout << "- Value type: "
            << ((v_type == TILEDB_INT32) ? "INT32" : "something went wrong")
            << "\n";
  std::cout << "- Value num: " << v_num << "\n";
  std::cout << "- Value: " << *(const int*)v << "\n";

  array.get_metadata("bb", &v_type, &v_num, &v);
  std::cout << "Details of item with key: bb\n";
  std::cout << "- Value type: "
            << ((v_type == TILEDB_FLOAT32) ? "FLOAT32" : "something went wrong")
            << "\n";
  std::cout << "- Value num: " << v_num << "\n";
  std::cout << "- Value: " << ((const float*)v)[0] << " "
            << ((const float*)v)[1] << "\n";

  // Enumerate all metadata items
  std::string key;
  uint64_t num = array.metadata_num();
  printf("Enumerate all metadata items:\n");
  for (uint64_t i = 0; i < num; ++i) {
    array.get_metadata_from_index(i, &key, &v_type, &v_num, &v);

    std::cout << "# Item " << i << "\n";
    const char* v_type_str = (v_type == TILEDB_INT32) ? "INT32" : "FLOAT32";
    std::cout << "- Key: " << key << "\n";
    std::cout << "- Value type: " << v_type_str << "\n";
    std::cout << "- Value num: " << v_num << "\n";
    std::cout << "- Value: ";
    if (v_type == TILEDB_INT32) {
      for (uint32_t j = 0; j < v_num; ++j)
        std::cout << ((const int*)v)[j] << " ";
    } else if (v_type == TILEDB_FLOAT32) {
      for (uint32_t j = 0; j < v_num; ++j)
        std::cout << ((const float*)v)[j] << " ";
    }
    std::cout << "\n";
  }

  // Close array
  array.close();
}

int main() {
  create_array();
  write_array_metadata();
  read_array_metadata();

  return 0;
}
