/**
 * @file   groups.cc
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
 * This program creates a hierarchy as shown below. Specifically, it creates
 * groups `my_group` and `sparse_arrays`, and
 * then some dense/sparse arrays.
 *
 * my_group/
 * ├── dense_arrays
 * │   ├── array_A
 * │   └── array_B
 * └── sparse_arrays
 *     ├── array_C
 *     └── array_D
 *
 * The program then shows how to group these together using the TileDB Group API
 */

#include <iostream>
#include <tiledb/tiledb>
#include <tiledb/tiledb_experimental>

using namespace tiledb;

void create_array(const std::string& array_name, tiledb_array_type_t type) {
  Context ctx;
  if (Object::object(ctx, array_name).type() == Object::Type::Array)
    return;

  Domain domain(ctx);
  domain.add_dimension(Dimension::create<int>(ctx, "rows", {{1, 4}}, 4))
      .add_dimension(Dimension::create<int>(ctx, "cols", {{1, 4}}, 4));
  ArraySchema schema(ctx, type);
  schema.set_domain(domain).set_order({{TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR}});
  schema.add_attribute(Attribute::create<int>(ctx, "a"));
  Array::create(array_name, schema);
}

void create_arrays_groups() {
  // Create groups
  tiledb::Context ctx;
  tiledb::create_group(ctx, "my_group");
  tiledb::create_group(ctx, "my_group/sparse_arrays");

  tiledb::VFS vfs(ctx);
  vfs.create_dir("my_group/dense_arrays");

  // Create arrays
  create_array("my_group/dense_arrays/array_A", TILEDB_DENSE);
  create_array("my_group/dense_arrays/array_B", TILEDB_DENSE);
  create_array("my_group/sparse_arrays/array_C", TILEDB_SPARSE);
  create_array("my_group/sparse_arrays/array_D", TILEDB_SPARSE);

  tiledb::Group group(ctx, "my_group", TILEDB_WRITE);
  group.add_member("my_group/dense_arrays/array_A", true);
  group.add_member("my_group/dense_arrays/array_B", true);
  group.add_member("my_group/sparse_arrays", true);

  tiledb::Group group_sparse(ctx, "my_group/sparse_arrays", TILEDB_WRITE);
  group_sparse.add_member("my_group/sparse_arrays/array_C", true);
  group_sparse.add_member("my_group/sparse_arrays/array_D", true);
}

void print_group() {
  tiledb::Context ctx;
  tiledb::Group group(ctx, "my_group", TILEDB_READ);
  std::cout << group.dump(true) << std::endl;
}

int main() {
  create_arrays_groups();
  print_group();

  return 0;
}
