/**
 * @file   object.cc
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
 *   https://docs.tiledb.io/en/latest/tutorials/object.html
 *
 * This program creates a hierarchy as shown below. Specificaly, it creates
 * groups `dense_arrays` and `sparse_arrays` in a group `my_group`, and
 * then some dense/sparse arrays and key-value store in those groups.
 *
 * my_group/
 * ├── dense_arrays
 * │   ├── array_A
 * │   ├── array_B
 * │   └── kv
 * └── sparse_arrays
 *     ├── array_C
 *     └── array_D
 *
 * The program then shows how to list this hierarchy, as well as
 * move/remove TileDB objects.
 */

#include <iostream>
#include <tiledb/tiledb>

using namespace tiledb;

void print_path(const std::string& path, tiledb::Object::Type type) {
  // Simply print the path and type
  std::cout << path << " ";
  switch (type) {
    case tiledb::Object::Type::Array:
      std::cout << "ARRAY";
      break;
    case tiledb::Object::Type::KeyValue:
      std::cout << "KEY_VALUE";
      break;
    case tiledb::Object::Type::Group:
      std::cout << "GROUP";
      break;
    default:
      std::cout << "INVALID";
  }
  std::cout << "\n";
}

int list_obj(const std::string& path) {
  // Create TileDB context
  tiledb::Context ctx;

  // List children
  std::cout << "\nListing hierarchy: \n";
  tiledb::ObjectIter obj_iter(ctx, path);
  for (const auto& object : obj_iter)
    print_path(object.uri(), object.type());

  // Walk in a path with a pre- and post-order traversal
  std::cout << "\nPreorder traversal: \n";
  obj_iter.set_recursive();  // Default order is preorder
  for (const auto& object : obj_iter)
    print_path(object.uri(), object.type());
  std::cout << "\nPostorder traversal: \n";
  obj_iter.set_recursive(TILEDB_POSTORDER);
  for (const auto& object : obj_iter)
    print_path(object.uri(), object.type());

  return 0;
}

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

void create_kv(const std::string& kv_name) {
  tiledb::Context ctx;
  if (Object::object(ctx, kv_name).type() == Object::Type::KeyValue)
    return;
  tiledb::MapSchema schema(ctx);
  tiledb::Attribute a = tiledb::Attribute::create<int>(ctx, "a");
  schema.add_attribute(a);
  tiledb::Map::create(kv_name, schema);
}

void move_remove_obj() {
  tiledb::Context ctx;
  tiledb::Object::move(ctx, "my_group", "my_group_2");
  tiledb::Object::remove(ctx, "my_group_2/dense_arrays");
  tiledb::Object::remove(ctx, "my_group_2/sparse_arrays/array_C");
}

void create_hierarchy() {
  // Create groups
  tiledb::Context ctx;
  tiledb::create_group(ctx, "my_group");
  tiledb::create_group(ctx, "my_group/dense_arrays");
  tiledb::create_group(ctx, "my_group/sparse_arrays");

  // Create arrays
  create_array("my_group/dense_arrays/array_A", TILEDB_DENSE);
  create_array("my_group/dense_arrays/array_B", TILEDB_DENSE);
  create_array("my_group/sparse_arrays/array_C", TILEDB_SPARSE);
  create_array("my_group/sparse_arrays/array_D", TILEDB_SPARSE);

  // Create key-value store
  create_kv("my_group/dense_arrays/kv");
}

int main() {
  create_hierarchy();
  list_obj("my_group");
  move_remove_obj();  // Renames `my_group` to `my_group_2`
  list_obj("my_group_2");

  return 0;
}
