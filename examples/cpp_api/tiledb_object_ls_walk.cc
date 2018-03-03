/**
 * @file   tiledb_object_ls_walk.cc
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
 * List/Walk a directory for TileDB Objects.
 *
 * Create some object hierarchy and then run:
 *
 * ./tiledb_object_ls_walk_cpp
 */

#include <tiledb/tiledb>

void print_path(const std::string& path, tiledb::Object::Type type);

int main() {
  // Create TileDB context
  tiledb::Context ctx;

  // List children
  std::cout << "List children: \n";
  tiledb::ObjectIter obj_iter(ctx, "my_group");
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

  // Walk in a path, but list only groups
  std::cout << "\nOnly groups: \n";
  obj_iter.set_iter_policy(true, false, false);
  for (const auto& object : obj_iter)
    print_path(object.uri(), object.type());

  // Walk in a path, but list only groups and arrays
  std::cout << "\nOnly groups and arrays: \n";
  obj_iter.set_iter_policy(true, true, false);
  for (const auto& object : obj_iter)
    print_path(object.uri(), object.type());

  return 0;
}

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
