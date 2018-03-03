/**
 * @file   tiledb_object_type.cc
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
 * It shows how to get the type of a TileDB object (resource).
 *
 * You need to run the following to make this work:
 *
 * ./tiledb_group_create_cpp
 * ./tiledb_dense_create_cpp
 * ./tiledb_kv_create_cpp
 * ./tiledb_object_type_cpp
 */

#include <tiledb/tiledb>

void print_object_type(tiledb::Object::Type type);

int main() {
  // Create TileDB context
  tiledb::Context ctx;

  // Print object types
  print_object_type(tiledb::Object::object(ctx, "my_group").type());
  print_object_type(tiledb::Object::object(ctx, "my_dense_array").type());
  print_object_type(tiledb::Object::object(ctx, "my_kv").type());
  print_object_type(tiledb::Object::object(ctx, "invalid_path").type());

  return 0;
}

void print_object_type(tiledb::Object::Type type) {
  switch (type) {
    case tiledb::Object::Type::Array:
      std::cout << "ARRAY\n";
      break;
    case tiledb::Object::Type::Group:
      std::cout << "GROUP\n";
      break;
    case tiledb::Object::Type::KeyValue:
      std::cout << "KEY_VALUE\n";
      break;
    case tiledb::Object::Type::Invalid:
      std::cout << "INVALID\n";
      break;
  }
}