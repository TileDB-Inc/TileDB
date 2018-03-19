/**
 * @file   tiledb_object_move.cc
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
 * It shows how to move/rename a TileDB object.
 *
 * You need to run the following to make this work:
 *
 * ./tiledb_group_create_cpp
 * ./tiledb_dense_create_cpp
 * ./tiledb_object_move_cpp
 */

#include <tiledb/tiledb>

int main() {
  // Create TileDB context
  tiledb::Context ctx;

  // Rename a valid group and array
  tiledb::Object::move(ctx, "my_group", "my_group_2");
  tiledb::Object::move(
      ctx, "my_dense_array", "my_group_2/dense_arrays/my_dense_array");

  // Rename an invalid path
  try {
    tiledb::Object::move(ctx, "invalid_path", "path");
  } catch (std::runtime_error& e) {
    std::cout << "Failed to move invalid path\n";
  }

  return 0;
}