/**
 * @file   tiledb_object_type.c
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2018 TileDB, Inc.
 * @copyright Copyright (c) 2017 MIT, Intel Corporation and TileDB, Inc.
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
 * This example retrieves the object type of four paths, `my_group`,
 * `my_dense_array`, `my_kv` and `some_invalid_path`. Before running this
 * program, we make sure that `my_group` is a valid TileDB group,
 * `my_dense_array` is a valid TileDB array, `my_kv` is a valid TileDB
 * key-value store, and `some_invalid_path` is a potentially existing directory
 * or file that is not a TileDB object. We also provide a simple function
 * `print_object_type` that prints the TileDB type to `stdout`.
 *
 * You need to run the following to make this work:
 *
 * ```
 * $ ./tiledb_group_create_c
 * $ ./tiledb_dense_create_c
 * $ ./tiledb_kv_create_c
 * $ mkdir some_invalid_path
 * $ ./tiledb_object_type_c
 * GROUP
 * ARRAY
 * KEY_VALUE
 * INVALID
 * ```
 */

#include <tiledb/tiledb.h>

void print_object_type(tiledb_object_t type);

int main() {
  // Create context
  tiledb_ctx_t* ctx;
  tiledb_ctx_alloc(&ctx, NULL);

  // Get object type for group
  tiledb_object_t type;
  tiledb_object_type(ctx, "my_group", &type);
  print_object_type(type);

  // Get object type for array
  tiledb_object_type(ctx, "my_dense_array", &type);
  print_object_type(type);

  // Get object type for key-value
  tiledb_object_type(ctx, "my_kv", &type);
  print_object_type(type);

  // Get invalid object type
  tiledb_object_type(ctx, "some_invalid_path", &type);
  print_object_type(type);

  // Clean up
  tiledb_ctx_free(&ctx);

  return 0;
}

void print_object_type(tiledb_object_t type) {
  switch (type) {
    case TILEDB_ARRAY:
      printf("ARRAY\n");
      break;
    case TILEDB_GROUP:
      printf("GROUP\n");
      break;
    case TILEDB_KEY_VALUE:
      printf("KEY_VALUE\n");
      break;
    case TILEDB_INVALID:
      printf("INVALID\n");
      break;
  }
}
