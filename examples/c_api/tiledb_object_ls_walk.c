/**
 * @file   tiledb_object_ls_walk.c
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2018 TileDB, Inc.
 * @copyright Copyright (c) 2016 MIT and Intel Corporation
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
 * List/Walk a directory for TileDB objects.
 *
 * First create some object hierarchy such as:
 *
 * my_group/
 * ├── dense_arrays
 * │   ├── __tiledb_group.tdb
 * │   ├── array_A
 * │   │   └── __array_schema.tdb
 * │   ├── array_B
 * │   │   └── __array_schema.tdb
 * │   └── kv
 * │       └── __kv_schema.tdb
 * └── sparse_arrays
 *     ├── __tiledb_group.tdb
 *     ├── array_C
 *     │   └── __array_schema.tdb
 *     └── array_D
 *         └── __array_schema.tdb
 *
 * ```
 * $ mkdir my_group
 * $ mkdir my_group/dense_arrays
 * $ mkdir my_group/sparse_arrays
 * $ mkdir my_group/dense_arrays/array_A
 * $ mkdir my_group/dense_arrays/array_B
 * $ mkdir my_group/dense_arrays/kv
 * $ mkdir my_group/sparse_arrays/array_C
 * $ mkdir my_group/sparse_arrays/array_D
 * $ touch my_group/dense_arrays/__tiledb_group.tdb
 * $ touch my_group/sparse_arrays/__tiledb_group.tdb
 * $ touch my_group/dense_arrays/array_A/__array_schema.tdb
 * $ touch my_group/dense_arrays/array_B/__array_schema.tdb
 * $ touch my_group/dense_arrays/kv/__kv_schema.tdb
 * $ touch my_group/sparse_arrays/array_C/__array_schema.tdb
 * $ touch my_group/sparse_arrays/array_D/__array_schema.tdb
 * ```
 *
 * This means that `dense_arrays` and sparse_arrays` are TileDB groups,
 * whereas `array_A/B/C/D` are TileDB arrays and `kv` is a key-value store.
 *
 * Then run:
 *
 * ```
 * $ ./tiledb_object_ls_walk_c
 * List children:
 * file://<cwd>/my_group/dense_arrays GROUP
 * file://<cwd>/my_group/sparse_arrays GROUP
 *
 * Preorder traversal:
 * file://<cwd>/my_group/dense_arrays GROUP
 * file://<cwd>/my_group/dense_arrays/array_A ARRAY
 * file://<cwd>/my_group/dense_arrays/array_B ARRAY
 * file://<cwd>/my_group/dense_arrays/kv KEY_VALUE
 * file://<cwd>/my_group/sparse_arrays GROUP
 * file://<cwd>/my_group/sparse_arrays/array_C ARRAY
 * file://<cwd>/my_group/sparse_arrays/array_D ARRAY
 *
 * Postorder traversal:
 * file://<cwd>/my_group/dense_arrays/array_A ARRAY
 * file://<cwd>/my_group/dense_arrays/array_B ARRAY
 * file://<cwd>/my_group/dense_arrays/kv KEY_VALUE
 * file://<cwd>/my_group/dense_arrays GROUP
 * file://<cwd>/my_group/sparse_arrays/array_C ARRAY
 * file://<cwd>/my_group/sparse_arrays/array_D ARRAY
 * file://<cwd>/my_group/sparse_arrays GROUP
 * ```
 *
 * We essentially wish to first list the TileDB objects in `my_group` (in
 * our current working directory), and then recursively traverse the TileDB
 * objects in folder `my_group` once in a preorder and once in a postorder
 * manner. We apply the `print_path` callback for each TileDB object found.
 * Any non-TileDB objects will be ignored.
 *
 * This callback simply prints the full path of each visited object along with
 * its type (array, group, key-value or invalid) on the screen. Notice that
 * this function always returns `1`. This means that the traversal will always
 * continue to the next TileDB object. Had it been set to `0`, the traversal
 * would finish after printing the first encountered TileDB object. Also we do
 * not utilize argument `data` at all. You can be very creative by choosing
 * different functions with different `data` inputs and different return values
 * to implement some pretty sophisticated functionality.
 */

#include <tiledb/tiledb.h>

int print_path(const char* path, tiledb_object_t type, void* data);

int main() {
  // Create TileDB context
  tiledb_ctx_t* ctx;
  tiledb_ctx_create(&ctx, NULL);

  // List children
  printf("List children:\n");
  tiledb_object_ls(ctx, "my_group", print_path, NULL);

  // Walk in a path with a pre- and post-order traversal
  printf("\nPreorder traversal:\n");
  tiledb_object_walk(ctx, "my_group", TILEDB_PREORDER, print_path, NULL);
  printf("\nPostorder traversal:\n");
  tiledb_object_walk(ctx, "my_group", TILEDB_POSTORDER, print_path, NULL);

  // Finalize context
  tiledb_ctx_free(&ctx);

  return 0;
}

int print_path(const char* path, tiledb_object_t type, void* data) {
  // Simply print the path and type
  (void)data;
  printf("%s ", path);
  switch (type) {
    case TILEDB_ARRAY:
      printf("ARRAY");
      break;
    case TILEDB_KEY_VALUE:
      printf("KEY_VALUE");
      break;
    case TILEDB_GROUP:
      printf("GROUP");
      break;
    default:
      printf("INVALID");
  }
  printf("\n");

  // Always iterate till the end
  return 1;
}
