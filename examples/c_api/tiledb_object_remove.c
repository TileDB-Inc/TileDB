/**
 * @file   tiledb_object_remove.c
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
 * This example shows how to delete TileDB arrays and groups. Note that this
 * program will not delete invalid paths (i.e., non-TileDB resources).
 *
 * You need to run the following to make this work:
 *
 * ```
 * $ ./tiledb_group_create_c
 * $ ./tiledb_dense_create_c
 * $ ./tiledb_dense_write_global_1_c
 * $ mkdir invalid_path
 * $ ls -1
 * my_dense_array
 * my_group
 * invalid_path
 * $ ./tiledb_object_remove_c
 * Failed deleting invalid path
 * $ ls -1
 * invalid_path
 * ```
 *
 * We first create TileDB group `my_group` and array `my_dense_array`. We also
 * manually create directory `invalid_path` that is not related to TileDB.
 * Invoking the program successfully deletes `my_group` and `my_dense_array`,
 * but fails to delete `invalid_path`.
 */

#include <stdio.h>
#include <tiledb/tiledb.h>

int main() {
  // Create context
  tiledb_ctx_t* ctx;
  tiledb_ctx_alloc(&ctx, NULL);

  // Delete a valid group and array
  tiledb_object_remove(ctx, "my_group");
  tiledb_object_remove(ctx, "my_dense_array");

  // Delete an invalid path
  int rc = tiledb_object_remove(ctx, "invalid_path");
  if (rc == TILEDB_ERR)
    printf("Failed to delete invalid path\n");

  // Clean up
  tiledb_ctx_free(&ctx);

  return 0;
}
