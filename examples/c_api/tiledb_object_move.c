/**
 * @file   tiledb_object_move.c
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
 * In this example we move a valid TileDB group and array, as well as an invalid
 * path to see what happens. Notice that the last input of the move API function
 * specifies whether the object to be moved should overwrite the new (target)
 * path if it exists.
 *
 * You need to run the following to make this work:
 *
 * ```
 * $ ./tiledb_group_create_c
 * $ ./tiledb_dense_create_c
 * $ mkdir my_group_2
 * $ mkdir invalid_path
 * $ ls -1
 * my_dense_array
 * my_group
 * my_group_2
 * invalid_path
 * $ ./tiledb_object_move_c
 * Failed moving invalid path
 * $ ls -1
 * my_group_2
 * invalid_path
 * $ ls -1 my_group_2
 * __tiledb_group.tdb
 * dense_arrays
 * sparse_arrays
 * $ ls -1 my_group_2/dense_arrays
 * __tiledb_group.tdb
 * my_dense_array
 * ```
 *
 * We first create TileDB group `my_group` and array `my_dense_array`. We also
 * manually create a directory called `my_group_2` and `invalid_path`. After
 * executing move, we first see that `my_group` is renamed to `my_group_2`,
 * **overwriting** the existing directory with the same name. Also
 * `my_dense_array` is moved inside `my_group_2/dense_arrays`. Notice finally
 * that TileDB did not move `invalid_path`, since this was not a valid
 * TileDB resource.
 */

#include <tiledb/tiledb.h>

int main() {
  // Create context
  tiledb_ctx_t* ctx;
  tiledb_ctx_alloc(&ctx, NULL);

  // Rename a valid group and array
  tiledb_object_move(ctx, "my_group", "my_group_2");
  tiledb_object_move(
      ctx, "my_dense_array", "my_group_2/dense_arrays/my_dense_array");

  // Rename an invalid path
  int rc = tiledb_object_move(ctx, "invalid_path", "path");
  if (rc == TILEDB_ERR)
    printf("Failed moving invalid path\n");

  // Clean up
  tiledb_ctx_free(&ctx);

  return 0;
}
