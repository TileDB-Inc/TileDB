/**
 * @file   tiledb_array_consolidate.c
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
 * This program shows how to consolidate arrays.
 *
 * One way to make this work is by first creating a dense array and making three
 * different writes:
 *
 * ```
 * $ ./tiledb_dense_create_c
 * $ ./tiledb_dense_write_global_1_c
 * $ ./tiledb_dense_write_global_subarray_c
 * $ ./tiledb_dense_write_unordered_c
 * $ ls -1 my_dense_array
 * __0x7ffff10e73c0_1517941950491
 * __0x7ffff10e73c0_1517941954698
 * __0x7ffff10e73c0_1517941959273
 * __array_schema.tdb
 * __lock.tdb
 * ```
 *
 * The above will create three fragments (appearing as separate subdirectories).
 *
 * Running this program will consolidate the 3 fragments/directories into a
 * single one.
 *
 * ```
 * $ ./tiledb_array_consolidate_c
 * $ ls -1 my_dense_array
 * __0x7ffff10e73c0_1517941970634_1517941959273
 * __lock.tdb
 * __array_schema.tdb
 * ```
 */

#include <tiledb/tiledb.h>

int main() {
  // Create TileDB context
  tiledb_ctx_t* ctx;
  tiledb_ctx_create(&ctx, NULL);

  // Consolidate array
  tiledb_array_consolidate(ctx, "my_dense_array");

  // Clean up
  tiledb_ctx_free(&ctx);

  return 0;
}
