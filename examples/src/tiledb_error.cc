/**
 * @file   tiledb_error.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017 TileDB, Inc.
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
 * This example shows how to catch errors. Program output:
 *
 * $ ./tiledb_error
 * Group created successfully!
 * [TileDB::OS] Error: Cannot create directory \
 * '<current_working_dir>/my_group'; Directory already exists
 */

#include <tiledb.h>

void print_error(tiledb_ctx_t* ctx);

int main() {
  // Create TileDB context
  tiledb_ctx_t* ctx;
  tiledb_ctx_create(&ctx, nullptr);

  // Create a group
  int rc = tiledb_group_create(ctx, "my_group");
  if (rc == TILEDB_OK)
    printf("Group created successfully!\n");
  else if (rc == TILEDB_ERR)
    print_error(ctx);

  // Create the same group again - ERROR
  rc = tiledb_group_create(ctx, "my_group");
  if (rc == TILEDB_OK)
    printf("Group created successfully!\n");
  else if (rc == TILEDB_ERR)
    print_error(ctx);

  // Clean up
  tiledb_ctx_free(ctx);

  return 0;
}

void print_error(tiledb_ctx_t* ctx) {
  tiledb_error_t* err;
  tiledb_error_last(ctx, &err);
  const char* msg;
  tiledb_error_message(ctx, err, &msg);
  printf("%s\n", msg);
  tiledb_error_free(ctx, err);
}
