/**
 * @file   errors.c
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2021 TileDB, Inc.
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
 * This example shows how to catch errors in TileDB.
 */

#include <stdio.h>
#include <tiledb/tiledb_experimental.h>

void print_last_error(tiledb_ctx_t* ctx);
void print_error(tiledb_error_t* err);

int main() {
  // Create TileDB context
  tiledb_ctx_t* ctx;
  tiledb_error_t* err;
  int rc = tiledb_ctx_alloc_with_error(NULL, &ctx, &err);
  if (rc == TILEDB_OK)
    printf("Context created successfully!\n");
  else if (rc == TILEDB_ERR) {
    print_error(err);
    return 1;
  }

  // Create a group. The code below creates a group `my_group` and prints a
  // message because (normally) it will succeed.
  tiledb_group_create(ctx, "my_group");
  if (rc == TILEDB_OK)
    printf("Group created successfully!\n");
  else if (rc == TILEDB_ERR)
    print_last_error(ctx);

  // Create the same group again. f we attempt to create the same group
  // `my_group` as shown below, TileDB will return an error and the example
  // will call function `print_error`.
  rc = tiledb_group_create(ctx, "my_group");
  if (rc == TILEDB_OK)
    printf("Group created successfully!\n");
  else if (rc == TILEDB_ERR)
    print_last_error(ctx);

  // Clean up
  tiledb_ctx_free(&ctx);

  return 0;
}

void print_last_error(tiledb_ctx_t* ctx) {
  // Retrieve the last error that occurred
  tiledb_error_t* err = NULL;
  tiledb_ctx_get_last_error(ctx, &err);

  print_error(err);

  // Clean up
  tiledb_error_free(&err);
}

void print_error(tiledb_error_t* err) {
  // Retrieve the error message by invoking `tiledb_error_message`.
  const char* msg;
  tiledb_error_message(err, &msg);
  printf("%s\n", msg);
}
