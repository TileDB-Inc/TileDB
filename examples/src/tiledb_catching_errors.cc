/**
 * @file   tiledb_catching_errors.cc
 *
 * @section LICENSE
 *
 * The MIT License
 * 
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
 * This examples shows how to catch errors.
 */

#include "tiledb.h"
#include <cstdio>

int main() {
  // Initialize context with the default configuration parameters
  tiledb_ctx_t* ctx;
  tiledb_ctx_create(&ctx);

  // Create a group
  int rc = tiledb_group_create(ctx, "my_group");
  if(rc == TILEDB_OK)
    printf("Group created successfully!\n");
  else if(rc == TILEDB_ERR) {
    tiledb_error_t* err;
    tiledb_error_last(ctx, &err);
    const char* msg;
    tiledb_error_message(ctx, err, &msg);
    printf("%s\n", msg); // prints empty string ""
    tiledb_error_free(err);
  }

  // Create the same group again - ERROR
  rc = tiledb_group_create(ctx, "my_group");
  if(rc == TILEDB_OK)
    printf("Group created successfully!\n");
  else if(rc == TILEDB_ERR) {
    tiledb_error_t* err;
    tiledb_error_last(ctx, &err);
    const char* msg;
    tiledb_error_message(ctx, err, &msg);
    printf("%s\n", msg); // prints empty string ""
    tiledb_error_free(err);
  }
  // Finalize context
  tiledb_ctx_free(ctx);

  return 0;
}
