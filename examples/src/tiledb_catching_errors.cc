/**
 * @file   tiledb_catching_errors.cc
 *
 * @section LICENSE
 *
 * The MIT License
 * 
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
 * This examples shows how to catch errors.
 */

#include "tiledb.h"
#include <cstdio>

int main() {
  // Initialize context with the default configuration parameters
  TileDB_CTX* tiledb_ctx;
  tiledb_ctx_init(&tiledb_ctx, NULL);

  // Create a workspace
  int rc = tiledb_workspace_create(tiledb_ctx, "my_workspace");
  if(rc == TILEDB_OK)
    printf("Workspace created successfully!\n");
  else if(rc == TILEDB_ERR)
    printf("%s\n", tiledb_errmsg);

  // Create the same workspace again - ERROR
  rc = tiledb_workspace_create(tiledb_ctx, "my_workspace");
  if(rc == TILEDB_OK)
    printf("Workspace created successfully!\n");
  else if(rc == TILEDB_ERR)
    printf("%s\n", tiledb_errmsg); // Print the TileDB error message

  // Finalize context
  tiledb_ctx_finalize(tiledb_ctx);

  return 0;
}
