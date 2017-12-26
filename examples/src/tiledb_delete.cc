/**
 * @file   tiledb_delete.cc
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
 * It shows how to move/rename a TileDB resource.
 */

#include <tiledb.h>
#include <iostream>

int main() {
  // Create context
  tiledb_ctx_t* ctx;
  tiledb_ctx_create(&ctx, nullptr);

  // Deletes a valid group and array
  tiledb_delete(ctx, "my_group");
  tiledb_delete(ctx, "my_dense_array");

  // Deletes an invalid path
  int rc = tiledb_delete(ctx, "some_invalid_path");
  if (rc == TILEDB_ERR)
    std::cout << "Failed deleting invalid path\n";

  // Clean up
  tiledb_ctx_free(ctx);

  return 0;
}
