/**
 * @file   tiledb_config.cc
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
 * It shows how to set the TileDB configuration parameters.
 */

#include "tiledb.h"
#include <cstring>

int main() {
  /* Create a TileDB configuration. */
  TileDB_Config tiledb_config;
  /* 
   * IMPORTANT: You need to zero out the members of the config structure if you
   * are setting only a subset of them, so that the rest can take default 
   * values.
   */
  memset(&tiledb_config, 0, sizeof(struct TileDB_Config));
  tiledb_config.home_ = "."; // TileDB home will be the current directory
  tiledb_config.read_method_ = TILEDB_IO_READ; // OS read instead of mmap

  // Initialize context with the default configuration parameters
  TileDB_CTX* tiledb_ctx;
  tiledb_ctx_init(&tiledb_ctx, &tiledb_config);

  /* --- Your code here --- */

  /* Finalize context. */
  tiledb_ctx_finalize(tiledb_ctx);

  return 0;
}
