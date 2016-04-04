/**
 * @file   tiledb_array_consolidate.cc
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
 * It shows how to consolidate arrays.
 */

#include "c_api.h"

int main() {
  // Intialize context with the default configuration parameters
  TileDB_CTX* tiledb_ctx;
  tiledb_ctx_init(&tiledb_ctx, NULL);

  // ----- Dense array

  // Initialize array
  TileDB_Array* tiledb_array;
  tiledb_array_init(                     
      tiledb_ctx,                              // Context
      &tiledb_array,                           // Array object
      "my_workspace/dense_arrays/my_array_A",  // Array name
      TILEDB_ARRAY_WRITE,                      // Mode
      NULL,                                    // Entire domain
      NULL,                                    // All attributes
      0);                                      // Number of attributes

  // Consolidate array
  tiledb_array_consolidate(tiledb_array); 

  // Finalize array
  tiledb_array_finalize(tiledb_array);

  // ----- Sparse array

  // Initialize array
  tiledb_array_init(                     
      tiledb_ctx,                              // Context
      &tiledb_array,                           // Array object
      "my_workspace/sparse_arrays/my_array_B", // Array name
      TILEDB_ARRAY_WRITE,                      // Mode
      NULL,                                    // Entire domain
      NULL,                                    // All attributes
      0);                                      // Number of attributes

  // Consolidate array
  tiledb_array_consolidate(tiledb_array); 

  // Finalize array
  tiledb_array_finalize(tiledb_array);

  // Finalize context
  tiledb_ctx_finalize(tiledb_ctx);

  return 0;
}
