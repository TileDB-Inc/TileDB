/**
 * @file   tiledb_array_read_dense_3.cc
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
 * It shows how to read from a dense array, resetting attributes and subarray.
 */

#include "c_api.h"
#include <cstdio>

int main() {
  // Initialize context with the default configuration parameters
  TileDB_CTX* tiledb_ctx;
  tiledb_ctx_init(&tiledb_ctx, NULL);

  // Subarray and attributes
  int64_t subarray[] = { 3, 3, 2, 2 }; 
  int64_t subarray_2[] = { 4, 4, 3, 3 }; 
  const char* attributes[] = { "a1" };
  const char* attributes_2[] = { "a2" };

  // Initialize array 
  TileDB_Array* tiledb_array;
  tiledb_array_init(
      tiledb_ctx,                                       // Context
      &tiledb_array,                                    // Array object
      "my_workspace/dense_arrays/my_array_A",           // Array name
      TILEDB_ARRAY_READ,                                // Mode
      subarray,                                         // Constrain in subarray
      attributes,                                       // Subset on attributes
      1);                                               // Number of attributes

  // Prepare cell buffers 
  int buffer_a1[1];
  size_t buffer_a2[1];
  char buffer_var_a2[10];
  void* buffers[] = { buffer_a1 };
  void* buffers_2[] = { buffer_a2, buffer_var_a2 };
  size_t buffer_sizes[] = { sizeof(buffer_a1) };
  size_t buffer_sizes_2[] = { sizeof(buffer_a2), sizeof(buffer_var_a2) };

  // Read from array - #1
  tiledb_array_read(tiledb_array, buffers, buffer_sizes); 
  printf("a1 for (3,2): %3d\n", buffer_a1[0]);

  // Reset subarray and attributes
  tiledb_array_reset_subarray(tiledb_array, subarray_2);
  tiledb_array_reset_attributes(tiledb_array, attributes_2, 1);

  // Read from array - #2
  tiledb_array_read(tiledb_array, buffers_2, buffer_sizes_2); 
  printf("a2 for (4,3): %3.*s\n", int(buffer_sizes_2[1]), buffer_var_a2);
 
  // Finalize the array
  tiledb_array_finalize(tiledb_array);

  /* Finalize context. */
  tiledb_ctx_finalize(tiledb_ctx);

  return 0;
}
