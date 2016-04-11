/**
 * @file   tiledb_array_write_sparse_4.cc
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
 * It shows how to write unsorted cells to a sparse array with two batch writes.
 */

#include "c_api.h"

int main() {
  // Initialize context with the default configuration parameters
  TileDB_CTX* tiledb_ctx;
  tiledb_ctx_init(&tiledb_ctx, NULL);

  // Initialize array
  TileDB_Array* tiledb_array;
  tiledb_array_init(
      tiledb_ctx,                                // Context 
      &tiledb_array,                             // Array object
      "my_workspace/sparse_arrays/my_array_B",   // Array name
      TILEDB_ARRAY_WRITE_UNSORTED,               // Mode
      NULL,                                      // Entire domain
      NULL,                                      // All attributes
      0);                                        // Number of attributes

  // Prepare cell buffers - #1
  int buffer_a1[] = { 7, 5, 0 };
  size_t buffer_a2[] = { 0, 4, 6 };
  const char buffer_var_a2[] = "hhhhffa";
  float buffer_a3[] = { 7.1,  7.2,  5.1,  5.2,  0.1,  0.2 };
  int64_t buffer_coords[] = { 3, 1, 3, 4, 1, 1 };
  const void* buffers[] = 
      { buffer_a1, buffer_a2, buffer_var_a2, buffer_a3, buffer_coords };
  size_t buffer_sizes[] = 
  { 
      sizeof(buffer_a1),  
      sizeof(buffer_a2),
      sizeof(buffer_var_a2)-1,  // No need to store the last '\0' character
      sizeof(buffer_a3),
      sizeof(buffer_coords)
  };

  // Write to array - #1
  tiledb_array_write(tiledb_array, buffers, buffer_sizes); 

  // Prepare cell buffers - #2
  int buffer_a1_2[] = { 6, 4, 3, 1, 2 };
  size_t buffer_a2_2[] = { 0, 3, 4, 8, 10 };
  const char buffer_var_a2_2[] = "gggeddddbbccc";
  float buffer_a3_2[] = 
      { 6.1,  6.2, 4.1,  4.2,  3.1,  3.2,  1.1,  1.2,  2.1,  2.2 };
  int64_t buffer_coords_2[] = { 4, 2, 3, 3, 2, 3, 1, 2, 1, 4 };
  const void* buffers_2[] = 
  { 
      buffer_a1_2,  
      buffer_a2_2, 
      buffer_var_a2_2,  
      buffer_a3_2, 
      buffer_coords_2 
  };
  size_t buffer_sizes_2[] = 
  { 
      sizeof(buffer_a1_2),  
      sizeof(buffer_a2_2),
      sizeof(buffer_var_a2_2)-1,  // No need to store the last '\0' character
      sizeof(buffer_a3_2),
      sizeof(buffer_coords_2)
  };

  // Write to array - #2
  tiledb_array_write(tiledb_array, buffers_2, buffer_sizes_2); 

  // Finalize array
  tiledb_array_finalize(tiledb_array);

  // Finalize context
  tiledb_ctx_finalize(tiledb_ctx);

  return 0;
}
