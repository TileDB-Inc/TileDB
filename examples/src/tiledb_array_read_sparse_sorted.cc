/**
 * @file   tiledb_array_read_sparse_sorted.cc
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
 * It shows how to read from a sparse array, constraining the read
 * to a specific subarray and subset of attributes. This time the cells are 
 * returned in row-major order within the specified subarray. 
 */

#include "tiledb.h"
#include <cstdio>

int main() {
  // Initialize context with the default configuration parameters
  tiledb_ctx_t* ctx;
  tiledb_ctx_create(&ctx);

  // Subarray and attributes
  int64_t subarray[] = { 3, 4, 2, 4 }; 
  const char* attributes[] = { "a1" };

  // Initialize array 
  tiledb_array_t* tiledb_array;
  tiledb_array_init(
      ctx,                                       // Context
      &tiledb_array,                                    // Array object
      "my_group/sparse_arrays/my_array_B",              // Array name
      TILEDB_ARRAY_READ_SORTED_ROW,                     // Mode
      subarray,                                         // Constrain in subarray
      attributes,                                       // Subset on attributes
      1);                                               // Number of attributes

  // Prepare cell buffers 
  int buffer_a1[2];
  void* buffers[] = { buffer_a1 };
  size_t buffer_sizes[] = { sizeof(buffer_a1) };


  // Loop until no overflow
  printf(" a1\n----\n");
  do {
    printf("Reading cells...\n"); 

    // Read from array
    tiledb_array_read(tiledb_array, buffers, buffer_sizes); 

    // Print cell values
    int64_t result_num = buffer_sizes[0] / sizeof(int);
    for(int i=0; i<result_num; ++i) { 
// TODO      if(buffer_a1[i] != TILEDB_EMPTY_INT32) // Check for deletion
        printf("%3d\n", buffer_a1[i]);
    }
  } while(tiledb_array_overflow(tiledb_array, 0) == 1);
 
  // Finalize the array
  tiledb_array_finalize(tiledb_array);

  /* Finalize context. */
  tiledb_ctx_free(ctx);

  return 0;
}
