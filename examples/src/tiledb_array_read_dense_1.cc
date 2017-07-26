/**
 * @file   tiledb_array_read_dense_1.cc
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
 * It shows how to read a complete dense array.
 */

#include "tiledb.h"
#include <cstdio>

int main() {
  // Initialize context with the default configuration parameters
  tiledb_ctx_t* ctx;
  tiledb_ctx_create(&ctx);

  // Initialize array 
  tiledb_array_t* tiledb_array;
  tiledb_array_init(
      ctx,                                       // Context
      &tiledb_array,                                    // Array object
      "my_group/dense_arrays/my_array_A",               // Array name
      TILEDB_ARRAY_READ,                                // Mode
      nullptr,                                          // Whole domain
      nullptr,                                          // All attributes
      0);                                               // Number of attributes

  // Prepare cell buffers 
  int buffer_a1[16];
  size_t buffer_a2[16];
  char buffer_var_a2[40];
  float buffer_a3[32];
  void* buffers[] = { buffer_a1, buffer_a2, buffer_var_a2, buffer_a3 };
  size_t buffer_sizes[] = 
  { 
      sizeof(buffer_a1),  
      sizeof(buffer_a2),
      sizeof(buffer_var_a2),
      sizeof(buffer_a3)
  };

  // Read from array
  tiledb_array_read(tiledb_array, buffers, buffer_sizes); 

  // Print only non-empty cell values
  int64_t result_num = buffer_sizes[0] / sizeof(int);
  printf(" a1\t    a2\t   (a3.first, a3.second)\n");
  printf("-----------------------------------------\n");
  for(int i=0; i<result_num; ++i) {
    printf("%3d", buffer_a1[i]);
    size_t var_size = (i != result_num-1) ? buffer_a2[i+1] - buffer_a2[i]
                                            : buffer_sizes[2] - buffer_a2[i];
    printf("\t %4.*s", int(var_size), &buffer_var_a2[buffer_a2[i]]);
    printf("\t\t (%5.1f, %5.1f)\n", buffer_a3[2*i], buffer_a3[2*i+1]);
  }

  // Finalize the array
  tiledb_array_finalize(tiledb_array);

  /* Finalize context. */
  tiledb_ctx_free(ctx);

  return 0;
}
