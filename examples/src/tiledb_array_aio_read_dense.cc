/**
 * @file   tiledb_array_aio_read_dense_1.cc
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
 * It shows how to read asynchronously from a dense array.
 */

#include "tiledb.h"
#include <cstdio>
#include <cstring>

// Simply prints the input string to stdout
void *print_upon_completion(void* s) {
  printf("%s\n", (char*) s);

  return NULL;
}

int main() {
  // Initialize context with the default configuration parameters
  TileDB_CTX* tiledb_ctx;
  tiledb_ctx_init(&tiledb_ctx, NULL);

  // Initialize array 
  TileDB_Array* tiledb_array;
  tiledb_array_init(
      tiledb_ctx,                                       // Context
      &tiledb_array,                                    // Array object
      "my_workspace/dense_arrays/my_array_A",           // Array name
      TILEDB_ARRAY_READ,                                // Mode
      NULL,                                             // Whole domain
      NULL,                                             // All attributes
      0);                                               // Number of attributes

  // Prepare subarray
  int64_t subarray[] = { 3, 4, 2, 4 }; // [3,4] on first dim, [2,4] on second

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

  // Prepare AIO request
  TileDB_AIO_Request tiledb_aio_request;
  memset(&tiledb_aio_request, 0, sizeof(struct TileDB_AIO_Request)); 
  tiledb_aio_request.buffers_ = buffers;
  tiledb_aio_request.buffer_sizes_ = buffer_sizes;
  tiledb_aio_request.subarray_ = subarray;
  tiledb_aio_request.completion_handle_ = print_upon_completion;
  char s[100] = "AIO request completed";
  tiledb_aio_request.completion_data_ = s; 

  // Read from array
  tiledb_array_aio_read(tiledb_array, &tiledb_aio_request); 

  // Wait for AIO to complete
  printf("AIO in progress\n");
  while(tiledb_aio_request.status_ != TILEDB_AIO_COMPLETED); 

  // Print cell values
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
  tiledb_ctx_finalize(tiledb_ctx);

  return 0;
}
