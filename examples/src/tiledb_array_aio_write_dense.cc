/**
 * @file   tiledb_array_aio_write_dense.cc
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
 * It shows how to write asynchronoulsy to a dense array. The case of sparse
 * array is similar.
 */

#include "tiledb.h"
#include <cstring>

// Simply prints the input string to stdout
void *print_upon_completion(void* s) {
  printf("%s\n", (char*) s);

  return nullptr;
}

int main() {
  // Initialize context with the default configuration parameters
  tiledb_ctx_t* ctx;
  tiledb_ctx_create(&ctx);

  // Initialize array
  tiledb_array_t* array;
  tiledb_array_init(
      ctx,                                // Context
      &array,                             // Array object
      "my_group/dense_arrays/my_array_A",        // Array name
      TILEDB_ARRAY_WRITE,                        // Mode
      nullptr,                                   // Entire domain
      nullptr,                                   // All attributes
      0);                                        // Number of attributes

  // Prepare cell buffers
  int buffer_a1[] = 
  {
      0,  1,  2,  3,                                     // Upper left tile 
      4,  5,  6,  7,                                     // Upper right tile
      8,  9,  10, 11,                                    // Lower left tile
      12, 13, 14, 15                                     // Lower right tile
  };
  size_t buffer_a2[] = 
  {
      0,  1,  3,  6,                                     // Upper left tile
      10, 11, 13, 16,                                    // Upper right tile
      20, 21, 23, 26,                                    // Lower left tile
      30, 31, 33, 36                                     // Lower right tile
  };
  char buffer_var_a2[] =
      "abbcccdddd"                                       // Upper left tile
      "effggghhhh"                                       // Upper right tile
      "ijjkkkllll"                                       // Lower left tile
      "mnnooopppp";                                      // Lower right tile
  float buffer_a3[] = 
  {
      0.1,  0.2,  1.1,  1.2,  2.1,  2.2,  3.1,  3.2,     // Upper left tile
      4.1,  4.2,  5.1,  5.2,  6.1,  6.2,  7.1,  7.2,     // Upper right tile
      8.1,  8.2,  9.1,  9.2,  10.1, 10.2, 11.1, 11.2,    // Lower left tile
      12.1, 12.2, 13.1, 13.2, 14.1, 14.2, 15.1, 15.2,    // Lower right tile
  };
  void* buffers[] = { buffer_a1, buffer_a2, buffer_var_a2, buffer_a3 };
  size_t buffer_sizes[] = 
  { 
      sizeof(buffer_a1),  
      sizeof(buffer_a2),
      sizeof(buffer_var_a2)-1,  // No need to store the last '\0' character
      sizeof(buffer_a3)
  };

  // Prepare AIO request
  char s[100] = "AIO request completed";
  tiledb_aio_request_t* aio_request;
  tiledb_aio_request_create(ctx, &aio_request);
  tiledb_aio_request_set_array(ctx, aio_request, array);
  tiledb_aio_request_set_buffers(ctx, aio_request, buffers, buffer_sizes);
  tiledb_aio_request_set_callback(ctx, aio_request, print_upon_completion, s);

  // Submit request
  tiledb_array_aio_submit(ctx, aio_request);

  // Wait for AIO to complete
  printf("AIO in progress\n");
  tiledb_aio_status_t aio_status;
  do {
    tiledb_aio_request_get_status(ctx, aio_request, &aio_status);
  } while(aio_status != TILEDB_AIO_COMPLETED);

  // Clean up
  tiledb_array_finalize(array);
  tiledb_ctx_free(ctx);
  tiledb_aio_request_free(aio_request);

  return 0;
}
