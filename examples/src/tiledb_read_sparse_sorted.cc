/**
 * @file   tiledb_read_sparse_sorted.cc
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

  // Prepare cell buffers
  int buffer_a1[2];
  void* buffers[] = { buffer_a1 };
  uint64_t buffer_sizes[] = { sizeof(buffer_a1) };

  // Create query
  tiledb_query_t* query;
  tiledb_query_create(
    ctx,
    &query,
    "my_group/sparse_arrays/my_array_B",
    TILEDB_READ_SORTED_ROW,
    subarray,
    attributes,
    1,
    buffers,
    buffer_sizes);

  // Loop until no overflow
  printf(" a1\n----\n");
  int overflow;
  do {
    printf("Reading cells...\n"); 

    // Read from array
    tiledb_query_submit(ctx, query);

    // Print cell values
    int64_t result_num = buffer_sizes[0] / sizeof(int);
    for(int i=0; i<result_num; ++i) { 
// TODO      if(buffer_a1[i] != TILEDB_EMPTY_INT32) // Check for deletion
        printf("%3d\n", buffer_a1[i]);
    }

    // Get overflow
    tiledb_query_get_overflow(ctx, query, "a1", &overflow);
  } while(overflow);

  // Clean up
  tiledb_query_free(query);
  tiledb_ctx_free(ctx);

  return 0;
}
