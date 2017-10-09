/**
 * @file   tiledb_3d_sparse_read.cc
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
 * It reads a subarray from the 3d sparse array ARRAYNAME.
 */

#include "tiledb.h"

#include <iostream>

#define MAX_CELL_NUM 1000
#define DIM_NUM 3
#define ARRAYNAME "3d_sparse_array"

int main() {
  // Initialize context with the default configuration parameters
  tiledb_ctx_t* ctx;
  tiledb_ctx_create(&ctx);

  // Prepare cell buffers
  int64_t* coords = new int64_t[DIM_NUM * MAX_CELL_NUM];
  int* a1 = new int[MAX_CELL_NUM];
  void* buffers[] = {a1, coords};
  uint64_t buffer_sizes[] = {DIM_NUM * MAX_CELL_NUM * sizeof(int64_t),
                             MAX_CELL_NUM * sizeof(int)};

  // Choose a subarray in (low, high) pairs per dimension
  int64_t subarray[] = {1, 10000, 5000, 10000, 1, 10000};

  // Create query
  tiledb_query_t* query;
  tiledb_query_create(
      ctx,
      &query,
      ARRAYNAME,
      TILEDB_READ,
      TILEDB_ROW_MAJOR,
      subarray,
      nullptr,
      0,
      buffers,
      buffer_sizes);

  // Submit query
  tiledb_query_submit(ctx, query);

  // Print cell values
  int64_t result_num = buffer_sizes[0] / sizeof(int);
  printf("coords\t\t         a1\n");
  printf("-----------------------------\n");
  for (int i = 0; i < result_num; ++i) {
    printf(
        "(%lld, %lld, %lld)",
        coords[3 * i],
        coords[3 * i + 1],
        coords[3 * i + 2]);
    printf("\t %3d\n", a1[i]);
  }

  // Clean up
  tiledb_ctx_free(ctx);
  delete[] coords;
  delete[] a1;
  return 0;
}
