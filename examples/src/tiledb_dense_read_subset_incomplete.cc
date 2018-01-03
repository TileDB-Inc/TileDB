/**
 * @file   tiledb_dense_read_subset_incomplete.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017 TileDB, Inc.
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
 * It shows how to read from a dense array, constraining the read
 * to a specific subarray and a subset of attributes. Moreover, the
 * program shows how to handle incomplete queries that did not complete
 * because the input buffers were not big enough to hold the entire
 * result.
 */

#include <tiledb.h>
#include <cstdio>

int main() {
  // Create TileDB context
  tiledb_ctx_t* ctx;
  tiledb_ctx_create(&ctx, nullptr);

  // Attributes to subset on
  const char* attributes[] = {"a1"};

  // Prepare cell buffers
  int buffer_a1[2];
  void* buffers[] = {buffer_a1};
  uint64_t buffer_sizes[] = {sizeof(buffer_a1)};

  // Create query
  uint64_t subarray[] = {3, 4, 2, 4};
  tiledb_query_t* query;
  tiledb_query_create(ctx, &query, "my_dense_array", TILEDB_READ);
  tiledb_query_set_subarray(ctx, query, subarray, TILEDB_UINT64);
  tiledb_query_set_buffers(ctx, query, attributes, 1, buffers, buffer_sizes);
  tiledb_query_set_layout(ctx, query, TILEDB_COL_MAJOR);

  // Loop until the query is completed
  printf(" a1\n----\n");
  tiledb_query_status_t status;
  do {
    printf("Reading cells...\n");
    tiledb_query_submit(ctx, query);

    // Print cell values
    uint64_t result_num = buffer_sizes[0] / sizeof(int);
    for (uint64_t i = 0; i < result_num; ++i)
      printf("%3d\n", buffer_a1[i]);

    // Get status
    tiledb_query_get_attribute_status(ctx, query, "a1", &status);
  } while (status == TILEDB_INCOMPLETE);

  // Clean up
  tiledb_query_free(ctx, query);
  tiledb_ctx_free(ctx);

  return 0;
}
