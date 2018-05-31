/**
 * @file   tiledb_sparse_read_subset.c
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2018 TileDB, Inc.
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
 * This example shows how to read from a sparse array, constraining the read
 * to a specific subarray and a subset of attributes. Moreover, the
 * program shows how to handle queries that did not complete
 * because the input buffers were not big enough to hold the entire
 * result.
 *
 * You need to run the following to make it work:
 *
 * ```
 * $ ./tiledb_sparse_create_c
 * $ ./tiledb_sparse_write_global_1_c
 * $ ./tiledb_sparse_read_subset_c
 * a1
 * ---
 * 5
 * 6
 * 7
 * ```
 *
 * The query returns the subarray depicted in blue in figure
 * `<TileDB-repo>/examples/figures/sparse_subarray.png`.
 *
 * The program prints the cell values of `a1` in the subarray in column-major
 * order.
 */

#include <tiledb/tiledb.h>

int main() {
  // Create TileDB context
  tiledb_ctx_t* ctx;
  tiledb_ctx_alloc(&ctx, NULL);

  // Open array
  tiledb_array_t* array;
  tiledb_array_alloc(ctx, "my_sparse_array", &array);
  tiledb_array_open(ctx, array, TILEDB_READ);

  // Prepare cell buffers. Notice that this time we prepare a buffer only for
  // `a1` (as we will not be querying the rest of the attributes) and we assign
  // space that **will not** be able to hold the entire result.
  int buffer_a1[3];
  void* buffers[] = {buffer_a1};
  uint64_t buffer_sizes[] = {sizeof(buffer_a1)};

  // Create the query, which focuses on subarray `[3,4], [2,4]` and attribute
  // `a1`. Also notice that we set the layout to `TILEDB_COL_MAJOR`, which will
  // retrieve the cells in column-major order within the selected subarray.
  tiledb_query_t* query;
  const char* attributes[] = {"a1"};
  uint64_t subarray[] = {3, 4, 2, 4};
  tiledb_query_alloc(ctx, array, TILEDB_READ, &query);
  tiledb_query_set_layout(ctx, query, TILEDB_COL_MAJOR);
  tiledb_query_set_subarray(ctx, query, subarray);
  tiledb_query_set_buffer(
      ctx, query, attributes[0], buffers[0], &buffer_sizes[0]);
  tiledb_query_submit(ctx, query);

  // Print the results
  printf("a1\n---\n");
  uint64_t result_num = buffer_sizes[0] / sizeof(int);
  for (uint64_t i = 0; i < result_num; ++i)
    printf("%d\n", buffer_a1[i]);

  // Finalize query
  tiledb_query_finalize(ctx, query);

  // Close array
  tiledb_array_close(ctx, array);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
  tiledb_ctx_free(&ctx);

  return 0;
}
