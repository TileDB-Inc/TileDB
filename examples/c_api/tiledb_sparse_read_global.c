/**
 * @file   tiledb_read_global.c
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
 * This example shows how to read the entire array we created in the global
 * cell order. It assumes that we have already created the 2D sparse
 * array from the previous examples.
 *
 * You need to run the following to make it work:
 *
 * ```
 * $ ./tiledb_sparse_create_c
 * $ ./tiledb_sparse_write_global_1_c
 * $ ./tiledb_sparse_read_global_c
 * Non-empty domain:
 * d1: (1, 4)
 * d2: (1, 4)
 *
 * Maximum buffer sizes:
 * a1: 32
 * a2: (64, 20)
 * a3: 64
 * __coords: 128
 *
 * Result num: 8
 *
 * __coords       a1       a2      a3[0]     a3[1]
 * -------------------------------------------------
 * (1, 1)         0         a       0.1       0.2
 * (1, 2)         1        bb       1.1       1.2
 * (1, 4)         2       ccc       2.1       2.2
 * (2, 3)         3      dddd       3.1       3.2
 * (3, 1)         4         e       4.1       4.2
 * (4, 2)         5        ff       5.1       5.2
 * (3, 3)         6       ggg       6.1       6.2
 * (3, 4)         7      hhhh       7.1       7.2
 * ```
 */

#include <stdio.h>
#include <stdlib.h>
#include <tiledb/tiledb.h>

int main() {
  // Create TileDB context
  tiledb_ctx_t* ctx;
  tiledb_ctx_alloc(&ctx, NULL);

  // Open array
  tiledb_array_t* array;
  tiledb_array_alloc(ctx, "my_sparse_array", &array);
  tiledb_array_open(ctx, array, TILEDB_READ);

  // Print non-empty domain
  int is_empty = 0;
  uint64_t domain[4];
  tiledb_array_get_non_empty_domain(ctx, array, domain, &is_empty);
  printf("Non-empty domain:\n");
  printf(
      "d1: (%llu, %llu)\n",
      (unsigned long long)domain[0],
      (unsigned long long)domain[1]);
  printf(
      "d2: (%llu, %llu)\n\n",
      (unsigned long long)domain[2],
      (unsigned long long)domain[3]);

  // Print maximum buffer sizes for each attribute
  const char* attributes[] = {"a1", "a2", "a3", TILEDB_COORDS};
  uint64_t buffer_sizes[5];
  uint64_t subarray[] = {1, 4, 1, 4};
  tiledb_array_compute_max_read_buffer_sizes(
      ctx, array, subarray, attributes, 4, &buffer_sizes[0]);
  printf("Maximum buffer sizes:\n");
  printf("a1: %llu\n", (unsigned long long)buffer_sizes[0]);
  printf(
      "a2: (%llu, %llu)\n",
      (unsigned long long)buffer_sizes[1],
      (unsigned long long)buffer_sizes[2]);
  printf("a3: %llu\n", (unsigned long long)buffer_sizes[3]);
  printf("%s: %llu\n\n", TILEDB_COORDS, (unsigned long long)buffer_sizes[4]);

  // Prepare cell buffers
  int* buffer_a1 = malloc(buffer_sizes[0]);
  uint64_t* buffer_a2 = malloc(buffer_sizes[1]);
  char* buffer_var_a2 = malloc(buffer_sizes[2]);
  float* buffer_a3 = malloc(buffer_sizes[3]);
  uint64_t* buffer_coords = malloc(buffer_sizes[4]);
  void* buffers[] = {
      buffer_a1, buffer_a2, buffer_var_a2, buffer_a3, buffer_coords};

  // We create a read query, specifying the layout of the results as
  // `TILEDB_GLOBAL_ORDER`. Notice also that we have not set the `subarray`
  // for the query, which means that we wish to get all the array cells.
  tiledb_query_t* query;

  tiledb_query_alloc(ctx, array, TILEDB_READ, &query);
  tiledb_query_set_layout(ctx, query, TILEDB_GLOBAL_ORDER);
  tiledb_query_set_buffer(
      ctx, query, attributes[0], buffers[0], &buffer_sizes[0]);
  tiledb_query_set_buffer_var(
      ctx,
      query,
      attributes[1],
      buffers[1],
      &buffer_sizes[1],
      buffers[2],
      &buffer_sizes[2]);
  tiledb_query_set_buffer(
      ctx, query, attributes[2], buffers[3], &buffer_sizes[3]);
  tiledb_query_set_buffer(
      ctx, query, attributes[3], buffers[4], &buffer_sizes[4]);

  // Submit query
  tiledb_query_submit(ctx, query);

  // Print cell values (assumes all attributes are read)
  uint64_t result_num = buffer_sizes[0] / sizeof(int);
  printf("Result num: %llu\n\n", (unsigned long long)result_num);
  printf("%8s%9s%9s%11s%10s\n", TILEDB_COORDS, "a1", "a2", "a3[0]", "a3[1]");
  printf("-------------------------------------------------\n");
  for (uint64_t i = 0; i < result_num; ++i) {
    printf(
        "(%lld, %lld)",
        (long long int)buffer_coords[2 * i],
        (long long int)buffer_coords[2 * i + 1]);
    printf("%10d", buffer_a1[i]);
    uint64_t var_size = (i != result_num - 1) ?
                            buffer_a2[i + 1] - buffer_a2[i] :
                            buffer_sizes[2] - buffer_a2[i];
    printf("%10.*s", (int)var_size, &buffer_var_a2[buffer_a2[i]]);
    printf("%10.1f%10.1f\n", buffer_a3[2 * i], buffer_a3[2 * i + 1]);
  }

  // Finalize query
  tiledb_query_finalize(ctx, query);

  // Close array
  tiledb_array_close(ctx, array);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
  tiledb_ctx_free(&ctx);
  free(buffer_a1);
  free(buffer_a2);
  free(buffer_var_a2);
  free(buffer_a3);
  free(buffer_coords);

  return 0;
}
