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
 * It shows how to read a complete sparse array in the global cell order.
 *
 * You need to run the following to make it work:
 *
 * $ ./tiledb_sparse_create_c
 * $ ./tiledb_sparse_write_global_1_c
 * $ ./tiledb_sparse_read_global_c
 */

#include <stdio.h>
#include <tiledb.h>

int main() {
  // Create TileDB context
  tiledb_ctx_t* ctx;
  tiledb_ctx_create(&ctx, NULL);

  // Print non-empty domain
  int is_empty = 0;
  uint64_t domain[4];
  tiledb_array_get_non_empty_domain(ctx, "my_sparse_array", domain, &is_empty);
  printf("Non-empty domain:\n");
  printf("d1: (%llu, %llu)\n", domain[0], domain[1]);
  printf("d2: (%llu, %llu)\n\n", domain[2], domain[3]);

  // Print maximum buffer sizes for each attribute
  const char* attributes[] = {"a1", "a2", "a3", TILEDB_COORDS};
  uint64_t buffer_sizes[5];
  uint64_t subarray[] = {1, 4, 1, 4};
  tiledb_array_compute_max_read_buffer_sizes(
      ctx, "my_sparse_array", subarray, attributes, 4, &buffer_sizes[0]);
  printf("Maximum buffer sizes:\n");
  printf("a1: %llu\n", buffer_sizes[0]);
  printf("a2: (%llu, %llu)\n", buffer_sizes[1], buffer_sizes[2]);
  printf("a3: %llu\n", buffer_sizes[3]);
  printf("%s: %llu\n\n", TILEDB_COORDS, buffer_sizes[4]);

  // Prepare cell buffers
  int buffer_a1[buffer_sizes[0] / sizeof(int)];
  uint64_t buffer_a2[buffer_sizes[1] / sizeof(uint64_t)];
  char buffer_var_a2[buffer_sizes[2] / sizeof(char)];
  float buffer_a3[buffer_sizes[3] / sizeof(float)];
  uint64_t buffer_coords[buffer_sizes[4] / sizeof(uint64_t)];
  void* buffers[] = {
      buffer_a1, buffer_a2, buffer_var_a2, buffer_a3, buffer_coords};

  // Create query
  tiledb_query_t* query;
  tiledb_query_create(ctx, &query, "my_sparse_array", TILEDB_READ);
  tiledb_query_set_layout(ctx, query, TILEDB_GLOBAL_ORDER);
  tiledb_query_set_buffers(ctx, query, attributes, 4, buffers, buffer_sizes);

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

  // Clean up
  tiledb_query_free(ctx, query);
  tiledb_ctx_free(ctx);

  return 0;
}
