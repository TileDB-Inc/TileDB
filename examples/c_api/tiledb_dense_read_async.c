/**
 * @file   tiledb_dense_read_async.c
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
 * This example shows how to read asynchronoulsy from a dense array. The case
 * of sparse arrays is similar.
 *
 * Run the following:
 *
 * ```
 * $ ./tiledb_dense_create_c
 * $ ./tiledb_dense_write_async_c
 * $ ./tiledb_dense_read_async_c
 * Query in progress
 * Result num: 16
 *
 * Callback: Query completed
 *    a1        a2     a3[0]     a3[1]
 * -----------------------------------------
 *    0         a       0.1       0.2
 *    1        bb       1.1       1.2
 *    2       ccc       2.1       2.2
 *    3      dddd       3.1       3.2
 *    4         e       4.1       4.2
 *    5        ff       5.1       5.2
 *    6       ggg       6.1       6.2
 *    7      hhhh       7.1       7.2
 *    8         i       8.1       8.2
 *    9        jj       9.1       9.2
 *   10       kkk      10.1      10.2
 *   11      llll      11.1      11.2
 *   12         m      12.1      12.2
 *   13        nn      13.1      13.2
 *   14       ooo      14.1      14.2
 *   15      pppp      15.1      15.2
 * ```
 *
 * Observe that `Query in progress` is printed out first and then the
 * main function waits for TileDB to process the query. When the query is
 * completed, the callback function is called, which prints
 * `Callback: Query completed` along with the results (the fact that
 * `Callback: Query completed` may be interleaved with the result printing
 * is because two threads are printing at the same time).
 */

#include <stdio.h>
#include <stdlib.h>
#include <tiledb/tiledb.h>

// Simply prints the input string to stdout
void print_upon_completion(void* s) {
  printf("%s\n", (char*)s);
}

int main() {
  // Create TileDB context
  tiledb_ctx_t* ctx;
  tiledb_ctx_alloc(&ctx, NULL);

  // Open array
  tiledb_array_t* array;
  tiledb_array_alloc(ctx, "my_dense_array", &array);
  tiledb_array_open(ctx, array, TILEDB_READ);

  // Calculate maximum buffer sizes for each attribute
  const char* attributes[] = {"a1", "a2", "a3"};
  uint64_t buffer_sizes[4];
  uint64_t subarray[] = {1, 4, 1, 4};
  tiledb_array_compute_max_read_buffer_sizes(
      ctx, array, subarray, attributes, 3, &buffer_sizes[0]);

  // Prepare cell buffers
  int* buffer_a1 = malloc(buffer_sizes[0]);
  uint64_t* buffer_a2 = malloc(buffer_sizes[1]);
  char* buffer_var_a2 = malloc(buffer_sizes[2]);
  float* buffer_a3 = malloc(buffer_sizes[3]);
  void* buffers[] = {buffer_a1, buffer_a2, buffer_var_a2, buffer_a3};

  // Create query
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

  // Submit query with callback
  char s[100] = "Callback: Query completed";
  tiledb_query_submit_async(ctx, query, print_upon_completion, s);

  // Wait for the query to complete. We can check the status of the query with
  // API calls. The status is "in progress" for as long as the query is
  // executing, which becomes "completed" as soon as the query completes.
  printf("Query in progress\n");
  tiledb_query_status_t status;
  do {
    tiledb_query_get_status(ctx, query, &status);
  } while (status != TILEDB_COMPLETED);

  // Print cell values (assumes all attributes are read)
  uint64_t result_num = buffer_sizes[0] / sizeof(int);
  printf("Result num: %llu\n\n", (unsigned long long)result_num);
  printf("%5s%10s%10s%10s\n", "a1", "a2", "a3[0]", "a3[1]");
  printf("-----------------------------------------\n");
  for (uint64_t i = 0; i < result_num; ++i) {
    printf("%5d", buffer_a1[i]);
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

  return 0;
}
