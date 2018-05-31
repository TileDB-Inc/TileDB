/**
 * @file   tiledb_sparse_write_global_2.c
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
 * This example shows how to write to a sparse array with two write queries,
 * assuming that the user provides the cells ordered in the array global cell
 * order.
 *
 * You need to run the following to make this work:
 *
 * ```
 * $ ./tiledb_sparse_create_c
 * $ ./tiledb_sparse_write_global_2_c
 * ```
 *
 * The resulting array is identical to that `tiledb_sparse_write_global_1.c`.
 * Moreover, note that there is a **single fragment** produced; the second
 * write essentially **appends** to the existing fragment.
 */

#include <tiledb/tiledb.h>

int main() {
  // Create TileDB context
  tiledb_ctx_t* ctx;
  tiledb_ctx_alloc(&ctx, NULL);

  // Open array
  tiledb_array_t* array;
  tiledb_array_alloc(ctx, "my_sparse_array", &array);
  tiledb_array_open(ctx, array, TILEDB_WRITE);

  // Prepare cell buffers - #1
  int buffer_a1[] = {0, 1, 2};
  uint64_t buffer_a2[] = {0, 1, 3, 6, 10, 11, 13, 16};
  char buffer_var_a2[] = "abbcccddddeffggghhhh";
  float buffer_a3[] = {0.1f,
                       0.2f,
                       1.1f,
                       1.2f,
                       2.1f,
                       2.2f,
                       3.1f,
                       3.2f,
                       4.1f,
                       4.2f,
                       5.1f,
                       5.2f,
                       6.1f,
                       6.2f,
                       7.1f,
                       7.2f};
  uint64_t buffer_coords[] = {1, 1, 1, 2};
  void* buffers[] = {
      buffer_a1, buffer_a2, buffer_var_a2, buffer_a3, buffer_coords};
  uint64_t buffer_sizes[] = {
      sizeof(buffer_a1),
      sizeof(buffer_a2),
      sizeof(buffer_var_a2) - 1,  // No need to store the last '\0' character
      sizeof(buffer_a3),
      sizeof(buffer_coords)};

  // Create query
  tiledb_query_t* query;
  const char* attributes[] = {"a1", "a2", "a3", TILEDB_COORDS};
  tiledb_query_alloc(ctx, array, TILEDB_WRITE, &query);
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

  // Submit query - #1
  tiledb_query_submit(ctx, query);

  // Prepare cell buffers - #2
  int buffer_a1_2[] = {3, 4, 5, 6, 7};
  uint64_t* buffer_a2_2 = NULL;
  char* buffer_var_a2_2 = NULL;
  float* buffer_a3_2 = NULL;
  uint64_t buffer_coords_2[] = {1, 4, 2, 3, 3, 1, 4, 2, 3, 3, 3, 4};
  void* buffers_2[] = {
      buffer_a1_2, buffer_a2_2, buffer_var_a2_2, buffer_a3_2, buffer_coords_2};
  uint64_t buffer_sizes_2[] = {
      sizeof(buffer_a1_2), 0, 0, 0, sizeof(buffer_coords_2)};

  // Reset buffers
  tiledb_query_set_buffer(
      ctx, query, attributes[0], buffers_2[0], &buffer_sizes_2[0]);
  tiledb_query_set_buffer_var(
      ctx,
      query,
      attributes[1],
      buffers_2[1],
      &buffer_sizes_2[1],
      buffers_2[2],
      &buffer_sizes_2[2]);
  tiledb_query_set_buffer(
      ctx, query, attributes[2], buffers_2[3], &buffer_sizes_2[3]);
  tiledb_query_set_buffer(
      ctx, query, attributes[3], buffers_2[4], &buffer_sizes_2[4]);

  // Submit query - #2
  tiledb_query_submit(ctx, query);

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
