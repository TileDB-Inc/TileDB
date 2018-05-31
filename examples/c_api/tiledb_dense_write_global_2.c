/**
 * @file   tiledb_dense_write_global_2.c
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
 * This example shows how to populate an entire array using two write queries,
 * confirming that the write query maintains some state. It is again assumed
 * that the user knows the global cell order and provides the values to
 * TileDB in that order.
 *
 * You need to run the following to make this work:
 *
 * ```
 * $ ./tiledb_dense_create_c
 * $ ./tiledb_dense_write_global_2_c
 * ```
 *
 * The resulting array is identical to that in example
 * `tiledb_dense_write_global_1.c`. Moreover, note that there is a
 * **single fragment** produced; the second write essentially **appends**
 * to the existing fragment.
 */

#include <stdio.h>
#include <tiledb/tiledb.h>

int main() {
  // Create TileDB context
  tiledb_ctx_t* ctx;
  tiledb_ctx_alloc(&ctx, NULL);

  // Open array
  tiledb_array_t* array;
  tiledb_array_alloc(ctx, "my_dense_array", &array);
  tiledb_array_open(ctx, array, TILEDB_WRITE);

  // Prepare cell buffers - #1
  // This time, we populate the buffers with only some portion of the
  // entire array.
  int buffer_a1[] = {0, 1, 2, 3, 4, 5};
  uint64_t buffer_a2[] = {0, 1, 3, 6, 10, 11, 13, 16};
  char buffer_var_a2[] = "abbcccddddeffggghhhh";
  float* buffer_a3 = NULL;
  void* buffers[] = {buffer_a1, buffer_a2, buffer_var_a2, buffer_a3};
  uint64_t buffer_sizes[] = {
      6 * sizeof(int),  // 6 cells on a1
      8 * sizeof(uint64_t),
      20,  // 8 cells on a2
      0    // no cells on a3
  };

  // Create query
  tiledb_query_t* query;
  const char* attributes[] = {"a1", "a2", "a3"};
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

  // Submit query - #1
  // This writes to the array only partially, keeping the **same fragment**
  // open and maintaining appropriate state.
  tiledb_query_submit(ctx, query);

  // Prepare cell buffers - #2
  // Populate new buffers with the remaining cells of the array.
  int buffer_a1_2[] = {6, 7, 8, 9, 10, 11, 12, 13, 14, 15};
  uint64_t buffer_a2_2[] = {0, 1, 3, 6, 10, 11, 13, 16};
  char buffer_var_a2_2[] = "ijjkkkllllmnnooopppp";
  float buffer_a3_2[] = {
      0.1f,  0.2f,  1.1f,  1.2f,  2.1f,  2.2f,  3.1f,  3.2f,
      4.1f,  4.2f,  5.1f,  5.2f,  6.1f,  6.2f,  7.1f,  7.2f,
      8.1f,  8.2f,  9.1f,  9.2f,  10.1f, 10.2f, 11.1f, 11.2f,
      12.1f, 12.2f, 13.1f, 13.2f, 14.1f, 14.2f, 15.1f, 15.2f,
  };
  void* buffers_2[] = {buffer_a1_2, buffer_a2_2, buffer_var_a2_2, buffer_a3_2};
  uint64_t buffer_sizes_2[] = {
      10 * sizeof(int),  // 6 cells on a1
      8 * sizeof(uint64_t),
      20,                 // 8 cells on a2
      32 * sizeof(float)  // 16 cells on a3 (2 values each)
  };

  // Reset buffers. Alternatively, instead of resetting the buffers, we could
  // repopulate the original buffers with the new cells. The result would
  // have been the same.
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
