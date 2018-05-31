/**
 * @file   tiledb_dense_write_unordered.c
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
 * This example shows how to write random (unordered) cells to a dense array.
 *
 * Make sure that there is no directory named `my_dense_array` in your
 * current working directory.
 *
 * You need to run the following to make this work:
 *
 * ```
 * $ ./tiledb_dense_create_c
 * $ ./tiledb_dense_write_unordered_c
 * ```
 *
 * The above will create a fragment that looks as in figure
 * `<TileDB-repo>/examples/figures/dense_write_unordered.png`.
 */

#include <tiledb/tiledb.h>

int main() {
  // Create TileDB context
  tiledb_ctx_t* ctx;
  tiledb_ctx_alloc(&ctx, NULL);

  // Open array
  tiledb_array_t* array;
  tiledb_array_alloc(ctx, "my_dense_array", &array);
  tiledb_array_open(ctx, array, TILEDB_WRITE);

  // We prepare buffers to write 4 cells on all three attributes. Observe
  // that now we need to prepare an extra buffer to specify the coordinates
  // of these cells, as TileDB otherwise would not know which (scattered)
  // cells we wish to write.
  int buffer_a1[] = {211, 213, 212, 208};
  uint64_t buffer_a2[] = {0, 4, 6, 7};
  char buffer_var_a2[] = "wwwwyyxu";
  float buffer_a3[] = {
      211.1f, 211.2f, 213.1f, 213.2f, 212.1f, 212.2f, 208.1f, 208.2f};
  uint64_t buffer_coords[] = {4, 2, 3, 4, 3, 3, 3, 1};
  void* buffers[] = {
      buffer_a1, buffer_a2, buffer_var_a2, buffer_a3, buffer_coords};
  uint64_t buffer_sizes[] = {
      sizeof(buffer_a1),
      sizeof(buffer_a2),
      sizeof(buffer_var_a2) - 1,  // No need to store the last '\0' character
      sizeof(buffer_a3),
      sizeof(buffer_coords)};

  // Create query. We do not need to specify any `subarray` argument. We need
  // to set the layout though to `TILEDB_UNORDERED`, so that TileDB knows that
  // we are writing random cells.
  tiledb_query_t* query;
  const char* attributes[] = {"a1", "a2", "a3", TILEDB_COORDS};
  tiledb_query_alloc(ctx, array, TILEDB_WRITE, &query);
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
  tiledb_query_set_layout(ctx, query, TILEDB_UNORDERED);

  // Submit query
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
