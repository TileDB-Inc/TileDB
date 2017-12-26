/**
 * @file   tiledb_sparse_write_unordered_1_again.cc
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
 * It shows how to write unordered cells to a sparse array in a single write.
 * This time we write 4 cells.
 *
 * You need to run the following to make this work:
 *
 * ./tiledb_sparse_create
 * ./tiledb_sparse_write_unordered_1_again
 */

#include <tiledb.h>

int main() {
  // Create TileDB context
  tiledb_ctx_t* ctx;
  tiledb_ctx_create(&ctx, nullptr);

  // Set attributes
  const char* attributes[] = {"a1", "a2", "a3", TILEDB_COORDS};

  // Prepare cell buffers
  int buffer_a1[] = {107, 104, 106, 105};
  uint64_t buffer_a2[] = {0, 3, 4, 5};
  char buffer_var_a2[] = "yyyuwvvvv";
  float buffer_a3[] = {107.1, 107.2, 104.1, 104.2, 106.1, 106.2, 105.1, 105.2};
  int64_t buffer_coords[] = {3, 4, 3, 2, 3, 3, 4, 1};
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
  tiledb_query_create(ctx, &query, "my_sparse_array", TILEDB_WRITE);
  tiledb_query_set_buffers(ctx, query, attributes, 4, buffers, buffer_sizes);
  tiledb_query_set_layout(ctx, query, TILEDB_UNORDERED);

  // Submit query
  tiledb_query_submit(ctx, query);

  // Clean up
  tiledb_query_free(ctx, query);
  tiledb_ctx_free(ctx);

  return 0;
}
