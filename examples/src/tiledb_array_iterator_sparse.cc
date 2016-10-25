/**
 * @file   tiledb_array_iterator_sparse.cc
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
 * It shows how to use an iterator for sparse arrays.
 */

#include "c_api.h"
#include <cstdio>

int main() {
  // Initialize context with the default configuration parameters
  TileDB_CTX* tiledb_ctx;
  tiledb_ctx_init(&tiledb_ctx, NULL);

  // Prepare cell buffers 
  int buffer_a1[3];
  void* buffers[] = { buffer_a1 };
  size_t buffer_sizes[] = { sizeof(buffer_a1) };

  // Subarray and attributes
  int64_t subarray[] = { 3, 4, 2, 4 }; 
  const char* attributes[] = { "a1" };

  // Initialize array 
  TileDB_ArrayIterator* tiledb_array_it;
  tiledb_array_iterator_init(
      tiledb_ctx,                                    // Context
      &tiledb_array_it,                              // Array iterator
      "my_workspace/sparse_arrays/my_array_B",       // Array name
      TILEDB_ARRAY_READ,                             // Mode
      subarray,                                      // Constrain in subarray
      attributes,                                    // Subset on attributes
      1,                                             // Number of attributes
      buffers,                                       // Buffers used internally
      buffer_sizes);                                 // Buffer sizes

  // Iterate over all values in subarray
  printf(" a1\n----\n");
  const int* a1_v;
  size_t a1_size;
  while(!tiledb_array_iterator_end(tiledb_array_it)) {
    // Get value
    tiledb_array_iterator_get_value(
        tiledb_array_it,     // Array iterator
        0,                   // Attribute id
        (const void**) &a1_v,// Value
        &a1_size);           // Value size (useful in variable-sized attributes)

    // Print value (if not a deletion)
    if(*a1_v != TILEDB_EMPTY_INT32)
      printf("%3d\n", *a1_v);

    // Advance iterator
    tiledb_array_iterator_next(tiledb_array_it);
  }
 
  // Finalize array 
  tiledb_array_iterator_finalize(tiledb_array_it);

  // Finalize context
  tiledb_ctx_finalize(tiledb_ctx);

  return 0;
}
