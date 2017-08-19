/**
 * @file   tiledb_array_write_dense_sorted.cc
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
 * It shows how to write to a dense array, providing the array cells sorted
 * in row-major order within the specified subarray. TileDB will properly
 * re-organize the cells into the global cell order, prior to writing them
 * on the disk. 
 */

#include "tiledb.h"

int main() {
  // Initialize context with the default configuration parameters
  tiledb_ctx_t* ctx;
  tiledb_ctx_create(&ctx);

  // Set the subarray where the write will focus on
  int64_t subarray[] = { 2, 3, 2, 4 };

  // Initialize array
  tiledb_array_t* tiledb_array;
  tiledb_array_init(
      ctx,                                // Context
      &tiledb_array,                             // Array object
      "my_group/dense_arrays/my_array_A",        // Array name
      TILEDB_ARRAY_WRITE_SORTED_ROW,             // Mode
      subarray,                                  // Subarray
      nullptr,                                   // All attributes
      0);                                        // Number of attributes

  // Prepare cell buffers
  int buffer_a1[] = { 9, 12, 13, 11, 14, 15 };
  size_t buffer_a2[] = { 0, 2, 3, 5, 9, 12 };
  const char buffer_var_a2[] = "jjmnnllllooopppp";
  float buffer_a3[] = 
      { 
        9.1,  9.2,  12.1, 12.2, 13.1, 13.2, 
        11.1, 11.2, 14.1, 14.2, 15.1, 15.2
      };
  const void* buffers[] = { buffer_a1, buffer_a2, buffer_var_a2, buffer_a3 };
  size_t buffer_sizes[] = 
  { 
      sizeof(buffer_a1),  
      sizeof(buffer_a2),
      sizeof(buffer_var_a2)-1,  // No need to store the last '\0' character
      sizeof(buffer_a3)
  };

  // Write to array
  tiledb_array_write(tiledb_array, buffers, buffer_sizes); 

  // Finalize array
  tiledb_array_finalize(tiledb_array);

  // Finalize context
  tiledb_ctx_free(ctx);

  return 0;
}
