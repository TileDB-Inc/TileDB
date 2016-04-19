/**
 * @file   tiledb_array_parallel_write_dense_2.cc
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
 * It shows how to write to a dense array in parallel with OpenMP.
 */

#include "c_api.h"
#include <omp.h>




// The function to be computed in parallel
void parallel_write(
    const TileDB_CTX* tiledb_ctx,
    const char* array_name,
    const void* subarray,
    const void** buffers,
    const size_t* buffer_sizes);

int main() {
  // Initialize context with the default configuration parameters
  TileDB_CTX* tiledb_ctx;
  tiledb_ctx_init(&tiledb_ctx, NULL);

  // Array name
  const char* array_name = "my_workspace/dense_arrays/my_array_A";

  // Prepare cell buffers
  // --- Upper left tile ---
  const int64_t subarray_1[] = { 1, 2, 1, 2 }; 
  int buffer_a1_1[] =  { 0, 1, 2, 3 }; 
  size_t buffer_a2_1[] = { 0, 1, 3, 6 }; 
  const char buffer_var_a2_1[] = "abbcccdddd";
  float buffer_a3_1[] = { 0.1, 0.2, 1.1, 1.2, 2.1, 2.2, 3.1, 3.2 };  
  const void* buffers_1[] = { 
      buffer_a1_1, buffer_a2_1, buffer_var_a2_1, buffer_a3_1 };
  size_t buffer_sizes_1[] = 
  { 
      sizeof(buffer_a1_1),  
      sizeof(buffer_a2_1),
      sizeof(buffer_var_a2_1)-1,  // No need to store the last '\0' character
      sizeof(buffer_a3_1)
  };
  // --- Upper right tile ---
  const int64_t subarray_2[] = { 1, 2, 3, 4 }; 
  int buffer_a1_2[] =  { 4, 5, 6, 7 }; 
  size_t buffer_a2_2[] = { 0, 1, 3, 6 }; 
  const char buffer_var_a2_2[] = "effggghhhh";
  float buffer_a3_2[] = { 4.1, 4.2, 5.1, 5.2, 6.1, 6.2, 7.1, 7.2 };  
  const void* buffers_2[] = { 
      buffer_a1_2, buffer_a2_2, buffer_var_a2_2, buffer_a3_2 };
  size_t buffer_sizes_2[] = 
  { 
      sizeof(buffer_a1_2),  
      sizeof(buffer_a2_2),
      sizeof(buffer_var_a2_2)-1,  // No need to store the last '\0' character
      sizeof(buffer_a3_2)
  };
  // --- Lower left tile ---
  const int64_t subarray_3[] = { 3, 4, 1, 2 }; 
  int buffer_a1_3[] =  { 8, 9, 10, 11 };
  size_t buffer_a2_3[] = { 0, 1, 3, 6 }; 
  const char buffer_var_a2_3[] = "ijjkkkllll";
  float buffer_a3_3[] = { 8.1, 8.2, 9.1, 9.2, 10.1, 10.2, 11.1, 11.2 };  
  const void* buffers_3[] = { 
      buffer_a1_3, buffer_a2_3, buffer_var_a2_3, buffer_a3_3 };
  size_t buffer_sizes_3[] = 
  { 
      sizeof(buffer_a1_3),  
      sizeof(buffer_a2_3),
      sizeof(buffer_var_a2_3)-1,  // No need to store the last '\0' character
      sizeof(buffer_a3_3)
  };
  // --- Lower right tile ---
  const int64_t subarray_4[] = { 3, 4, 3, 4 }; 
  int buffer_a1_4[] =  { 12, 13, 14, 15 }; 
  size_t buffer_a2_4[] = { 0, 1, 3, 6 };   
  const char buffer_var_a2_4[] = "mnnooopppp"; 
  float buffer_a3_4[] = { 12.1, 12.2, 13.1, 13.2, 14.1, 14.2, 15.1, 15.2 };  
  const void* buffers_4[] = { 
      buffer_a1_4, buffer_a2_4, buffer_var_a2_4, buffer_a3_4 };
  size_t buffer_sizes_4[] = 
  { 
      sizeof(buffer_a1_4),  
      sizeof(buffer_a2_4),
      sizeof(buffer_var_a2_4)-1,  // No need to store the last '\0' character
      sizeof(buffer_a3_4)
  };

  // Write in parallel
  #pragma omp parallel for
  for(int i=0; i<4; ++i) {
    // Populate thread data
    const void** buffers;
    const size_t* buffer_sizes;
    const void* subarray;
    if(i==0) {         // First tile
      buffers = buffers_1;
      buffer_sizes = buffer_sizes_1;
      subarray = subarray_1;
    } else if(i==1) {  // Second tile
      buffers = buffers_2;
      buffer_sizes = buffer_sizes_2;
      subarray = subarray_2;
    } else if(i==2) {  // Third tile
      buffers = buffers_3;
      buffer_sizes = buffer_sizes_3;
      subarray = subarray_3;
    } else if(i==3) {  // Fourth tile
      buffers = buffers_4;
      buffer_sizes = buffer_sizes_4;
      subarray = subarray_4;
    }

    // Write
    parallel_write(
        tiledb_ctx,
        array_name,
        subarray,
        buffers,
        buffer_sizes);
  }

  // Finalize context
  tiledb_ctx_finalize(tiledb_ctx);

  return 0;
}

void parallel_write(
    const TileDB_CTX* tiledb_ctx,
    const char* array_name,
    const void* subarray,
    const void** buffers,
    const size_t* buffer_sizes) {
  // Initialize array
  TileDB_Array* tiledb_array;
  tiledb_array_init(
      tiledb_ctx,                                // Context 
      &tiledb_array,                             // Array object
      array_name,                                // Array name
      TILEDB_ARRAY_WRITE,                        // Mode
      subarray,                                  // Subarray
      NULL,                                      // All attributes
      0);                                        // Number of attributes

  // Write to array
  tiledb_array_write(tiledb_array, buffers, buffer_sizes); 

  // Finalize array
  tiledb_array_finalize(tiledb_array);
}

