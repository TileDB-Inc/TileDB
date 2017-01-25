/**
 * @file   tiledb_array_parallel_consolidate_dense.cc
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
 * It shows how to mix reading and consolidation on a dense array in parallel.
 * This program works with pthreads, but it is very easy to write a similar 
 * program for OpenMP.
 */

#include "tiledb.h"
#include <pthread.h>
#include <stdio.h>



// The consolidation function to be computed in parallel
void *parallel_consolidate(void* args);

// The read function to be computed in parallel
void *parallel_read(void* args);

// The arguments for each invocation of parallel_write
typedef struct _thread_data_t {
    const TileDB_CTX* tiledb_ctx;
    const char* array_name;
    const void* subarray;
    void** buffers;
    size_t* buffer_sizes;
    int count;
} thread_data_t;

int main() {
  // Initialize context with the default configuration parameters
  TileDB_CTX* tiledb_ctx;
  tiledb_ctx_init(&tiledb_ctx, NULL);

  // Array name
  const char* array_name = "my_workspace/dense_arrays/my_array_A";

  // Prepare cell buffers
  // --- Upper left tile ---
  const int64_t subarray_1[] = { 1, 2, 1, 2 }; 
  int buffer_a1_1[4]; 
  void* buffers_1[] = { buffer_a1_1 };
  size_t buffer_sizes_1[] = { sizeof(buffer_a1_1) };
  // --- Upper right tile ---
  const int64_t subarray_2[] = { 1, 2, 3, 4 }; 
  int buffer_a1_2[4]; 
  void* buffers_2[] = { buffer_a1_2 };
  size_t buffer_sizes_2[] = { sizeof(buffer_a1_2) };
  // --- Lower left tile ---
  const int64_t subarray_3[] = { 3, 4, 1, 2 }; 
  int buffer_a1_3[4];
  void* buffers_3[] = { buffer_a1_3 };
  size_t buffer_sizes_3[] = { sizeof(buffer_a1_3) };
  // --- Lower right tile ---
  const int64_t subarray_4[] = { 3, 4, 3, 4 }; 
  int buffer_a1_4[4]; 
  void* buffers_4[] = { buffer_a1_4 };
  size_t buffer_sizes_4[] = { sizeof(buffer_a1_4) };

  // Initialize 4 pthreads for reading, plus 1 for consolidation
  pthread_t threads[5];
  thread_data_t thread_data[5];

  // Write in parallel
  for(int i=0; i<5; ++i) {
    // Populate the thread data 
    thread_data[i].tiledb_ctx = tiledb_ctx;
    thread_data[i].array_name = array_name;
    if(i==0) {         // First tile
      thread_data[i].buffers = buffers_1;
      thread_data[i].buffer_sizes = buffer_sizes_1;
      thread_data[i].subarray = subarray_1;
    } else if(i==1) {  // Second tile
      thread_data[i].buffers = buffers_2;
      thread_data[i].buffer_sizes = buffer_sizes_2;
      thread_data[i].subarray = subarray_2;
    } else if(i==2) {  // Third tile
      thread_data[i].buffers = buffers_3;
      thread_data[i].buffer_sizes = buffer_sizes_3;
      thread_data[i].subarray = subarray_3;
    } else if(i==3) {  // Fourth tile
      thread_data[i].buffers = buffers_4;
      thread_data[i].buffer_sizes = buffer_sizes_4;
      thread_data[i].subarray = subarray_4;
    }

    // Create thread
    if(i>=0 && i<4)
      pthread_create(&threads[i], NULL, parallel_read, &thread_data[i]);
    else // i == 4
      pthread_create(&threads[i], NULL, parallel_consolidate, &thread_data[i]);
  }

  // Wait till all threads finish
  for(int i=0; i<5; ++i)
    pthread_join(threads[i], NULL);

  // Output result
  int total_count = 0;
  for(int i=0; i<4; ++i) 
    total_count += thread_data[i].count;
  printf("Number of a1 values greater than 10: %d \n", total_count);

  // Finalize context
  tiledb_ctx_finalize(tiledb_ctx);

  return 0;
}

void *parallel_consolidate(void* args) {
  // Get arguments
  thread_data_t* data = (thread_data_t*) args;

  // Consolidate array
  printf("Started consolidation\n");
  tiledb_array_consolidate(data->tiledb_ctx, data->array_name);
  printf("Finished consolidation\n");

  return 0;
}

void *parallel_read(void* args) {
  // Get arguments
  thread_data_t* data = (thread_data_t*) args;

  // Only attribute "a1" is needed
  const char* attributes[] = { "a1" };

  // Initialize array
  TileDB_Array* tiledb_array;
  tiledb_array_init(
      data->tiledb_ctx,                          // Context 
      &tiledb_array,                             // Array object
      data->array_name,                          // Array name
      TILEDB_ARRAY_READ,                         // Mode
      data->subarray,                            // Subarray
      attributes,                                // Subset on attributes
      1);                                        // Number of attributes

  // Read from array
  printf("Started reading\n");
  tiledb_array_read(tiledb_array, data->buffers, data->buffer_sizes); 
  printf("Finished reading\n");

  // Count number of a1 values greater than 10
  data->count = 0;
  int* a1 = (int*) data->buffers[0];
  int num = data->buffer_sizes[0] / sizeof(int);
  for(int i=0; i<num; ++i) {
    if(a1[i] > 10)
      ++data->count;
  }

  // Finalize array
  tiledb_array_finalize(tiledb_array);

  return 0;
}

