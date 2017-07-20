/**
 * @file   tiledb_array_read_mpi_io_dense.cc
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
 * It shows how to read from a dense array in parallel with MPI, activating
 * also the MPI-IO read mode (although the latter is optional - the user
 * could alternatively use mmap or standard OS read). Note that the case
 * of sparse arrays is similar.
 */

#include "tiledb.h"
#include <cstdio>
#include <cstring>



#ifdef HAVE_MPI
#include <mpi.h>

int main(int argc, char** argv) {
  // Initialize MPI and get rank
  MPI_Init(&argc, &argv);
  int rank;
  MPI_Comm mpi_comm = MPI_COMM_WORLD;
  MPI_Comm_rank(mpi_comm, &rank);
 
  // Initialize context with the default configuration parameters
  tiledb_ctx_t* ctx;
  tiledb_ctx_create(&ctx);
 
  tiledb_config_t* config;
  tiledb_config_create(ctx, &config);
  tiledb_config_set_read_method(ctx, config, TILEDB_IO_METHOD_MPI);
  tiledb_config_set_mpi_comm(ctx, config, &mpi_comm);

  // Array name
  const char* array_name = "my_group/dense_arrays/my_array_A";

  // Prepare cell buffers
  // --- Upper left tile ---
  const int64_t subarray_0[] = { 1, 2, 1, 2 }; 
  // --- Upper right tile ---
  const int64_t subarray_1[] = { 1, 2, 3, 4 }; 
  // --- Lower left tile ---
  const int64_t subarray_2[] = { 3, 4, 1, 2 }; 
  // --- Lower right tile ---
  const int64_t subarray_3[] = { 3, 4, 3, 4 }; 

  // Set buffers
  int buffer[4]; 
  void* buffers[] = { buffer };
  size_t buffer_sizes[] = { sizeof(buffer) };

  // Only attribute "a1" is needed
  const char* attributes[] = { "a1" };

  // Choose subarray based on rank
  const int64_t* subarray;
  if(rank == 0)
    subarray = subarray_0;
  else if(rank == 1)
    subarray = subarray_1;
  else if(rank == 2)
    subarray = subarray_2;
  else if(rank == 3)
    subarray = subarray_3;
  
  // Initialize array
  tiledb_array_t* tiledb_array;
  tiledb_array_init(
      ctx,                          // Context
      &tiledb_array,                       // Array object
      array_name,                          // Array name
      TILEDB_ARRAY_READ,                   // Mode
      subarray,                            // Subarray
      attributes,                          // Subset on attributes
      1);                                  // Number of attributes

  // Read from array
  tiledb_array_read(tiledb_array, buffers, buffer_sizes); 

  // Finalize array
  tiledb_array_finalize(tiledb_array);

  // Output result
  int total_count = 0;
  for(int i=0; i<4; ++i) 
    if(buffer[i] > 10)
      ++total_count;
  printf("Process %d: Number of a1 values greater "
         "than 10: %d \n", rank, total_count);
  
  tiledb_config_free(config); 

  // Finalize context
  tiledb_ctx_free(ctx);

  // Finalize MPI
  MPI_Finalize();

  return 0;
}

#else

int main() {
  printf("MPI not supported.\n");

  return 0;
}

#endif
