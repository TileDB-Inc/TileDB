/*
 * File: tiledb_array_read_dense_2.cc
 * 
 * It shows how to read from a dense array, constraining the read
 * to a specific subarray and subset of attributes. Moreover, the
 * program shows how to detect buffer overflow.
 *
 * It assumes that the following programs have been run:
 *    - tiledb_workspace_group_create.cc
 *    - tiledb_array_create_dense.cc
 *    - tiledb_array_write_dense_1.cc
 */

#include "c_api.h"
#include <cstdio>

int main() {
  // Intialize context with the default configuration parameters
  TileDB_CTX* tiledb_ctx;
  tiledb_ctx_init(&tiledb_ctx, NULL);

  // Subarray and attributes
  int64_t subarray[] = { 3, 4, 2, 4 }; 
  const char* attributes[] = { "a1" };

  // Initialize array 
  TileDB_Array* tiledb_array;
  tiledb_array_init(
      tiledb_ctx,                                       // Context
      &tiledb_array,                                    // Array object
      "my_workspace/dense_arrays/my_array_A",           // Array name
      TILEDB_ARRAY_READ,                                // Mode
      subarray,                                         // Constrain in subarray
      attributes,                                       // Subset on attributes
      1);                                               // Number of attributes

  // Prepare cell buffers 
  int buffer_a1[3];
  void* buffers[] = { buffer_a1 };
  size_t buffer_sizes[] = { sizeof(buffer_a1) };


  // Loop until no overflow
  printf(" a1\n----\n");
  do {
    printf("Reading cells...\n"); 

    // Read from array
    tiledb_array_read(tiledb_array, buffers, buffer_sizes); 

    // Print cell values
    int64_t result_num = buffer_sizes[0] / sizeof(int);
    for(int i=0; i<result_num; ++i) 
      printf("%3d\n", buffer_a1[i]);
  } while(tiledb_array_overflow(tiledb_array, 0) == 1);
 
  // Finalize the array
  tiledb_array_finalize(tiledb_array);

  /* Finalize context. */
  tiledb_ctx_finalize(tiledb_ctx);

  return 0;
}
