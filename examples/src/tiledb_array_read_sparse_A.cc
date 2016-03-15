/*
 * File: tiledb_array_read_sparse_A.cc
 * 
 * Demonstrates how to read from dense array "workspace/A".
 */

#include "c_api.h"
#include <iostream>

int main() {
  /* Intialize context with the default configuration parameters. */
  TileDB_CTX* tiledb_ctx;
  tiledb_ctx_init(&tiledb_ctx, NULL);

  /* Initialize a range. */
  const int64_t range[] = { 2, 3, 1, 4 };

  /* Subset over attribute "a1" and the coordinates. */
  const char* attributes[] = { "a1", TILEDB_COORDS };

  /* Initialize the array in READ mode. */
  TileDB_Array* tiledb_array;
  tiledb_array_init(
      tiledb_ctx, 
      &tiledb_array,
      "workspace/sparse_A",
      TILEDB_ARRAY_READ,
      range, 
      attributes,           
      2);      

  /* Prepare cell buffers for attribute "a1". */
  int buffer_a1[9];
  int64_t buffer_coords[18];
  void* buffers[] = { buffer_a1, buffer_coords };
  size_t buffer_sizes[2] = { 6*sizeof(int), 12*sizeof(int64_t) };

  /* Read from array. */
  tiledb_array_read(tiledb_array, buffers, buffer_sizes); 

  /* Print the read values. */
  int result_num = buffer_sizes[0] / sizeof(int);
  for(int i=0; i<result_num; ++i) { 
    std::cout << "(" << buffer_coords[2*i] << ", " << buffer_coords[2*i+1]  
              << "): " << buffer_a1[i] << "\n";
  }

  /* Finalize the array. */
  tiledb_array_finalize(tiledb_array);

  /* Finalize context. */
  tiledb_ctx_finalize(tiledb_ctx);

  return 0;
}
