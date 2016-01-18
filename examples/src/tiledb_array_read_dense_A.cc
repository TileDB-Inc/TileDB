/*
 * File: tiledb_array_read_dense_A.cc
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
  const int64_t range[] = { 1, 4, 2, 4 };

  /* Subset over attribute "a1". */
  const char* attributes[] = { "a1" };

  /* Initialize the array in READ mode. */
  TileDB_Array* tiledb_array;
  tiledb_array_init(
      tiledb_ctx, 
      &tiledb_array,
      "workspace/dense_A",
      TILEDB_READ,
      range, 
      attributes,           
      1);      

  /* Prepare cell buffers for attributes "a1" and "a2". */
  int buffer_a1[12];
  void* buffers[] = { buffer_a1 };
  size_t buffer_sizes[1] = { 12*sizeof(int) };

  /* Read from array. */
  tiledb_array_read(tiledb_array, buffers, buffer_sizes); 

  /* Print the read values. */
  for(int i=0; i<12; ++i) 
    std::cout << buffer_a1[i] << "\n";

  /* Finalize the array. */
  tiledb_array_finalize(tiledb_array);

  /* Finalize context. */
  tiledb_ctx_finalize(tiledb_ctx);

  return 0;
}
