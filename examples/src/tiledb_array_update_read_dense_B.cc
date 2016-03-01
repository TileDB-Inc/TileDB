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
  const int64_t range[] = { 3, 3, 8, 8 };

  /* Subset over attribute "a1". */
  const char* attributes[] = { "a1" };

  /* Initialize the array in READ mode. */
  TileDB_Array* tiledb_array;
  tiledb_array_init(
      tiledb_ctx, 
      &tiledb_array,
      "workspace/dense_B",
      TILEDB_READ,
      range, 
      attributes,           
      1);      

  /* Prepare cell buffers for attribute "a1". */
  int buffer_a1[64];
  size_t buffer_a2[64];
  char buffer_a2_var[500];
  float buffer_a3[128];
  void* buffers[] = { buffer_a1 };
  size_t buffer_sizes[] = { sizeof(buffer_a1) };

  /* Read from array. */
  tiledb_array_read(tiledb_array, buffers, buffer_sizes); 

  /* Print the read values. */
  int64_t result_num = buffer_sizes[0] / sizeof(int);
  for(int i=0; i<result_num; ++i) 
    std::cout << buffer_a1[i] << "\n";

  /* Finalize the array. */
  tiledb_array_finalize(tiledb_array);

  /* Finalize context. */
  tiledb_ctx_finalize(tiledb_ctx);

  return 0;
}
