/*
 * File: tiledb_array_write_dense_A.cc
 * 
 * Demonstrates how to write to dense array "workspace/A", in dense mode.
 */

#include "c_api.h"
#include <iostream>

int main() {
  /* Intialize context with the default configuration parameters. */
  TileDB_CTX* tiledb_ctx;
  tiledb_ctx_init(&tiledb_ctx, NULL);

  /* Initialize the array in WRITE mode. */
  TileDB_Array* tiledb_array;
  tiledb_array_init(
      tiledb_ctx, 
      &tiledb_array,
      "workspace/sparse_A",
      TILEDB_ARRAY_WRITE,
      NULL,            // No range - entire domain
      NULL,            // No projection - all attributes
      0);              // Meaningless when "attributes" is NULL

  /* Prepare cell buffers for attributes "a1" and "a2". */
  int buffer_a1[6];
  float buffer_a2[6];
  int64_t buffer_coords[12];
  const void* buffers[] = { buffer_a1, buffer_a2, buffer_coords};
  size_t buffer_sizes[3] = { 
      6*sizeof(int) , 
      6*sizeof(float), 
      12*sizeof(int64_t) };

  /* Populate attribute buffers with some arbitrary values. */
  for(int i=0; i<6; ++i) {
    buffer_a1[i] = i;
    buffer_a2[i] = 100.0 + i;
  }

  /* Populate coordinates buffer with some arbitrary values. */
  buffer_coords[0] = 1; buffer_coords[1] = 1;
  buffer_coords[2] = 2; buffer_coords[3] = 1;
  buffer_coords[4] = 2; buffer_coords[5] = 2;
  buffer_coords[6] = 4; buffer_coords[7] = 2;
  buffer_coords[8] = 3; buffer_coords[9] = 3;
  buffer_coords[10] = 1; buffer_coords[11] = 4;

  /* Write to array. */
  tiledb_array_write(tiledb_array, buffers, buffer_sizes); 

  /* Finalize the array. */
  tiledb_array_finalize(tiledb_array);

  /* Finalize context. */
  tiledb_ctx_finalize(tiledb_ctx);

  return 0;
}
