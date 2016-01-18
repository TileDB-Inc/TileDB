/*
 * File: tiledb_array_write_dense_A.cc
 * 
 * Demonstrates how to write to dense array "workspace/A", in dense mode.
 */

#include "c_api.h"

int main() {
  /* Intialize context with the default configuration parameters. */
  TileDB_CTX* tiledb_ctx;
  tiledb_ctx_init(&tiledb_ctx, NULL);

  /* Initialize the array in WRITE mode. */
  TileDB_Array* tiledb_array;
  tiledb_array_init(
      tiledb_ctx, 
      &tiledb_array,
      "workspace/dense_A",
      TILEDB_WRITE,
      NULL,            // No range - entire domain
      NULL,            // No projection - all attributes
      0);              // Meaningless when "attributes" is NULL

  /* Prepare cell buffers for attributes "a1" and "a2". */
  int buffer_a1[16];
  float buffer_a2[16];
  const void* buffers[] = { buffer_a1, buffer_a2};
  size_t buffer_sizes[2] = { 16*sizeof(int) , 16*sizeof(float) };

  /* Populate buffers with some arbitrary values. */
  for(int i=0; i<16; ++i) {
    buffer_a1[i] = i;
    buffer_a2[i] = 100.0 + i;
  }

  /* Write to array. */
  tiledb_array_write(tiledb_array, buffers, buffer_sizes); 

  /* Finalize the array. */
  tiledb_array_finalize(tiledb_array);

  /* Finalize context. */
  tiledb_ctx_finalize(tiledb_ctx);

  return 0;
}
