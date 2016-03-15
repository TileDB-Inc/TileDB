/*
 * File: tiledb_array_write_dense_A.cc
 * 
 * Demonstrates how to write to dense array "workspace/A", in dense mode.
 */

#include "c_api.h"
#include "cstring"
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
      "workspace/dense_var_A",
      TILEDB_ARRAY_WRITE,
      NULL,            // No range - entire domain
      NULL,            // No projection - all attributes
      0);              // Meaningless when "attributes" is NULL

  /* Prepare cell buffers for attributes "a1" and "a2". */
  size_t buffer_a1[16] = {0, 3, 8, 11, 18, 21, 26, 28, 33, 
                          39, 42, 45, 50, 54, 60, 63};
  const char buffer_var_a1[] = 
      "aa\0bbbb\0cc\0dddddd\0ee\0ffff\0g\0hhhh\0iiiii\0jj\0"
      "kk\0llll\0mmm\0nnnnn\0oo\0pp";

  float buffer_a2[16];
  const void* buffers[] = { buffer_a1, buffer_var_a1, buffer_a2};
  size_t buffer_sizes[3] = { sizeof(buffer_a1), sizeof(buffer_var_a1), 
                             sizeof(buffer_a2) };

  /* Populate buffers with some arbitrary values. */
  for(int i=0; i<16; ++i) {
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
