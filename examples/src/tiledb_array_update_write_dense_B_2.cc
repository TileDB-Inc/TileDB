/*
 * Demonstrates how to write to dense array "workspace/A", in dense mode.
 */

#include "c_api.h"
#include <iostream>

int main() {
  /* Intialize context with the default configuration parameters. */
  TileDB_CTX* tiledb_ctx;
  tiledb_ctx_init(&tiledb_ctx, NULL);

  const int64_t range[] = { 3, 4, 6, 8 };

  /* Initialize the array in WRITE mode. */
  TileDB_Array* tiledb_array;
  tiledb_array_init(
      tiledb_ctx, 
      &tiledb_array,
      "workspace/dense_B",
      TILEDB_WRITE,
      range,           // No range - entire domain
      NULL,            // No projection - all attributes
      0);              // Meaningless when "attributes" is NULL

  /* Prepare cell buffers for attributes "a1" and "a2". */
  int buffer_a1[] = 
  {
    TILEDB_EMPTY_INT32, 125, TILEDB_EMPTY_INT32, 127, 
    128, 129, 130, 131
  };
  size_t buffer_a2[] = 
  {
    0, 1, 2, 3, 4, 5, 6, 7
  };
  char buffer_a2_var[] = 
  {
    TILEDB_EMPTY_CHAR, '+', TILEDB_EMPTY_CHAR, '+', 
    '+', '+', '+', '+'
  };
  float buffer_a3[] = 
  {
    TILEDB_EMPTY_FLOAT32, TILEDB_EMPTY_FLOAT32, 125.1, 125.2, 
    TILEDB_EMPTY_FLOAT32, TILEDB_EMPTY_FLOAT32, 127.1, 127.2, 
    128.1, 128.2, 129.1, 129.2, 130.1, 130.2, 131.1, 131.2
  };
  const void* buffers[] = { buffer_a1, buffer_a2, buffer_a2_var, buffer_a3 };
  size_t buffer_sizes[] = 
  { 
    sizeof(buffer_a1) , 
    sizeof(buffer_a2), 
    sizeof(buffer_a2_var),
    sizeof(buffer_a3)
  };

  /* Write to array. */
  tiledb_array_write(tiledb_array, buffers, buffer_sizes); 

  /* Finalize the array. */
  tiledb_array_finalize(tiledb_array);

  /* Finalize context. */
  tiledb_ctx_finalize(tiledb_ctx);

  return 0;
}
