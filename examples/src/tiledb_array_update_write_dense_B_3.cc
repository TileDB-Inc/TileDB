/*
 * Demonstrates how to write to sparse array "workspace/sparse_A", in 
 * in unsorted mode.
 */

#include "c_api.h"

int main() {
  /* Intialize context with the default configuration parameters. */
  TileDB_CTX* tiledb_ctx;
  tiledb_ctx_init(&tiledb_ctx, NULL);

  const char* attributes[] = {TILEDB_COORDS, "a1", "a2", "a3"};

  /* Initialize the array in WRITE mode. */
  TileDB_Array* tiledb_array;
  tiledb_array_init(
      tiledb_ctx, 
      &tiledb_array,
      "workspace/dense_B",
      TILEDB_ARRAY_WRITE_UNSORTED,
      NULL,            // No range - entire domain
      attributes,      // No projection - all attributes
      4);              // Meaningless when "attributes" is NULL

  /* Prepare cell buffers for attributes "a1" and "a2". */
  int64_t buffer_coords[] 
  {
    4, 4,
    4, 6,
    5, 6,
//    5, 8,
    5, 7,
    7, 1,
    7, 2, 
    8, 3
  };
  int buffer_a1[] =
  {
    223, 227, 241, 244, 248, 249, 254
  };
  size_t buffer_a2[] =
  {
    0, 1, 2, 3, 4, 5, 6
  };
  char buffer_var_a2[] = 
  { 
    'A', 'B', 'C', 'D', 'E', 'F', 'G'
  };
  float buffer_a3[] = 
  {
    223.1, 223.2,
    227.1, 227.2,
    241.1, 241.2,
    244.1, 244.2,
    248.1, 248.2,
    249.1, 249.2,
    254.1, 254.2
  };
  const void* buffers[] = 
  { 
    buffer_coords, buffer_a1, buffer_a2, buffer_var_a2, buffer_a3
  };
  size_t buffer_sizes[] = { 
      sizeof(buffer_coords), 
      sizeof(buffer_a1), 
      sizeof(buffer_a2), 
      sizeof(buffer_var_a2), 
      sizeof(buffer_a3) };

  /* Write to array. */
  tiledb_array_write(tiledb_array, buffers, buffer_sizes); 

  /* Finalize the array. */
  tiledb_array_finalize(tiledb_array);

  /* Finalize context. */
  tiledb_ctx_finalize(tiledb_ctx);

  return 0;
}
