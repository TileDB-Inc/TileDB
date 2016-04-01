/*
 * File: tiledb_array_consolidate.cc
 * 
 * It shows how to consolidate arrays.
 */

#include "c_api.h"

int main() {
  // Intialize context with the default configuration parameters
  TileDB_CTX* tiledb_ctx;
  tiledb_ctx_init(&tiledb_ctx, NULL);

  // ----- Dense array

  // Initialize array
  TileDB_Array* tiledb_array;
  tiledb_array_init(                     
      tiledb_ctx,                              // Context
      &tiledb_array,                           // Array object
      "my_workspace/dense_arrays/my_array_A",  // Array name
      TILEDB_ARRAY_WRITE,                      // Mode
      NULL,                                    // Entire domain
      NULL,                                    // All attributes
      0);                                      // Number of attributes

  // Consolidate array
  tiledb_array_consolidate(tiledb_array); 

  // Finalize array
  tiledb_array_finalize(tiledb_array);

  // ----- Sparse array

  // Initialize array
  tiledb_array_init(                     
      tiledb_ctx,                              // Context
      &tiledb_array,                           // Array object
      "my_workspace/sparse_arrays/my_array_B", // Array name
      TILEDB_ARRAY_WRITE,                      // Mode
      NULL,                                    // Entire domain
      NULL,                                    // All attributes
      0);                                      // Number of attributes

  // Consolidate array
  tiledb_array_consolidate(tiledb_array); 

  // Finalize array
  tiledb_array_finalize(tiledb_array);

  // Finalize context
  tiledb_ctx_finalize(tiledb_ctx);

  return 0;
}
