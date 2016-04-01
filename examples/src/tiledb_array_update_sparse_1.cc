/*
 * File: tiledb_array_update_sparse_1.cc
 * 
 * It shows how to update a sparse array. Observe that this is simply
 * a write operation.
 */

#include "c_api.h"

int main() {
  // Intialize context with the default configuration parameters
  TileDB_CTX* tiledb_ctx;
  tiledb_ctx_init(&tiledb_ctx, NULL);

  // Initialize array
  TileDB_Array* tiledb_array;
  tiledb_array_init(
      tiledb_ctx,                                // Context 
      &tiledb_array,                             // Array object
      "my_workspace/sparse_arrays/my_array_B",   // Array name
      TILEDB_ARRAY_WRITE_UNSORTED,               // Mode
      NULL,                                      // Entire domain
      NULL,                                      // All attributes
      0);                                        // Number of attributes

  // Prepare cell buffers
  int buffer_a1[] = { 109, 104, 108, 105 };
  size_t buffer_a2[] = { 0, 1, 2, 6 };
  const char buffer_var_a2[] = "uwvvvvyyy";
  float buffer_a3[] = 
  { 109.1,  109.2,  104.1,  104.2,  108.1,  108.2,  105.1,  105.2 };
  int64_t buffer_coords[] = { 3, 2, 3, 3, 4, 1, 3, 4 };
  const void* buffers[] = 
      { buffer_a1, buffer_a2, buffer_var_a2, buffer_a3, buffer_coords };
  size_t buffer_sizes[] = 
  { 
      sizeof(buffer_a1),  
      sizeof(buffer_a2),
      sizeof(buffer_var_a2)-1,  // No need to store the last '\0' character
      sizeof(buffer_a3),
      sizeof(buffer_coords)
  };

  // Write to array
  tiledb_array_write(tiledb_array, buffers, buffer_sizes); 

  // Finalize array
  tiledb_array_finalize(tiledb_array);

  // Finalize context
  tiledb_ctx_finalize(tiledb_ctx);

  return 0;
}
