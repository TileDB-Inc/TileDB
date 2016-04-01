/*
 * File: tiledb_array_update_dense_1.cc
 * 
 * It shows how to update a dense array, writing into a subarray of the
 * of the array domain. Observe that updates are carried out as simple
 * writes.
 *
 * It assumes that the following programs have been run:
 *    - tiledb_workspace_group_create.cc
 *    - tiledb_array_create_dense.cc
 *    - tiledb_array_write_dense_1.cc
 */

#include "c_api.h"

int main() {
  // Intialize context with the default configuration parameters
  TileDB_CTX* tiledb_ctx;
  tiledb_ctx_init(&tiledb_ctx, NULL);

  int64_t subarray[] = { 3, 4, 3, 4 };

  // Initialize array
  TileDB_Array* tiledb_array;
  tiledb_array_init(
      tiledb_ctx,                                // Context 
      &tiledb_array,                             // Array object
      "my_workspace/dense_arrays/my_array_A",    // Array name
      TILEDB_ARRAY_WRITE,                        // Mode
      subarray,                                  // Subarray
      NULL,                                      // All attributes
      0);                                        // Number of attributes

  // Prepare cell buffers
  int buffer_a1[] = { 112,  113,  114,  115 };
  size_t buffer_a2[] = { 0,  1,  3,  6 };
  const char buffer_var_a2[] = "MNNOOOPPPP";
  float buffer_a3[] = 
      { 112.1, 112.2, 113.1, 113.2, 114.1, 114.2, 115.1, 115.2 };
  const void* buffers[] = { buffer_a1, buffer_a2, buffer_var_a2, buffer_a3 };
  size_t buffer_sizes[] = 
  { 
      sizeof(buffer_a1),  
      sizeof(buffer_a2),
      sizeof(buffer_var_a2)-1,  // No need to store the last '\0' character
      sizeof(buffer_a3)
  };

  // Write to array
  tiledb_array_write(tiledb_array, buffers, buffer_sizes); 

  // Finalize array
  tiledb_array_finalize(tiledb_array);

  // Finalize context
  tiledb_ctx_finalize(tiledb_ctx);

  return 0;
}
