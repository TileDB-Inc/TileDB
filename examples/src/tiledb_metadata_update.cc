/*
 * File: tiledb_metadata_update.cc
 * 
 * It shows how to update a metadata object.
 *
 * It assumes that the following programs have been run:
 *    - tiledb_workspace_group_create.cc
 *    - tiledb_array_create_sparse.cc
 *    - tiledb_metadata_create.cc
 *    - tiledb_metadata_write.cc
 */

#include "c_api.h"

int main() {
  // Intialize context with the default configuration parameters
  TileDB_CTX* tiledb_ctx;
  tiledb_ctx_init(&tiledb_ctx, NULL);

  // Initialize metadata
  TileDB_Metadata* tiledb_metadata;
  tiledb_metadata_init(
      tiledb_ctx,                                    // Context
      &tiledb_metadata,                              // Metadata object
      "my_workspace/sparse_arrays/my_array_B/meta",  // Metadata name
      TILEDB_METADATA_WRITE,                         // Mode
      NULL,                                          // All attributes
      0);                                            // Number of attributes 

  // Prepare cell buffers
  int buffer_a1[] = { 100, TILEDB_EMPTY_INT32 };
  size_t buffer_a2[] = { 0, 1 };
  char buffer_var_a2[] = { 'A', TILEDB_EMPTY_CHAR };
  char keys[] = "k1\0k2";
  size_t buffer_keys[] = { 0, 3 };
  char buffer_var_keys[] = 
  {
       'k', '1', '\0',                               // k1 modified key value
        TILEDB_EMPTY_CHAR                            // k2 deleted value
  };
  const void* buffers[] = 
  { 
      buffer_a1,                                     // a1
      buffer_a2, buffer_var_a2,                      // a2
      buffer_keys, buffer_var_keys                   // TILEDB_KEY
  };         
  size_t buffer_sizes[] = { 
      sizeof(buffer_a1),                             // a1
      sizeof(buffer_a2), sizeof(buffer_var_a2),      // a2 
      sizeof(buffer_keys), sizeof(buffer_var_keys)   // TILEDB_KEY
  };
  size_t keys_size = sizeof(keys);

  // Write metadata
  tiledb_metadata_write(
      tiledb_metadata,                               // Metadata object
      keys,                                          // Keys
      keys_size,                                     // Keys size
      buffers,                                       // Attribute buffers
      buffer_sizes);                                 // Attribute buffer sizes

  // Finalize the metadata
  tiledb_metadata_finalize(tiledb_metadata);

  // Finalize context
  tiledb_ctx_finalize(tiledb_ctx);

  return 0;
}
