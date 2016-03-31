/*
 * File: tiledb_metadata_consolidate.cc
 * 
 * It shows how to consolidate metadata.
 *
 * It assumes that the following programs have been run:
 *    - tiledb_workspace_group_create.cc
 *    - tiledb_array_create_sparse.cc
 *    - tiledb_metadata_create.cc
 *    - tiledb_metadata_write.cc
 *    - tiledb_metadata_update.cc
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
      0);

  // Consolidate metadata
  tiledb_metadata_consolidate(tiledb_metadata); 

  // Finalize metadata
  tiledb_metadata_finalize(tiledb_metadata);

  // Finalize context
  tiledb_ctx_finalize(tiledb_ctx);

  return 0;
}
