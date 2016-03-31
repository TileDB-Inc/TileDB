/*
 * File: tiledb_clear_delete_move.cc
 * 
 * It shows how to clear, delete and move TileDB objects.
 *
 * It assumes that the following programs have been run:
 *    - tiledb_workspace_group_create.cc
 *    - tiledb_array_create_dense.cc
 *    - tiledb_array_create_sparse.cc
 *    - tiledb_array_write_dense_1.cc
 *    - tiledb_array_write_sparse_1.cc
 *    - tiledb_metadata_create.cc
 *    - tiledb_metadata_write.cc
 *    - tiledb_metadata_update.cc
 */

#include "c_api.h"

int main() {
  // Intialize context with the default configuration parameters
  TileDB_CTX* tiledb_ctx;
  tiledb_ctx_init(&tiledb_ctx, NULL);

  // Clear an array
  tiledb_clear(tiledb_ctx, "my_workspace/sparse_arrays/my_array_B");

  // Delete a group
  tiledb_delete(tiledb_ctx, "my_workspace/dense_arrays");

  // Move a workspace
  tiledb_move(tiledb_ctx, "my_workspace", "my_workspace_2");

  // Finalize context
  tiledb_ctx_finalize(tiledb_ctx);

  return 0;
}
