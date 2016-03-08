/*
 * File: tiledb_array_read_dense_A.cc
 * 
 * Demonstrates how to read from dense array "workspace/A".
 */

#include "c_api.h"

int main() {
  /* Intialize context with the default configuration parameters. */
  TileDB_CTX* tiledb_ctx;
  tiledb_ctx_init(&tiledb_ctx, NULL);

  // Create the workspace
  tiledb_workspace_create(tiledb_ctx, "new_workspace");

  /* Finalize context. */
  tiledb_ctx_finalize(tiledb_ctx);

  return 0;
}
