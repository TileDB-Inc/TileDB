/*
 * File: tiledb_workspace_group_create.cc
 * 
 * Creating workspaces and groups.
 */

#include "c_api.h"
#include <cstdio>

int main() {
  // Intialize context with the default configuration parameters
  TileDB_CTX* tiledb_ctx;
  tiledb_ctx_init(&tiledb_ctx, NULL);

  // Create a workspace
  tiledb_workspace_create(tiledb_ctx, "my_workspace");

  // Create a group in the worskpace
  tiledb_group_create(tiledb_ctx, "my_workspace/dense_arrays");

  // Create two groups in the worskpace
  tiledb_group_create(tiledb_ctx, "my_workspace/sparse_arrays");

  // Finalize context
  tiledb_ctx_finalize(tiledb_ctx);

  return 0;
}
