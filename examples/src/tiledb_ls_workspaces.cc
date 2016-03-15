/*
 * Demonstrates how to read from dense array "workspace/A".
 */

#include "c_api.h"
#include <iostream>

int main() {
  /* Intialize context with the default configuration parameters. */
  TileDB_CTX* tiledb_ctx;
  tiledb_ctx_init(&tiledb_ctx, NULL);

  // Get workspaces
  char* workspaces[100];
  int workspace_num = 100;
  for(int i=0; i<workspace_num; ++i)
    workspaces[i] = new char[TILEDB_NAME_MAX_LEN];
  tiledb_ls_workspaces(tiledb_ctx, (char**) workspaces, &workspace_num);

  // Print workspace
  for(int i=0; i<workspace_num; ++i)
    std::cout << workspaces[i] << "\n";

  // Clean up
  for(int i=0; i<workspace_num; ++i)
    delete [] workspaces[i];

  /* Finalize context. */
  tiledb_ctx_finalize(tiledb_ctx);

  return 0;
}
