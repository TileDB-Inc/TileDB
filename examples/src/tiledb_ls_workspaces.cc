/*
 * File: tiledb_ls_workspaces.cc
 * 
 * It shows how to list the TileDB workspaces.
 *
 * It assumes that the following programs have been run:
 *    - tiledb_workspace_group_create.cc
 */

#include "c_api.h"
#include <cstdio>
#include <cstdlib>

int main() {
  // Intialize context with the default configuration parameters
  TileDB_CTX* tiledb_ctx;
  tiledb_ctx_init(&tiledb_ctx, NULL);

  // Initialize variables
  char* dirs[10];
  int dir_num = 10;
  int dir_types[10];
  for(int i=0; i<dir_num; ++i)
    dirs[i] = (char*) malloc(TILEDB_NAME_MAX_LEN);

  // List workspaces
  tiledb_ls_workspaces(tiledb_ctx, (char**) dirs, &dir_num);
  for(int i=0; i<dir_num; ++i)
    printf("%s\n", dirs[i]);
 
  // Clean up
  for(int i=0; i<dir_num; ++i)
    free(dirs[i]);

  // Finalize context
  tiledb_ctx_finalize(tiledb_ctx);

  return 0;
}
