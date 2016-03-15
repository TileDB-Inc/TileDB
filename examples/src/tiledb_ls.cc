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
  char* dirs[100];
  int dir_num = 100;
  int dir_types[100];
  const char* parent_dir = "workspace";
  for(int i=0; i<dir_num; ++i)
    dirs[i] = new char[TILEDB_NAME_MAX_LEN];
  tiledb_ls(tiledb_ctx, parent_dir, (char**) dirs, dir_types, &dir_num);

  // Print workspace
  for(int i=0; i<dir_num; ++i) {
    std::cout << dirs[i] << " ";
    if(dir_types[i] == TILEDB_ARRAY)
      std::cout << "ARRAY\n";
    else if(dir_types[i] == TILEDB_METADATA)
      std::cout << "METADATA\n";
    else if(dir_types[i] == TILEDB_GROUP)
      std::cout << "GROUP\n";
    else if(dir_types[i] == TILEDB_WORKSPACE)
      std::cout << "WORKSPACE\n";
  }
 
  // Clean up
  for(int i=0; i<dir_num; ++i)
    delete [] dirs[i];

  /* Finalize context. */
  tiledb_ctx_finalize(tiledb_ctx);

  return 0;
}
