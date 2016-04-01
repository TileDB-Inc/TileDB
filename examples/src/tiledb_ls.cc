/*
 * File: tiledb_list.cc
 * 
 * It shows how to explore the contents of a TileDB directory.
 */

#include "c_api.h"
#include <cstdio>
#include <cstdlib>

int main(int argc, char** argv) {
  // Sanity check
  if(argc != 2) {
    fprintf(stderr, "Usage: ./tiledb_list parent_dir\n");
    return -1;
  }

  // Intialize context with the default configuration parameters
  TileDB_CTX* tiledb_ctx;
  tiledb_ctx_init(&tiledb_ctx, NULL);

  // Initialize variables
  char* dirs[10];
  int dir_num = 10;
  int dir_types[10];
  for(int i=0; i<dir_num; ++i)
    dirs[i] = (char*) malloc(TILEDB_NAME_MAX_LEN);

  // List TileDB objects
  tiledb_ls(
      tiledb_ctx,                                    // Context
      argv[1],                                       // Parent directory
      (char**) dirs,                                 // Directories
      dir_types,                                     // Directory types
      &dir_num);                                     // Directory number

  // Print TileDB objects
  for(int i=0; i<dir_num; ++i) {
    printf("%s ", dirs[i]);
    if(dir_types[i] == TILEDB_ARRAY)
      printf("ARRAY\n");
    else if(dir_types[i] == TILEDB_METADATA)
      printf("METADATA\n");
    else if(dir_types[i] == TILEDB_GROUP)
      printf("GROUP\n");
    else if(dir_types[i] == TILEDB_WORKSPACE)
      printf("WORKSPACE\n");
  }
 
  // Clean up
  for(int i=0; i<dir_num; ++i)
    free(dirs[i]);

  // Finalize context
  tiledb_ctx_finalize(tiledb_ctx);

  return 0;
}
