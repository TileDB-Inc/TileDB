/* 
 * File: tiledb_workspace_create.cc 
 * 
 * Creating a TileDB workspace. 
 */ 

#include "tiledb.h" 

int main() { 

  /* Initialize context. */ 
  TileDB_CTX* tiledb_ctx; 
  tiledb_ctx_init(&tiledb_ctx); 

  /* Create workspace. */ 
  tiledb_workspace_create(tiledb_ctx, "my_workspace", "my_catalog"); 

  /* Finalize context. */ 
  tiledb_ctx_finalize(tiledb_ctx); 

  return 0; 
}
