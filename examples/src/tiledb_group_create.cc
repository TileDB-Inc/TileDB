/* 
 * File: tiledb_group_create.cc 
 * 
 * Creating a TileDB group. 
 */ 

#include "tiledb.h" 

int main() { 

  /* Initialize context */ 
  TileDB_CTX* tiledb_ctx; 
  tiledb_ctx_init(&tiledb_ctx); 

  /* Create group */ 
  tiledb_group_create(tiledb_ctx, "my_group"); 

  /* Finalize context */ 
  tiledb_ctx_finalize(tiledb_ctx); 

  return 0; 
}
