/*
 * File: tiledb_array_create_dense_var_A.cc
 * 
 * Demonstrates how to create a dense array, called "dense_var_A".
 */

#include "c_api.h"
#include <stdlib.h>
#include <string.h>

int main() {
  /* Initialize context with the default configuration parameters. */
  TileDB_CTX* tiledb_ctx;
  tiledb_ctx_init(&tiledb_ctx, NULL);

 /* 
  * Prepare the array schema struct, initializing all numeric members to 0
  * and pointers to NULL. 
  */
  TileDB_ArraySchema array_schema;

  // Set array schema
  const char* attributes[] = { "a1", "a2" };
  const char* dimensions[] = { "d1", "d2" };
  int64_t domain[] = { 1, 4, 1, 4 };
  int64_t tile_extents[] = { 2, 2 };
  const int types[] = { TILEDB_CHAR, TILEDB_FLOAT32, TILEDB_INT64 };
  const int cell_val_num[] = { TILEDB_VAR_NUM, 1 };
  tiledb_array_set_schema(
      // The array schema object
      &array_schema,
      // Array name
      "workspace/dense_var_A",
      // Attributes
      attributes,
      // Number of attributes
      2,
      // Dimensions
      dimensions,
      // Number of dimensions
      2,
      // Dense array
      1,
      // Domain
      domain,
      // Domain length in bytes
      4*sizeof(int64_t),
      // Tile extents (no regular tiles defined)
      tile_extents,
      // Tile extents in bytes
      2*sizeof(int64_t), 
      // Types 
      types,
      // Number of cell values per attribute (NULL means 1 everywhere)
      cell_val_num,
      // Cell order
      TILEDB_ROW_MAJOR,
      // Tile order (0 means ignore in sparse arrays and default in dense)
      0,
      // Capacity
      4,
      // Compression
      NULL
  );

  /* Create the array. */
  tiledb_array_create(tiledb_ctx, &array_schema); 

  /* Finalize context. */
  tiledb_ctx_finalize(tiledb_ctx);

  return 0;
}
