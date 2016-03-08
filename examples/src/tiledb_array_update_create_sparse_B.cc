/*
 * 
 * Demonstrates how to create a dense array, called "dense_A".
 */

#include "c_api.h"

int main() {
  /* Intialize context with the default configuration parameters. */
  TileDB_CTX* tiledb_ctx;
  tiledb_ctx_init(&tiledb_ctx, NULL);

  // Set array schema
  TileDB_ArraySchema array_schema;
  const char* attributes[] = { "a1", "a2", "a3" };
  const char* dimensions[] = { "d1", "d2" };
  int64_t domain[] = { 1, 8, 1, 8 };
  int64_t tile_extents[] = { 2, 2 };
  const int types[] = 
      { TILEDB_INT32, TILEDB_CHAR, TILEDB_FLOAT32, TILEDB_INT64 };
  const int cell_val_num[] = { 1, TILEDB_VAR_NUM, 2 };
  tiledb_array_set_schema(
      // The array schema struct
      &array_schema,
      // Array name
      "workspace/sparse_B",
      // Attributes
      attributes,
      // Number of attributes
      3,
      // Dimensions
      dimensions,
      // Number of dimensions
      2,
      // Sparse array
      0,
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
