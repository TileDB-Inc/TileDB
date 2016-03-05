/*
 * 
 * Demonstrates how to create a dense array, called "dense_A".
 */

#include "c_api.h"

int main() {
  /* Intialize context with the default configuration parameters. */
  TileDB_CTX* tiledb_ctx;
  tiledb_ctx_init(&tiledb_ctx, NULL);

 /* 
  * Prepare the array schema struct, initalizing all numeric members to 0
  * and pointers to NULL. 
  */
  TileDB_ArraySchema array_schema = {};

  /* Set array name to "dense_B", inside (existing) workspace "workspace". */
  array_schema.array_name_ = "workspace/sparse_B";

  /* Set attributes and number of attributes. */
  const char* attributes[] = { "a1", "a2", "a3" };
  array_schema.attributes_ = attributes;
  array_schema.attribute_num_ = 3;

  /* Set cell order. */ 
  array_schema.cell_order_ = "row-major";

  /* Set dimensions and number of dimensions. */
  const char* dimensions[] = { "d1", "d2" };
  array_schema.dimensions_ = dimensions;
  array_schema.dim_num_ = 2; 

  /* Set types. */
  const char* types[] = { "int32", "char:var", "float32:2", "int64" };
  array_schema.types_ = types; 

  /* Set domain to [1,4], [1,4]. */
  const int64_t domain[] = { 1, 8, 1, 8};
  array_schema.domain_ = domain;

  /* The array has regular, 2x2 tiles. */
  const int64_t tile_extents[] = { 2, 2 };
  array_schema.tile_extents_ = tile_extents;

  /* The array is sparse. */
  array_schema.dense_ = 0;

  /* Compression for "a1" is GZIP, none for the rest. */
  const char* compression[] = { "NONE", "NONE", "NONE", "NONE" };
  array_schema.compression_ = compression;

  /* Set capacity. */
  array_schema.capacity_ = 4;

  /* Create the array. */
  tiledb_array_create(tiledb_ctx, &array_schema); 

  /* Finalize context. */
  tiledb_ctx_finalize(tiledb_ctx);

  return 0;
}
