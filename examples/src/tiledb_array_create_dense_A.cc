/*
 * File: tiledb_array_create_dense_A.cc
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

  /* Set array name to "dense_A", inside (existing) workspace "workspace". */
  array_schema.array_name_ = "workspace/dense_A";

  /* Set attributes and number of attributes. */
  const char* attributes[] = { "a1", "a2" };
  array_schema.attributes_ = attributes;
  array_schema.attribute_num_ = 2;

  /* Set cell order. */ 
  array_schema.cell_order_ = "column-major";

  /* Set dimensions and number of dimensions. */
  const char* dimensions[] = { "d1", "d2" };
  array_schema.dimensions_ = dimensions;
  array_schema.dim_num_ = 2; 

  /* 
   * Set types: **int32** for "a1", **float32** for "a2" and **int64** for the 
   * coordinates. 
   */
  const char* types[] = { "int32", "float32", "int64" };
  array_schema.types_ = types; 

  /* Set domain to [1,4], [1,4]. */
  const int64_t domain[] = { 1, 4, 1, 4};
  array_schema.domain_ = domain;

  /* The array has regular, 2x2 tiles. */
  const int64_t tile_extents[] = { 2, 2 };
  array_schema.tile_extents_ = tile_extents;

  /* The array is dense. */
  array_schema.dense_ = 1;

  /* Compression for "a1" is GZIP, none for the rest. */
  const char* compression[] = { "GZIP", "NONE", "NONE" };
  array_schema.compression_ = compression;

  /* 
   * NOTE: The rest of the array schema members will be set to default values.
   * This implies that the array has "row-major" tile order,
   * and consolidation step equal to 1.
   */

  /* Create the array. */
  tiledb_array_create(tiledb_ctx, &array_schema); 

  /* Finalize context. */
  tiledb_ctx_finalize(tiledb_ctx);

  return 0;
}
