/*
 * File: tiledb_array_create_sparse_A.cc
 * 
 * Demonstrates how to create a sparse array, called "sparse_A".
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

  /* Set array name to "sparse_A", inside (existing) workspace "workspace". */
  array_schema.array_name_ = "workspace/sparse_A";

  /* Set attributes and number of attributes. */
  const char* attributes[] = { "a1", "a2" };
  array_schema.attributes_ = attributes;
  array_schema.attribute_num_ = 2;

  /* Set capacity. */
  array_schema.capacity_ = 4;

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

  /* 
   * NOTE: The rest of the array schema members will be set to default values.
   * This implies that the array is sparse, has irregular tiles, no compression,
   * and consolidation step equal to 1.
   */

  /* Create the array. */
  tiledb_array_create(tiledb_ctx, &array_schema); 

  /* Finalize context. */
  tiledb_ctx_finalize(tiledb_ctx);

  return 0;
}
