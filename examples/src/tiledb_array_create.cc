/*
 * File: tiledb_array_create.cc
 * 
 * Creating an array. 
 */

#include "tiledb.h"

int main() {
  /* Intialize context. */
  TileDB_CTX* tiledb_ctx;
  tiledb_ctx_init(&tiledb_ctx);

 /*
  * Define the array schema struct.
  * IMPORTANT: Initialize it as empty, so that all numeric members get set
  * to 0, and all pointers to NULL. This is vital so that TileDB can set
  * default values for the members not set by the user.
  */
  TileDB_ArraySchema array_schema = {};

  /* Set array name to "A", inside (existing) workspace "my_workspace". */
  array_schema.array_name_ = "my_workspace/A";

  /* Set attributes and number of attributes. */
  int attribute_num = 2;
  array_schema.attributes_ = new const char*[attribute_num];
  array_schema.attributes_[0] = "a1";
  array_schema.attributes_[1] = "a2";
  array_schema.attribute_num_ = attribute_num;

  /* Set capacity. */
  array_schema.capacity_ = 2000;

  /* Set cell order. */ 
  array_schema.cell_order_ = "column-major";

  /* Set dimensions and number of dimensions. */
  int dim_num = 2;
  array_schema.dimensions_ = new const char*[dim_num];
  array_schema.dimensions_[0] = "d1";
  array_schema.dimensions_[1] = "d2";
  array_schema.dim_num_ = dim_num; 

  /* Set domain to [1,100], [1,100]. */
  array_schema.domain_ = new double[2*dim_num];
  array_schema.domain_[0] = 1;
  array_schema.domain_[1] = 100;
  array_schema.domain_[2] = 1;
  array_schema.domain_[3] = 100;

  /* 
   * Set types: **int32** for "a1", **float32** for "a2" and **int64** for the 
   * coordinates. 
   */
  array_schema.types_ = new const char*[attribute_num+1]; 
  array_schema.types_[0] = "int32";
  array_schema.types_[1] = "float32";
  array_schema.types_[2] = "int64";

  /* 
   * NOTE: The rest of the array schema members will be set to default values.
   * This implies that the array we are creating is sparse, with irregular
   * tiles, no compression, and consolidation step equal to 1.
   */

  /* Create the array. */
  tiledb_array_create(tiledb_ctx, &array_schema); 

  /* Finalize context. */
  tiledb_ctx_finalize(tiledb_ctx);

  /* Clean up. */
  delete [] array_schema.attributes_;
  delete [] array_schema.dimensions_;
  delete [] array_schema.domain_;
  delete [] array_schema.types_;

  return 0;
}
