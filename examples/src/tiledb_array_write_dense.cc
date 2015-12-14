/*
 * File: tiledb_array_write_dense.cc
 * 
 * Creating and writing to a dense array. 
 */

#include "tiledb.h"

/* Defined after the main function. */
void create_a_dense_array(TileDB_CTX* tiledb_ctx);

int main() {
  /* Intialize context. */
  TileDB_CTX* tiledb_ctx;
  tiledb_ctx_init(&tiledb_ctx);

  /*
   * We first create a dense array "my_workspace/B", which has 2 dimensions,
   * a single **float** attribute "a1", domain [1,4], [1,4], and tile extent 2 
   * across each dimension. Both tile and cell orders are **row-major**.
   */
  create_a_dense_array(tiledb_ctx);

  /* 
   * We will write all 16 cells of the array, in the array tile and cell 
   * order. We first open the array in write mode.
   */
  int ad = tiledb_array_open(
               tiledb_ctx, 
               "my_workspace/B", 
               TILEDB_ARRAY_MODE_WRITE);

  /* 
   * Allocate space for the cell values. In our simple example, each cell
   * has a single **float** attribute. We will populate the 16 cells
   * with float values from 0.1 to 1.6, having a step of 0.1.
   */
  float cells[16]; 
  cells[0] = 0.1;
  for(int i=1; i<16; ++i)
    cells[i] = cells[i-1] + 0.1; 

  /* 
   * Write cells to array. Based on the tiling of the array and the
   * tile and cell orders, the cells are written in the following 
   * order of coordinates:
   * (1,1), (1,2), (2,1), (2,2), (1,3), (1,4), (2,3), (2,4)
   * (3,1), (3,2), (4,1), (4,2), (3,3), (3,4), (4,3), (4,4)
   */
  for(int i=0; i<16; ++i)
    tiledb_array_write_dense(tiledb_ctx, ad, &cells[i]);

  /* Close the array. */
  tiledb_array_close(tiledb_ctx, ad);

  /* Finalize context. */
  tiledb_ctx_finalize(tiledb_ctx);

  return 0;
}

void create_a_dense_array(TileDB_CTX* tiledb_ctx) {

  /* Prepare the array schema. */
  TileDB_ArraySchema array_schema = {};

  /* Set array name to "B", inside (existing) workspace "my_workspace". */
  array_schema.array_name_ = "my_workspace/B";

  /* Set attributes and number of attributes. */
  int attribute_num = 1;
  array_schema.attributes_ = new const char*[attribute_num];
  array_schema.attributes_[0] = "a1";
  array_schema.attribute_num_ = attribute_num;

  /* Set dimensions and number of dimensions. */
  int dim_num = 2;
  array_schema.dimensions_ = new const char*[dim_num];
  array_schema.dimensions_[0] = "d1";
  array_schema.dimensions_[1] = "d2";
  array_schema.dim_num_ = dim_num; 

  /* The array is dense. */
  array_schema.dense_ = 1;

  /* Set tile extents. */
  array_schema.tile_extents_ = new double[dim_num];
  array_schema.tile_extents_[0] = 2;
  array_schema.tile_extents_[1] = 2;

  /* Set domain to [1,3], [1,3]. */
  array_schema.domain_ = new double[2*dim_num];
  array_schema.domain_[0] = 1;
  array_schema.domain_[1] = 4;
  array_schema.domain_[2] = 1;
  array_schema.domain_[3] = 4;

  /* Set types: **float32** for "a1" and **int64** for the coordinates. */
  array_schema.types_ = new const char*[attribute_num+1]; 
  array_schema.types_[0] = "float32";
  array_schema.types_[1] = "int64";

  /* Create the array. */
  tiledb_array_create(tiledb_ctx, &array_schema); 

  /* Clean up. */
  delete [] array_schema.attributes_;
  delete [] array_schema.dimensions_;
  delete [] array_schema.tile_extents_;
  delete [] array_schema.domain_;
  delete [] array_schema.types_;
}
