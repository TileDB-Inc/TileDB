/*
 * 
 * Demonstrates how to create a dense array, called "dense_A".
 */

#include "c_api.h"
#include <iostream>

int main() {
  /* Intialize context with the default configuration parameters. */
  TileDB_CTX* tiledb_ctx;
  tiledb_ctx_init(&tiledb_ctx, NULL);

  /* Initialize a range. */
  const int64_t range[] = { 1, 8, 1, 8 };

  /* Initialize the array in READ mode. */
  TileDB_Array* tiledb_array;
  tiledb_array_init(
      tiledb_ctx, 
      &tiledb_array,
      "workspace/dense_B",
      TILEDB_READ,
      range, 
      NULL,           
      1);

  // Get array schema
  TileDB_ArraySchema array_schema;
  tiledb_array_get_schema(tiledb_array, &array_schema); 

  // Print schema
  std::cout << "Array name:\n";
  std::cout << array_schema.array_name_ << "\n"; 
  std::cout << "Attribute num:\n";
  std::cout << array_schema.attribute_num_ << "\n"; 
  std::cout << "Attributes:\n";
  for(int i=0; i<array_schema.attribute_num_; ++i)
    std::cout << array_schema.attributes_[i] << "\n"; 
  std::cout << "Dim num:\n";
  std::cout << array_schema.dim_num_ << "\n"; 
  std::cout << "Dimensions:\n";
  for(int i=0; i<array_schema.dim_num_; ++i)
    std::cout << array_schema.dimensions_[i] << "\n"; 
  if(array_schema.types_[array_schema.attribute_num_] == TILEDB_INT32) {
    std::cout << "Domain:\n";
    int* domain = static_cast<int*>(array_schema.domain_);
    for(int i=0; i<array_schema.dim_num_; ++i) 
      std::cout << "[" << domain[2*i] << ", " << domain[2*i+1] << "]\n";
    std::cout << "Tile extents:\n";
    int* tile_extents = static_cast<int*>(array_schema.tile_extents_);
    if(tile_extents == NULL) {
      std::cout << "NULL\n";
    } else {
      for(int i=0; i<array_schema.dim_num_; ++i) 
        std::cout << tile_extents[i] << "\n";
    }
  } else if(array_schema.types_[array_schema.attribute_num_] == TILEDB_INT64) {
    std::cout << "Domain:\n";
    int64_t* domain = static_cast<int64_t*>(array_schema.domain_);
    for(int i=0; i<array_schema.dim_num_; ++i) 
      std::cout << "[" << domain[2*i] << ", " << domain[2*i+1] << "]\n";
    std::cout << "Tile extents:\n";
    int64_t* tile_extents = static_cast<int64_t*>(array_schema.tile_extents_);
    if(tile_extents == NULL) {
      std::cout << "NULL\n";
    } else {
      for(int i=0; i<array_schema.dim_num_; ++i) 
        std::cout << tile_extents[i] << "\n";
    }
  } else if(array_schema.types_[array_schema.attribute_num_] == TILEDB_FLOAT32) {
    std::cout << "Domain:\n";
    float* domain = static_cast<float*>(array_schema.domain_);
    for(int i=0; i<array_schema.dim_num_; ++i) 
      std::cout << "[" << domain[2*i] << ", " << domain[2*i+1] << "]\n";
    std::cout << "Tile extents:\n";
    float* tile_extents = static_cast<float*>(array_schema.tile_extents_);
    if(tile_extents == NULL) {
      std::cout << "NULL\n";
    } else {
      for(int i=0; i<array_schema.dim_num_; ++i) 
        std::cout << tile_extents[i] << "\n";
    }
  } else if(array_schema.types_[array_schema.attribute_num_] == TILEDB_FLOAT64) {
    std::cout << "Domain:\n";
    double* domain = static_cast<double*>(array_schema.domain_);
    for(int i=0; i<array_schema.dim_num_; ++i) 
      std::cout << "[" << domain[2*i] << ", " << domain[2*i+1] << "]\n";
    std::cout << "Tile extents:\n";
    double* tile_extents = static_cast<double*>(array_schema.tile_extents_);
    if(tile_extents == NULL) {
      std::cout << "NULL\n";
    } else {
      for(int i=0; i<array_schema.dim_num_; ++i) 
        std::cout << tile_extents[i] << "\n";
    }
  }
  std::cout << "Types:\n";
  for(int i=0; i<array_schema.attribute_num_+1; ++i) 
    if(array_schema.types_[i] == TILEDB_INT32) 
      std::cout << "int32\n";
    else if(array_schema.types_[i] == TILEDB_INT64) 
      std::cout << "int64\n";
    else if(array_schema.types_[i] == TILEDB_FLOAT32) 
      std::cout << "float32\n";
    else if(array_schema.types_[i] == TILEDB_FLOAT64) 
      std::cout << "float64\n";
    else if(array_schema.types_[i] == TILEDB_CHAR) 
      std::cout << "char\n";
  std::cout << "Cell val num:\n";
  for(int i=0; i<array_schema.attribute_num_; ++i) 
    if(array_schema.cell_val_num_[i] == TILEDB_VAR_NUM)
      std::cout << "var\n";
    else
      std::cout << array_schema.cell_val_num_[i] << "\n";
  std::cout << "Cell order:\n";
  if(array_schema.cell_order_ == TILEDB_ROW_MAJOR) 
    std::cout << "row-major\n";
  else if(array_schema.cell_order_ == TILEDB_COL_MAJOR) 
    std::cout << "column-major\n";
  else if(array_schema.cell_order_ == TILEDB_HILBERT) 
    std::cout << "hilbert\n";
  std::cout << "Tile order:\n";
  if(array_schema.tile_order_ == TILEDB_ROW_MAJOR) 
    std::cout << "row-major\n";
  else if(array_schema.tile_order_ == TILEDB_COL_MAJOR) 
    std::cout << "column-major\n";
  std::cout << "Capacity:\n";
  std::cout << array_schema.capacity_ << "\n";
  std::cout << "Compression:\n";
  for(int i=0; i<array_schema.attribute_num_+1; ++i) 
    if(array_schema.compression_[i] == TILEDB_NO_COMPRESSION)
      std::cout << "no compression\n";
    else if(array_schema.compression_[i] == TILEDB_GZIP)
      std::cout << "gzip\n";

  // Free array schema
  tiledb_array_free_schema(&array_schema);

  /* Finalize the array. */
  tiledb_array_finalize(tiledb_array);

  /* Finalize context. */
  tiledb_ctx_finalize(tiledb_ctx);

  return 0;
}
