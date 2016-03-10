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

  // Get array schema
  TileDB_MetadataSchema metadata_schema;
  tiledb_metadata_load_schema(tiledb_ctx, "workspace/meta_A", &metadata_schema); 

  // Print schema
  std::cout << "Metadata name:\n";
  std::cout << metadata_schema.metadata_name_ << "\n"; 
  std::cout << "Attribute num:\n";
  std::cout << metadata_schema.attribute_num_ << "\n"; 
  std::cout << "Attributes:\n";
  for(int i=0; i<metadata_schema.attribute_num_; ++i)
    std::cout << metadata_schema.attributes_[i] << "\n"; 
  std::cout << "Types:\n";
  for(int i=0; i<metadata_schema.attribute_num_; ++i) 
    if(metadata_schema.types_[i] == TILEDB_INT32) 
      std::cout << "int32\n";
    else if(metadata_schema.types_[i] == TILEDB_INT64) 
      std::cout << "int64\n";
    else if(metadata_schema.types_[i] == TILEDB_FLOAT32) 
      std::cout << "float32\n";
    else if(metadata_schema.types_[i] == TILEDB_FLOAT64) 
      std::cout << "float64\n";
    else if(metadata_schema.types_[i] == TILEDB_CHAR) 
      std::cout << "char\n";
  std::cout << "Cell val num:\n";
  for(int i=0; i<metadata_schema.attribute_num_; ++i) 
    if(metadata_schema.cell_val_num_[i] == TILEDB_VAR_NUM)
      std::cout << "var\n";
    else
      std::cout << metadata_schema.cell_val_num_[i] << "\n";
  std::cout << "Capacity:\n";
  std::cout << metadata_schema.capacity_ << "\n";
  std::cout << "Compression:\n";
  for(int i=0; i<metadata_schema.attribute_num_+1; ++i) 
    if(metadata_schema.compression_[i] == TILEDB_NO_COMPRESSION)
      std::cout << "no compression\n";
    else if(metadata_schema.compression_[i] == TILEDB_GZIP)
      std::cout << "gzip\n";
  
  // Free array schema
  tiledb_metadata_free_schema(&metadata_schema);

  /* Finalize context. */
  tiledb_ctx_finalize(tiledb_ctx);

  return 0;
}
