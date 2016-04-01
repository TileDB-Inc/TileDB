/*
 * File: tiledb_metadata_primitive.cc
 * 
 * It shows how to initialize/finalize a metadata object and explore its schema.
 */

#include "c_api.h"
#include <cstdio>

// Prints some schema info (you can enhance this to print the entire schema)
void print_some_metadata_schema_info(
    const TileDB_MetadataSchema* metadata_schema);

int main() {
  /* Intialize context with the default configuration parameters. */
  TileDB_CTX* tiledb_ctx;
  tiledb_ctx_init(&tiledb_ctx, NULL);

  // ----- Get schema without metadata initialization ----- //

  // Load metadata schema when the metadata object is not initialized
  TileDB_MetadataSchema metadata_schema;
  tiledb_metadata_load_schema(
      tiledb_ctx,                                     // Context 
      "my_workspace/sparse_arrays/my_array_B/meta",   // Metadata name
      &metadata_schema);                              // Metadata schema struct

  // Print some metadata schema info
  print_some_metadata_schema_info(&metadata_schema);

  // Free metadata schema
  tiledb_metadata_free_schema(&metadata_schema);

  // ----- Get schema after metadata initialization ----- //

  // Initialize metadata
  TileDB_Metadata* tiledb_metadata;
  tiledb_metadata_init(
      tiledb_ctx,                                     // Context 
      &tiledb_metadata,                               // Array object
      "my_workspace/sparse_arrays/my_array_B/meta",   // Array name
      TILEDB_METADATA_READ,                           // Mode
      NULL,                                           // Attributes (all)
      0);                                             // Number of attributes

  // Get metadata schema when the metadata object is initialized
  tiledb_metadata_get_schema(tiledb_metadata, &metadata_schema); 

  // Print some schema info
  print_some_metadata_schema_info(&metadata_schema);

  // Free metadata schema
  tiledb_metadata_free_schema(&metadata_schema);

  // Finalize metadata
  tiledb_metadata_finalize(tiledb_metadata);

  // Finalize context
  tiledb_ctx_finalize(tiledb_ctx);

  return 0;
}

void print_some_metadata_schema_info(
    const TileDB_MetadataSchema* metadata_schema) {
  printf("Metadata name: %s\n",  metadata_schema->metadata_name_);
  printf("Attributes: ");
  for(int i=0; i<metadata_schema->attribute_num_; ++i)
    printf("%s ", metadata_schema->attributes_[i]); 
  printf("\n");
}
