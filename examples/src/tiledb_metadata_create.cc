/*
 * File: tiledb_metadata_create.cc
 * 
 * It creates a metadata object.
 *
 * It assumes that the following programs have been run:
 *    - tiledb_workspace_group_create.cc
 *    - tiledb_array_create_sparse.cc
 */

#include "c_api.h"

int main() {
  // Initialize context with the default configuration parameters
  TileDB_CTX* tiledb_ctx;
  tiledb_ctx_init(&tiledb_ctx, NULL);

  // Prepare parameters for metadata schema
  const char* metadata_name = "my_workspace/sparse_arrays/my_array_B/meta";
  const char* attributes[] = { "a1", "a2" }; // Two attributes
  const int cell_val_num[] = 
  {
      1,                           // a1
      TILEDB_VAR_NUM               // a2
  };
  const int compression[] = 
  { 
        TILEDB_GZIP,              // a1 
        TILEDB_GZIP,              // a2
        TILEDB_NO_COMPRESSION     // TILEDB_KEY
  };
  const int types[] = 
  { 
      TILEDB_INT32,                // a1
      TILEDB_CHAR                  // a2
  };

  // Set metadata schema
  TileDB_MetadataSchema metadata_schema;
  tiledb_metadata_set_schema(
      &metadata_schema,            // Metadata schema struct
      metadata_name,               // Metadata name
      attributes,                  // Attributes
      2,                           // Number of attributes
      4,                           // Capacity
      cell_val_num,                // Number of cell values per attribute  
      compression,                 // Compression
      types                        // Types
  );

  // Create metadata
  tiledb_metadata_create(tiledb_ctx, &metadata_schema); 

  // Free metadata schema
  tiledb_metadata_free_schema(&metadata_schema);

  // Finalize context
  tiledb_ctx_finalize(tiledb_ctx);

  return 0;
}
