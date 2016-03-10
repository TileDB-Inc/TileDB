/*
 */

#include "c_api.h"
#include <stdlib.h>
#include <string.h>

int main() {
  /* Initialize context with the default configuration parameters. */
  TileDB_CTX* tiledb_ctx;
  tiledb_ctx_init(&tiledb_ctx, NULL);

  // Set metadata schema
  TileDB_MetadataSchema metadata_schema;
  const char* attributes[] = { "a1", "a2" };
  const int types[] = { TILEDB_INT32, TILEDB_FLOAT32 };
  tiledb_metadata_set_schema(
      // The array schema struct
      &metadata_schema,
      // Array name
      "workspace/meta_A",
      // Attributes
      attributes,
      // Number of attributes
      2,
      // Types 
      types,
      // Number of cell values per attribute (NULL means 1 everywhere)
      NULL,
      // Capacity
      4,
      // Compression
      NULL
  );

  /* Create the array. */
  tiledb_metadata_create(tiledb_ctx, &metadata_schema); 

  /* Finalize context. */
  tiledb_ctx_finalize(tiledb_ctx);

  return 0;
}
