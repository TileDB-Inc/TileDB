/*
 * Demonstrates how to write to dense array "workspace/A", in dense mode.
 */

#include "c_api.h"
#include <iostream>

int main() {
  /* Intialize context with the default configuration parameters. */
  TileDB_CTX* tiledb_ctx;
  tiledb_ctx_init(&tiledb_ctx, NULL);

  /* Initialize the array in WRITE mode. */
  TileDB_Metadata* tiledb_metadata;
  tiledb_metadata_init(
      tiledb_ctx, 
      &tiledb_metadata,
      "~/.tiledb/master_catalog",
      TILEDB_METADATA_WRITE,
      NULL,            // No projection - all attributes
      0);              // Meaningless when "attributes" is NULL

  /* Consolidate metadata. */
  tiledb_metadata_consolidate(tiledb_metadata); 

  /* Finalize the metadata. */
  tiledb_metadata_finalize(tiledb_metadata);

  /* Finalize context. */
  tiledb_ctx_finalize(tiledb_ctx);

  return 0;
}
