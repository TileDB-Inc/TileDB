/*
 * File: tiledb_array_write_dense_A.cc
 * 
 * Demonstrates how to write to dense array "workspace/A", in dense mode.
 */

#include "c_api.h"
#include <iostream>

int main() {
  /* Intialize context with the default configuration parameters. */
  TileDB_CTX* tiledb_ctx;
  tiledb_ctx_init(&tiledb_ctx, NULL);

  /* Initialize the metadata in WRITE mode. */
  TileDB_Metadata* tiledb_metadata;
  tiledb_metadata_init(
      tiledb_ctx, 
      &tiledb_metadata,
      "workspace/meta_A",
      TILEDB_METADATA_WRITE,
      NULL,            // No projection - all attributes
      0);              // Meaningless when "attributes" is NULL

  /* Prepare cell buffers for attributes "a1" and "a2". */
  int buffer_a1[] = { 1, 2, 3 };
  float buffer_a2[] = { 1.0, 1.1, 1.2 };
  char buffer_keys[] = "key1\0key2\0key3";
  const void* buffers[] = { buffer_a1, buffer_a2};
  size_t buffer_sizes[] = { 
      sizeof(buffer_a1), 
      sizeof(buffer_a2), 
  };
  size_t keys_size = sizeof(buffer_keys);

  /* Write to metadata. */
  tiledb_metadata_write(tiledb_metadata, buffer_keys, keys_size, buffers, buffer_sizes); 

  /* Finalize the metadata. */
  tiledb_metadata_finalize(tiledb_metadata);

  /* Finalize context. */
  tiledb_ctx_finalize(tiledb_ctx);

  return 0;
}
