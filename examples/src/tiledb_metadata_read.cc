/*
 * Demonstrates how to read from dense array "workspace/A".
 */

#include "c_api.h"
#include <iostream>

int main() {
  /* Intialize context with the default configuration parameters. */
  TileDB_CTX* tiledb_ctx;
  tiledb_ctx_init(&tiledb_ctx, NULL);

  /* Subset over attribute "a1". */
  const char* attributes[] = { "a1" };

  /* Initialize the array in READ mode. */
  TileDB_Metadata* tiledb_metadata;
  tiledb_metadata_init(
      tiledb_ctx, 
      &tiledb_metadata,
      "workspace/meta_A",
      TILEDB_METADATA_READ,
      attributes,           
      1);      

  /* Prepare cell buffers for attribute "a1". */
  int buffer_a1[10];
  void* buffers[] = { buffer_a1 };
  size_t buffer_sizes[1] = { sizeof(buffer_a1) };

  /* Read from array. */
  tiledb_metadata_read(tiledb_metadata, "key1", buffers, buffer_sizes); 
  std::cout << buffer_a1[0] << "\n";
  tiledb_metadata_read(tiledb_metadata, "key2", buffers, buffer_sizes); 
  std::cout << buffer_a1[0] << "\n";

  /* Finalize the array. */
  tiledb_metadata_finalize(tiledb_metadata);

  /* Finalize context. */
  tiledb_ctx_finalize(tiledb_ctx);

  return 0;
}
