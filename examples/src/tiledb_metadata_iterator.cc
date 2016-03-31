/*
 * File: tiledb_metadata_iterator.cc
 * 
 * It shows how to use a metadata iterator.
 *
 * It assumes that the following programs have been run:
 *    - tiledb_workspace_group_create.cc
 *    - tiledb_array_create_sparse.cc
 *    - tiledb_metadata_create.cc
 *    - tiledb_metadata_write.cc
 *    - tiledb_metadata_update.cc
 */

#include "c_api.h"
#include <cstdio>

int main() {
  /* Intialize context with the default configuration parameters. */
  TileDB_CTX* tiledb_ctx;
  tiledb_ctx_init(&tiledb_ctx, NULL);

  // Subset over the attributes
  const char* attributes[] = { TILEDB_KEY };

  // Prepare cell buffers
  size_t buffer_key[8];
  char buffer_key_var[500];
  void* buffers[] = { buffer_key, buffer_key_var };
  size_t buffer_sizes[] = { sizeof(buffer_key), sizeof(buffer_key_var) };

  // Initialize metadata iterator
  TileDB_MetadataIterator* tiledb_metadata_iterator;
  tiledb_metadata_iterator_init(
      tiledb_ctx,                                    // Context
      &tiledb_metadata_iterator,                     // Metadata iterator
      "my_workspace/sparse_arrays/my_array_B/meta",  // Metadata name
      attributes,                                    // Attributes
      1,                                             // Number of attributes
      buffers,                                       // Buffers for internal use
      buffer_sizes);                                 // Sizes of buffers

  // Iterate over the metadata
  const char* key;
  size_t key_size;
  while(!tiledb_metadata_iterator_end(tiledb_metadata_iterator)) {
    // Get value 
    tiledb_metadata_iterator_get_value(
        tiledb_metadata_iterator,                    // Metadata iterator
        0,                                           // Attribute id
        (const void**) &key,                         // Key
        &key_size);                                  // Key size

    // Print only if it is not empty
    if(key[0] != TILEDB_EMPTY_CHAR)
      printf("%s\n", key);

    // Advance iterator
    tiledb_metadata_iterator_next(tiledb_metadata_iterator);
  }

  // Finalize metadata iterator
  tiledb_metadata_iterator_finalize(tiledb_metadata_iterator);

  // Finalize context
  tiledb_ctx_finalize(tiledb_ctx);

  return 0;
}
