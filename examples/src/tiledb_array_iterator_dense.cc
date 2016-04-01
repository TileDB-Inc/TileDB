/*
 * File: tiledb_array_iterator_dense.cc
 * 
 * It shows how to use an iterator for dense arrays.
 *
 * It assumes that the following programs have been run:
 *    - tiledb_workspace_group_create.cc
 *    - tiledb_array_create_dense.cc
 *    - tiledb_array_write_dense_1.cc
 *    - tiledb_array_update_dense_1.cc
 *    - tiledb_array_update_dense_2.cc
 */

#include "c_api.h"
#include <cstdio>

int main() {
  // Intialize context with the default configuration parameters
  TileDB_CTX* tiledb_ctx;
  tiledb_ctx_init(&tiledb_ctx, NULL);

  // Prepare cell buffers 
  int buffer_a1[3];
  void* buffers[] = { buffer_a1 };
  size_t buffer_sizes[] = { sizeof(buffer_a1) };

  // Subarray and attributes
  int64_t subarray[] = { 3, 4, 2, 4 }; 
  const char* attributes[] = { "a1" };

  // Initialize array 
  TileDB_ArrayIterator* tiledb_array_it;
  tiledb_array_iterator_init(
      tiledb_ctx,                                    // Context
      &tiledb_array_it,                              // Array iterator
      "my_workspace/dense_arrays/my_array_A",        // Array name
      subarray,                                      // Constrain in subarray
      attributes,                                    // Subset on attributes
      1,                                             // Number of attributes
      buffers,                                       // Buffers used internally
      buffer_sizes);                                 // Buffer sizes

  // Iterate over all values in subarray
  printf(" a1\n----\n");
  const int* a1_v;
  size_t a1_size;
  while(!tiledb_array_iterator_end(tiledb_array_it)) {
    // Get value
    tiledb_array_iterator_get_value(
        tiledb_array_it,     // Array iterator
        0,                   // Attribute id
        (const void**) &a1_v,// Value
        &a1_size);           // Value size (useful in variable-sized attributes)
    printf("%3d\n", *a1_v);

    // Advance iterator
    tiledb_array_iterator_next(tiledb_array_it);
  }
 
  // Finalize array 
  tiledb_array_iterator_finalize(tiledb_array_it);

  // Finalize context
  tiledb_ctx_finalize(tiledb_ctx);

  return 0;
}
