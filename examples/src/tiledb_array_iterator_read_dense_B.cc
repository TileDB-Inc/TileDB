/*
 * 
 * Demonstrates how to read from dense array "workspace/A".
 */

#include "c_api.h"
#include <iostream>

int main() {
  /* Intialize context with the default configuration parameters. */
  TileDB_CTX* tiledb_ctx;
  tiledb_ctx_init(&tiledb_ctx, NULL);

  /* Initialize a range. */
  const int64_t range[] = { 2, 3, 1, 2 };

  /* Subset over attribute "a1". */
  //const char* attributes[] = { "a1" };
  const char* attributes[] = { "a2" };

  /* Prepare cell buffers for attribute "a1". */
  int buffer_a1[8];
  size_t buffer_a2[8];
  char buffer_a2_var[50];
  float buffer_a3[128];
  //void* buffers[] = { buffer_a1 };
  //size_t buffer_sizes[] = { sizeof(buffer_a1) };
  void* buffers[] = { buffer_a2, buffer_a2_var };
  size_t buffer_sizes[] = { sizeof(buffer_a2), sizeof(buffer_a2_var) };

  /* Initialize the array in READ mode. */
  TileDB_ArrayIterator* tiledb_array_iterator;
  tiledb_array_iterator_init(
      tiledb_ctx, 
      &tiledb_array_iterator,
      "workspace/dense_B",
      NULL, // range, 
      attributes,           
      1,
      buffers,
      buffer_sizes);      

  /* Read from array. */
  const char* b;
  size_t b_size;

  while(!tiledb_array_iterator_end(tiledb_array_iterator)) {
    tiledb_array_iterator_get_value(tiledb_array_iterator, 0, (const void**) &b, &b_size);
    std::cout << b << " " << b_size << "\n";
    tiledb_array_iterator_next(tiledb_array_iterator);
  }

  /* Finalize the array iterator. */
  tiledb_array_iterator_finalize(tiledb_array_iterator);

  /* Finalize context. */
  tiledb_ctx_finalize(tiledb_ctx);

  return 0;
}
