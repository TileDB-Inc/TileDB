/*
 * File: tiledb_array_read_dense_A.cc
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
  const int64_t range[] = { 1, 8, 1, 8 };

  /* Subset over attribute "a1". */
  const char* attributes[] = { "a1" };
  //const char* attributes[] = { "a2" };

  /* Initialize the array in READ mode. */
  TileDB_Array* tiledb_array;
  tiledb_array_init(
      tiledb_ctx, 
      &tiledb_array,
      "workspace/dense_B",
      TILEDB_ARRAY_READ,
      range, 
      attributes,           
      1);      

  /* Prepare cell buffers for attribute "a1". */
  int buffer_a1[64];
  size_t buffer_a2[64];
  char buffer_a2_var[500];
  float buffer_a3[128];
  void* buffers[] = { buffer_a1 };
  size_t buffer_sizes[] = { sizeof(buffer_a1) };
  //void* buffers[] = { buffer_a2, buffer_a2_var };
  //size_t buffer_sizes[] = { sizeof(buffer_a2), sizeof(buffer_a2_var) };

  /* Read from array. */
  tiledb_array_read(tiledb_array, buffers, buffer_sizes); 

  /* Print the read values. */
  //int64_t result_num = buffer_sizes[0] / sizeof(int);
  int64_t result_num = buffer_sizes[0] / sizeof(size_t);
  for(int i=0; i<result_num; ++i) 
//    std::cout << buffer_a2_var[buffer_a2[i]] << "\n";
    if(buffer_a1[i] == TILEDB_EMPTY_INT32)
      std::cout << "EMPTY\n";
    else
      std::cout << buffer_a1[i] << "\n";
//    if(buffer_a2_var[buffer_a2[i]] == TILEDB_EMPTY_CHAR)
//      std::cout << "EMPTY\n";
//    else
//      std::cout << buffer_a2_var[buffer_a2[i]] << "\n";

  /* Finalize the array. */
  tiledb_array_finalize(tiledb_array);

  /* Finalize context. */
  tiledb_ctx_finalize(tiledb_ctx);

  return 0;
}
