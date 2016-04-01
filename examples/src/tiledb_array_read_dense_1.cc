/*
 * File: tiledb_array_read_dense_1.cc
 * 
 * It shows how to read a complete dense array.
 */

#include "c_api.h"
#include <cstdio>

int main() {
  // Intialize context with the default configuration parameters
  TileDB_CTX* tiledb_ctx;
  tiledb_ctx_init(&tiledb_ctx, NULL);

  // Initialize array 
  TileDB_Array* tiledb_array;
  tiledb_array_init(
      tiledb_ctx,                                       // Context
      &tiledb_array,                                    // Array object
      "my_workspace/dense_arrays/my_array_A",           // Array name
      TILEDB_ARRAY_READ,                                // Mode
      NULL,                                             // Whole domain
      NULL,                                             // All attributes
      0);                                               // Number of attributes

  // Prepare cell buffers 
  int buffer_a1[16];
  size_t buffer_a2[16];
  char buffer_var_a2[40];
  float buffer_a3[32];
  void* buffers[] = { buffer_a1, buffer_a2, buffer_var_a2, buffer_a3 };
  size_t buffer_sizes[] = 
  { 
      sizeof(buffer_a1),  
      sizeof(buffer_a2),
      sizeof(buffer_var_a2),
      sizeof(buffer_a3)
  };

  // Read from array
  tiledb_array_read(tiledb_array, buffers, buffer_sizes); 

  // Print cell values
  int64_t result_num = buffer_sizes[0] / sizeof(int);
  printf(" a1\t    a2\t   (a3.first, a3.second)\n");
  printf("-----------------------------------------\n");
  for(int i=0; i<result_num; ++i) { 
    printf("%3d", buffer_a1[i]);
    size_t var_size = (i != result_num-1) ? buffer_a2[i+1] - buffer_a2[i] 
                                          : buffer_sizes[2] - buffer_a2[i];
    printf("\t %4.*s", var_size, &buffer_var_a2[buffer_a2[i]]);
    printf("\t\t (%5.1f, %5.1f)\n", buffer_a3[2*i], buffer_a3[2*i+1]);
  }

  // Finalize the array
  tiledb_array_finalize(tiledb_array);

  /* Finalize context. */
  tiledb_ctx_finalize(tiledb_ctx);

  return 0;
}
