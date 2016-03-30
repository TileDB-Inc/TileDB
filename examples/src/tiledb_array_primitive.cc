/*
 * File: tiledb_array_primitive.cc
 * 
 * It shows how to initialize/finalize an array, and explore its schema.
 *
 * It assumes that the following programs have been run:
 *    - tiledb_workspace_group_create.cc
 *    - tiledb_array_create_dense.cc
 *    - tiledb_array_create_sparse.cc
 */

#include "c_api.h"
#include <cstdio>

// Prints some schema info (you can enhance this to print the entire schema)
void print_some_array_schema_info(const TileDB_ArraySchema* array_schema);

int main() {
  /* Intialize context with the default configuration parameters. */
  TileDB_CTX* tiledb_ctx;
  tiledb_ctx_init(&tiledb_ctx, NULL);

  // ----- Dense array ----- //

  // Load array schema when the array is not initialized
  TileDB_ArraySchema array_schema;
  tiledb_array_load_schema(
      tiledb_ctx,                               // Context 
      "my_workspace/dense_arrays/my_array_A",   // Array name
      &array_schema);                           // Array schema struct

  // Print some array schema info
  print_some_array_schema_info(&array_schema);

  // Free array schema
  tiledb_array_free_schema(&array_schema);

  // ----- Sparse array ----- //

  // Initialize array
  TileDB_Array* tiledb_array;
  tiledb_array_init(
      tiledb_ctx,                                // Context 
      &tiledb_array,                             // Array object
      "my_workspace/sparse_arrays/my_array_B",   // Array name
      TILEDB_ARRAY_READ,                         // Mode
      NULL,                                      // Subarray (whole domain)
      NULL,                                      // Attributes (all attributes)
      0);                                        // Number of attributes

  // Get array schema when the array is initialized
  tiledb_array_get_schema(tiledb_array, &array_schema); 

  // Print some schema info
  print_some_array_schema_info(&array_schema);

  // Free array schema
  tiledb_array_free_schema(&array_schema);

  // Finalize array
  tiledb_array_finalize(tiledb_array);

  // Finalize context
  tiledb_ctx_finalize(tiledb_ctx);

  return 0;
}

void print_some_array_schema_info(const TileDB_ArraySchema* array_schema) {
  printf("Array name: %s\n",  array_schema->array_name_);
  printf("Attributes: ");
  for(int i=0; i<array_schema->attribute_num_; ++i)
    printf("%s ", array_schema->attributes_[i]); 
  printf("\n");
  if(array_schema->dense_)
    printf("The array is dense\n");
  else
    printf("The array is sparse\n");
}
