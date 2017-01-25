/**
 * @file   tiledb_array_primitive.cc
 *
 * @section LICENSE
 *
 * The MIT License
 * 
 * @copyright Copyright (c) 2016 MIT and Intel Corporation
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 * 
 * @section DESCRIPTION
 *
 * It shows how to initialize/finalize an array, and explore its schema.
 */

#include "tiledb.h"
#include <cstdio>

// Prints some schema info (you can enhance this to print the entire schema)
void print_some_array_schema_info(const TileDB_ArraySchema* array_schema);

int main() {
  /* Initialize context with the default configuration parameters. */
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
