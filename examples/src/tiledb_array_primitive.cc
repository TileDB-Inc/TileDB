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
void print_some_array_schema_info(const tiledb_array_schema_t* array_schema);

int main() {
  /* Initialize context with the default configuration parameters. */
  tiledb_ctx_t* ctx;
  tiledb_ctx_create(&ctx);

  // ----- Dense array ----- //

  // Load array schema when the array is not initialized
  tiledb_array_schema_t* array_schema;
  tiledb_array_schema_load(ctx, &array_schema, "my_group/dense_arrays/my_array_A");

  // Print some array schema info
  print_some_array_schema_info(array_schema);

  // Free array schema
  tiledb_array_schema_free(array_schema);

  // Finalize context
  tiledb_ctx_free(ctx);

  return 0;
}

void print_some_array_schema_info(const tiledb_array_schema_t* array_schema) {
  // TODO: Create a C API for dumping the array schema
}
