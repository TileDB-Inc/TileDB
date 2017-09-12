/**
 * @file   tiledb_array_create_sparse.cc
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
 * It shows how to create a sparse array.
 *
 * Program output:
 *            $ ./tiledb_array_create_sparse
 *            - Array name: <current_working_dir>/my_sparse_array
 *            - Array type: sparse
 *            - Cell order: row-major
 *            - Tile order: row-major
 *            - Capacity: 2
 *
 *            ### Dimension ###
 *            - Name: d1
 *            - Type: INT64
 *            - Compressor: GZIP
 *            - Compression level: -1
 *            - Domain: [1,4]
 *            - Tile extent: 2
 *
 *            ### Dimension ###
 *            - Name: d2
 *            - Type: INT64
 *            - Compressor: GZIP
 *            - Compression level: -1
 *            - Domain: [1,4]
 *            - Tile extent: 2
 *
 *            ### Attribute ###
 *            - Name: a1
 *            - Type: INT32
 *            - Compressor: RLE
 *            - Compression level: -1
 *            - Cell val num: 1
 *
 *            ### Attribute ###
 *            - Name: a2
 *            - Type: CHAR
 *            - Compressor: BZIP2
 *            - Compression level: -1
 *            - Cell val num: var
 *
 *            ### Attribute ###
 *            - Name: a3
 *            - Type: FLOAT32
 *            - Compressor: BLOSC_SNAPPY
 *            - Compression level: -1
 *            - Cell val num: 2
 */

#include "tiledb.h"

#include <cstdlib>

int main() {
  // Initialize context with the default configuration parameters
  tiledb_ctx_t* ctx;
  tiledb_ctx_create(&ctx);

  // Delete existing array
  system("rm -rf my_sparse_array");

  // Prepare parameters for array_schema schema
  const char* array_name = "my_sparse_array";

  // Attributes
  tiledb_attribute_t* a1;
  tiledb_attribute_create(ctx, &a1, "a1", TILEDB_INT32);
  tiledb_attribute_set_compressor(ctx, a1, TILEDB_RLE, -1);
  tiledb_attribute_set_cell_val_num(ctx, a1, 1);
  tiledb_attribute_t* a2;
  tiledb_attribute_create(ctx, &a2, "a2", TILEDB_CHAR);
  tiledb_attribute_set_compressor(ctx, a2, TILEDB_BZIP2, -1);
  tiledb_attribute_set_cell_val_num(ctx, a2, tiledb_var_num());
  tiledb_attribute_t* a3;
  tiledb_attribute_create(ctx, &a3, "a3", TILEDB_FLOAT32);
  tiledb_attribute_set_compressor(ctx, a3, TILEDB_BLOSC_SNAPPY, -1);
  tiledb_attribute_set_cell_val_num(ctx, a3, 2);

  // Domain and tile extents
  int64_t domain[] = { 1, 4, 1, 4 };
  int64_t tile_extents[] = { 2, 2 };

  // Dimensions
  tiledb_dimension_t* d1;
  tiledb_dimension_create(ctx, &d1, "d1", TILEDB_INT64, &domain[0], &tile_extents[0]);
  tiledb_dimension_set_compressor(ctx, d1, TILEDB_GZIP, -1);
  tiledb_dimension_t* d2;
  tiledb_dimension_create(ctx, &d2, "d2", TILEDB_INT64, &domain[2], &tile_extents[1]);
  tiledb_dimension_set_compressor(ctx, d2, TILEDB_GZIP, -1);

  // Create array_schema schema
  tiledb_array_schema_t* array_schema;
  tiledb_array_schema_create(ctx, &array_schema, array_name);
  tiledb_array_schema_set_array_type(ctx, array_schema, TILEDB_SPARSE);
  tiledb_array_schema_set_capacity(ctx, array_schema, 2);
  tiledb_array_schema_add_attribute(ctx, array_schema, a1);
  tiledb_array_schema_add_attribute(ctx, array_schema, a2);
  tiledb_array_schema_add_attribute(ctx, array_schema, a3);
  tiledb_array_schema_add_dimension(ctx, array_schema, d1);
  tiledb_array_schema_add_dimension(ctx, array_schema, d2);

  // Check array schema
  if(tiledb_array_schema_check(ctx, array_schema) != TILEDB_OK)
    printf("Invalid array schema\n");

  // Create array
  tiledb_array_create(ctx, array_schema);

  // Load and dump array schema to make sure the array was created correctly
  tiledb_array_schema_t* loaded_array_schema;
  tiledb_array_schema_load(ctx, &loaded_array_schema, array_name);
  tiledb_array_schema_dump(ctx, loaded_array_schema, stdout);

  // Clean up
  tiledb_attribute_free(ctx, a1);
  tiledb_attribute_free(ctx, a2);
  tiledb_attribute_free(ctx, a3);
  tiledb_dimension_free(ctx, d1);
  tiledb_dimension_free(ctx, d2);
  tiledb_array_schema_free(ctx, array_schema);
  tiledb_ctx_free(ctx);

  return 0;
}
