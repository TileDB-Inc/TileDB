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
 */

#include "tiledb.h"

int main() {
  // Initialize context with the default configuration parameters
  tiledb_ctx_t* ctx;
  tiledb_ctx_create(&ctx);

  // Prepare parameters for array schema
  const char* array_name = "my_group/sparse_arrays/my_array_B";

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

  // Create array schema
  tiledb_array_schema_t* array_schema;
  tiledb_array_schema_create(ctx, &array_schema, array_name);
  tiledb_array_schema_set_array_type(ctx, array_schema, TILEDB_SPARSE);
  tiledb_array_schema_set_capacity(ctx, array_schema, 2);
  tiledb_array_schema_add_attribute(ctx, array_schema, a1);
  tiledb_array_schema_add_attribute(ctx, array_schema, a2);
  tiledb_array_schema_add_attribute(ctx, array_schema, a3);
  tiledb_array_schema_add_dimension(ctx, array_schema, d1);
  tiledb_array_schema_add_dimension(ctx, array_schema, d2);

  // Create array
  tiledb_array_create(ctx, array_schema);

  // Clean up
  tiledb_attribute_free(a1);
  tiledb_attribute_free(a2);
  tiledb_attribute_free(a3);
  tiledb_dimension_free(d1);
  tiledb_dimension_free(d2);
  tiledb_array_schema_free(array_schema);
  tiledb_ctx_free(ctx);

  return 0;
}
