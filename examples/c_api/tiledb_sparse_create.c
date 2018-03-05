/**
 * @file   tiledb_sparse_create.c
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2018 TileDB, Inc.
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
 * It shows how to create a sparse array. Make sure that no directory exists
 * with the name `my_sparse_array` in the current working directory.
 *
 * The created array looks as in figure
 * `<TileDB-repo>/examples/figures/sparse_schema.png`.
 *
 */

#include <tiledb/tiledb.h>

int main() {
  // Create TileDB context
  tiledb_ctx_t* ctx;
  tiledb_ctx_create(&ctx, NULL);

  // Create two dimensions with names `d1` and `d2`. They both have type
  // `uint64`, domain `[1,4]` and tile extent `2`.
  uint64_t dim_domain[] = {1, 4, 1, 4};
  uint64_t tile_extents[] = {2, 2};
  tiledb_dimension_t* d1;
  tiledb_dimension_create(
      ctx, &d1, "d1", TILEDB_UINT64, &dim_domain[0], &tile_extents[0]);
  tiledb_dimension_t* d2;
  tiledb_dimension_create(
      ctx, &d2, "d2", TILEDB_UINT64, &dim_domain[2], &tile_extents[1]);

  // Create domain
  tiledb_domain_t* domain;
  tiledb_domain_create(ctx, &domain);
  tiledb_domain_add_dimension(ctx, domain, d1);
  tiledb_domain_add_dimension(ctx, domain, d2);

  // Create three attributes `a1`, `a2`, and `a3`. The first is of type `int32`
  // and is compressed with `blosc-lz`, the second is of type `var char` and is
  // compressed with `gzip`, and the third is of type `float32:2` (that admits
  // two `float32` values per cell) and is compressed with `zstd`. Note that all
  // compression levels are set to `-1`, which implies the default level for
  // each compressor.
  tiledb_attribute_t* a1;
  tiledb_attribute_create(ctx, &a1, "a1", TILEDB_INT32);
  tiledb_attribute_set_compressor(ctx, a1, TILEDB_BLOSC_LZ, -1);
  tiledb_attribute_set_cell_val_num(ctx, a1, 1);
  tiledb_attribute_t* a2;
  tiledb_attribute_create(ctx, &a2, "a2", TILEDB_CHAR);
  tiledb_attribute_set_compressor(ctx, a2, TILEDB_GZIP, -1);
  tiledb_attribute_set_cell_val_num(ctx, a2, TILEDB_VAR_NUM);
  tiledb_attribute_t* a3;
  tiledb_attribute_create(ctx, &a3, "a3", TILEDB_FLOAT32);
  tiledb_attribute_set_compressor(ctx, a3, TILEDB_ZSTD, -1);
  tiledb_attribute_set_cell_val_num(ctx, a3, 2);

  // We create the array schema by setting the array type to `TILEDB_SPARSE`,
  // and the cell and tile order to `row-major`. We set the data tile capacity
  // to `2`. We also assign the array domain and attributes we created above.
  tiledb_array_schema_t* array_schema;
  tiledb_array_schema_create(ctx, &array_schema, TILEDB_SPARSE);
  tiledb_array_schema_set_cell_order(ctx, array_schema, TILEDB_ROW_MAJOR);
  tiledb_array_schema_set_tile_order(ctx, array_schema, TILEDB_ROW_MAJOR);
  tiledb_array_schema_set_capacity(ctx, array_schema, 2);
  tiledb_array_schema_set_domain(ctx, array_schema, domain);
  tiledb_array_schema_add_attribute(ctx, array_schema, a1);
  tiledb_array_schema_add_attribute(ctx, array_schema, a2);
  tiledb_array_schema_add_attribute(ctx, array_schema, a3);

  // Check array schema
  if (tiledb_array_schema_check(ctx, array_schema) != TILEDB_OK) {
    printf("Invalid array schema\n");
    return -1;
  }

  // Create array
  tiledb_array_create(ctx, "my_sparse_array", array_schema);

  // Clean up
  tiledb_attribute_free(ctx, &a1);
  tiledb_attribute_free(ctx, &a2);
  tiledb_attribute_free(ctx, &a3);
  tiledb_dimension_free(ctx, &d1);
  tiledb_dimension_free(ctx, &d2);
  tiledb_domain_free(ctx, &domain);
  tiledb_array_schema_free(ctx, &array_schema);
  tiledb_ctx_free(&ctx);

  return 0;
}
