/**
 * @file   tiledb_array_schema.cc
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
 * Explores the C API for the array schema.
 *
 * Program output:
 *      $ ./tiledb_array_schema
 *      First dump:
 *      - Array name: <current_working_dir>/my_array
 *      - Array type: dense
 *      - Cell order: row-major
 *      - Tile order: row-major
 *      - Capacity: 10000
 *
 *      Second dump:
 *      - Array name: <current_working_dir>/my_array
 *      - Array type: sparse
 *      - Cell order: col-major
 *      - Tile order: col-major
 *      - Capacity: 10
 *
 *      Third dump:
 *      - Array name: <current_working_dir>/my_array
 *      - Array type: sparse
 *      - Cell order: col-major
 *      - Tile order: col-major
 *      - Capacity: 10
 *
 *      ### Dimension ###
 *      - Name: d1
 *      - Type: UINT64
 *      - Compressor: NO_COMPRESSION
 *      - Compression level: -1
 *      - Domain: [0,1000]
 *      - Tile extent: 10
 *
 *      ### Dimension ###
 *      - Name: d2
 *      - Type: UINT64
 *      - Compressor: RLE
 *      - Compression level: -1
 *      - Domain: [100,10000]
 *      - Tile extent: 100
 *
 *      ### Attribute ###
 *      - Name: a1
 *      - Type: INT32
 *      - Compressor: NO_COMPRESSION
 *      - Compression level: -1
 *      - Cell val num: 3
 *
 *      ### Attribute ###
 *      - Name: a2
 *      - Type: FLOAT32
 *      - Compressor: GZIP
 *      - Compression level: -1
 *      - Cell val num: 1
 *
 *      From getters:
 *      - Array name: <current_working_dir>/my_array
 *      - Array type: sparse
 *      - Cell order: col-major
 *      - Tile order: col-major
 *      - Capacity: 10
 *
 *      Array schema attribute names:
 *      * a1
 *      * a2
 *
 *      Array schema dimension names:
 *      * d1
 *      * d2
 */

#include "tiledb.h"

int main() {
  // Create TileDB context
  tiledb_ctx_t* ctx;
  tiledb_ctx_create(&ctx);

  // Create array schema
  tiledb_array_schema_t* array_schema;
  tiledb_array_schema_create(ctx, &array_schema, "my_array");

  // Print array schema contents
  printf("First dump:\n");
  tiledb_array_schema_dump(ctx, array_schema, stdout);

  // Set some values
  tiledb_array_schema_set_array_type(ctx, array_schema, TILEDB_SPARSE);
  tiledb_array_schema_set_tile_order(ctx, array_schema, TILEDB_COL_MAJOR);
  tiledb_array_schema_set_cell_order(ctx, array_schema, TILEDB_COL_MAJOR);
  tiledb_array_schema_set_capacity(ctx, array_schema, 10);

  // Print array schema contents again
  printf("\nSecond dump:\n");
  tiledb_array_schema_dump(ctx, array_schema, stdout);

  // Add attributes
  tiledb_attribute_t *a1, *a2;
  tiledb_attribute_create(ctx, &a1, "a1", TILEDB_INT32);
  tiledb_attribute_create(ctx, &a2, "a2", TILEDB_FLOAT32);
  tiledb_attribute_set_cell_val_num(ctx, a1, 3);
  tiledb_attribute_set_compressor(ctx, a2, TILEDB_GZIP, -1);
  tiledb_array_schema_add_attribute(ctx, array_schema, a1);
  tiledb_array_schema_add_attribute(ctx, array_schema, a2);

  // Add dimensions
  tiledb_dimension_t *d1, *d2;
  uint64_t d1_domain[] = {0, 1000};
  uint64_t d2_domain[] = {100, 10000};
  uint64_t d1_extent = 10;
  uint64_t d2_extent = 100;
  tiledb_dimension_create(ctx, &d1, "d1", TILEDB_UINT64, d1_domain, &d1_extent);
  tiledb_dimension_create(ctx, &d2, "d2", TILEDB_UINT64, d2_domain, &d2_extent);
  tiledb_dimension_set_compressor(ctx, d2, TILEDB_RLE, -1);
  tiledb_array_schema_add_dimension(ctx, array_schema, d1);
  tiledb_array_schema_add_dimension(ctx, array_schema, d2);

  // Print array schema contents again
  printf("\nThird dump:\n");
  tiledb_array_schema_dump(ctx, array_schema, stdout);

  // Get some values using getters
  const char* array_name;
  tiledb_array_type_t array_type;
  uint64_t capacity;
  tiledb_layout_t tile_order, cell_order;
  tiledb_array_schema_get_array_name(ctx, array_schema, &array_name);
  tiledb_array_schema_get_array_type(ctx, array_schema, &array_type);
  tiledb_array_schema_get_capacity(ctx, array_schema, &capacity);
  tiledb_array_schema_get_tile_order(ctx, array_schema, &tile_order);
  tiledb_array_schema_get_cell_order(ctx, array_schema, &cell_order);

  // Print from getters
  printf("\nFrom getters:\n");
  printf("- Array name: %s\n", array_name);
  printf("- Array type: %s\n", (array_type == TILEDB_DENSE) ? "dense" : "sparse");
  printf("- Cell order: %s\n", (cell_order == TILEDB_ROW_MAJOR) ? "row-major" : "col-major");
  printf("- Tile order: %s\n", (tile_order == TILEDB_ROW_MAJOR) ? "row-major" : "col-major");
  printf("- Capacity: %llu\n", capacity);

  // Print the attribute names using iterators
  printf("\nArray schema attribute names: \n");
  tiledb_attribute_iter_t* attr_iter;
  const tiledb_attribute_t* attr;
  const char* attr_name;
  tiledb_attribute_iter_create(ctx, array_schema, &attr_iter);
  int done;
  tiledb_attribute_iter_done(ctx, attr_iter, &done);
  while(done != 1) {
    tiledb_attribute_iter_here(ctx, attr_iter, &attr);
    tiledb_attribute_get_name(ctx, attr, &attr_name);
    printf("* %s\n", attr_name);
    tiledb_attribute_iter_next(ctx, attr_iter);
    tiledb_attribute_iter_done(ctx, attr_iter, &done);
  }

  // Print the dimension names using iterators
  printf("\nArray schema dimension names: \n");
  tiledb_dimension_iter_t* dim_iter;
  const tiledb_dimension_t* dim;
  const char* dim_name;
  tiledb_dimension_iter_create(ctx, array_schema, &dim_iter);
  tiledb_dimension_iter_done(ctx, dim_iter, &done);
  while(done != 1) {
    tiledb_dimension_iter_here(ctx, dim_iter, &dim);
    tiledb_dimension_get_name(ctx, dim, &dim_name);
    printf("* %s\n", dim_name);
    tiledb_dimension_iter_next(ctx, dim_iter);
    tiledb_dimension_iter_done(ctx, dim_iter, &done);
  }

  // Use the following to go the beginning of the dimension list
  // tiledb_dimension_iter_first(ctx, dim_iter);

  // Clean up
  tiledb_attribute_free(ctx, a1);
  tiledb_attribute_free(ctx, a2);
  tiledb_attribute_iter_free(ctx, attr_iter);
  tiledb_dimension_iter_free(ctx, dim_iter);
  tiledb_array_schema_free(ctx, array_schema);
  tiledb_ctx_free(ctx);

  return 0;
}

