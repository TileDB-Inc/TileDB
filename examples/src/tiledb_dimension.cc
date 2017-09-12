/**
 * @file   tiledb_dimension.cc
 *
 * @section LICENSE
 *
 * The MIT License
 * 
 * @copyright Copyright (c) 2017 MIT, Intel Corporation and TileDB, Inc.
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
 *
 * Explores the C API for handling TileDB dimensions.
 *
 * Program output:
 *
 *     $ ./tiledb_dimension
 *     First dump:
 *     ### Dimension ###
 *     - Name: d1
 *     - Type: UINT64
 *     - Compressor: NO_COMPRESSION
 *     - Compression level: -1
 *     - Domain: [0,1000]
 *     - Tile extent: 10
 *
 *     Second dump:
 *     ### Dimension ###
 *     - Name: d1
 *     - Type: UINT64
 *     - Compressor: ZSTD
 *     - Compression level: 6
 *     - Domain: [0,1000]
 *     - Tile extent: 10
 *
 *     From getters:
 *     - Name: d1
 *     - Type: UINT64
 *     - Compressor: ZSTD
 *     - Compression level: 6
 *     - Domain: [0,1000]
 *     - Tile extent: 10
 */

#include "tiledb.h"

int main() {
  // Create TileDB context
  tiledb_ctx_t* ctx;
  tiledb_ctx_create(&ctx);

  // Create dimension
  tiledb_dimension_t* dim;
  uint64_t domain [] = { 0, 1000 };
  uint64_t tile_extent = 10;
  tiledb_dimension_create(ctx, &dim, "d1", TILEDB_UINT64, domain, &tile_extent);


  // Print dimension contents
  printf("First dump:\n");
  tiledb_dimension_dump(ctx, dim, stdout);


  // Set compressor
  tiledb_dimension_set_compressor(ctx, dim, TILEDB_ZSTD, 6);

  // Print attribute contents again
  printf("\nSecond dump:\n");
  tiledb_dimension_dump(ctx, dim, stdout);

  // Use getters
  const char* dim_name;
  tiledb_datatype_t dim_type;
  tiledb_compressor_t dim_cmp;
  int dim_cmp_l;
  const void* dim_domain;
  const void* dim_tile_extent;
  tiledb_dimension_get_name(ctx, dim, &dim_name);
  tiledb_dimension_get_type(ctx, dim, &dim_type);
  tiledb_dimension_get_compressor(ctx, dim, &dim_cmp, &dim_cmp_l);
  tiledb_dimension_get_domain(ctx, dim, &dim_domain);
  tiledb_dimension_get_tile_extent(ctx, dim, &dim_tile_extent);

  // Print retrieved info
  printf("\n From getters:\n");
  printf("- Name: %s\n", dim_name);
  printf("- Type: %s\n", (dim_type == TILEDB_UINT64) ? "UINT64" : "Error");
  printf("- Compressor: %s\n", (dim_cmp == TILEDB_ZSTD) ? "ZSTD" : "Error");
  printf("- Compression level: %u\n", dim_cmp_l);
  printf("- Domain: [%llu,%llu]\n", ((uint64_t*)dim_domain)[0], ((uint64_t*)dim_domain)[1]);
  printf("- Tile extent: %llu\n", *((uint64_t*)dim_tile_extent));

  // Clean up
  tiledb_dimension_free(ctx, dim);
  tiledb_ctx_free(ctx);

  return 0;
}
