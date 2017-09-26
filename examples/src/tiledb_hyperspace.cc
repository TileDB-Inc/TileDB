/**
 * @file   tiledb_hyperspace.cc
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
 * Explores the C API for handling a TileDB array hyperspace.
 *
 * Program output:
 *
 *     $ ./tiledb_hyperspace
 *       First dump:
 *       === Hyperspace ===
 *       - Dimensions type: UINT64
 *
 *       ### Dimension ###
 *       - Name: d1
 *       - Domain: [0,1000]
 *       - Tile extent: 10
 *
 *       ### Dimension ###
 *       - Name: d2
 *       - Domain: [100,1000]
 *       - Tile extent: 5
 *
 *       From getter:
 *       - Dimensions type: UINT64
 *
 *       From dimension iterator:
 *       ### Dimension ###
 *       - Name: d1
 *       - Domain: [0,1000]
 *       - Tile extent: 10
 *
 *       ### Dimension ###
 *       - Name: d2
 *       - Domain: [100,1000]
 *       - Tile extent: 5
 *
 */

#include "tiledb.h"

int main() {
  // Create TileDB context
  tiledb_ctx_t* ctx;
  tiledb_ctx_create(&ctx);

  // Create hyperspace
  uint64_t domain_d1 [] = { 0, 1000 };
  uint64_t domain_d2 [] = { 100, 1000 };
  uint64_t tile_extent_d1 = 10;
  uint64_t tile_extent_d2 = 5;
  tiledb_hyperspace_t* hyperspace;
  tiledb_hyperspace_create(ctx, &hyperspace, TILEDB_UINT64);
  tiledb_hyperspace_add_dimension(ctx, hyperspace, "d1", domain_d1, &tile_extent_d1);
  tiledb_hyperspace_add_dimension(ctx, hyperspace, "d2", domain_d2, &tile_extent_d2);

  // Print dimension contents
  printf("First dump:\n");
  tiledb_hyperspace_dump(ctx, hyperspace, stdout);

  // Get the type
  tiledb_datatype_t type;
  tiledb_hyperspace_get_type(ctx, hyperspace, &type);

  // Print retrieved info
  printf("\n From getter:\n");
  printf("- Dimensions type: %s\n", (type == TILEDB_UINT64) ? "UINT64" : "Error");

  // Dump dimensions using an iterator
  const tiledb_dimension_t* dim;
  tiledb_dimension_iter_t* dim_it;
  tiledb_dimension_iter_create(ctx, hyperspace, &dim_it);
  int done;
  tiledb_dimension_iter_done(ctx, dim_it, &done);
  printf("\n From dimension iterator:\n");
  while(done != 1) {
    tiledb_dimension_iter_here(ctx, dim_it, &dim);
    tiledb_dimension_dump(ctx, dim, stdout);
    tiledb_dimension_iter_next(ctx, dim_it);
    tiledb_dimension_iter_done(ctx, dim_it, &done);
    printf("\n");
  }

  // Use the following to go the beginning of the dimension list
  // tiledb_dimension_iter_first(ctx, dim_it);

  // Clean up
  tiledb_dimension_iter_free(ctx, dim_it);
  tiledb_hyperspace_free(ctx, hyperspace);
  tiledb_ctx_free(ctx);


  return 0;
}
