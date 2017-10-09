/**
 * @file   tiledb_3d_sparse_create.cc
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
 * This creates a 3D sparse array with domain [1,10000], [1,10000], [1,10000]
 * and prints out the detailed array metadata. The domain values are int64,
 * whereas there is a single attribute of type int. The coordinates are
 * compressed by default with double-delta, whereas the attribute is not
 * compressed.
 */

#include "tiledb.h"

#include <cstdlib>

int main() {
  // Initialize context with the default configuration parameters
  tiledb_ctx_t* ctx;
  tiledb_ctx_create(&ctx);

  // Prepare parameters for array_metadata metadata
  const char* array_name = "3d_sparse_array";

  // Attributes
  tiledb_attribute_t* a1;
  tiledb_attribute_create(ctx, &a1, "a1", TILEDB_INT32);

  // Domain and tile extents
  int64_t dim_domain[] = {1, 10000, 1, 10000, 1, 10000};
  int64_t tile_extents[] = {1000, 1000, 1000};

  // Hyperspace
  tiledb_domain_t* domain;
  tiledb_domain_create(ctx, &domain, TILEDB_INT64);
  tiledb_domain_add_dimension(
      ctx, domain, "d1", &dim_domain[0], &tile_extents[0]);
  tiledb_domain_add_dimension(
      ctx, domain, "d2", &dim_domain[2], &tile_extents[1]);
  tiledb_domain_add_dimension(
      ctx, domain, "d3", &dim_domain[4], &tile_extents[2]);

  // Create array_metadata metadata
  tiledb_array_metadata_t* array_metadata;
  tiledb_array_metadata_create(ctx, &array_metadata, array_name);
  tiledb_array_metadata_set_array_type(ctx, array_metadata, TILEDB_SPARSE);
  tiledb_array_metadata_set_capacity(ctx, array_metadata, 10000);
  tiledb_array_metadata_set_domain(ctx, array_metadata, domain);
  tiledb_array_metadata_add_attribute(ctx, array_metadata, a1);

  // Check array metadata
  if (tiledb_array_metadata_check(ctx, array_metadata) != TILEDB_OK) {
    printf("Invalid array metadata\n");
    return -1;
  }

  // Create array
  tiledb_array_create(ctx, array_metadata);

  // Load and dump array metadata to make sure the array was created correctly
  tiledb_array_metadata_t* loaded_array_metadata;
  tiledb_array_metadata_load(ctx, &loaded_array_metadata, array_name);
  tiledb_array_metadata_dump(ctx, loaded_array_metadata, stdout);

  // Clean up
  tiledb_attribute_free(ctx, a1);
  tiledb_domain_free(ctx, domain);
  tiledb_array_metadata_free(ctx, array_metadata);
  tiledb_ctx_free(ctx);

  return 0;
}
