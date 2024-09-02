/**
 * @file   current_domain.c
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2024 TileDB, Inc.
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
 * When run, this program will create a simple 1D sparse array with a current
 * domain, print it, expand it with array schema evolution, and print it again.
 */

#include <stdio.h>
#include <stdlib.h>
#include <tiledb/tiledb.h>
#include <tiledb/tiledb_experimental.h>

// Name of array.
const char* array_name = "current_domain_array";

void create_array() {
  // Create TileDB context
  tiledb_ctx_t* ctx;
  tiledb_ctx_alloc(NULL, &ctx);

  // The array will be 1000x1 with dimension "d1", with domain [1,1000].
  int dim_domain[] = {1, 1000};
  int tile_extent = 50;
  tiledb_dimension_t* d1;
  tiledb_dimension_alloc(
      ctx, "d1", TILEDB_INT32, &dim_domain[0], &tile_extent, &d1);

  // Create domain
  tiledb_domain_t* domain;
  tiledb_domain_alloc(ctx, &domain);
  tiledb_domain_add_dimension(ctx, domain, d1);

  // Create current domain
  tiledb_current_domain_t* current_domain;
  tiledb_current_domain_create(ctx, &current_domain);

  // Create an n-dimensional rectangle
  tiledb_ndrectangle_t* ndrect;
  tiledb_ndrectangle_alloc(ctx, domain, &ndrect);

  // Assign the range [1, 100] to the rectangle's first dimension
  int32_t expanded_current_domain[] = {1, 100};
  tiledb_range_t range = {
      &expanded_current_domain[0],
      sizeof(expanded_current_domain[0]),
      &expanded_current_domain[1],
      sizeof(expanded_current_domain[1])};
  tiledb_ndrectangle_set_range_for_name(ctx, ndrect, "d1", &range);

  // Assign the rectangle to the current domain
  tiledb_current_domain_set_ndrectangle(ctx, current_domain, ndrect);

  // Create a single attribute "a" so each cell can store an integer
  tiledb_attribute_t* a;
  tiledb_attribute_alloc(ctx, "a", TILEDB_INT32, &a);

  // Create array schema
  tiledb_array_schema_t* array_schema;
  tiledb_array_schema_alloc(ctx, TILEDB_SPARSE, &array_schema);
  tiledb_array_schema_set_cell_order(ctx, array_schema, TILEDB_ROW_MAJOR);
  tiledb_array_schema_set_tile_order(ctx, array_schema, TILEDB_ROW_MAJOR);
  tiledb_array_schema_set_domain(ctx, array_schema, domain);
  tiledb_array_schema_add_attribute(ctx, array_schema, a);

  // Assign the current domain to the array schema
  tiledb_array_schema_set_current_domain(ctx, array_schema, current_domain);

  // Create array
  tiledb_array_create(ctx, array_name, array_schema);

  // Clean up
  tiledb_array_schema_free(&array_schema);
  tiledb_attribute_free(&a);
  tiledb_ndrectangle_free(&ndrect);
  tiledb_current_domain_free(&current_domain);
  tiledb_domain_free(&domain);
  tiledb_dimension_free(&d1);
  tiledb_ctx_free(&ctx);
}

void print_current_domain() {
  // Create TileDB context
  tiledb_ctx_t* ctx;
  tiledb_ctx_alloc(NULL, &ctx);

  // Load array schema
  tiledb_array_schema_t* array_schema;
  tiledb_array_schema_load(ctx, array_name, &array_schema);

  // Get current domain
  tiledb_current_domain_t* current_domain;
  tiledb_array_schema_get_current_domain(ctx, array_schema, &current_domain);

  // Check if current domain is empty
  uint32_t is_empty;
  tiledb_current_domain_get_is_empty(ctx, current_domain, &is_empty);

  if (is_empty) {
    printf("Current domain: empty\n");
  } else {
    // Get current domain type
    tiledb_current_domain_type_t current_domain_type;
    tiledb_current_domain_get_type(ctx, current_domain, &current_domain_type);

    if (current_domain_type == TILEDB_NDRECTANGLE) {
      printf("Current domain type: NDRECTANGLE\n");

      // Get the ND rectangle
      tiledb_ndrectangle_t* ndrect;
      tiledb_current_domain_get_ndrectangle(ctx, current_domain, &ndrect);

      tiledb_range_t range;
      tiledb_ndrectangle_get_range_from_name(ctx, ndrect, "d1", &range);

      printf(
          "Current domain range: [%d, %d]\n",
          *(int*)range.min,
          *(int*)range.max);

      // Get datatype of range
      tiledb_datatype_t dtype;
      tiledb_ndrectangle_get_dtype(ctx, ndrect, 0, &dtype);
      const char* dtype_str;
      tiledb_datatype_to_str(dtype, &dtype_str);
      printf("Range 0 dtype: %s\n", dtype_str);

      // Get datatype of range by name
      tiledb_ndrectangle_get_dtype_from_name(ctx, ndrect, "d1", &dtype);
      tiledb_datatype_to_str(dtype, &dtype_str);
      printf("Range 0 dtype by name: %s\n", dtype_str);

      // Get dim num
      uint32_t ndim;
      tiledb_ndrectangle_get_dim_num(ctx, ndrect, &ndim);
      printf("Range 0 dtype by name: %d\n", ndim);

      // Clean up
      tiledb_ndrectangle_free(&ndrect);
    } else {
      printf("Current domain type: unknown\n");
    }
  }

  // Clean up
  tiledb_current_domain_free(&current_domain);
  tiledb_array_schema_free(&array_schema);
  tiledb_ctx_free(&ctx);
}

void expand_current_domain() {
  // Create TileDB context
  tiledb_ctx_t* ctx;
  tiledb_ctx_alloc(NULL, &ctx);

  // Load array schema
  tiledb_array_schema_t* array_schema;
  tiledb_array_schema_load(ctx, array_name, &array_schema);

  // Get domain
  tiledb_domain_t* domain;
  tiledb_array_schema_get_domain(ctx, array_schema, &domain);

  // Create schema evolution
  tiledb_array_schema_evolution_t* schema_evolution;
  tiledb_array_schema_evolution_alloc(ctx, &schema_evolution);

  // Create the new current domain
  tiledb_current_domain_t* new_current_domain;
  tiledb_current_domain_create(ctx, &new_current_domain);

  // Create an n-dimensional rectangle
  tiledb_ndrectangle_t* ndrect;
  tiledb_ndrectangle_alloc(ctx, domain, &ndrect);

  // Assign the range [1, 200] to the rectangle's first dimension
  int32_t expanded_current_domain[] = {1, 200};
  tiledb_range_t range = {
      &expanded_current_domain[0],
      sizeof(expanded_current_domain[0]),
      &expanded_current_domain[1],
      sizeof(expanded_current_domain[1])};
  tiledb_ndrectangle_set_range_for_name(ctx, ndrect, "d1", &range);

  // Set the rectangle to the current domain
  tiledb_current_domain_set_ndrectangle(ctx, new_current_domain, ndrect);

  // Expand the current domain
  tiledb_array_schema_evolution_expand_current_domain(
      ctx, schema_evolution, new_current_domain);

  // Evolve the array
  tiledb_array_evolve(ctx, array_name, schema_evolution);

  // Clean up
  tiledb_ndrectangle_free(&ndrect);
  tiledb_current_domain_free(&new_current_domain);
  tiledb_array_schema_evolution_free(&schema_evolution);
  tiledb_domain_free(&domain);
  tiledb_array_schema_free(&array_schema);
  tiledb_ctx_free(&ctx);
}

int main() {
  // Get object type
  tiledb_ctx_t* ctx;
  tiledb_ctx_alloc(NULL, &ctx);
  tiledb_object_t type;
  tiledb_object_type(ctx, array_name, &type);
  tiledb_ctx_free(&ctx);

  if (type != TILEDB_ARRAY) {
    create_array();
    print_current_domain();
    expand_current_domain();
  }

  print_current_domain();
  return 0;
}
