/**
 * @file   encryption.c
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2018 TileDB, Inc.
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
 * This is a part of the TileDB encryption tutorial:
 *   https://docs.tiledb.io/en/latest/tutorials/encryption.html
 *
 * When run, this program will create an encrypted 2D dense array, write some
 * data to it, and read a slice of the data back.
 *
 */

#include <stdio.h>
#include <string.h>
#include <tiledb/tiledb.h>

// Name of array.
const char* array_name = "encrypted_array";

// The 256-bit encryption key, stored as a string for convenience.
const char encryption_key[32 + 1] = "0123456789abcdeF0123456789abcdeF";

void create_array() {
  // Create TileDB context
  tiledb_ctx_t* ctx;
  tiledb_ctx_alloc(NULL, &ctx);

  // The array will be 4x4 with dimensions "rows" and "cols", with domain [1,4].
  int dim_domain[] = {1, 4, 1, 4};
  int tile_extents[] = {4, 4};
  tiledb_dimension_t* d1;
  tiledb_dimension_alloc(
      ctx, "rows", TILEDB_INT32, &dim_domain[0], &tile_extents[0], &d1);
  tiledb_dimension_t* d2;
  tiledb_dimension_alloc(
      ctx, "cols", TILEDB_INT32, &dim_domain[2], &tile_extents[1], &d2);

  // Create domain
  tiledb_domain_t* domain;
  tiledb_domain_alloc(ctx, &domain);
  tiledb_domain_add_dimension(ctx, domain, d1);
  tiledb_domain_add_dimension(ctx, domain, d2);

  // Create a single attribute "a" so each (i,j) cell can store an integer
  tiledb_attribute_t* a;
  tiledb_attribute_alloc(ctx, "a", TILEDB_INT32, &a);

  // Create array schema
  tiledb_array_schema_t* array_schema;
  tiledb_array_schema_alloc(ctx, TILEDB_DENSE, &array_schema);
  tiledb_array_schema_set_cell_order(ctx, array_schema, TILEDB_ROW_MAJOR);
  tiledb_array_schema_set_tile_order(ctx, array_schema, TILEDB_ROW_MAJOR);
  tiledb_array_schema_set_domain(ctx, array_schema, domain);
  tiledb_array_schema_add_attribute(ctx, array_schema, a);

  // Create encrypted array with AES-256-GCM.
  tiledb_array_create_with_key(
      ctx,
      array_name,
      array_schema,
      TILEDB_AES_256_GCM,
      encryption_key,
      (uint32_t)strlen(encryption_key));

  // Clean up
  tiledb_attribute_free(&a);
  tiledb_dimension_free(&d1);
  tiledb_dimension_free(&d2);
  tiledb_domain_free(&domain);
  tiledb_array_schema_free(&array_schema);
  tiledb_ctx_free(&ctx);
}

void write_array() {
  // Create TileDB context
  tiledb_ctx_t* ctx;
  tiledb_ctx_alloc(NULL, &ctx);

  // Open encrypted array for writing
  tiledb_array_t* array;
  tiledb_array_alloc(ctx, array_name, &array);
  tiledb_array_open_with_key(
      ctx,
      array,
      TILEDB_WRITE,
      TILEDB_AES_256_GCM,
      encryption_key,
      (uint32_t)strlen(encryption_key));

  // Prepare some data for the array
  int data[] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};
  uint64_t data_size = sizeof(data);

  // Create the query
  tiledb_query_t* query;
  tiledb_query_alloc(ctx, array, TILEDB_WRITE, &query);
  tiledb_query_set_layout(ctx, query, TILEDB_ROW_MAJOR);
  tiledb_query_set_buffer(ctx, query, "a", data, &data_size);

  // Submit query
  tiledb_query_submit(ctx, query);

  // Close array
  tiledb_array_close(ctx, array);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
  tiledb_ctx_free(&ctx);
}

void read_array() {
  // Create TileDB context
  tiledb_ctx_t* ctx;
  tiledb_ctx_alloc(NULL, &ctx);

  // Open encrypted array for reading
  tiledb_array_t* array;
  tiledb_array_alloc(ctx, array_name, &array);
  tiledb_array_open_with_key(
      ctx,
      array,
      TILEDB_READ,
      TILEDB_AES_256_GCM,
      encryption_key,
      (uint32_t)strlen(encryption_key));

  // Slice only rows 1, 2 and cols 2, 3, 4
  int subarray[] = {1, 2, 2, 4};

  // Prepare the vector that will hold the result (of size 6 elements)
  int data[6];
  uint64_t data_size = sizeof(data);

  // Create query
  tiledb_query_t* query;
  tiledb_query_alloc(ctx, array, TILEDB_READ, &query);
  tiledb_query_set_subarray(ctx, query, subarray);
  tiledb_query_set_layout(ctx, query, TILEDB_ROW_MAJOR);
  tiledb_query_set_buffer(ctx, query, "a", data, &data_size);

  // Submit query
  tiledb_query_submit(ctx, query);

  // Close array
  tiledb_array_close(ctx, array);

  // Print out the results.
  for (int i = 0; i < 6; ++i)
    printf("%d ", data[i]);
  printf("\n");

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
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
    write_array();
  }

  read_array();

  return 0;
}