/**
 * @file   fragment_info.c
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2021 TileDB, Inc.
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
 * When run, this program will create a simple 2D dense array, write some data
 * with one query (creating a fragment) and collect information on the fragment.
 */

#include <stdio.h>
#include <string.h>
#include <tiledb/tiledb.h>

// Name of array.
const char* array_name = "fragment_info_array";

void create_array() {
  // Create TileDB context
  tiledb_ctx_t* ctx;
  tiledb_ctx_alloc(NULL, &ctx);

  // The array will be 4x4 with dimensions "rows" and "cols", with domain [1,4].
  int dim_domain[] = {1, 4, 1, 4};
  int tile_extents[] = {2, 2};
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

  // Create array
  tiledb_array_create(ctx, array_name, array_schema);

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

  // Open array for writing
  tiledb_array_t* array;
  tiledb_array_alloc(ctx, array_name, &array);
  tiledb_array_open(ctx, array, TILEDB_WRITE);

  // Prepare some data for the array
  int data[] = {1, 2, 3, 4, 5, 6, 7, 8};
  uint64_t data_size = sizeof(data);

  // Write in subarray [1,2], [1,4]
  int subarray[] = {1, 2, 1, 4};

  // Create the query
  tiledb_query_t* query;
  tiledb_query_alloc(ctx, array, TILEDB_WRITE, &query);
  tiledb_query_set_subarray(ctx, query, subarray);
  tiledb_query_set_layout(ctx, query, TILEDB_ROW_MAJOR);
  tiledb_query_set_data_buffer(ctx, query, "a", data, &data_size);

  // Submit query
  tiledb_query_submit(ctx, query);

  // Close array
  tiledb_array_close(ctx, array);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
  tiledb_ctx_free(&ctx);
}

void get_fragment_info() {
  // Create TileDB context
  tiledb_ctx_t* ctx;
  tiledb_ctx_alloc(NULL, &ctx);

  // Create fragment info object
  tiledb_fragment_info_t* fragment_info;
  tiledb_fragment_info_alloc(ctx, array_name, &fragment_info);

  // Load fragment
  tiledb_fragment_info_load(ctx, fragment_info);

  // Get number of written fragments.
  uint32_t num;
  tiledb_fragment_info_get_fragment_num(ctx, fragment_info, &num);
  printf("The number of written fragments is %d.\n", num);

  // Get fragment URI
  const char* uri;
  tiledb_fragment_info_get_fragment_uri(ctx, fragment_info, 0, &uri);
  printf("The fragment URI is %s.\n", uri);

  // Get fragment size
  uint64_t size;
  tiledb_fragment_info_get_fragment_size(ctx, fragment_info, 0, &size);
  printf("The fragment size is %u.\n", (uint32_t)size);

  // Check if the fragment is dense or sparse.
  int32_t dense;
  tiledb_fragment_info_get_dense(ctx, fragment_info, 0, &dense);
  if (dense == 1)
    printf("The fragment is dense.\n");
  else
    printf("The fragment is sparse.\n");

  // Get the fragment timestamp range
  uint64_t start;
  uint64_t end;
  tiledb_fragment_info_get_timestamp_range(ctx, fragment_info, 0, &start, &end);
  printf(
      "The fragment's timestamp range is {%u, %u}.\n",
      (uint32_t)start,
      (uint32_t)end);

  // Get the number of cells written to the fragment.
  uint64_t cell_num;
  tiledb_fragment_info_get_cell_num(ctx, fragment_info, 0, &cell_num);
  printf(
      "The number of cells written to the fragment is %u.\n",
      (uint32_t)cell_num);

  // Get the format version of the fragment.
  uint32_t version;
  tiledb_fragment_info_get_version(ctx, fragment_info, 0, &version);
  printf("The fragment's format version is %d.\n", version);

  // Check if fragment has consolidated metadata.
  // If not, get the number of fragments with unconsolidated metadata
  //  in the fragment info object.
  int32_t consolidated;
  tiledb_fragment_info_has_consolidated_metadata(
      ctx, fragment_info, 0, &consolidated);
  if (consolidated != 0) {
    printf("The fragment has consolidated metadata.\n");
  } else {
    uint32_t unconsolidated;
    tiledb_fragment_info_get_unconsolidated_metadata_num(
        ctx, fragment_info, &unconsolidated);
    printf(
        "The fragment has %d unconsolidated metadata fragments.\n",
        unconsolidated);
  }

  // Get non-empty domain from index
  uint64_t non_empty_dom[2];
  tiledb_fragment_info_get_non_empty_domain_from_index(
      ctx, fragment_info, 0, 0, &non_empty_dom[0]);

  // Clean up
  tiledb_fragment_info_free(&fragment_info);
  tiledb_ctx_free(&ctx);
}

int main() {
  // Get object type
  tiledb_ctx_t* ctx;
  tiledb_ctx_alloc(NULL, &ctx);
  tiledb_object_t type;
  tiledb_object_type(ctx, array_name, &type);

  if (type == TILEDB_ARRAY) {
    tiledb_object_remove(ctx, array_name);
  }
  tiledb_ctx_free(&ctx);

  create_array();
  write_array();
  get_fragment_info();

  return 0;
}