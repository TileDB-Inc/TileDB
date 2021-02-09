/**
 * @file   array_metadata.c
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2018-2021 TileDB, Inc.
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
 * This program shows how to write, read and consolidate array metadata.
 */

#include <stdio.h>
#include <stdlib.h>
#include <tiledb/tiledb.h>

// Name of array.
const char* array_name = "array_metadata_array";

void create_array() {
  // Create TileDB context
  tiledb_ctx_t* ctx;
  tiledb_ctx_alloc(NULL, &ctx);

  // Create some array (it can be dense or sparse, with
  // any number of dimensions and attributes).
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
  tiledb_array_schema_alloc(ctx, TILEDB_SPARSE, &array_schema);
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

void write_array_metadata() {
  // Create TileDB context
  tiledb_ctx_t* ctx;
  tiledb_ctx_alloc(NULL, &ctx);

  // Open array for writing
  tiledb_array_t* array;
  tiledb_array_alloc(ctx, array_name, &array);
  tiledb_array_open(ctx, array, TILEDB_WRITE);

  // Write some metadata
  int v = 100;
  tiledb_array_put_metadata(ctx, array, "aaa", TILEDB_INT32, 1, &v);
  float f[] = {1.1f, 1.2f};
  tiledb_array_put_metadata(ctx, array, "bb", TILEDB_FLOAT32, 2, f);

  // Close array - Important so that the metadata get flushed
  tiledb_array_close(ctx, array);

  // Clean up
  tiledb_array_free(&array);
  tiledb_ctx_free(&ctx);
}

void read_array_metadata() {
  // Create TileDB context
  tiledb_ctx_t* ctx;
  tiledb_ctx_alloc(NULL, &ctx);

  // Open array for reading
  tiledb_array_t* array;
  tiledb_array_alloc(ctx, array_name, &array);
  tiledb_array_open(ctx, array, TILEDB_READ);

  // Read with key
  tiledb_datatype_t v_type;
  uint32_t v_num;
  const void* v;
  tiledb_array_get_metadata(ctx, array, "aaa", &v_type, &v_num, &v);
  printf("Details of item with key: '%s'\n", "aaa");
  printf(
      "- Value type: %s\n",
      (v_type == TILEDB_INT32) ? "INT32" : "something went wrong");
  printf("- Value num: %u\n", v_num);
  printf("- Value: %i\n", *(const int*)v);

  tiledb_array_get_metadata(ctx, array, "bb", &v_type, &v_num, &v);
  printf("Details of item with key: '%s'\n", "bb");
  printf(
      "- Value type: %s\n",
      (v_type == TILEDB_FLOAT32) ? "FLOAT32" : "something went wrong");
  printf("- Value num: %u\n", v_num);
  printf("- Value: %f, %f\n", ((const float*)v)[0], ((const float*)v)[1]);

  // Enumerate all metadata items
  uint64_t num = 0;
  const char* key;
  uint32_t key_len;
  tiledb_array_get_metadata_num(ctx, array, &num);
  printf("Enumerate all metadata items:\n");
  for (uint64_t i = 0; i < num; ++i) {
    tiledb_array_get_metadata_from_index(
        ctx, array, i, &key, &key_len, &v_type, &v_num, &v);

    printf("# Item %i\n", (int)i);
    const char* v_type_str = (v_type == TILEDB_INT32) ? "INT32" : "FLOAT32";
    printf("- Key: %.*s\n", key_len, key);
    printf("- Value type: %s\n", v_type_str);
    printf("- Value num: %u\n", v_num);
    printf("- Value: ");
    if (v_type == TILEDB_INT32) {
      for (uint32_t j = 0; j < v_num; ++j)
        printf("%i ", ((const int*)v)[j]);
    } else if (v_type == TILEDB_FLOAT32) {
      for (uint32_t j = 0; j < v_num; ++j)
        printf("%f ", ((const float*)v)[j]);
    }
    printf("\n");
  }

  // Close array
  tiledb_array_close(ctx, array);
  tiledb_array_free(&array);
  tiledb_ctx_free(&ctx);
}

int main() {
  create_array();
  write_array_metadata();
  read_array_metadata();

  return 0;
}
