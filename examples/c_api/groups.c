/**
 * @file   groups.c
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022 TileDB, Inc.
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
 * This program creates a hierarchy as shown below. Specifically, it creates
 * groups `my_group` and `sparse_arrays`, and
 * then some dense/sparse arrays.
 *
 * my_group/
 * ├── dense_arrays
 * │   ├── array_A
 * │   └── array_B
 * └── sparse_arrays
 *     ├── array_C
 *     └── array_D
 *
 * The program then shows how to group these together using the TileDB Group API
 */

#include <stdio.h>
#include <stdlib.h>
#include <tiledb/tiledb.h>
#include <tiledb/tiledb_experimental.h>

void create_array(const char* array_name, tiledb_array_type_t type) {
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
  tiledb_array_schema_alloc(ctx, type, &array_schema);
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

void create_arrays_groups() {
  // Create context
  tiledb_ctx_t* ctx;
  tiledb_ctx_alloc(NULL, &ctx);

  // Create groups
  tiledb_group_create(ctx, "my_group");
  tiledb_group_create(ctx, "my_group/sparse_arrays");

  // Create dense_arrays folder
  tiledb_vfs_t* vfs;
  tiledb_vfs_alloc(ctx, NULL, &vfs);
  tiledb_vfs_create_dir(ctx, vfs, "my_group/dense_arrays");
  tiledb_vfs_free(&vfs);

  // Create arrays
  create_array("my_group/dense_arrays/array_A", TILEDB_DENSE);
  create_array("my_group/dense_arrays/array_B", TILEDB_DENSE);
  create_array("my_group/sparse_arrays/array_C", TILEDB_SPARSE);
  create_array("my_group/sparse_arrays/array_D", TILEDB_SPARSE);

  // Add members to groups
  tiledb_group_t* my_group;
  tiledb_group_t* sparse_arrays_group;

  tiledb_group_alloc(ctx, "my_group", &my_group);
  tiledb_group_open(ctx, my_group, TILEDB_WRITE);

  tiledb_group_add_member(ctx, my_group, "dense_arrays/array_A", 1, NULL);
  tiledb_group_add_member(ctx, my_group, "dense_arrays/array_B", 1, "array_b");
  tiledb_group_add_member(
      ctx, my_group, "sparse_arrays", 1, "sparse_arrays_group");

  tiledb_group_alloc(ctx, "my_group/sparse_arrays", &sparse_arrays_group);
  tiledb_group_open(ctx, sparse_arrays_group, TILEDB_WRITE);
  tiledb_group_add_member(ctx, sparse_arrays_group, "array_C", 1, NULL);
  tiledb_group_add_member(ctx, sparse_arrays_group, "array_D", 1, NULL);

  tiledb_group_close(ctx, my_group);
  tiledb_group_close(ctx, sparse_arrays_group);

  // Clean up
  tiledb_group_free(&my_group);
  tiledb_group_free(&sparse_arrays_group);
  tiledb_ctx_free(&ctx);
}

void print_group() {
  // Create context
  tiledb_ctx_t* ctx;
  tiledb_ctx_alloc(NULL, &ctx);
  tiledb_group_t* my_group;

  tiledb_group_alloc(ctx, "my_group", &my_group);
  tiledb_group_open(ctx, my_group, TILEDB_READ);

  tiledb_string_t* str;
  const char* str_ptr;
  size_t str_len;
  tiledb_group_dump_str_v2(ctx, my_group, &str, 1);

  tiledb_string_view(str, &str_ptr, &str_len);
  printf("%s\n", str_ptr);

  tiledb_string_free(&str);
  tiledb_group_close(ctx, my_group);
  tiledb_group_free(&my_group);
}

int main() {
  create_arrays_groups();
  print_group();

  return 0;
}
