/**
 * @file   object.c
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2021 TileDB, Inc.
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
 * groups `dense_arrays` and `sparse_arrays` in a group `my_group`, and
 * then some dense/sparse arrays and key-value store in those groups.
 *
 * my_group/
 * ├── dense_arrays
 * │   ├── array_A
 * │   └── array_B
 * └── sparse_arrays
 *     ├── array_C
 *     └── array_D
 *
 * The program then shows how to list this hierarchy, as well as
 * move/remove TileDB objects.
 */

#include <stdio.h>
#include <tiledb/tiledb.h>

int print_path(const char* path, tiledb_object_t type, void* data) {
  // Simply print the path and type
  (void)data;
  printf("%s ", path);
  switch (type) {
    case TILEDB_ARRAY:
      printf("ARRAY");
      break;
    case TILEDB_GROUP:
      printf("GROUP");
      break;
    default:
      printf("INVALID");
  }
  printf("\n");

  // Always iterate till the end
  return 1;
}

int list_obj(const char* path) {
  // Create TileDB context
  tiledb_ctx_t* ctx;
  tiledb_ctx_alloc(NULL, &ctx);

  // List children
  printf("\nListing hierarchy:\n");
  tiledb_object_ls(ctx, path, print_path, NULL);

  // Walk in a path with a pre- and post-order traversal
  printf("\nPreorder traversal:\n");
  tiledb_object_walk(ctx, path, TILEDB_PREORDER, print_path, NULL);
  printf("\nPostorder traversal:\n");
  tiledb_object_walk(ctx, path, TILEDB_POSTORDER, print_path, NULL);

  // Finalize context
  tiledb_ctx_free(&ctx);

  return 0;
}

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

void move_remove_obj() {
  // Create context
  tiledb_ctx_t* ctx;
  tiledb_ctx_alloc(NULL, &ctx);

  // Move and remove
  tiledb_object_move(ctx, "my_group", "my_group_2");
  tiledb_object_remove(ctx, "my_group_2/dense_arrays");
  tiledb_object_remove(ctx, "my_group_2/sparse_arrays/array_C");

  // Clean up
  tiledb_ctx_free(&ctx);
}

void create_hierarchy() {
  // Create context
  tiledb_ctx_t* ctx;
  tiledb_ctx_alloc(NULL, &ctx);

  // Create groups
  tiledb_group_create(ctx, "my_group");
  tiledb_group_create(ctx, "my_group/dense_arrays");
  tiledb_group_create(ctx, "my_group/sparse_arrays");

  // Create arrays
  create_array("my_group/dense_arrays/array_A", TILEDB_DENSE);
  create_array("my_group/dense_arrays/array_B", TILEDB_DENSE);
  create_array("my_group/sparse_arrays/array_C", TILEDB_SPARSE);
  create_array("my_group/sparse_arrays/array_D", TILEDB_SPARSE);

  // Clean up
  tiledb_ctx_free(&ctx);
}

int main() {
  create_hierarchy();
  list_obj("my_group");
  move_remove_obj();  // Renames `my_group` to `my_group_2`
  list_obj("my_group_2");

  return 0;
}
