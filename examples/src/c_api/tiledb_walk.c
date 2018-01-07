/**
 * @file   tiledb_walk.c
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017 TileDB, Inc.
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
 * It shows how to explore the contents of a TileDB directory.
 */

#include <tiledb.h>

int print_path(const char* path, tiledb_object_t type, void* data);

int main() {
  // Create TileDB context
  tiledb_ctx_t* ctx;
  tiledb_ctx_create(&ctx, NULL);

  // Walk in a path with a pre- and post-order traversal
  printf("Preorder traversal:\n");
  tiledb_walk(ctx, "my_group", TILEDB_PREORDER, print_path, NULL);
  printf("\nPostorder traversal:\n");
  tiledb_walk(ctx, "my_group", TILEDB_POSTORDER, print_path, NULL);

  // Finalize context
  tiledb_ctx_free(ctx);

  return 0;
}

int print_path(const char* path, tiledb_object_t type, void* data) {
  // Simply print the path and type
  (void)data;
  printf("%s ", path);
  switch (type) {
    case TILEDB_ARRAY:
      printf("ARRAY");
      break;
    case TILEDB_KEY_VALUE:
      printf("KEY_VALUE");
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
