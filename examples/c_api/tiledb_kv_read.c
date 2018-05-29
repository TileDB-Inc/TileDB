/**
 * @file   tiledb_kv_read.c
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2018 TileDB, Inc.
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
 * This example shows how to read from a key-value store.
 *
 * You need to run the following to make it work:
 *
 * ```
 * $ ./tiledb_kv_create_c
 * $ ./tiledb_kv_write_c
 * $ ./tiledb_kv_read_c
 * a1	a2	(a3[0], a3[1])
 * -----------------------------
 * 1	a	(1.1, 1.2)
 *
 * Item with key '12345' does not exist
 * ```
 */

#include <assert.h>
#include <tiledb/tiledb.h>

int main() {
  // Create TileDB context
  tiledb_ctx_t* ctx;
  tiledb_ctx_alloc(&ctx, NULL);

  // Open a key-value store
  tiledb_kv_t* kv;
  tiledb_kv_alloc(ctx, "my_kv", &kv);
  tiledb_kv_open(ctx, kv, NULL, 0);

  // Get key-value item
  int key = 100;
  tiledb_datatype_t key_type = TILEDB_INT32;
  uint64_t key_size = sizeof(int);
  tiledb_kv_item_t* kv_item = NULL;
  tiledb_kv_get_item(ctx, kv, &kv_item, &key, key_type, key_size);

  // Get values
  const void *a1, *a2, *a3;
  tiledb_datatype_t a1_type, a2_type, a3_type;
  uint64_t a1_size, a2_size, a3_size;
  tiledb_kv_item_get_value(ctx, kv_item, "a1", &a1, &a1_type, &a1_size);
  tiledb_kv_item_get_value(ctx, kv_item, "a2", &a2, &a2_type, &a2_size);
  tiledb_kv_item_get_value(ctx, kv_item, "a3", &a3, &a3_type, &a3_size);

  // Verify correct types
  assert(
      a1_type == TILEDB_INT32 && a2_type == TILEDB_CHAR &&
      a3_type == TILEDB_FLOAT32);

  // Print result
  printf("a1\ta2\t(a3[0], a3[1])\n");
  printf("-----------------------------\n");
  printf("%d\t", *((const int*)a1));
  printf("%.*s\t", (int)a2_size, (const char*)a2);
  printf("(%.1f, %.1f)\n", ((const float*)a3)[0], ((const float*)a3)[1]);

  // Try to get item that does not exist
  tiledb_kv_item_t* kv_item_2 = NULL;
  int key2 = 12345;
  tiledb_kv_get_item(ctx, kv, &kv_item_2, &key2, key_type, key_size);
  if (kv_item_2 == NULL)
    printf("\nItem with key '%d' does not exist\n", key2);

  // Close kv
  tiledb_kv_close(ctx, kv);

  // Clean up
  tiledb_kv_free(&kv);
  tiledb_kv_item_free(&kv_item);
  tiledb_ctx_free(&ctx);

  return 0;
}
