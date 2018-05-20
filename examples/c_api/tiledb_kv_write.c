/**
 * @file   tiledb_kv_write.c
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
 * It shows how to write to a key-value store.
 *
 * Run the following:
 *
 * $ ./tiledb_kv_create_c
 * $ ./tiledb_kv_write_c
 */

#include <string.h>
#include <tiledb/tiledb.h>

int main() {
  // Create TileDB context
  tiledb_ctx_t* ctx;
  tiledb_ctx_create(&ctx, NULL);

  // We first create some key-value items. Note that at this point these
  // are independent of the key-value store they will be inserted into.
  // Each item can have a key of any type, and values on any attribute
  // of any type.

  // Create first key-value item object
  int key1 = 100;
  int key1_a1 = 1;
  const char* key1_a2 = "a";
  float key1_a3[] = {1.1f, 1.2f};
  tiledb_kv_item_t* kv_item1;
  tiledb_kv_item_create(ctx, &kv_item1);
  tiledb_kv_item_set_key(ctx, kv_item1, &key1, TILEDB_INT32, sizeof(key1));
  tiledb_kv_item_set_value(
      ctx, kv_item1, "a1", &key1_a1, TILEDB_INT32, sizeof(key1_a1));
  tiledb_kv_item_set_value(
      ctx, kv_item1, "a2", key1_a2, TILEDB_CHAR, strlen(key1_a2));
  tiledb_kv_item_set_value(
      ctx, kv_item1, "a3", &key1_a3, TILEDB_FLOAT32, sizeof(key1_a3));

  // Create second key-value item object
  float key2 = 200.0;
  int key2_a1 = 2;
  const char* key2_a2 = "bb";
  float key2_a3[] = {2.1f, 2.2f};
  tiledb_kv_item_t* kv_item2;
  tiledb_kv_item_create(ctx, &kv_item2);
  tiledb_kv_item_set_key(ctx, kv_item2, &key2, TILEDB_FLOAT32, sizeof(key2));
  tiledb_kv_item_set_value(
      ctx, kv_item2, "a1", &key2_a1, TILEDB_INT32, sizeof(key2_a1));
  tiledb_kv_item_set_value(
      ctx, kv_item2, "a2", key2_a2, TILEDB_CHAR, strlen(key2_a2));
  tiledb_kv_item_set_value(
      ctx, kv_item2, "a3", &key2_a3, TILEDB_FLOAT32, sizeof(key2_a3));

  // Create third key-value item object
  double key3[] = {300.0, 300.1};
  int key3_a1 = 3;
  const char* key3_a2 = "ccc";
  float key3_a3[] = {3.1f, 3.2f};
  tiledb_kv_item_t* kv_item3;
  tiledb_kv_item_create(ctx, &kv_item3);
  tiledb_kv_item_set_key(ctx, kv_item3, &key3, TILEDB_FLOAT64, sizeof(key3));
  tiledb_kv_item_set_value(
      ctx, kv_item3, "a1", &key3_a1, TILEDB_INT32, sizeof(key3_a1));
  tiledb_kv_item_set_value(
      ctx, kv_item3, "a2", key3_a2, TILEDB_CHAR, strlen(key3_a2));
  tiledb_kv_item_set_value(
      ctx, kv_item3, "a3", &key3_a3, TILEDB_FLOAT32, sizeof(key3_a3));

  // Create fourth key-value item object
  char key4[] = "key_4";
  int key4_a1 = 4;
  const char* key4_a2 = "dddd";
  float key4_a3[] = {4.1f, 4.2f};
  tiledb_kv_item_t* kv_item4;
  tiledb_kv_item_create(ctx, &kv_item4);
  tiledb_kv_item_set_key(ctx, kv_item4, &key4, TILEDB_CHAR, strlen(key4));
  tiledb_kv_item_set_value(
      ctx, kv_item4, "a1", &key4_a1, TILEDB_INT32, sizeof(key4_a1));
  tiledb_kv_item_set_value(
      ctx, kv_item4, "a2", key4_a2, TILEDB_CHAR, strlen(key4_a2));
  tiledb_kv_item_set_value(
      ctx, kv_item4, "a3", &key4_a3, TILEDB_FLOAT32, sizeof(key4_a3));

  // Open the key-value store
  tiledb_kv_t* kv;
  tiledb_kv_open(ctx, &kv, "my_kv", NULL, 0);

  // We add items to a key-value store in the code snippets below. Note that
  // when an item is added to the key-value store, it is only buffered in
  // main-memory. To persist the buffered items in storage, the key-value store
  // must be "flushed". The user can set the number of maximum buffered items
  // in the key-value store via a function API as shown below.

  // Flush every 100 added items
  tiledb_kv_set_max_buffered_items(ctx, kv, 100);

  // Add key-value items to key-value store
  tiledb_kv_add_item(ctx, kv, kv_item1);
  tiledb_kv_add_item(ctx, kv, kv_item2);

  // Force-write the buffered items to the persistent storage
  tiledb_kv_flush(ctx, kv);

  // Write more items - these will be flushed upon closing kv
  tiledb_kv_add_item(ctx, kv, kv_item3);
  tiledb_kv_add_item(ctx, kv, kv_item4);

  // It is important to always close the key-value store, since
  // this operation flushes all buffered items to the persistent storage.
  tiledb_kv_close(ctx, kv);

  // Each flush operation generates a new fragment in the key-value store.
  // In case this happens multiple times, it is a good idea to consolidate
  // the key-value store (similar to consolidating arrays). Note though
  // that this is optional
  tiledb_kv_consolidate(ctx, "my_kv");

  // Clean up
  tiledb_kv_free(&kv);
  tiledb_kv_item_free(&kv_item1);
  tiledb_kv_item_free(&kv_item2);
  tiledb_kv_item_free(&kv_item3);
  tiledb_kv_item_free(&kv_item4);
  tiledb_ctx_free(&ctx);

  return 0;
}
