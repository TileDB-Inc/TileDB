/**
 * @file   kv.c
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2018-2019 TileDB, Inc.
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
 * This is a part of the TileDB quickstart tutorial:
 *   https://docs.tiledb.io/en/latest/kv.html
 *
 * This program shows the various ways you can use a TileDB map (key-value
 * store).
 *
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <tiledb/tiledb.h>

// Name of array.
const char* kv_name = "kv_array";

void create_kv() {
  // Create TileDB context
  tiledb_ctx_t* ctx;
  tiledb_ctx_alloc(NULL, &ctx);

  // Create a KV with two attributes
  tiledb_attribute_t* a1;
  tiledb_attribute_alloc(ctx, "a1", TILEDB_INT32, &a1);
  tiledb_attribute_t* a2;
  tiledb_attribute_alloc(ctx, "a2", TILEDB_FLOAT32, &a2);

  // Create KV schema
  tiledb_kv_schema_t* kv_schema;
  tiledb_kv_schema_alloc(ctx, &kv_schema);
  tiledb_kv_schema_add_attribute(ctx, kv_schema, a1);
  tiledb_kv_schema_add_attribute(ctx, kv_schema, a2);

  // Create array
  tiledb_kv_create(ctx, kv_name, kv_schema);

  // Clean up
  tiledb_attribute_free(&a1);
  tiledb_attribute_free(&a2);
  tiledb_kv_schema_free(&kv_schema);
  tiledb_ctx_free(&ctx);
}

void write_kv() {
  // Create TileDB context
  tiledb_ctx_t* ctx;
  tiledb_ctx_alloc(NULL, &ctx);

  // Create first item
  const char* key1 = "key_1";
  int key1_a1 = 1;
  float key1_a2 = 1.1f;
  tiledb_kv_item_t* kv_item1;
  tiledb_kv_item_alloc(ctx, &kv_item1);
  tiledb_kv_item_set_key(ctx, kv_item1, key1, TILEDB_CHAR, strlen(key1) + 1);
  tiledb_kv_item_set_value(
      ctx, kv_item1, "a1", &key1_a1, TILEDB_INT32, sizeof(key1_a1));
  tiledb_kv_item_set_value(
      ctx, kv_item1, "a2", &key1_a2, TILEDB_FLOAT32, sizeof(key1_a2));

  // Create second item
  const char* key2 = "key_2";
  int key2_a1 = 2;
  float key2_a2 = 2.1f;
  tiledb_kv_item_t* kv_item2;
  tiledb_kv_item_alloc(ctx, &kv_item2);
  tiledb_kv_item_set_key(ctx, kv_item2, key2, TILEDB_CHAR, strlen(key2) + 1);
  tiledb_kv_item_set_value(
      ctx, kv_item2, "a1", &key2_a1, TILEDB_INT32, sizeof(key2_a1));
  tiledb_kv_item_set_value(
      ctx, kv_item2, "a2", &key2_a2, TILEDB_FLOAT32, sizeof(key2_a2));

  // Create third item
  const char* key3 = "key_3";
  int key3_a1 = 3;
  float key3_a2 = 3.1f;
  tiledb_kv_item_t* kv_item3;
  tiledb_kv_item_alloc(ctx, &kv_item3);
  tiledb_kv_item_set_key(ctx, kv_item3, key3, TILEDB_CHAR, strlen(key3) + 1);
  tiledb_kv_item_set_value(
      ctx, kv_item3, "a1", &key3_a1, TILEDB_INT32, sizeof(key3_a1));
  tiledb_kv_item_set_value(
      ctx, kv_item3, "a2", &key3_a2, TILEDB_FLOAT32, sizeof(key3_a2));

  // Open the key-value store
  tiledb_kv_t* kv;
  tiledb_kv_alloc(ctx, kv_name, &kv);
  tiledb_kv_open(ctx, kv, TILEDB_WRITE);

  tiledb_kv_add_item(ctx, kv, kv_item1);
  tiledb_kv_add_item(ctx, kv, kv_item2);
  tiledb_kv_flush(ctx, kv);

  tiledb_kv_add_item(ctx, kv, kv_item3);
  tiledb_kv_flush(ctx, kv);

  tiledb_kv_close(ctx, kv);

  // Clean up
  tiledb_kv_free(&kv);
  tiledb_kv_item_free(&kv_item1);
  tiledb_kv_item_free(&kv_item2);
  tiledb_kv_item_free(&kv_item3);
  tiledb_ctx_free(&ctx);
}

void read_kv() {
  // Create TileDB context
  tiledb_ctx_t* ctx;
  tiledb_ctx_alloc(NULL, &ctx);

  // Open array for reading
  tiledb_kv_t* kv;
  tiledb_kv_alloc(ctx, kv_name, &kv);
  tiledb_kv_open(ctx, kv, TILEDB_READ);

  // Read first item
  const char* key1 = "key_1";
  uint64_t key1_size = strlen(key1) + 1;
  tiledb_kv_item_t* kv_item1 = NULL;
  tiledb_kv_get_item(ctx, kv, key1, TILEDB_CHAR, key1_size, &kv_item1);
  const void *key1_a1, *key1_a2;
  tiledb_datatype_t key1_a1_type, key1_a2_type;
  uint64_t key1_a1_size, key1_a2_size;
  tiledb_kv_item_get_value(
      ctx, kv_item1, "a1", &key1_a1, &key1_a1_type, &key1_a1_size);
  tiledb_kv_item_get_value(
      ctx, kv_item1, "a2", &key1_a2, &key1_a2_type, &key1_a2_size);

  // Read second item
  const char* key2 = "key_2";
  uint64_t key2_size = strlen(key2) + 1;
  tiledb_kv_item_t* kv_item2 = NULL;
  tiledb_kv_get_item(ctx, kv, key2, TILEDB_CHAR, key2_size, &kv_item2);
  const void *key2_a1, *key2_a2;
  tiledb_datatype_t key2_a1_type, key2_a2_type;
  uint64_t key2_a1_size, key2_a2_size;
  tiledb_kv_item_get_value(
      ctx, kv_item2, "a1", &key2_a1, &key2_a1_type, &key2_a1_size);
  tiledb_kv_item_get_value(
      ctx, kv_item2, "a2", &key2_a2, &key2_a2_type, &key2_a2_size);

  // Read third item
  const char* key3 = "key_3";
  uint64_t key3_size = strlen(key3) + 1;
  tiledb_kv_item_t* kv_item3 = NULL;
  tiledb_kv_get_item(ctx, kv, key3, TILEDB_CHAR, key3_size, &kv_item3);
  const void *key3_a1, *key3_a2;
  tiledb_datatype_t key3_a1_type, key3_a2_type;
  uint64_t key3_a1_size, key3_a2_size;
  tiledb_kv_item_get_value(
      ctx, kv_item3, "a1", &key3_a1, &key3_a1_type, &key3_a1_size);
  tiledb_kv_item_get_value(
      ctx, kv_item3, "a2", &key3_a2, &key3_a2_type, &key3_a2_size);

  // Print results
  printf("Simple read\n");
  printf("key_1, a1: %d\n", *((int*)key1_a1));
  printf("key_1, a2: %f\n", *((float*)key1_a2));
  printf("key_2, a1: %d\n", *((int*)key2_a1));
  printf("key_3, a2: %f\n", *((float*)key3_a2));

  // Close kv
  tiledb_kv_close(ctx, kv);

  // Clean up
  tiledb_kv_free(&kv);
  tiledb_kv_item_free(&kv_item1);
  tiledb_kv_item_free(&kv_item2);
  tiledb_kv_item_free(&kv_item3);
  tiledb_ctx_free(&ctx);
}

void iter_kv() {
  // Create TileDB context
  tiledb_ctx_t* ctx;
  tiledb_ctx_alloc(NULL, &ctx);

  // Open array for reading
  tiledb_kv_t* kv;
  tiledb_kv_alloc(ctx, kv_name, &kv);
  tiledb_kv_open(ctx, kv, TILEDB_READ);

  // Create a kv iterator
  tiledb_kv_iter_t* kv_iter;
  tiledb_kv_iter_alloc(ctx, kv, &kv_iter);

  printf("\nIterating over KV items\n");

  int done;
  tiledb_kv_iter_done(ctx, kv_iter, &done);
  while (done == 0) {
    tiledb_kv_item_t* kv_item;
    tiledb_kv_iter_here(ctx, kv_iter, &kv_item);

    const void* key;
    tiledb_datatype_t key_type;
    uint64_t key_size;
    tiledb_kv_item_get_key(ctx, kv_item, &key, &key_type, &key_size);

    const void* a1_value;
    tiledb_datatype_t a1_value_type;
    uint64_t a1_value_size;
    tiledb_kv_item_get_value(
        ctx, kv_item, "a1", &a1_value, &a1_value_type, &a1_value_size);

    const void* a2_value;
    tiledb_datatype_t a2_value_type;
    uint64_t a2_value_size;
    tiledb_kv_item_get_value(
        ctx, kv_item, "a2", &a2_value, &a2_value_type, &a2_value_size);

    printf(
        "key: %.*s, a1: %d, a2: %f\n",
        (int)key_size,
        (char*)key,
        *((int*)a1_value),
        *((float*)a2_value));

    tiledb_kv_item_free(&kv_item);
    tiledb_kv_iter_next(ctx, kv_iter);
    tiledb_kv_iter_done(ctx, kv_iter, &done);
  }

  // Close the kv
  tiledb_kv_close(ctx, kv);

  // Clean up
  tiledb_kv_free(&kv);
  tiledb_kv_iter_free(&kv_iter);
  tiledb_ctx_free(&ctx);
}

int main() {
  // Get object type
  tiledb_ctx_t* ctx;
  tiledb_ctx_alloc(NULL, &ctx);
  tiledb_object_t type;
  tiledb_object_type(ctx, kv_name, &type);
  tiledb_ctx_free(&ctx);

  if (type != TILEDB_KEY_VALUE) {
    create_kv();
    write_kv();
  }

  read_kv();
  iter_kv();

  return 0;
}
