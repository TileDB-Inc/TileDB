/**
 * @file   quickstart_kv.c
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
 * This is a part of the TileDB quickstart tutorial:
 *   https://docs.tiledb.io/en/1.3.0/tutorials/quickstart.html
 *
 * When run, this program will create a simple key-value store (i.e., a map),
 * write some data to it, and read data based on keys.
 *
 */

#include <stdio.h>
#include <string.h>
#include <tiledb/tiledb.h>

// Name of kv store.
const char* kv_name = "quickstart_kv";

void create_kv() {
  // Create TileDB context
  tiledb_ctx_t* ctx;
  tiledb_ctx_alloc(NULL, &ctx);

  // Create attribute
  tiledb_attribute_t* a;
  tiledb_attribute_alloc(ctx, "a", TILEDB_INT32, &a);

  // Create kv schema
  tiledb_kv_schema_t* kv_schema;
  tiledb_kv_schema_alloc(ctx, &kv_schema);
  tiledb_kv_schema_add_attribute(ctx, kv_schema, a);

  // Create kv
  tiledb_kv_create(ctx, kv_name, kv_schema);

  // Clean up
  tiledb_attribute_free(&a);
  tiledb_kv_schema_free(&kv_schema);
  tiledb_ctx_free(&ctx);
}

void write_kv() {
  // Create TileDB context
  tiledb_ctx_t* ctx;
  tiledb_ctx_alloc(NULL, &ctx);

  // Create item #1
  const char* key_1 = "key_1";
  int value_1 = 1;
  tiledb_kv_item_t* kv_item_1;
  tiledb_kv_item_alloc(ctx, &kv_item_1);
  tiledb_kv_item_set_key(ctx, kv_item_1, &key_1, TILEDB_CHAR, strlen(key_1));
  tiledb_kv_item_set_value(
      ctx, kv_item_1, "a", &value_1, TILEDB_INT32, sizeof(int));

  // Create item #2
  const char* key_2 = "key_2";
  int value_2 = 2;
  tiledb_kv_item_t* kv_item_2;
  tiledb_kv_item_alloc(ctx, &kv_item_2);
  tiledb_kv_item_set_key(ctx, kv_item_2, &key_2, TILEDB_CHAR, strlen(key_2));
  tiledb_kv_item_set_value(
      ctx, kv_item_2, "a", &value_2, TILEDB_INT32, sizeof(int));

  // Create item #3
  const char* key_3 = "key_3";
  int value_3 = 3;
  tiledb_kv_item_t* kv_item_3;
  tiledb_kv_item_alloc(ctx, &kv_item_3);
  tiledb_kv_item_set_key(ctx, kv_item_3, &key_3, TILEDB_CHAR, strlen(key_3));
  tiledb_kv_item_set_value(
      ctx, kv_item_3, "a", &value_3, TILEDB_INT32, sizeof(int));

  // Open the key-value store
  tiledb_kv_t* kv;
  tiledb_kv_alloc(ctx, kv_name, &kv);
  tiledb_kv_open(ctx, kv, NULL, 0);

  // Add key-value items to key-value store
  tiledb_kv_add_item(ctx, kv, kv_item_1);
  tiledb_kv_add_item(ctx, kv, kv_item_2);
  tiledb_kv_add_item(ctx, kv, kv_item_3);

  // It is important to always close the key-value store, since
  // this operation flushes all buffered items to the persistent storage.
  tiledb_kv_close(ctx, kv);

  // Clean up
  tiledb_kv_free(&kv);
  tiledb_kv_item_free(&kv_item_1);
  tiledb_kv_item_free(&kv_item_2);
  tiledb_kv_item_free(&kv_item_3);
  tiledb_ctx_free(&ctx);
}

void read_kv() {
  // Create TileDB context
  tiledb_ctx_t* ctx;
  tiledb_ctx_alloc(NULL, &ctx);

  // Open a key-value store
  tiledb_kv_t* kv;
  tiledb_kv_alloc(ctx, kv_name, &kv);
  tiledb_kv_open(ctx, kv, NULL, 0);

  // Read item #1
  const char* key_1 = "key_1";
  tiledb_datatype_t key_type_1 = TILEDB_CHAR;
  uint64_t key_size_1 = strlen(key_1);
  tiledb_kv_item_t* kv_item_1 = NULL;
  tiledb_kv_get_item(ctx, kv, &key_1, key_type_1, key_size_1, &kv_item_1);

  // Get value from item #1
  const void* value_1;
  tiledb_datatype_t value_type_1;
  uint64_t value_size_1;
  tiledb_kv_item_get_value(
      ctx, kv_item_1, "a", &value_1, &value_type_1, &value_size_1);

  // Read item #2
  const char* key_2 = "key_2";
  tiledb_datatype_t key_type_2 = TILEDB_CHAR;
  uint64_t key_size_2 = strlen(key_2);
  tiledb_kv_item_t* kv_item_2 = NULL;
  tiledb_kv_get_item(ctx, kv, &key_2, key_type_2, key_size_2, &kv_item_2);

  // Get value from item #2
  const void* value_2;
  tiledb_datatype_t value_type_2;
  uint64_t value_size_2;
  tiledb_kv_item_get_value(
      ctx, kv_item_2, "a", &value_2, &value_type_2, &value_size_2);

  // Read item #3
  const char* key_3 = "key_3";
  tiledb_datatype_t key_type_3 = TILEDB_CHAR;
  uint64_t key_size_3 = strlen(key_3);
  tiledb_kv_item_t* kv_item_3 = NULL;
  tiledb_kv_get_item(ctx, kv, &key_3, key_type_3, key_size_3, &kv_item_3);

  // Get value from item #3
  const void* value_3;
  tiledb_datatype_t value_type_3;
  uint64_t value_size_3;
  tiledb_kv_item_get_value(
      ctx, kv_item_3, "a", &value_3, &value_type_3, &value_size_3);

  // Print results
  printf("key_1: %d\n", *((const int*)value_1));
  printf("key_2: %d\n", *((const int*)value_2));
  printf("key_3: %d\n", *((const int*)value_3));

  // Close kv
  tiledb_kv_close(ctx, kv);

  // Clean up
  tiledb_kv_free(&kv);
  tiledb_kv_item_free(&kv_item_1);
  tiledb_kv_item_free(&kv_item_2);
  tiledb_kv_item_free(&kv_item_3);
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

  return 0;
}