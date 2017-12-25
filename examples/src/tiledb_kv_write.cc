/**
 * @file   tiledb_kv_write.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017 TileDB, Inc.
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
 * $ ./tiledb_kv_create
 * $ ./tiledb_kv_write
 */

#include <tiledb.h>
#include <cstring>

int main() {
  // Create TileDB context
  tiledb_ctx_t* ctx;
  tiledb_ctx_create(&ctx, nullptr);

  // Attributes and value sizes
  const char* attributes[] = {"a1", "a2", "a3"};
  tiledb_datatype_t types[] = {TILEDB_INT32, TILEDB_CHAR, TILEDB_FLOAT32};
  unsigned int nitems[] = {1, tiledb_var_num(), 2};

  // Key-values with three attributes
  int key1 = 100;
  int key1_a1 = 1;
  const char* key1_a2 = "a";
  float key1_a3[] = {1.1, 1.2};

  float key2 = 200.0;
  int key2_a1 = 2;
  const char* key2_a2 = "bb";
  float key2_a3[] = {2.1, 2.2};

  double key3[] = {300.0, 300.1};
  int key3_a1 = 3;
  const char* key3_a2 = "ccc";
  float key3_a3[] = {3.1, 3.2};

  char key4[] = "key_4";
  int key4_a1 = 4;
  const char* key4_a2 = "dddd";
  float key4_a3[] = {4.1, 4.2};

  // Create key-values
  tiledb_kv_t* kv;
  tiledb_kv_create(ctx, &kv, 3, attributes, types, nitems);

  // Add keys
  tiledb_kv_add_key(ctx, kv, &key1, TILEDB_INT32, sizeof(key1));
  tiledb_kv_add_key(ctx, kv, &key2, TILEDB_FLOAT32, sizeof(key2));
  tiledb_kv_add_key(ctx, kv, &key3, TILEDB_FLOAT64, sizeof(key3));
  tiledb_kv_add_key(ctx, kv, &key4, TILEDB_CHAR, strlen(key4));

  // Add attribute "a1" values
  tiledb_kv_add_value(ctx, kv, 0, &key1_a1);
  tiledb_kv_add_value(ctx, kv, 0, &key2_a1);
  tiledb_kv_add_value(ctx, kv, 0, &key3_a1);
  tiledb_kv_add_value(ctx, kv, 0, &key4_a1);

  // Add attribute "a2" values
  tiledb_kv_add_value_var(ctx, kv, 1, key1_a2, strlen(key1_a2));
  tiledb_kv_add_value_var(ctx, kv, 1, key2_a2, strlen(key2_a2));
  tiledb_kv_add_value_var(ctx, kv, 1, key3_a2, strlen(key3_a2));
  tiledb_kv_add_value_var(ctx, kv, 1, key4_a2, strlen(key4_a2));

  // Add attribute "a3" values
  tiledb_kv_add_value(ctx, kv, 2, &key1_a3);
  tiledb_kv_add_value(ctx, kv, 2, &key2_a3);
  tiledb_kv_add_value(ctx, kv, 2, &key3_a3);
  tiledb_kv_add_value(ctx, kv, 2, &key4_a3);

  // Create query
  tiledb_query_t* query;
  tiledb_query_create(ctx, &query, "my_kv", TILEDB_WRITE);
  tiledb_query_set_kv(ctx, query, kv);

  // Submit query
  tiledb_query_submit(ctx, query);

  // Clean up
  tiledb_query_free(ctx, query);
  tiledb_kv_free(ctx, kv);
  tiledb_ctx_free(ctx);

  return 0;
}
