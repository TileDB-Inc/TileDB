/**
 * @file   tiledb_kv_read.cc
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
 * It shows how to read from a key-value store.
 *
 * You need to run the following to make it work:
 *
 * $ ./tiledb_kv_create
 * $ ./tiledb_kv_write
 * $ ./tiledb_kv_read
 */

#include <tiledb.h>
#include <iostream>

int main() {
  // Create TileDB context
  tiledb_ctx_t* ctx;
  tiledb_ctx_create(&ctx, nullptr);

  // Set attributes
  const char* attributes[] = {"a1", "a2", "a3"};
  tiledb_datatype_t types[] = {TILEDB_INT32, TILEDB_CHAR, TILEDB_FLOAT32};
  unsigned int nitems[] = {1, TILEDB_VAR_NUM, 2};

  // Prepare key
  int key = 100;
  tiledb_datatype_t key_type = TILEDB_INT32;
  uint64_t key_size = sizeof(int);

  // Create key-values
  tiledb_kv_t* kv;
  tiledb_kv_create(ctx, &kv, 3, attributes, types, nitems);

  // Create query
  tiledb_query_t* query;
  tiledb_query_create(ctx, &query, "my_kv", TILEDB_READ);
  tiledb_query_set_kv_key(ctx, query, &key, key_type, key_size);
  tiledb_query_set_kv(ctx, query, kv);

  // Submit query
  tiledb_query_submit(ctx, query);

  // Key is not retrieved
  void* key_r;
  uint64_t key_size_r;
  tiledb_datatype_t key_type_r;
  if (tiledb_kv_get_key(ctx, kv, 0, &key_r, &key_type_r, &key_size_r) ==
      TILEDB_ERR)
    printf("Key attributes are not retrieved when reading with a single key\n");

  // Print result
  void *a1, *a2, *a3;
  uint64_t a2_size;
  std::cout << "a1, a2, (a3.first, a3.second)\n";
  std::cout << "-----------------------------\n";
  tiledb_kv_get_value(ctx, kv, 0, 0, &a1);
  tiledb_kv_get_value_var(ctx, kv, 0, 1, &a2, &a2_size);
  tiledb_kv_get_value(ctx, kv, 0, 2, &a3);
  std::cout << *((int*)a1);
  std::cout << ", " << std::string((const char*)a2, a2_size);
  std::cout << ", (" << ((float*)a3)[0] << ", " << ((float*)a3)[1] << ")\n";

  // Clean up
  tiledb_kv_free(ctx, kv);
  tiledb_query_free(ctx, query);
  tiledb_ctx_free(ctx);

  return 0;
}
