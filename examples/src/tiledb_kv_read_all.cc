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
 * $ ./tiledb_kv_read_all
 */

#include <tiledb.h>
#include <iostream>

void print_results(tiledb_ctx_t* ctx, tiledb_kv_t* kv);
void print_key(void* key, tiledb_datatype_t key_type, uint64_t key_size);

int main() {
  // Create TileDB context
  tiledb_ctx_t* ctx;
  tiledb_ctx_create(&ctx, nullptr);

  // Set attributes
  const char* attributes[] = {"a1", "a2", "a3"};
  tiledb_datatype_t types[] = {TILEDB_INT32, TILEDB_CHAR, TILEDB_FLOAT32};
  unsigned int nitems[] = {1, TILEDB_VAR_NUM, 2};

  // Create key-values
  tiledb_kv_t* kv;
  tiledb_kv_create(ctx, &kv, 3, attributes, types, nitems);
  tiledb_kv_set_buffer_size(ctx, kv, 1000);

  // Create query
  tiledb_query_t* query;
  tiledb_query_create(ctx, &query, "my_kv", TILEDB_READ);
  tiledb_query_set_kv(ctx, query, kv);

  // Submit query
  tiledb_query_submit(ctx, query);

  // Print the results
  print_results(ctx, kv);

  // Clean up
  tiledb_kv_free(ctx, kv);
  tiledb_query_free(ctx, query);
  tiledb_ctx_free(ctx);

  return 0;
}

void print_results(tiledb_ctx_t* ctx, tiledb_kv_t* kv) {
  uint64_t key_num, a1_num, a2_num, a3_num;
  tiledb_kv_get_key_num(ctx, kv, &key_num);
  tiledb_kv_get_value_num(ctx, kv, 0, &a1_num);
  tiledb_kv_get_value_num(ctx, kv, 1, &a2_num);
  tiledb_kv_get_value_num(ctx, kv, 2, &a3_num);

  // Sanity check
  if (a1_num != key_num || a2_num != key_num || a3_num != key_num) {
    std::cout << "key_num: " << key_num << "a1_num: " << a1_num
              << "a2_num: " << a2_num << "a3_num: " << a3_num << "\n";
    std::cout << "Key/value number mismatch\n";
    return;
  }

  void *a1, *a2, *a3, *key_r;
  uint64_t key_size_r, a2_size;
  tiledb_datatype_t key_type_r;

  std::cout << "key, key_type, a1, a2, (a3.first, a3.second)\n";
  std::cout << "--------------------------------------------\n";
  for (uint64_t i = 0; i < key_num; ++i) {
    tiledb_kv_get_key(ctx, kv, i, &key_r, &key_type_r, &key_size_r);
    tiledb_kv_get_value(ctx, kv, i, 0, &a1);
    tiledb_kv_get_value_var(ctx, kv, i, 1, &a2, &a2_size);
    tiledb_kv_get_value(ctx, kv, i, 2, &a3);
    print_key(key_r, key_type_r, key_size_r);
    std::cout << ", " << *((int*)a1);
    std::cout << ", " << std::string((const char*)a2, a2_size);
    std::cout << ", (" << ((float*)a3)[0] << ", " << ((float*)a3)[1] << ")\n";
  }
}

void print_key(void* key, tiledb_datatype_t key_type, uint64_t key_size) {
  auto nitems = 0;
  switch (key_type) {
    case TILEDB_INT32:
      nitems = (int)(key_size / sizeof(int));
      for (int i = 0; i < nitems; ++i)
        std::cout << ((int*)key)[i] << " ";
      std::cout << "\b, int";
      break;
    case TILEDB_FLOAT32:
      nitems = (int)(key_size / sizeof(float));
      for (int i = 0; i < nitems; ++i)
        std::cout << ((float*)key)[i] << " ";
      std::cout << "\b, float32";
      break;
    case TILEDB_FLOAT64:
      nitems = (int)(key_size / sizeof(double));
      for (int i = 0; i < nitems; ++i)
        std::cout << ((double*)key)[i] << " ";
      std::cout << "\b, float64";
      break;
    case TILEDB_CHAR:
      nitems = (int)(key_size / sizeof(char));
      for (int i = 0; i < nitems; ++i)
        std::cout << ((char*)key)[i];
      std::cout << ", char";
      break;
    default:
      std::cout << "Other types are not supported in this example. It should "
                   "be trivial "
                   "to extend to other types following this example\n";
  }
}
