/**
 * @file   tiledb_kv_iter.cc
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
 * This example shows how to read all items from a key-value store using an
 * iterator.
 *
 * You need to run the following to make it work:
 *
 * ```
 * $ ./tiledb_kv_create_c
 * $ ./tiledb_kv_write_c
 * $ ./tiledb_kv_iter_c
 * key: 200.0, float32
 * a1: 2, int
 * a2: bb, char
 * a3: 2.1 2.2, float32
 * -------
 * key: 300.0 300.1, float64
 * a1: 3, int
 * a2: ccc, char
 * a3: 3.1 3.2, float32
 * -------
 * key: key_4, char
 * a1: 4, int
 * a2: dddd, char
 * a3: 4.1 4.2, float32
 * -------
 * key: 100, int
 * a1: 1, int
 * a2: a, char
 * a3: 1.1 1.2, float32
 * -------
 * ```
 */

#include <tiledb/tiledb.h>

void print_kv_item(tiledb_ctx_t* ctx, tiledb_kv_item_t* kv_item);
void print(const void* v, tiledb_datatype_t type, uint64_t size);

int main() {
  // Create TileDB context
  tiledb_ctx_t* ctx;
  tiledb_ctx_alloc(&ctx, NULL);

  // Create key-value iterator
  const char* attributes[] = {"a1", "a2", "a3"};

  // Open a kv
  tiledb_kv_t* kv;
  tiledb_kv_alloc(ctx, "my_kv", &kv);
  tiledb_kv_open(ctx, kv, attributes, 3);

  // Create a kv iterator
  tiledb_kv_iter_t* kv_iter;
  tiledb_kv_iter_alloc(ctx, kv, &kv_iter);

  int done;
  tiledb_kv_iter_done(ctx, kv_iter, &done);
  while (done == 0) {
    tiledb_kv_item_t* kv_item;
    tiledb_kv_iter_here(ctx, kv_iter, &kv_item);
    print_kv_item(ctx, kv_item);
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

  return 0;
}

void print_kv_item(tiledb_ctx_t* ctx, tiledb_kv_item_t* kv_item) {
  // Get and print key
  const void* key;
  tiledb_datatype_t key_type;
  uint64_t key_size;
  tiledb_kv_item_get_key(ctx, kv_item, &key, &key_type, &key_size);
  printf("key: ");
  print(key, key_type, key_size);
  printf("\n");

  // Get and print value for attribute "a1"
  const void* value;
  tiledb_datatype_t value_type;
  uint64_t value_size;
  printf("a1: ");
  tiledb_kv_item_get_value(
      ctx, kv_item, "a1", &value, &value_type, &value_size);
  print(value, value_type, value_size);
  printf("\n");

  // Get and print value for attribute "a2"
  printf("a2: ");
  tiledb_kv_item_get_value(
      ctx, kv_item, "a2", &value, &value_type, &value_size);
  print(value, value_type, value_size);
  printf("\n");

  // Get and print value for attribute "a3"
  printf("a3: ");
  tiledb_kv_item_get_value(
      ctx, kv_item, "a3", &value, &value_type, &value_size);
  print(value, value_type, value_size);
  printf("\n-------\n");
}

void print(const void* v, tiledb_datatype_t type, uint64_t size) {
  int nitems = 0;
  switch (type) {
    case TILEDB_INT32:
      nitems = (int)(size / sizeof(int));
      for (int i = 0; i < nitems; ++i)
        printf("%d ", ((int*)v)[i]);
      printf("\b, int");
      break;
    case TILEDB_FLOAT32:
      nitems = (int)(size / sizeof(float));
      for (int i = 0; i < nitems; ++i)
        printf("%.1f ", ((float*)v)[i]);
      printf("\b, float32");
      break;
    case TILEDB_FLOAT64:
      nitems = (int)(size / sizeof(double));
      for (int i = 0; i < nitems; ++i)
        printf("%.1lf ", ((double*)v)[i]);
      printf("\b, float64");
      break;
    case TILEDB_CHAR:
      nitems = (int)(size / sizeof(char));
      for (int i = 0; i < nitems; ++i)
        printf("%c", ((char*)v)[i]);
      printf(", char");
      break;
    default:
      printf(
          "Other types than int32, float32, float64 and char are not "
          "supported in this example. It should be trivial "
          "to extend to other types following this example\n");
  }
}
