
/**
 * @file   tiledb_kv_create.cc
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
 * It shows how to create a key-value store. Simply run:
 *
 * $ ./tiledb_kv_create
 */

#include <tiledb.h>

int main() {
  // Create TileDB context
  tiledb_ctx_t* ctx;
  tiledb_ctx_create(&ctx, nullptr);

  // Create attributes
  tiledb_attribute_t* a1;
  tiledb_attribute_create(ctx, &a1, "a1", TILEDB_INT32);
  tiledb_attribute_set_compressor(ctx, a1, TILEDB_BLOSC, -1);
  tiledb_attribute_set_cell_val_num(ctx, a1, 1);
  tiledb_attribute_t* a2;
  tiledb_attribute_create(ctx, &a2, "a2", TILEDB_CHAR);
  tiledb_attribute_set_compressor(ctx, a2, TILEDB_GZIP, -1);
  tiledb_attribute_set_cell_val_num(ctx, a2, TILEDB_VAR_NUM);
  tiledb_attribute_t* a3;
  tiledb_attribute_create(ctx, &a3, "a3", TILEDB_FLOAT32);
  tiledb_attribute_set_compressor(ctx, a3, TILEDB_ZSTD, -1);
  tiledb_attribute_set_cell_val_num(ctx, a3, 2);

  // Create array metadata
  const char* array_name = "my_kv";
  tiledb_array_metadata_t* array_metadata;
  tiledb_array_metadata_create(ctx, &array_metadata, array_name);
  tiledb_array_metadata_add_attribute(ctx, array_metadata, a1);
  tiledb_array_metadata_add_attribute(ctx, array_metadata, a2);
  tiledb_array_metadata_add_attribute(ctx, array_metadata, a3);

  // Set array as key-value
  tiledb_array_metadata_set_as_kv(ctx, array_metadata);

  // Check array metadata
  if (tiledb_array_metadata_check(ctx, array_metadata) != TILEDB_OK) {
    printf("Invalid array metadata\n");
    return -1;
  }

  // Check if array is defined as kv
  int as_kv;
  tiledb_array_metadata_get_as_kv(ctx, array_metadata, &as_kv);
  if (as_kv)
    printf("Array in defined as a key-value store\n");

  // Create array (which is defined as a key-value store)
  tiledb_array_create(ctx, array_metadata);

  // Clean up
  tiledb_attribute_free(ctx, a1);
  tiledb_attribute_free(ctx, a2);
  tiledb_attribute_free(ctx, a3);
  tiledb_array_metadata_free(ctx, array_metadata);
  tiledb_ctx_free(ctx);

  return 0;
}
