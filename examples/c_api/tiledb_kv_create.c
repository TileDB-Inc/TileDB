/**
 * @file   tiledb_kv_create.c
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
 * This example shows how to create a key-value store. he creation of a
 * key-value schema is similar to the case of arrays, since TileDB implements
 * a key-value store as a sparse array. The main difference in the API usage
 * is that the underlying array type and array domain are preset by TileDB for
 * key-value stores and, therefore, the user does not explicitly set them.
 *
 * Simply run:
 *
 * ```
 * $ ./tiledb_kv_create_c
 * - Array type: sparse
 * - Cell order: row-major
 * - Tile order: row-major
 * - Capacity: 10000
 * - Coordinates compressor: BLOSC_ZSTD
 * - Coordinates compression level: -1
 *
 * === Domain ===
 * - Dimensions type: UINT64
 *
 * ### Dimension ###
 * - Name: __key_dim_1
 * - Domain: [0,18446744073709551615]
 * - Tile extent: null
 *
 * ### Dimension ###
 * - Name: __key_dim_2
 * - Domain: [0,18446744073709551615]
 * - Tile extent: null
 *
 * ### Attribute ###
 * - Name: __key
 * - Type: CHAR
 * - Compressor: BLOSC_ZSTD
 * - Compression level: -1
 * - Cell val num: var
 *
 * ### Attribute ###
 * - Name: __key_type
 * - Type: CHAR
 * - Compressor: BLOSC_ZSTD
 * - Compression level: -1
 * - Cell val num: 1
 *
 * ### Attribute ###
 * - Name: a1
 * - Type: INT32
 * - Compressor: BLOSC_LZ
 * - Compression level: -1
 * - Cell val num: 1
 *
 * ### Attribute ###
 * - Name: a2
 * - Type: CHAR
 * - Compressor: GZIP
 * - Compression level: -1
 * - Cell val num: var
 *
 * ### Attribute ###
 * - Name: a3
 * - Type: FLOAT32
 * - Compressor: ZSTD
 * - Compression level: -1
 * - Cell val num: 2
 * ```
 */

#include <tiledb/tiledb.h>

int main() {
  // Create TileDB context
  tiledb_ctx_t* ctx;
  tiledb_ctx_create(&ctx, NULL);

  // Create attributes
  tiledb_attribute_t* a1;
  tiledb_attribute_create(ctx, &a1, "a1", TILEDB_INT32);
  tiledb_attribute_set_compressor(ctx, a1, TILEDB_BLOSC_LZ, -1);
  tiledb_attribute_set_cell_val_num(ctx, a1, 1);
  tiledb_attribute_t* a2;
  tiledb_attribute_create(ctx, &a2, "a2", TILEDB_CHAR);
  tiledb_attribute_set_compressor(ctx, a2, TILEDB_GZIP, -1);
  tiledb_attribute_set_cell_val_num(ctx, a2, TILEDB_VAR_NUM);
  tiledb_attribute_t* a3;
  tiledb_attribute_create(ctx, &a3, "a3", TILEDB_FLOAT32);
  tiledb_attribute_set_compressor(ctx, a3, TILEDB_ZSTD, -1);
  tiledb_attribute_set_cell_val_num(ctx, a3, 2);

  // Create kv schema
  const char* kv_uri = "my_kv";
  tiledb_kv_schema_t* kv_schema;
  tiledb_kv_schema_create(ctx, &kv_schema);
  tiledb_kv_schema_add_attribute(ctx, kv_schema, a1);
  tiledb_kv_schema_add_attribute(ctx, kv_schema, a2);
  tiledb_kv_schema_add_attribute(ctx, kv_schema, a3);

  // Check kv schema
  if (tiledb_kv_schema_check(ctx, kv_schema) != TILEDB_OK) {
    printf("Invalid key-value schema\n");
    return -1;
  }

  // Dump the key-value schema in ASCII format in standard output
  tiledb_kv_schema_dump(ctx, kv_schema, stdout);

  // Create kv
  tiledb_kv_create(ctx, kv_uri, kv_schema);

  // Clean up
  tiledb_attribute_free(ctx, &a1);
  tiledb_attribute_free(ctx, &a2);
  tiledb_attribute_free(ctx, &a3);
  tiledb_kv_schema_free(ctx, &kv_schema);
  tiledb_ctx_free(&ctx);

  return 0;
}
