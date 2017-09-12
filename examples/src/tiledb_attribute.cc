/**
 * @file   tiledb_attribute.cc
 *
 * @section LICENSE
 *
 * The MIT License
 * 
 * @copyright Copyright (c) 2017 MIT, Intel Corporation and TileDB, Inc.
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
 *
 * Explores the C API for handling TileDB attributes.
 *
 * Program output:
 *
 *     $ ./tiledb_attribute
 *     First dump:
 *     ### Attribute ###
 *     - Name: a1
 *     - Type: INT64
 *     - Compressor: NO_COMPRESSION
 *     - Compression level: -1
 *     - Cell val num: 1
 *
 *     Second dump:
 *     ### Attribute ###
 *     - Name: a1
 *     - Type: INT64
 *     - Compressor: BLOSC_LZ
 *     - Compression level: 4
 *     - Cell val num: var
 *
 *     From getters:
 *     - Name: a1
 *     - Type: INT64
 *     - Compressor: BLOSC
 *     - Compression level: 4
 *     - Cell val num: var
 */

#include "tiledb.h"

int main() {
  // Create TileDB context
  tiledb_ctx_t* ctx;
  tiledb_ctx_create(&ctx);

  // Create attribute
  tiledb_attribute_t* attr;
  tiledb_attribute_create(ctx, &attr, "a1", TILEDB_INT64);

  // Print attribute contents
  printf("First dump:\n");
  tiledb_attribute_dump(ctx, attr, stdout);

  // Set compressor and number of values per cell
  tiledb_attribute_set_compressor(ctx, attr, TILEDB_BLOSC, 4);
  tiledb_attribute_set_cell_val_num(ctx, attr, tiledb_var_num());

  // Print attribute contents again
  printf("\nSecond dump:\n");
  tiledb_attribute_dump(ctx, attr, stdout);

  // Use getters
  const char* attr_name;
  tiledb_datatype_t attr_type;
  tiledb_compressor_t attr_cmp;
  int attr_cmp_l;
  unsigned int attr_cnum;
  tiledb_attribute_get_name(ctx, attr, &attr_name);
  tiledb_attribute_get_type(ctx, attr, &attr_type);
  tiledb_attribute_get_compressor(ctx, attr, &attr_cmp, &attr_cmp_l);
  tiledb_attribute_get_cell_val_num(ctx, attr, &attr_cnum);

  // Print retrieved info
  printf("\n From getters:\n");
  printf("- Name: %s\n", attr_name);
  printf("- Type: %s\n", (attr_type == TILEDB_INT64) ? "INT64" : "Error");
  printf("- Compressor: %s\n", (attr_cmp == TILEDB_BLOSC) ? "BLOSC" : "Error");
  printf("- Compression level: %u\n", attr_cmp_l);
  printf("- Cell val num: %s\n", (attr_cnum == tiledb_var_num()) ? "var" : "Error");

  // Clean up
  tiledb_attribute_free(ctx, attr);
  tiledb_ctx_free(ctx);

  return 0;
}
