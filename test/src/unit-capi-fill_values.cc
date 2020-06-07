/**
 * @file   unit-capi-fill_values.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2020 TileDB Inc.
 * @copyright Copyright (c) 2016 MIT and Intel Corporation
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
 * Tests the attribute fill values with the C API.
 */

#include "catch.hpp"
#include "tiledb/sm/c_api/tiledb.h"

#include <iostream>

void check_dump(
    tiledb_ctx_t* ctx, tiledb_attribute_t* a, const std::string& gold_out) {
  FILE* gold_fout = fopen("gold_fout.txt", "w");
  fwrite(gold_out.c_str(), sizeof(char), gold_out.size(), gold_fout);
  fclose(gold_fout);
  FILE* fout = fopen("fout.txt", "w");
  tiledb_attribute_dump(ctx, a, fout);
  fclose(fout);
#ifdef _WIN32
  CHECK(!system("FC gold_fout.txt fout.txt > nul"));
#else
  CHECK(!system("diff gold_fout.txt fout.txt"));
#endif

  // Clean up
  tiledb_vfs_t* vfs;
  tiledb_vfs_alloc(ctx, nullptr, &vfs);
  CHECK(tiledb_vfs_remove_file(ctx, vfs, "gold_fout.txt") == TILEDB_OK);
  CHECK(tiledb_vfs_remove_file(ctx, vfs, "fout.txt") == TILEDB_OK);
  tiledb_vfs_free(&vfs);
}

TEST_CASE(
    "C API: Test fill values, basic errors", "[capi][fill-values][basic]") {
  int32_t value = 5;
  uint64_t value_size = sizeof(int32_t);

  tiledb_ctx_t* ctx;
  int32_t rc = tiledb_ctx_alloc(nullptr, &ctx);
  CHECK(rc == TILEDB_OK);

  // Fixed-sized
  tiledb_attribute_t* a;
  rc = tiledb_attribute_alloc(ctx, "a", TILEDB_INT32, &a);
  CHECK(rc == TILEDB_OK);

  // Null value
  rc = tiledb_attribute_set_fill_value(ctx, a, NULL, value_size);
  CHECK(rc == TILEDB_ERR);

  // Zero size
  rc = tiledb_attribute_set_fill_value(ctx, a, &value, 0);
  CHECK(rc == TILEDB_ERR);

  // Wrong size
  rc = tiledb_attribute_set_fill_value(ctx, a, &value, 100);
  CHECK(rc == TILEDB_ERR);

  // Get default
  const void* value_ptr;
  rc = tiledb_attribute_get_fill_value(ctx, a, &value_ptr, &value_size);
  CHECK(rc == TILEDB_OK);
  CHECK(*(const int32_t*)value_ptr == -2147483648);
  CHECK(value_size == sizeof(int32_t));

  // Check dump
  std::string dump = std::string("### Attribute ###\n") + "- Name: a\n" +
                     "- Type: INT32\n" + "- Cell val num: 1\n" +
                     "- Filters: 0\n" + "- Fill value: -2147483648\n";
  check_dump(ctx, a, dump);

  // Correct setter
  rc = tiledb_attribute_set_fill_value(ctx, a, &value, value_size);
  CHECK(rc == TILEDB_OK);

  // Get the set value
  rc = tiledb_attribute_get_fill_value(ctx, a, &value_ptr, &value_size);
  CHECK(rc == TILEDB_OK);
  CHECK(*(const int32_t*)value_ptr == 5);
  CHECK(value_size == sizeof(int32_t));

  // Check dump
  dump = std::string("### Attribute ###\n") + "- Name: a\n" +
         "- Type: INT32\n" + "- Cell val num: 1\n" + "- Filters: 0\n" +
         "- Fill value: 5\n";
  check_dump(ctx, a, dump);

  // Setting the cell val num, also sets the fill value to a new default
  rc = tiledb_attribute_set_cell_val_num(ctx, a, 2);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_attribute_get_fill_value(ctx, a, &value_ptr, &value_size);
  CHECK(rc == TILEDB_OK);
  CHECK(((const int32_t*)value_ptr)[0] == -2147483648);
  CHECK(((const int32_t*)value_ptr)[1] == -2147483648);
  CHECK(value_size == 2 * sizeof(int32_t));

  // Check dump
  dump = std::string("### Attribute ###\n") + "- Name: a\n" +
         "- Type: INT32\n" + "- Cell val num: 2\n" + "- Filters: 0\n" +
         "- Fill value: -2147483648, -2147483648\n";
  check_dump(ctx, a, dump);

  // Set a fill value that is comprised of two integers
  int32_t value_2[2] = {1, 2};
  rc = tiledb_attribute_set_fill_value(ctx, a, value_2, sizeof(value_2));
  CHECK(rc == TILEDB_OK);

  // Get the new value back
  rc = tiledb_attribute_get_fill_value(ctx, a, &value_ptr, &value_size);
  CHECK(rc == TILEDB_OK);
  CHECK(((const int32_t*)value_ptr)[0] == 1);
  CHECK(((const int32_t*)value_ptr)[1] == 2);
  CHECK(value_size == 2 * sizeof(int32_t));

  // Check dump
  dump = std::string("### Attribute ###\n") + "- Name: a\n" +
         "- Type: INT32\n" + "- Cell val num: 2\n" + "- Filters: 0\n" +
         "- Fill value: 1, 2\n";
  check_dump(ctx, a, dump);

  // Make the attribute var-sized
  rc = tiledb_attribute_set_cell_val_num(ctx, a, TILEDB_VAR_NUM);
  CHECK(rc == TILEDB_OK);

  // Check dump
  dump = std::string("### Attribute ###\n") + "- Name: a\n" +
         "- Type: INT32\n" + "- Cell val num: var\n" + "- Filters: 0\n" +
         "- Fill value: -2147483648\n";
  check_dump(ctx, a, dump);

  // Get the default var-sized fill value
  rc = tiledb_attribute_get_fill_value(ctx, a, &value_ptr, &value_size);
  CHECK(rc == TILEDB_OK);
  CHECK(*(const int32_t*)value_ptr == -2147483648);
  CHECK(value_size == sizeof(int32_t));

  // Set a new fill value for the var-sized attribute
  int32_t value_3[3] = {1, 2, 3};
  rc = tiledb_attribute_set_fill_value(ctx, a, value_3, sizeof(value_3));
  CHECK(rc == TILEDB_OK);

  // Get the new fill value
  rc = tiledb_attribute_get_fill_value(ctx, a, &value_ptr, &value_size);
  CHECK(rc == TILEDB_OK);
  CHECK(((const int32_t*)value_ptr)[0] == 1);
  CHECK(((const int32_t*)value_ptr)[1] == 2);
  CHECK(((const int32_t*)value_ptr)[2] == 3);
  CHECK(value_size == 3 * sizeof(int32_t));

  // Check dump
  dump = std::string("### Attribute ###\n") + "- Name: a\n" +
         "- Type: INT32\n" + "- Cell val num: var\n" + "- Filters: 0\n" +
         "- Fill value: 1, 2, 3\n";
  check_dump(ctx, a, dump);

  // Clean up
  tiledb_ctx_free(&ctx);
  tiledb_attribute_free(&a);
}
