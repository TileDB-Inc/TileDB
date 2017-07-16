/**
 * @file unit-capi-attribute.cc
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
 * Tests for the C API tiledb_attribute_t spec.
 */

#include <cassert>
#include <cstdio>
#include <cstring>
#include "catch.hpp"
#include "tiledb.h"

struct AttributeFx {
  // Test parameters
  const char* ATTR_NAME_1 = "a1";
  const char* ATTR_NAME_2 = "a2";
  tiledb_datatype_t ATTR_TYPE_1 = TILEDB_INT64;
  tiledb_datatype_t ATTR_TYPE_2 = TILEDB_UINT8;
  const char* ATTR_TYPE_STR_1 = "INT64";
  const char* ATTR_TYPE_STR_2 = "UINT8";
  tiledb_compressor_t ATTR_COMPRESSOR_1 = TILEDB_NO_COMPRESSION;
  tiledb_compressor_t ATTR_COMPRESSOR_2 = TILEDB_BLOSC_ZSTD;
  const char* ATTR_COMPRESSOR_STR_1 = "NO_COMPRESSION";
  const char* ATTR_COMPRESSOR_STR_2 = "BLOSC_ZSTD";
  int ATTR_COMPRESSION_LEVEL_1 = -1;
  int ATTR_COMPRESSION_LEVEL_2 = 5;
  const char* ATTR_COMPRESSION_LEVEL_STR_1 = "-1";
  const char* ATTR_COMPRESSION_LEVEL_STR_2 = "5";
  unsigned int ATTR_CELL_VAL_NUM_1 = 1;
  unsigned int ATTR_CELL_VAL_NUM_2 = 4;
  const char* ATTR_CELL_VAL_NUM_STR_1 = "1";
  const char* ATTR_CELL_VAL_NUM_STR_2 = "4";

  // TileDB context
  tiledb_ctx_t* ctx_;

  // TileDB attribute
  tiledb_attribute_t* a_;

  AttributeFx() {
    // Initialize context
    int rc = tiledb_ctx_create(&ctx_);
    assert(rc == TILEDB_OK);
  }

  ~AttributeFx() {
    // Free TileDB context
    tiledb_ctx_free(ctx_);
  }

  void check_dump(
      const std::string& name,
      const std::string& type,
      const std::string& compressor,
      const std::string& compression_level,
      const std::string& cell_val_num) {
    // Check dump
    std::string attr_dump_str = std::string("### Attribute ###\n") +
                                "- Name: " + name + "\n" + "- Type: " + type +
                                "\n" + "- Compressor: " + compressor + "\n" +
                                "- Compression level: " + compression_level +
                                "\n" + "- Cell val num: " + cell_val_num + "\n";
    std::string filename_gold = name + "_gold.txt";
    FILE* attr_fout = fopen(filename_gold.c_str(), "w");
    const char* attr_dump = attr_dump_str.c_str();
    fwrite(attr_dump, sizeof(char), strlen(attr_dump), attr_fout);
    fclose(attr_fout);
    std::string filename = name + ".txt";
    FILE* fout = fopen(filename.c_str(), "w");
    tiledb_attribute_dump(ctx_, a_, fout);
    fclose(fout);
    std::string cmd_diff =
        std::string("diff ") + filename + " " + filename_gold;
    CHECK(!system(cmd_diff.c_str()));
    std::string cmd_rm = std::string("rm ") + filename + " " + filename_gold;
    system(cmd_rm.c_str());
  }
};

TEST_CASE_METHOD(
    AttributeFx, "C API: Test attribute with some default members") {
  int rc;

  // Create attribute
  rc = tiledb_attribute_create(ctx_, &a_, ATTR_NAME_1, ATTR_TYPE_1);
  REQUIRE(rc == TILEDB_OK);

  // Check name
  const char* name;
  rc = tiledb_attribute_get_name(ctx_, a_, &name);
  REQUIRE(rc == TILEDB_OK);
  CHECK_THAT(name, Catch::Equals(ATTR_NAME_1));

  // Check type
  tiledb_datatype_t type;
  rc = tiledb_attribute_get_type(ctx_, a_, &type);
  REQUIRE(rc == TILEDB_OK);
  CHECK(type == ATTR_TYPE_1);

  // Check (default) compressor
  tiledb_compressor_t compressor;
  int compression_level;
  rc = tiledb_attribute_get_compressor(
      ctx_, a_, &compressor, &compression_level);
  REQUIRE(rc == TILEDB_OK);
  CHECK(compressor == ATTR_COMPRESSOR_1);
  CHECK(compression_level == ATTR_COMPRESSION_LEVEL_1);

  // Check (default) cell val num
  unsigned int cell_val_num;
  rc = tiledb_attribute_get_cell_val_num(ctx_, a_, &cell_val_num);
  REQUIRE(rc == TILEDB_OK);
  CHECK(cell_val_num == ATTR_CELL_VAL_NUM_1);

  // Check dump
  check_dump(
      ATTR_NAME_1,
      ATTR_TYPE_STR_1,
      ATTR_COMPRESSOR_STR_1,
      ATTR_COMPRESSION_LEVEL_STR_1,
      ATTR_CELL_VAL_NUM_STR_1);

  // Clean up
  tiledb_attribute_free(a_);
}

TEST_CASE_METHOD(
    AttributeFx, "C API: Test attribute with all members specified") {
  int rc;

  // Create attribute
  rc = tiledb_attribute_create(ctx_, &a_, ATTR_NAME_2, ATTR_TYPE_2);
  REQUIRE(rc == TILEDB_OK);

  // Set compressor
  rc = tiledb_attribute_set_compressor(
      ctx_, a_, ATTR_COMPRESSOR_2, ATTR_COMPRESSION_LEVEL_2);
  REQUIRE(rc == TILEDB_OK);

  // Set cell val num
  rc = tiledb_attribute_set_cell_val_num(ctx_, a_, ATTR_CELL_VAL_NUM_2);
  REQUIRE(rc == TILEDB_OK);

  // Check name
  const char* name;
  rc = tiledb_attribute_get_name(ctx_, a_, &name);
  REQUIRE(rc == TILEDB_OK);
  CHECK_THAT(name, Catch::Equals(ATTR_NAME_2));

  // Check type
  tiledb_datatype_t type;
  rc = tiledb_attribute_get_type(ctx_, a_, &type);
  REQUIRE(rc == TILEDB_OK);
  CHECK(type == ATTR_TYPE_2);

  // Check compressor
  tiledb_compressor_t compressor;
  int compression_level;
  rc = tiledb_attribute_get_compressor(
      ctx_, a_, &compressor, &compression_level);
  REQUIRE(rc == TILEDB_OK);
  CHECK(compressor == ATTR_COMPRESSOR_2);
  CHECK(compression_level == ATTR_COMPRESSION_LEVEL_2);

  // Check cell val num
  unsigned int cell_val_num;
  rc = tiledb_attribute_get_cell_val_num(ctx_, a_, &cell_val_num);
  REQUIRE(rc == TILEDB_OK);
  CHECK(cell_val_num == ATTR_CELL_VAL_NUM_2);

  // Check dump
  check_dump(
      ATTR_NAME_2,
      ATTR_TYPE_STR_2,
      ATTR_COMPRESSOR_STR_2,
      ATTR_COMPRESSION_LEVEL_STR_2,
      ATTR_CELL_VAL_NUM_STR_2);

  // Clean up
  tiledb_attribute_free(a_);
}
