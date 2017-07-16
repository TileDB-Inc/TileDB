/**
 * @file unit-capi-dimension.cc
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
 * Tests for the C API tiledb_dimension_t spec.
 */

#include <cassert>
#include <cstdio>
#include <cstring>
#include "catch.hpp"
#include "tiledb.h"

struct DimensionFx {
  // Test parameters
  const char* DIM_NAME_1 = "d1";
  const tiledb_datatype_t DIM_TYPE_1 = TILEDB_INT64;
  const char* DIM_TYPE_STR_1 = "INT64";
  const int64_t DIM_DOMAIN_1[2] = {1, 1000};
  const char* DIM_DOMAIN_STR_1 = "[1,1000]";
  const int64_t DIM_TILE_EXTENT_1 = 10;
  const char* DIM_TILE_EXTENT_STR_1 = "10";
  const tiledb_compressor_t DIM_COMPRESSOR_1 = TILEDB_NO_COMPRESSION;
  const char* DIM_COMPRESSOR_STR_1 = "NO_COMPRESSION";
  const int DIM_COMPRESSION_LEVEL_1 = -1;
  const char* DIM_COMPRESSION_LEVEL_STR_1 = "-1";
  const char* DIM_NAME_2 = "d2";
  const tiledb_datatype_t DIM_TYPE_2 = TILEDB_UINT16;
  const char* DIM_TYPE_STR_2 = "UINT16";
  const uint16_t DIM_DOMAIN_2[2] = {1, 100};
  const char* DIM_DOMAIN_STR_2 = "[1,100]";
  const uint16_t DIM_TILE_EXTENT_2 = 20;
  const char* DIM_TILE_EXTENT_STR_2 = "20";
  const tiledb_compressor_t DIM_COMPRESSOR_2 = TILEDB_BLOSC_ZSTD;
  const char* DIM_COMPRESSOR_STR_2 = "BLOSC_ZSTD";
  const int DIM_COMPRESSION_LEVEL_2 = 5;
  const char* DIM_COMPRESSION_LEVEL_STR_2 = "5";

  // TileDB context
  tiledb_ctx_t* ctx_;

  // TileDB dimension
  tiledb_dimension_t* d_;

  DimensionFx() {
    // Initialize context
    int rc = tiledb_ctx_create(&ctx_);
    assert(rc == TILEDB_OK);
  }

  ~DimensionFx() {
    // Free TileDB context
    tiledb_ctx_free(ctx_);
  }

  void check_dump(
      const std::string& name,
      const std::string& type,
      const std::string& compressor,
      const std::string& compression_level,
      const std::string& domain,
      const std::string& tile_extent) {
    std::string dim_dump_str =
        std::string("### Dimension ###\n") + "- Name: " + name + "\n" +
        "- Type: " + type + "\n" + "- Compressor: " + compressor + "\n" +
        "- Compression level: " + compression_level + "\n" +
        "- Domain: " + domain + "\n" + "- Tile extent: " + tile_extent + "\n";
    const char* dim_dump = dim_dump_str.c_str();
    std::string filename_gold = name + "_gold.txt";
    FILE* dim_fout = fopen(filename_gold.c_str(), "w");
    fwrite(dim_dump, sizeof(char), strlen(dim_dump), dim_fout);
    fclose(dim_fout);
    std::string filename = name + ".txt";
    FILE* fout = fopen(filename.c_str(), "w");
    tiledb_dimension_dump(ctx_, d_, fout);
    fclose(fout);
    std::string cmd_diff =
        std::string("diff ") + filename + " " + filename_gold;
    CHECK(!system(cmd_diff.c_str()));
    std::string cmd_rm = std::string("rm ") + filename + " " + filename_gold;
    system(cmd_rm.c_str());
  }
};

TEST_CASE_METHOD(
    DimensionFx, "C API: Test dimension with some default members") {
  int rc;

  // Create dimension
  rc = tiledb_dimension_create(
      ctx_, &d_, DIM_NAME_1, DIM_TYPE_1, DIM_DOMAIN_1, &DIM_TILE_EXTENT_1);
  REQUIRE(rc == TILEDB_OK);

  // Check name
  const char* name;
  rc = tiledb_dimension_get_name(ctx_, d_, &name);
  REQUIRE(rc == TILEDB_OK);
  CHECK_THAT(name, Catch::Equals(DIM_NAME_1));

  // Check type
  tiledb_datatype_t type;
  rc = tiledb_dimension_get_type(ctx_, d_, &type);
  REQUIRE(rc == TILEDB_OK);
  CHECK(type == DIM_TYPE_1);

  // Check (default) compressor
  tiledb_compressor_t compressor;
  int compression_level;
  rc = tiledb_dimension_get_compressor(
      ctx_, d_, &compressor, &compression_level);
  REQUIRE(rc == TILEDB_OK);
  CHECK(compressor == DIM_COMPRESSOR_1);
  CHECK(compression_level == DIM_COMPRESSION_LEVEL_1);

  // Check domain
  const void* domain;
  rc = tiledb_dimension_get_domain(ctx_, d_, &domain);
  REQUIRE(rc == TILEDB_OK);
  CHECK(!memcmp(DIM_DOMAIN_1, domain, sizeof(DIM_DOMAIN_1)));

  // Check tile extent
  const void* tile_extent;
  rc = tiledb_dimension_get_tile_extent(ctx_, d_, &tile_extent);
  REQUIRE(rc == TILEDB_OK);
  CHECK(!memcmp(tile_extent, &DIM_TILE_EXTENT_1, sizeof(DIM_TILE_EXTENT_1)));

  // Check dump
  check_dump(
      DIM_NAME_1,
      DIM_TYPE_STR_1,
      DIM_COMPRESSOR_STR_1,
      DIM_COMPRESSION_LEVEL_STR_1,
      DIM_DOMAIN_STR_1,
      DIM_TILE_EXTENT_STR_1);

  // Clean up
  tiledb_dimension_free(d_);
}

TEST_CASE_METHOD(
    DimensionFx, "C API: Test dimension with all members specified") {
  int rc;

  // Create dimension
  rc = tiledb_dimension_create(
      ctx_, &d_, DIM_NAME_2, DIM_TYPE_2, DIM_DOMAIN_2, &DIM_TILE_EXTENT_2);
  REQUIRE(rc == TILEDB_OK);

  // Set compressor
  rc = tiledb_dimension_set_compressor(
      ctx_, d_, DIM_COMPRESSOR_2, DIM_COMPRESSION_LEVEL_2);
  REQUIRE(rc == TILEDB_OK);

  // Check name
  const char* name;
  rc = tiledb_dimension_get_name(ctx_, d_, &name);
  REQUIRE(rc == TILEDB_OK);
  CHECK_THAT(name, Catch::Equals(DIM_NAME_2));

  // Check type
  tiledb_datatype_t type;
  rc = tiledb_dimension_get_type(ctx_, d_, &type);
  REQUIRE(rc == TILEDB_OK);
  CHECK(type == DIM_TYPE_2);

  // Check compressor
  tiledb_compressor_t compressor;
  int compression_level;
  rc = tiledb_dimension_get_compressor(
      ctx_, d_, &compressor, &compression_level);
  REQUIRE(rc == TILEDB_OK);
  CHECK(compressor == DIM_COMPRESSOR_2);
  CHECK(compression_level == DIM_COMPRESSION_LEVEL_2);

  // Check domain
  const void* domain;
  rc = tiledb_dimension_get_domain(ctx_, d_, &domain);
  REQUIRE(rc == TILEDB_OK);
  CHECK(!memcmp(DIM_DOMAIN_2, domain, sizeof(DIM_DOMAIN_2)));

  // Check tile extent
  const void* tile_extent;
  rc = tiledb_dimension_get_tile_extent(ctx_, d_, &tile_extent);
  REQUIRE(rc == TILEDB_OK);
  CHECK(!memcmp(tile_extent, &DIM_TILE_EXTENT_2, sizeof(DIM_TILE_EXTENT_2)));

  // Check dump
  check_dump(
      DIM_NAME_2,
      DIM_TYPE_STR_2,
      DIM_COMPRESSOR_STR_2,
      DIM_COMPRESSION_LEVEL_STR_2,
      DIM_DOMAIN_STR_2,
      DIM_TILE_EXTENT_STR_2);

  // Clean up
  tiledb_dimension_free(d_);
}
