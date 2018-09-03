/**
 * @file   unit-capi-filter.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2018 TileDB Inc.
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
 * Tests the C API filter.
 */

#include "catch.hpp"
#include "tiledb/sm/c_api/tiledb.h"
#include "tiledb/sm/misc/constants.h"

TEST_CASE("C API: Test filter set option", "[capi], [filter]") {
  tiledb_ctx_t* ctx;
  int rc = tiledb_ctx_alloc(nullptr, &ctx);
  REQUIRE(rc == TILEDB_OK);

  tiledb_filter_t* filter;
  rc = tiledb_filter_alloc(ctx, TILEDB_FILTER_BZIP2, &filter);
  REQUIRE(rc == TILEDB_OK);

  int level = 5;
  rc = tiledb_filter_set_option(ctx, filter, TILEDB_COMPRESSION_LEVEL, &level);
  REQUIRE(rc == TILEDB_OK);

  rc = tiledb_filter_set_option(ctx, filter, TILEDB_COMPRESSION_LEVEL, nullptr);
  REQUIRE(rc == TILEDB_ERR);

  rc = tiledb_filter_get_option(ctx, filter, TILEDB_COMPRESSION_LEVEL, nullptr);
  REQUIRE(rc == TILEDB_ERR);

  level = 0;
  rc = tiledb_filter_get_option(ctx, filter, TILEDB_COMPRESSION_LEVEL, &level);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(level == 5);

  tiledb_filter_type_t type;
  rc = tiledb_filter_get_type(ctx, filter, &type);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(type == TILEDB_FILTER_BZIP2);

  // Clean up
  tiledb_filter_free(&filter);
  tiledb_ctx_free(&ctx);
}

TEST_CASE("C API: Test filter list", "[capi], [filter]") {
  tiledb_ctx_t* ctx;
  int rc = tiledb_ctx_alloc(nullptr, &ctx);
  REQUIRE(rc == TILEDB_OK);

  tiledb_filter_list_t* filter_list;
  rc = tiledb_filter_list_alloc(ctx, &filter_list);
  REQUIRE(rc == TILEDB_OK);

  unsigned nfilters;
  rc = tiledb_filter_list_get_nfilters(ctx, filter_list, &nfilters);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(nfilters == 0);

  tiledb_filter_t* filter_out;
  rc = tiledb_filter_list_get_filter_from_index(
      ctx, filter_list, 0, &filter_out);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(filter_out == nullptr);
  rc = tiledb_filter_list_get_filter_from_index(
      ctx, filter_list, 1, &filter_out);
  REQUIRE(rc == TILEDB_ERR);

  tiledb_filter_t* filter;
  rc = tiledb_filter_alloc(ctx, TILEDB_FILTER_BZIP2, &filter);
  REQUIRE(rc == TILEDB_OK);

  int level = 5;
  rc = tiledb_filter_set_option(ctx, filter, TILEDB_COMPRESSION_LEVEL, &level);
  REQUIRE(rc == TILEDB_OK);

  rc = tiledb_filter_list_add_filter(ctx, filter_list, filter);
  REQUIRE(rc == TILEDB_OK);

  rc = tiledb_filter_list_get_nfilters(ctx, filter_list, &nfilters);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(nfilters == 1);

  rc = tiledb_filter_list_get_filter_from_index(
      ctx, filter_list, 0, &filter_out);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(filter_out != nullptr);

  level = 0;
  rc = tiledb_filter_get_option(ctx, filter, TILEDB_COMPRESSION_LEVEL, &level);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(level == 5);

  tiledb_filter_free(&filter_out);

  rc = tiledb_filter_list_get_filter_from_index(
      ctx, filter_list, 1, &filter_out);
  REQUIRE(rc == TILEDB_ERR);

  // Clean up
  tiledb_filter_free(&filter);
  tiledb_filter_list_free(&filter_list);
  tiledb_ctx_free(&ctx);
}

TEST_CASE("C API: Test filter list on attribute", "[capi], [filter]") {
  tiledb_ctx_t* ctx;
  int rc = tiledb_ctx_alloc(nullptr, &ctx);
  REQUIRE(rc == TILEDB_OK);

  tiledb_filter_t* filter;
  rc = tiledb_filter_alloc(ctx, TILEDB_FILTER_BZIP2, &filter);
  REQUIRE(rc == TILEDB_OK);

  int level = 5;
  rc = tiledb_filter_set_option(ctx, filter, TILEDB_COMPRESSION_LEVEL, &level);
  REQUIRE(rc == TILEDB_OK);

  tiledb_filter_list_t* filter_list;
  rc = tiledb_filter_list_alloc(ctx, &filter_list);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_filter_list_add_filter(ctx, filter_list, filter);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_filter_list_set_max_chunk_size(ctx, filter_list, 1024);
  REQUIRE(rc == TILEDB_OK);

  tiledb_attribute_t* attr;
  rc = tiledb_attribute_alloc(ctx, "a", TILEDB_INT32, &attr);
  REQUIRE(rc == TILEDB_OK);

  rc = tiledb_attribute_set_filter_list(ctx, attr, filter_list);
  REQUIRE(rc == TILEDB_OK);

  tiledb_filter_list_t* filter_list_out;
  rc = tiledb_attribute_get_filter_list(ctx, attr, &filter_list_out);
  REQUIRE(rc == TILEDB_OK);

  unsigned nfilters;
  rc = tiledb_filter_list_get_nfilters(ctx, filter_list_out, &nfilters);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(nfilters == 1);

  tiledb_filter_t* filter_out;
  rc = tiledb_filter_list_get_filter_from_index(
      ctx, filter_list_out, 0, &filter_out);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(filter_out != nullptr);

  level = 0;
  rc = tiledb_filter_get_option(
      ctx, filter_out, TILEDB_COMPRESSION_LEVEL, &level);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(level == 5);

  uint32_t max_chunk_size;
  rc = tiledb_filter_list_get_max_chunk_size(
      ctx, filter_list_out, &max_chunk_size);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(max_chunk_size == 1024);

  tiledb_filter_free(&filter_out);
  tiledb_filter_list_free(&filter_list_out);

  // Clean up
  tiledb_attribute_free(&attr);
  tiledb_filter_free(&filter);
  tiledb_filter_list_free(&filter_list);
  tiledb_ctx_free(&ctx);
}