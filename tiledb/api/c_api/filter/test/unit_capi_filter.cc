/**
 * @file tiledb/api/c_api/filter/test/unit_capi_filter.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022 TileDB, Inc.
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
 */

#define CATCH_CONFIG_MAIN
#include <test/support/tdb_catch.h>
#include "../filter_api_external.h"

TEST_CASE("C API: tiledb_filter_alloc argument validation", "[capi][filter]") {
  tiledb_ctx_t* ctx;
  tiledb_filter_t* filter;
  capi_return_t rc = tiledb_ctx_alloc(nullptr, &ctx);
  REQUIRE(tiledb_status(rc) == TILEDB_OK);

  SECTION("null context") {
    rc = tiledb_filter_alloc(nullptr, TILEDB_FILTER_NONE, &filter);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null filter pointer") {
    rc = tiledb_filter_alloc(ctx, TILEDB_FILTER_NONE, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("invalid filter type") {
    rc = tiledb_filter_alloc(
        ctx, static_cast<tiledb_filter_type_t>(9001), &filter);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  tiledb_ctx_free(&ctx);
}

TEST_CASE("C API: tiledb_filter_free argument validation", "[capi][filter]") {
  REQUIRE_NOTHROW(tiledb_filter_free(nullptr));
}

TEST_CASE(
    "C API: tiledb_filter_get_type argument validation", "[capi][filter]") {
  tiledb_ctx_t* ctx;
  tiledb_filter_t* filter;
  tiledb_filter_type_t filter_type;
  capi_return_t rc = tiledb_ctx_alloc(nullptr, &ctx);
  REQUIRE(tiledb_status(rc) == TILEDB_OK);
  rc = tiledb_filter_alloc(ctx, TILEDB_FILTER_NONE, &filter);
  REQUIRE(tiledb_status(rc) == TILEDB_OK);
  SECTION("null context") {
    rc = tiledb_filter_get_type(nullptr, filter, &filter_type);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null filter") {
    rc = tiledb_filter_get_type(ctx, nullptr, &filter_type);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null pointer to filter type") {
    rc = tiledb_filter_get_type(ctx, filter, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  tiledb_filter_free(&filter);
  tiledb_ctx_free(&ctx);
}

TEST_CASE(
    "C API: tiledb_filter_set_option argument validation", "[capi][filter]") {
  tiledb_ctx_t* ctx;
  tiledb_filter_t* filter;
  capi_return_t rc = tiledb_ctx_alloc(nullptr, &ctx);
  REQUIRE(tiledb_status(rc) == TILEDB_OK);
  rc = tiledb_filter_alloc(ctx, TILEDB_FILTER_BZIP2, &filter);
  REQUIRE(tiledb_status(rc) == TILEDB_OK);
  int value;
  SECTION("null context") {
    rc = tiledb_filter_set_option(
        nullptr, filter, TILEDB_COMPRESSION_LEVEL, &value);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null filter") {
    rc = tiledb_filter_set_option(
        ctx, nullptr, TILEDB_COMPRESSION_LEVEL, &value);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("bad option identifier") {
    rc = tiledb_filter_set_option(
        ctx, filter, static_cast<tiledb_filter_option_t>(9001), &value);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null option value") {
    rc = tiledb_filter_set_option(
        ctx, filter, TILEDB_COMPRESSION_LEVEL, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  tiledb_filter_free(&filter);
  tiledb_ctx_free(&ctx);
}

TEST_CASE(
    "C API: tiledb_filter_get_option argument validation", "[capi][filter]") {
  tiledb_ctx_t* ctx;
  tiledb_filter_t* filter;
  capi_return_t rc = tiledb_ctx_alloc(nullptr, &ctx);
  REQUIRE(tiledb_status(rc) == TILEDB_OK);
  rc = tiledb_filter_alloc(ctx, TILEDB_FILTER_BZIP2, &filter);
  REQUIRE(tiledb_status(rc) == TILEDB_OK);
  int value;
  SECTION("null context") {
    rc = tiledb_filter_get_option(
        nullptr, filter, TILEDB_COMPRESSION_LEVEL, &value);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null filter") {
    rc = tiledb_filter_get_option(
        ctx, nullptr, TILEDB_COMPRESSION_LEVEL, &value);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("bad option") {
    rc = tiledb_filter_get_option(
        ctx, filter, static_cast<tiledb_filter_option_t>(9001), &value);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null value") {
    rc = tiledb_filter_get_option(
        ctx, filter, TILEDB_COMPRESSION_LEVEL, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  tiledb_filter_free(&filter);
  tiledb_ctx_free(&ctx);
}

TEST_CASE("C API: Test filter set option", "[capi][filter]") {
  tiledb_ctx_t* ctx;
  int rc = tiledb_ctx_alloc(nullptr, &ctx);
  REQUIRE(tiledb_status(rc) == TILEDB_OK);

  tiledb_filter_t* filter;
  rc = tiledb_filter_alloc(ctx, TILEDB_FILTER_BZIP2, &filter);
  REQUIRE(tiledb_status(rc) == TILEDB_OK);

  int level = 5;
  rc = tiledb_filter_set_option(ctx, filter, TILEDB_COMPRESSION_LEVEL, &level);
  REQUIRE(tiledb_status(rc) == TILEDB_OK);

  rc = tiledb_filter_set_option(ctx, filter, TILEDB_COMPRESSION_LEVEL, nullptr);
  REQUIRE(tiledb_status(rc) == TILEDB_ERR);

  rc = tiledb_filter_get_option(ctx, filter, TILEDB_COMPRESSION_LEVEL, nullptr);
  REQUIRE(tiledb_status(rc) == TILEDB_ERR);

  level = 0;
  rc = tiledb_filter_get_option(ctx, filter, TILEDB_COMPRESSION_LEVEL, &level);
  REQUIRE(tiledb_status(rc) == TILEDB_OK);
  REQUIRE(level == 5);

  tiledb_filter_type_t type;
  rc = tiledb_filter_get_type(ctx, filter, &type);
  REQUIRE(tiledb_status(rc) == TILEDB_OK);
  REQUIRE(type == TILEDB_FILTER_BZIP2);

  // Clean up
  tiledb_filter_free(&filter);
  tiledb_ctx_free(&ctx);
}
