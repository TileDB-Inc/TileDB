/**
 * @file   unit-capi-filter.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2021 TileDB Inc.
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

TEST_CASE("C API: tiledb_filter_alloc validation", "[capi][filter]") {
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

TEST_CASE("C API: tiledb_filter_free validation", "[capi][filter]") {
  REQUIRE_NOTHROW(tiledb_filter_free(nullptr));
}

TEST_CASE("C API: tiledb_filter_get_type validation", "[capi][filter]") {
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

TEST_CASE("C API: tiledb_filter_set_option validation", "[capi][filter]") {
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

TEST_CASE("C API: tiledb_filter_get_option validation", "[capi][filter]") {
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

TEST_CASE("C API: tiledb_filter_list_alloc validation", "[capi][filter-list]") {
  tiledb_ctx_t* ctx;
  capi_return_t rc = tiledb_ctx_alloc(nullptr, &ctx);
  REQUIRE(tiledb_status(rc) == TILEDB_OK);
  tiledb_filter_list_t* filter_list;

  SECTION("null context") {
    rc = tiledb_filter_list_alloc(nullptr, &filter_list);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null filter list pointer") {
    rc = tiledb_filter_list_alloc(ctx, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE("C API: tiledb_filter_list_free validation", "[capi][filter-list]") {
  REQUIRE_NOTHROW(tiledb_filter_list_free(nullptr));
}

TEST_CASE(
    "C API: tiledb_filter_list_add_filter validation", "[capi][filter-list]") {
  tiledb_ctx_t* ctx;
  int rc = tiledb_ctx_alloc(nullptr, &ctx);
  REQUIRE(tiledb_status(rc) == TILEDB_OK);
  tiledb_filter_list_t* filter_list;
  rc = tiledb_filter_list_alloc(ctx, &filter_list);
  REQUIRE(tiledb_status(rc) == TILEDB_OK);
  tiledb_filter_t* filter;
  rc = tiledb_filter_alloc(ctx, TILEDB_FILTER_NONE, &filter);
  REQUIRE(tiledb_status(rc) == TILEDB_OK);

  SECTION("null context") {
    rc = tiledb_filter_list_add_filter(nullptr, filter_list, filter);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null filter list") {
    rc = tiledb_filter_list_add_filter(ctx, nullptr, filter);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null filter") {
    rc = tiledb_filter_list_add_filter(ctx, filter_list, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }

  tiledb_filter_free(&filter);
  tiledb_filter_list_free(&filter_list);
  tiledb_ctx_free(&ctx);
}

TEST_CASE(
    "C API: tiledb_filter_list_set_max_chunk_size validation",
    "[capi][filter-list]") {
  tiledb_ctx_t* ctx;
  int rc = tiledb_ctx_alloc(nullptr, &ctx);
  REQUIRE(tiledb_status(rc) == TILEDB_OK);
  tiledb_filter_list_t* filter_list;
  rc = tiledb_filter_list_alloc(ctx, &filter_list);
  REQUIRE(tiledb_status(rc) == TILEDB_OK);
  constexpr uint32_t mcs{4 * 1024 * 1024};

  SECTION("null context") {
    rc = tiledb_filter_list_set_max_chunk_size(nullptr, filter_list, mcs);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null filter list") {
    rc = tiledb_filter_list_set_max_chunk_size(ctx, nullptr, mcs);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  /*
   * There is no validation test for an invalid max_chunk_size. The underlying
   * function in `FilterPipeline` does no validation; every `uint_32` is
   * accepted as valid.
   */

  tiledb_filter_list_free(&filter_list);
  tiledb_ctx_free(&ctx);
}

TEST_CASE(
    "C API: tiledb_filter_list_get_max_chunk_size validation",
    "[capi][filter-list]") {
  tiledb_ctx_t* ctx;
  int rc = tiledb_ctx_alloc(nullptr, &ctx);
  REQUIRE(tiledb_status(rc) == TILEDB_OK);
  tiledb_filter_list_t* filter_list;
  rc = tiledb_filter_list_alloc(ctx, &filter_list);
  REQUIRE(tiledb_status(rc) == TILEDB_OK);
  uint32_t mcs;

  SECTION("null context") {
    rc = tiledb_filter_list_get_max_chunk_size(nullptr, filter_list, &mcs);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null filter list") {
    rc = tiledb_filter_list_get_max_chunk_size(ctx, nullptr, &mcs);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null max chunk size") {
    rc = tiledb_filter_list_get_max_chunk_size(ctx, filter_list, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }

  tiledb_filter_list_free(&filter_list);
  tiledb_ctx_free(&ctx);
}
TEST_CASE(
    "C API: tiledb_filter_list_get_nfilters validation",
    "[capi][filter-list]") {
  tiledb_ctx_t* ctx;
  int rc = tiledb_ctx_alloc(nullptr, &ctx);
  REQUIRE(tiledb_status(rc) == TILEDB_OK);
  tiledb_filter_list_t* filter_list;
  rc = tiledb_filter_list_alloc(ctx, &filter_list);
  REQUIRE(tiledb_status(rc) == TILEDB_OK);
  tiledb_filter_t* filter;

  SECTION("null context") {
    rc = tiledb_filter_list_get_filter_from_index(
        nullptr, filter_list, 0, &filter);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null filter list") {
    rc = tiledb_filter_list_get_filter_from_index(ctx, nullptr, 0, &filter);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null max chunk size") {
    rc = tiledb_filter_list_get_filter_from_index(ctx, filter_list, 0, &filter);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }

  tiledb_filter_list_free(&filter_list);
  tiledb_ctx_free(&ctx);
}

TEST_CASE(
    "C API: tiledb_filter_list_get_filter_from_index validation",
    "[capi][filter-list]") {
  tiledb_ctx_t* ctx;
  int rc = tiledb_ctx_alloc(nullptr, &ctx);
  REQUIRE(tiledb_status(rc) == TILEDB_OK);
  tiledb_filter_list_t* filter_list;
  rc = tiledb_filter_list_alloc(ctx, &filter_list);
  tiledb_filter_t* filter;
  rc = tiledb_filter_alloc(ctx, TILEDB_FILTER_NONE, &filter);
  REQUIRE(tiledb_status(rc) == TILEDB_OK);
  // A filter list of length one avoids the index always being invalid.
  rc = tiledb_filter_list_add_filter(ctx, filter_list, filter);
  REQUIRE(tiledb_status(rc) == TILEDB_OK);

  tiledb_filter_t* filter_out;
  SECTION("null context") {
    rc = tiledb_filter_list_get_filter_from_index(
        nullptr, filter_list, 0, &filter_out);
  }
  SECTION("null filter list") {
    rc = tiledb_filter_list_get_filter_from_index(ctx, nullptr, 0, &filter_out);
  }
  SECTION("invalid index") {
    rc = tiledb_filter_list_get_filter_from_index(
        ctx, filter_list, 9001, &filter_out);
  }
  SECTION("null filter pointer") {
    rc = tiledb_filter_list_get_filter_from_index(ctx, filter_list, 0, nullptr);
  }

  tiledb_filter_free(&filter);
  tiledb_filter_list_free(&filter_list);
  tiledb_ctx_free(&ctx);
}

TEST_CASE("C API: Test filter list", "[capi][filter-list]") {
  tiledb_ctx_t* ctx;
  auto rc = tiledb_ctx_alloc(nullptr, &ctx);
  REQUIRE(tiledb_status(rc) == TILEDB_OK);

  tiledb_filter_list_t* filter_list;
  rc = tiledb_filter_list_alloc(ctx, &filter_list);
  REQUIRE(tiledb_status(rc) == TILEDB_OK);

  unsigned nfilters;
  tiledb_filter_t* filter_out;

  /*
   * Zero length filter list
   */
  // Length 0
  rc = tiledb_filter_list_get_nfilters(ctx, filter_list, &nfilters);
  REQUIRE(tiledb_status(rc) == TILEDB_OK);
  REQUIRE(nfilters == 0);
  // Index zero should fail
  rc = tiledb_filter_list_get_filter_from_index(
      ctx, filter_list, 0, &filter_out);
  REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  // Index one should fail
  rc = tiledb_filter_list_get_filter_from_index(
      ctx, filter_list, 1, &filter_out);
  REQUIRE(tiledb_status(rc) == TILEDB_ERR);

  /*
   * Length one filter list
   */
  tiledb_filter_t* filter;
  rc = tiledb_filter_alloc(ctx, TILEDB_FILTER_BZIP2, &filter);
  REQUIRE(tiledb_status(rc) == TILEDB_OK);

  int level = 5;
  rc = tiledb_filter_set_option(ctx, filter, TILEDB_COMPRESSION_LEVEL, &level);
  REQUIRE(tiledb_status(rc) == TILEDB_OK);
  level = 0;
  rc = tiledb_filter_get_option(ctx, filter, TILEDB_COMPRESSION_LEVEL, &level);
  REQUIRE(tiledb_status(rc) == TILEDB_OK);
  REQUIRE(level == 5);

  rc = tiledb_filter_list_add_filter(ctx, filter_list, filter);
  REQUIRE(tiledb_status(rc) == TILEDB_OK);

  // Length 1
  rc = tiledb_filter_list_get_nfilters(ctx, filter_list, &nfilters);
  REQUIRE(tiledb_status(rc) == TILEDB_OK);
  REQUIRE(nfilters == 1);
  // Index zero should succeed
  rc = tiledb_filter_list_get_filter_from_index(
      ctx, filter_list, 0, &filter_out);
  REQUIRE(tiledb_status(rc) == TILEDB_OK);
  REQUIRE(filter_out != nullptr);
  // Index zero should fail
  rc = tiledb_filter_list_get_filter_from_index(
      ctx, filter_list, 1, &filter_out);
  REQUIRE(tiledb_status(rc) == TILEDB_ERR);

  // Clean up
  tiledb_filter_free(&filter_out);
  tiledb_filter_free(&filter);
  tiledb_filter_list_free(&filter_list);
  tiledb_ctx_free(&ctx);
}

TEST_CASE("C API: Test filter list on attribute", "[capi][filter-list]") {
  tiledb_ctx_t* ctx;
  int rc = tiledb_ctx_alloc(nullptr, &ctx);
  REQUIRE(tiledb_status(rc) == TILEDB_OK);

  tiledb_filter_t* filter;
  rc = tiledb_filter_alloc(ctx, TILEDB_FILTER_BZIP2, &filter);
  REQUIRE(tiledb_status(rc) == TILEDB_OK);

  int level = 5;
  rc = tiledb_filter_set_option(ctx, filter, TILEDB_COMPRESSION_LEVEL, &level);
  REQUIRE(tiledb_status(rc) == TILEDB_OK);

  tiledb_filter_list_t* filter_list;
  rc = tiledb_filter_list_alloc(ctx, &filter_list);
  REQUIRE(tiledb_status(rc) == TILEDB_OK);
  rc = tiledb_filter_list_add_filter(ctx, filter_list, filter);
  REQUIRE(tiledb_status(rc) == TILEDB_OK);
  rc = tiledb_filter_list_set_max_chunk_size(ctx, filter_list, 1024);
  REQUIRE(tiledb_status(rc) == TILEDB_OK);

  tiledb_attribute_t* attr;
  rc = tiledb_attribute_alloc(ctx, "a", TILEDB_INT32, &attr);
  REQUIRE(tiledb_status(rc) == TILEDB_OK);

  rc = tiledb_attribute_set_filter_list(ctx, attr, filter_list);
  REQUIRE(tiledb_status(rc) == TILEDB_OK);

  tiledb_filter_list_t* filter_list_out;
  rc = tiledb_attribute_get_filter_list(ctx, attr, &filter_list_out);
  REQUIRE(tiledb_status(rc) == TILEDB_OK);

  unsigned nfilters;
  rc = tiledb_filter_list_get_nfilters(ctx, filter_list_out, &nfilters);
  REQUIRE(tiledb_status(rc) == TILEDB_OK);
  REQUIRE(nfilters == 1);

  tiledb_filter_t* filter_out;
  rc = tiledb_filter_list_get_filter_from_index(
      ctx, filter_list_out, 0, &filter_out);
  REQUIRE(tiledb_status(rc) == TILEDB_OK);
  REQUIRE(filter_out != nullptr);

  level = 0;
  rc = tiledb_filter_get_option(
      ctx, filter_out, TILEDB_COMPRESSION_LEVEL, &level);
  REQUIRE(tiledb_status(rc) == TILEDB_OK);
  REQUIRE(level == 5);

  uint32_t max_chunk_size;
  rc = tiledb_filter_list_get_max_chunk_size(
      ctx, filter_list_out, &max_chunk_size);
  REQUIRE(tiledb_status(rc) == TILEDB_OK);
  REQUIRE(max_chunk_size == 1024);

  tiledb_filter_free(&filter_out);
  tiledb_filter_list_free(&filter_list_out);

  // Clean up
  tiledb_attribute_free(&attr);
  tiledb_filter_free(&filter);
  tiledb_filter_list_free(&filter_list);
  tiledb_ctx_free(&ctx);
}
