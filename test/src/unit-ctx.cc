/**
 * @file   unit-ctx.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2021 TileDB Inc.
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
 * Tests for TileDB context object.
 */

#include <test/support/tdb_catch.h>
#include "test/support/src/helpers.h"
#include "tiledb/api/c_api/context/context_api_internal.h"
#include "tiledb/sm/c_api/tiledb_struct_def.h"
#include "tiledb/sm/cpp_api/tiledb"

#include <cstring>
#include <iostream>

using namespace tiledb::test;

TEST_CASE("C API: Test context tags", "[capi][ctx-tags]") {
  tiledb_ctx_t* ctx = vanilla_context_c();

  // Only run these tests if the rest client has been initialized
  if (ctx->has_rest_client()) {
    auto& rest_client{ctx->rest_client()};
    SECTION("Defaults") {
      REQUIRE(rest_client.extra_headers().size() == 2);
      REQUIRE(rest_client.extra_headers().at("x-tiledb-api-language") == "c");
      REQUIRE(
          rest_client.extra_headers().at("x-tiledb-version") ==
          (std::to_string(tiledb::sm::constants::library_version[0]) + "." +
           std::to_string(tiledb::sm::constants::library_version[1]) + "." +
           std::to_string(tiledb::sm::constants::library_version[2])));
    }
    SECTION("tiledb_ctx_set_tag") {
      REQUIRE(tiledb_ctx_set_tag(ctx, "tag1", "value1") == TILEDB_OK);
      REQUIRE(rest_client.extra_headers().size() == 3);
      REQUIRE(rest_client.extra_headers().at("tag1") == "value1");

      REQUIRE(tiledb_ctx_set_tag(ctx, "tag2", "value2") == TILEDB_OK);
      REQUIRE(rest_client.extra_headers().size() == 4);
      REQUIRE(rest_client.extra_headers().at("tag2") == "value2");

      REQUIRE(tiledb_ctx_set_tag(ctx, "tag1", "value3") == TILEDB_OK);
      REQUIRE(rest_client.extra_headers().size() == 4);
      REQUIRE(rest_client.extra_headers().at("tag1") == "value3");
    }
  }
  tiledb_ctx_free(&ctx);
}

TEST_CASE("C++ API: Test context tags", "[cppapi][ctx-tags]") {
  tiledb::Context& ctx = vanilla_context_cpp();

  // Only run these tests if the rest client has been initialized
  if (ctx.ptr().get()->has_rest_client()) {
    auto& rest_client{ctx.ptr().get()->rest_client()};
    SECTION("Defaults") {
      REQUIRE(rest_client.extra_headers().size() == 2);
      REQUIRE(rest_client.extra_headers().at("x-tiledb-api-language") == "c++");
      REQUIRE(
          rest_client.extra_headers().at("x-tiledb-version") ==
          (std::to_string(tiledb::sm::constants::library_version[0]) + "." +
           std::to_string(tiledb::sm::constants::library_version[1]) + "." +
           std::to_string(tiledb::sm::constants::library_version[2])));
    }
    SECTION("tiledb_ctx_set_tag") {
      REQUIRE_NOTHROW(ctx.set_tag("tag1", "value1"));
      REQUIRE(rest_client.extra_headers().size() == 3);
      REQUIRE(rest_client.extra_headers().at("tag1") == "value1");

      REQUIRE_NOTHROW(ctx.set_tag("tag2", "value2"));
      REQUIRE(rest_client.extra_headers().size() == 4);
      REQUIRE(rest_client.extra_headers().at("tag2") == "value2");

      REQUIRE_NOTHROW(ctx.set_tag("tag1", "value3"));
      REQUIRE(rest_client.extra_headers().size() == 4);
      REQUIRE(rest_client.extra_headers().at("tag1") == "value3");
    }
  }
}
