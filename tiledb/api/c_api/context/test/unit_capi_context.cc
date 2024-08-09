/**
 * @file tiledb/api/c_api/context/test/unit_capi_context.cc
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

#include <catch2/catch_test_macros.hpp>
#include "../context_api_external.h"
#include "../context_api_internal.h"
#include "tiledb/api/c_api_test_support/testsupport_capi_context.h"

using tiledb::api::test_support::ordinary_context;

TEST_CASE("C API: tiledb_ctx_alloc argument validation", "[capi][context]") {
  SECTION("Success, null config") {
    tiledb_ctx_handle_t* ctx{nullptr};
    auto rc{tiledb_ctx_alloc(nullptr, &ctx)};
    CHECK(tiledb_status(rc) == TILEDB_OK);
    REQUIRE(ctx != nullptr);
    tiledb_ctx_free(&ctx);
  }

  SECTION("Success, non null config") {
    tiledb_ctx_handle_t* ctx{nullptr};
    tiledb_config_handle_t* config{nullptr};
    tiledb_error_handle_t* err{nullptr};
    auto rc{tiledb_config_alloc(&config, &err)};
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
    rc = tiledb_ctx_alloc(nullptr, &ctx);
    CHECK(tiledb_status(rc) == TILEDB_OK);
    REQUIRE(ctx != nullptr);
    tiledb_ctx_free(&ctx);
    tiledb_config_free(&config);
  }

  SECTION("null context") {
    auto rc{tiledb_ctx_alloc(nullptr, nullptr)};
    CHECK(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE("C API: tiledb_ctx_free argument validation", "[capi][context]") {
  tiledb_ctx_handle_t* ctx{nullptr};
  auto rc{tiledb_ctx_alloc(nullptr, &ctx)};
  REQUIRE(tiledb_status(rc) == TILEDB_OK);
  REQUIRE(ctx != nullptr);
  CHECK_NOTHROW(tiledb_ctx_free(&ctx));
}

TEST_CASE(
    "C API: tiledb_ctx_get_stats argument validation", "[capi][context]") {
  // No success section to avoid touching the stats object
  SECTION("bad context") {
    char* p;
    auto rc{tiledb_ctx_get_stats(nullptr, &p)};
    CHECK(rc == TILEDB_INVALID_CONTEXT);
  }
  SECTION("bad stats") {
    ordinary_context x;
    auto rc{tiledb_ctx_get_stats(x.context, nullptr)};
    CHECK(rc == TILEDB_ERR);
  }
}

TEST_CASE(
    "C API: tiledb_ctx_get_config argument validation", "[capi][context]") {
  SECTION("bad context") {
    tiledb_config_handle_t* config{nullptr};
    auto rc{tiledb_ctx_get_config(nullptr, &config)};
    CHECK(rc == TILEDB_INVALID_CONTEXT);
  }
  SECTION("bad config") {
    ordinary_context x;
    auto rc{tiledb_ctx_get_config(x.context, nullptr)};
    CHECK(rc == TILEDB_ERR);
  }
}

TEST_CASE(
    "C API: tiledb_ctx_get_last_error argument validation", "[capi][context]") {
  SECTION("bad context") {
    tiledb_error_handle_t* error{nullptr};
    auto rc{tiledb_ctx_get_last_error(nullptr, &error)};
    CHECK(rc == TILEDB_INVALID_CONTEXT);
  }
  SECTION("bad error") {
    ordinary_context x;
    auto rc{tiledb_ctx_get_last_error(x.context, nullptr)};
    CHECK(rc == TILEDB_ERR);
  }
}

TEST_CASE(
    "C API: tiledb_ctx_is_supported_fs argument validation",
    "[capi][context]") {
  SECTION("bad context") {
    int32_t result;
    auto rc{tiledb_ctx_is_supported_fs(nullptr, TILEDB_MEMFS, &result)};
    CHECK(rc == TILEDB_INVALID_CONTEXT);
  }
  /*
   * No SECTION("bad filesystem") because this function can't be exercised.
   * `StorageManagerStub` does not have a VFS instance and any attempt to call
   * into it will throw. The argument, though, isn't validated in any way, and
   * an invalid argument will return an OK status and a false result, when it
   * should return an ERR status with an invalid argument error message. We
   * can't test for that, however, while we're using the stub.
   */
  SECTION("bad result") {
    ordinary_context x;
    auto rc{tiledb_ctx_is_supported_fs(x.context, TILEDB_MEMFS, nullptr)};
    CHECK(rc == TILEDB_ERR);
  }
}

TEST_CASE(
    "C API: tiledb_ctx_cancel_tasks argument validation", "[capi][context]") {
  SECTION("bad context") {
    auto rc{tiledb_ctx_cancel_tasks(nullptr)};
    CHECK(rc == TILEDB_INVALID_CONTEXT);
  }
}

TEST_CASE("C API: tiledb_ctx_set_tag argument validation", "[capi][context]") {
  const char key[]{"foo"};
  const char value[]{"bar"};
  SECTION("bad context") {
    auto rc{tiledb_ctx_set_tag(nullptr, key, value)};
    CHECK(rc == TILEDB_INVALID_CONTEXT);
  }
  SECTION("bad key") {
    ordinary_context x;
    auto rc{tiledb_ctx_set_tag(x.context, nullptr, value)};
    CHECK(rc == TILEDB_ERR);
  }
  SECTION("bad value") {
    ordinary_context x;
    auto rc{tiledb_ctx_set_tag(x.context, key, nullptr)};
    CHECK(rc == TILEDB_ERR);
  }
}
