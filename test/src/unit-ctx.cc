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
#include "test/support/src/vfs_helpers.h"
#include "tiledb/api/c_api/context/context_api_internal.h"
#include "tiledb/sm/c_api/tiledb_struct_def.h"
#include "tiledb/sm/cpp_api/tiledb"
#include "tiledb/sm/rest/rest_client_remote.h"

#include <cstring>
#include <iostream>

TEST_CASE("C API: Test context tags", "[capi][ctx-tags]") {
  tiledb_ctx_t* ctx = nullptr;
  REQUIRE(tiledb_ctx_alloc(nullptr, &ctx) == TILEDB_OK);

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
  tiledb::Context ctx;

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

TEST_CASE("C++ API: Test REST version endpoint", "[cppapi][rest][version]") {
  tiledb::test::VFSTestSetup vfs_test_setup;
  tiledb::Config config;
  std::string serialization_format = GENERATE("JSON", "CAPNP");
  config["rest.server_serialization_format"] = serialization_format;
  config["rest.server_address"] = "http://127.0.0.1:8181";

  // Only run these tests if the rest client has been initialized
  if (vfs_test_setup.is_rest()) {
    // Update the config in each section so that we always have a fresh Context.
    // Otherwise, the JSON test will initialize rest_tiledb_version_ for CAPNP.
    DYNAMIC_SECTION(
        "GET request to retrieve REST tiledb version - "
        << serialization_format) {
      vfs_test_setup.update_config(config.ptr().get());
      auto ctx = vfs_test_setup.ctx();
      REQUIRE(ctx.ptr()->rest_client().get_rest_version() == "2.28.0");
    }
    DYNAMIC_SECTION(
        "Initialization of rest_tiledb_version_ on first access - "
        << serialization_format) {
      vfs_test_setup.update_config(config.ptr().get());
      auto ctx = vfs_test_setup.ctx();
      REQUIRE(ctx.ptr()->rest_client().rest_version() == "2.28.0");
    }
  }
}
