/**
 * @file tiledb/api/c_api/backend/test/unit_capi_backend.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2024 TileDB Inc.
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
 * Tests the backend C API.
 */

#include <test/support/tdb_catch.h>
#include "tiledb/api/c_api/backend/backend_api_external.h"
#include "tiledb/api/c_api/config/config_api_external.h"
#include "tiledb/api/c_api/context/context_api_external.h"

struct BackendTestCase {
  BackendTestCase(
      const char* uri, tiledb_data_protocol_t expected, const char* description)
      : uri_(uri)
      , expected_(expected)
      , description_(description) {
  }

  const char* uri_;
  tiledb_data_protocol_t expected_;
  const char* description_;

  void run(tiledb_ctx_t* ctx) {
    tiledb_data_protocol_t backend;
    REQUIRE(tiledb_uri_get_data_protocol(ctx, uri_, &backend) == TILEDB_OK);
    REQUIRE(backend == expected_);
  }
};

TEST_CASE("C API: Test backend identification", "[capi][backend]") {
  // Create a context for the tests
  tiledb_config_t* config = nullptr;
  tiledb_error_t* error = nullptr;
  tiledb_ctx_t* ctx = nullptr;
  REQUIRE(tiledb_config_alloc(&config, &error) == TILEDB_OK);
  REQUIRE(tiledb_ctx_alloc(config, &ctx) == TILEDB_OK);
  tiledb_config_free(&config);

  // clang-format off
  BackendTestCase test = GENERATE(
    BackendTestCase("s3://bucket/path", TILEDB_BACKEND_S3, "S3 URI"),
    BackendTestCase("http://example.com/path", TILEDB_BACKEND_S3, "HTTP URI (treated as S3)"),
    BackendTestCase("https://example.com/path", TILEDB_BACKEND_S3, "HTTPS URI (treated as S3)"),
    BackendTestCase("azure://container/path", TILEDB_BACKEND_AZURE, "Azure URI"),
    BackendTestCase("gcs://bucket/path", TILEDB_BACKEND_GCS, "GCS URI"),
    BackendTestCase("gs://bucket/path", TILEDB_BACKEND_GCS, "GS URI"));
  // clang-format on

  DYNAMIC_SECTION(test.description_) {
    test.run(ctx);
  }

  tiledb_ctx_free(&ctx);
}

TEST_CASE(
    "C API: Test backend identification with invalid URI",
    "[capi][backend][error]") {
  // Create a context for the tests
  tiledb_config_t* config = nullptr;
  tiledb_error_t* error = nullptr;
  tiledb_ctx_t* ctx = nullptr;
  REQUIRE(tiledb_config_alloc(&config, &error) == TILEDB_OK);
  REQUIRE(tiledb_ctx_alloc(config, &ctx) == TILEDB_OK);
  tiledb_config_free(&config);

  tiledb_data_protocol_t backend;

  // Empty URI should fail
  REQUIRE(tiledb_uri_get_data_protocol(ctx, "", &backend) == TILEDB_ERR);

  // Invalid scheme should fail
  REQUIRE(
      tiledb_uri_get_data_protocol(ctx, "invalid://path", &backend) ==
      TILEDB_ERR);

  tiledb_ctx_free(&ctx);
}
