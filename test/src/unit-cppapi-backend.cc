/**
 * @file unit-cppapi-backend.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2024 TileDB, Inc.
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
 * Tests the C++ API for backend identification.
 */

#include <test/support/tdb_catch.h>
#include "tiledb/sm/cpp_api/backend.h"
#include "tiledb/sm/cpp_api/context.h"

using namespace tiledb;

TEST_CASE("C++ API: Backend - S3 URIs", "[cppapi][backend][s3]") {
  Context ctx;

  SECTION("s3:// scheme") {
    auto backend = Backend::from_uri(ctx, "s3://bucket/path");
    REQUIRE(backend == Backend::Type::S3);
    REQUIRE(backend.to_str() == "S3");
  }

  SECTION("http:// scheme") {
    auto backend = Backend::from_uri(ctx, "http://example.com/path");
    REQUIRE(backend == Backend::Type::S3);
  }

  SECTION("https:// scheme") {
    auto backend = Backend::from_uri(ctx, "https://example.com/path");
    REQUIRE(backend == Backend::Type::S3);
  }
}

TEST_CASE("C++ API: Backend - Azure URIs", "[cppapi][backend][azure]") {
  Context ctx;

  auto backend = Backend::from_uri(ctx, "azure://container/path");
  REQUIRE(backend == Backend::Type::Azure);
  REQUIRE(backend.to_str() == "Azure");
}

TEST_CASE("C++ API: Backend - GCS URIs", "[cppapi][backend][gcs]") {
  Context ctx;

  SECTION("gcs:// scheme") {
    auto backend = Backend::from_uri(ctx, "gcs://bucket/path");
    REQUIRE(backend == Backend::Type::GCS);
    REQUIRE(backend.to_str() == "GCS");
  }

  SECTION("gs:// scheme") {
    auto backend = Backend::from_uri(ctx, "gs://bucket/path");
    REQUIRE(backend == Backend::Type::GCS);
  }
}

TEST_CASE("C++ API: Backend - Type conversions", "[cppapi][backend]") {
  SECTION("From C type") {
    Backend backend(TILEDB_BACKEND_S3);
    REQUIRE(backend.type() == Backend::Type::S3);
    REQUIRE(backend.c_type() == TILEDB_BACKEND_S3);
  }

  SECTION("To C type") {
    Backend backend(Backend::Type::Azure);
    REQUIRE(backend.c_type() == TILEDB_BACKEND_AZURE);
  }
}

TEST_CASE("C++ API: Backend - Equality operators", "[cppapi][backend]") {
  Backend s3_backend(Backend::Type::S3);
  Backend azure_backend(Backend::Type::Azure);

  SECTION("Backend to Backend") {
    REQUIRE(s3_backend == s3_backend);
    REQUIRE(s3_backend != azure_backend);
  }

  SECTION("Backend to Type") {
    REQUIRE(s3_backend == Backend::Type::S3);
    REQUIRE(s3_backend != Backend::Type::Azure);
  }
}

TEST_CASE("C++ API: Backend - String representation", "[cppapi][backend]") {
  SECTION("S3") {
    Backend backend(Backend::Type::S3);
    REQUIRE(backend.to_str() == "S3");
  }

  SECTION("Azure") {
    Backend backend(Backend::Type::Azure);
    REQUIRE(backend.to_str() == "Azure");
  }

  SECTION("GCS") {
    Backend backend(Backend::Type::GCS);
    REQUIRE(backend.to_str() == "GCS");
  }

  SECTION("TileDB v1") {
    Backend backend(Backend::Type::TileDB_v1);
    REQUIRE(backend.to_str() == "TileDB_v1");
  }

  SECTION("TileDB v2") {
    Backend backend(Backend::Type::TileDB_v2);
    REQUIRE(backend.to_str() == "TileDB_v2");
  }

  SECTION("Invalid") {
    Backend backend(Backend::Type::Invalid);
    REQUIRE(backend.to_str() == "Invalid");
  }
}

TEST_CASE("C++ API: Backend - Stream operator", "[cppapi][backend]") {
  Backend backend(Backend::Type::S3);
  std::stringstream ss;
  ss << backend;
  REQUIRE(ss.str() == "S3");
}

TEST_CASE("C++ API: Backend - Invalid URI", "[cppapi][backend][error]") {
  Context ctx;

  SECTION("Empty URI") {
    REQUIRE_THROWS(Backend::from_uri(ctx, ""));
  }

  SECTION("Invalid scheme") {
    REQUIRE_THROWS(Backend::from_uri(ctx, "invalid://path"));
  }
}
