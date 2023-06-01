/**
 * @file unit_as_built.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2023 TileDB, Inc.
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
 * Tests the tiledb::as_built namespace.
 */

#include "tiledb/as_built/as_built.h"

#include <tdb_catch.h>
#include <iostream>

namespace built = tiledb::as_built;
const std::string dump_str_ = built::dump();
const json dump_ = json::parse(dump_str_);

TEST_CASE("as_built: Ensure dump is valid", "[as_built][dump]") {
  CHECK_NOTHROW(built::dump());
}

TEST_CASE("as_built: Ensure dump has json output", "[as_built][dump][json]") {
  CHECK_NOTHROW((void)json::parse(dump_str_));
}

TEST_CASE("as_built: Print dump", "[as_built][dump][.print_json]") {
  std::cerr << dump_str_ << std::endl;
}

TEST_CASE("as_built: Ensure dump is non-empty", "[as_built][dump][non-empty]") {
  REQUIRE(!dump_str_.empty());
}

TEST_CASE("as_built: Validate top-level key", "[as_built][top-level]") {
  auto x{dump_["as_built"]};
  CHECK(x.type() == nlohmann::detail::value_t::object);
  CHECK(!x.empty());
}

TEST_CASE("as_built: Validate parameters key", "[as_built][parameters]") {
  auto x{dump_["as_built"]["parameters"]};
  CHECK(x.type() == nlohmann::detail::value_t::object);
  CHECK(!x.empty());
}

TEST_CASE(
    "as_built: Validate storage_backends key", "[as_built][storage_backends]") {
  auto x{dump_["as_built"]["parameters"]["storage_backends"]};
  CHECK(x.type() == nlohmann::detail::value_t::object);
  CHECK(!x.empty());
}

TEST_CASE(
    "as_built: storage_backends attributes", "[as_built][storage_backends]") {
  auto x{dump_["as_built"]["parameters"]["storage_backends"]};
  CHECK(!x.empty());

#ifdef TILEDB_AZURE
  CHECK(x["azure"] == "on");
#else
  CHECK(x["azure"] == "off");
#endif  // TILEDB_AZURE

#ifdef TILEDB_GCS
  CHECK(x["gcs"] == "on");
#else
  CHECK(x["gcs"] == "off");
#endif  // TILEDB_GCS

#ifdef TILEDB_S3
  CHECK(x["s3"] == "on");
#else
  CHECK(x["s3"] == "off");
#endif  // TILEDB_S3
}

TEST_CASE("as_built: Validate support key", "[as_built][support]") {
  auto x{dump_["as_built"]["parameters"]["support"]};
  CHECK(x.type() == nlohmann::detail::value_t::object);
  CHECK(!x.empty());
}

TEST_CASE("as_built: support attributes", "[as_built][support]") {
  auto x{dump_["as_built"]["parameters"]["support"]};
  CHECK(!x.empty());

#ifdef TILEDB_SERIALIZATION
  CHECK(x["serialization"] == "on");
#else
  CHECK(x["serialization"] == "off");
#endif  // TILEDB_SERIALIZATION
}
