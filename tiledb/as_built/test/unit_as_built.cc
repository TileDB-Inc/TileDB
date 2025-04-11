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

namespace as_built = tiledb::as_built;

const std::string dump_str() noexcept {
  try {
    return as_built::dump();
  } catch (...) {
    return "";
  }
}
static const std::string dump_str_{dump_str()};

std::optional<json> dump_json(std::string dump_str) noexcept {
  try {
    return json::parse(dump_str);
  } catch (...) {
    return std::nullopt;
  }
}
static const std::optional<json> dump_{dump_json(dump_str())};

TEST_CASE("as_built: Ensure dump() does not throw", "[as_built][dump]") {
  std::string x;
  CHECK_NOTHROW(x = as_built::dump());
  CHECK(x.compare(dump_str_) == 0);
}

TEST_CASE("as_built: Ensure dump is non-empty", "[as_built][dump][non-empty]") {
  REQUIRE(!dump_str_.empty());
}

TEST_CASE("as_built: Print dump", "[as_built][dump][.print_json]") {
  std::cerr << dump_str_ << std::endl;
}

TEST_CASE("as_built: Ensure dump has json output", "[as_built][dump][json]") {
  json x;
  CHECK_NOTHROW(x = json::parse(dump_str_));
  CHECK(!x.is_null());
  CHECK(dump_ != std::nullopt);
  CHECK(x == dump_);
}

/**
 * Note: x must be constructed with either "auto x(...)" or "auto x = ..."
 * and NOT with "auto x{...}" as a workaround for a compiler-variant issue
 * with the JSON parser.
 **/
TEST_CASE("as_built: Validate top-level key", "[as_built][top-level]") {
  auto x(dump_.value()["as_built"]);
  CHECK(x.type() == nlohmann::detail::value_t::object);
  CHECK(!x.empty());
}

TEST_CASE("as_built: Validate parameters key", "[as_built][parameters]") {
  auto x(dump_.value()["as_built"]["parameters"]);
  CHECK(x.type() == nlohmann::detail::value_t::object);
  CHECK(!x.empty());
}

TEST_CASE(
    "as_built: Validate storage_backends key", "[as_built][storage_backends]") {
  auto x(dump_.value()["as_built"]["parameters"]["storage_backends"]);
  CHECK(x.type() == nlohmann::detail::value_t::object);
  CHECK(!x.empty());
}

TEST_CASE(
    "as_built: storage_backends attributes", "[as_built][storage_backends]") {
  auto x(dump_.value()["as_built"]["parameters"]["storage_backends"]);
  CHECK(!x.empty());

#ifdef HAVE_AZURE
  CHECK(x["azure"]["enabled"] == true);
#else
  CHECK(x["azure"]["enabled"] == false);
#endif  // HAVE_AZURE

#ifdef HAVE_GCS
  CHECK(x["gcs"]["enabled"] == true);
#else
  CHECK(x["gcs"]["enabled"] == false);
#endif  // HAVE_GCS

  CHECK(x["hdfs"]["enabled"] == false);

#ifdef HAVE_S3
  CHECK(x["s3"]["enabled"] == true);
#else
  CHECK(x["s3"]["enabled"] == false);
#endif  // HAVE_S3
}

TEST_CASE("as_built: Validate support key", "[as_built][support]") {
  auto x(dump_.value()["as_built"]["parameters"]["support"]);
  CHECK(x.type() == nlohmann::detail::value_t::object);
  CHECK(!x.empty());
}

TEST_CASE("as_built: support attributes", "[as_built][support]") {
  auto x(dump_.value()["as_built"]["parameters"]["support"]);
  CHECK(!x.empty());

#ifdef TILEDB_SERIALIZATION
  CHECK(x["serialization"]["enabled"] == true);
#else
  CHECK(x["serialization"]["enabled"] == false);
#endif  // TILEDB_SERIALIZATION
}
