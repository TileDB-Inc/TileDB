/**
 * @file   unit-capi-as_built.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2023 TileDB Inc
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
 * Tests the as_built C API.
 * Note: this is a duplication of unit_as_built.cc but validates the namespace
 * within the C API compilation unit.
 */

#include <test/support/tdb_catch.h>
#include <nlohmann/json.hpp>
#include "tiledb/sm/c_api/tiledb_experimental.h"

#include <iostream>

using json = nlohmann::json;

static std::string dump_str() {
  tiledb_string_t* out;
  tiledb_as_built_dump(&out);
  const char* out_ptr;
  size_t out_length;
  tiledb_string_view(out, &out_ptr, &out_length);
  std::string out_str(out_ptr, out_length);
  tiledb_string_free(&out);
  return out_str;
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

TEST_CASE(
    "C API: as_built: Ensure dump is non-empty",
    "[capi][as_built][dump][non-empty]") {
  REQUIRE(!dump_str_.empty());
}

TEST_CASE(
    "C API: as_built: Print dump", "[capi][as_built][dump][.print_json]") {
  std::cerr << dump_str_ << std::endl;
}

TEST_CASE(
    "C API: as_built: Ensure dump has json output",
    "[capi][as_built][dump][json]") {
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
TEST_CASE(
    "C API: as_built: Validate top-level key", "[capi][as_built][top-level]") {
  auto x(dump_.value()["as_built"]);
  CHECK(x.type() == nlohmann::detail::value_t::object);
  CHECK(!x.empty());
}

TEST_CASE(
    "C API: as_built: Validate parameters key",
    "[capi][as_built][parameters]") {
  auto x(dump_.value()["as_built"]["parameters"]);
  CHECK(x.type() == nlohmann::detail::value_t::object);
  CHECK(!x.empty());
}

TEST_CASE(
    "C API: as_built: Validate storage_backends key",
    "[capi][as_built][storage_backends]") {
  auto x(dump_.value()["as_built"]["parameters"]["storage_backends"]);
  CHECK(x.type() == nlohmann::detail::value_t::object);
  CHECK(!x.empty());
}

TEST_CASE(
    "C API: as_built: storage_backends attributes",
    "[capi][as_built][storage_backends]") {
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

#ifdef HAVE_S3
  CHECK(x["s3"]["enabled"] == true);
#else
  CHECK(x["s3"]["enabled"] == false);
#endif  // HAVE_S3
}

TEST_CASE(
    "C API: as_built: Validate support key", "[capi][as_built][support]") {
  auto x(dump_.value()["as_built"]["parameters"]["support"]);
  CHECK(x.type() == nlohmann::detail::value_t::object);
  CHECK(!x.empty());
}

TEST_CASE("C API: as_built: support attributes", "[capi][as_built][support]") {
  auto x(dump_.value()["as_built"]["parameters"]["support"]);
  CHECK(!x.empty());

#ifdef TILEDB_SERIALIZATION
  CHECK(x["serialization"]["enabled"] == true);
#else
  CHECK(x["serialization"]["enabled"] == false);
#endif  // TILEDB_SERIALIZATION
}
