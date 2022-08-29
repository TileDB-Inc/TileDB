/**
 * @file tiledb/sm/config/test/unit_config.cc
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
 *
 * This file defines a test `main()`
 */

#define CATCH_CONFIG_MAIN
#include <test/support/tdb_catch.h>
#include "../../misc/parse_argument.h"
#include "../config.h"

using tiledb::sm::Config;
using tiledb::sm::utils::parse::convert;

/*
 * These are the types for which `Config::get` is explicitly specialized.
 */
using config_types = std::
    tuple<bool, int, uint32_t, int64_t, uint64_t, float, double, std::string>;

TEMPLATE_LIST_TEST_CASE("Config::get<T> - found", "[config]", config_types) {
  Config c{};
  std::string key{"the_key"};
  CHECK(c.set(key, "1").ok());
  REQUIRE_NOTHROW(c.get<TestType>(key));
  auto found_value{c.get<TestType>(key)};
  CHECK(found_value.has_value());
  TestType expected_value;
  CHECK(convert("1", &expected_value).ok());
  CHECK(found_value.value() == expected_value);

  // Check setting a new value to existing config key; Check bool is false
  CHECK(c.set(key, "0").ok());
  REQUIRE_NOTHROW(c.get<TestType>(key));
  found_value = c.get<TestType>(key);
  CHECK(found_value.has_value());
  CHECK(convert("0", &expected_value).ok());
  CHECK(found_value.value() == expected_value);
}

TEMPLATE_LIST_TEST_CASE(
    "Config::get<T> - not found", "[config]", config_types) {
  Config c{};
  std::string key{"the_key"};
  REQUIRE_NOTHROW(c.get<TestType>(key));
  auto found_value{c.get<TestType>(key)};
  CHECK(!found_value.has_value());
}

TEMPLATE_LIST_TEST_CASE(
    "Config::get<T> - must_find found", "[config]", config_types) {
  Config c{};
  std::string key{"the_key"};
  c.set(key, "1");
  REQUIRE_NOTHROW(c.get<TestType>(key, Config::must_find));
  auto found_value{c.get<TestType>(key, Config::must_find)};
  TestType expected_value{};
  CHECK(convert("1", &expected_value).ok());
  CHECK(found_value == expected_value);

  // Check setting a new value to existing config key; Check bool is false
  c.set(key, "0");
  REQUIRE_NOTHROW(c.get<TestType>(key, Config::must_find));
  found_value = c.get<TestType>(key, Config::must_find);
  CHECK(convert("0", &expected_value).ok());
  CHECK(found_value == expected_value);
}

TEMPLATE_LIST_TEST_CASE(
    "Config::get<T> - must_find not found", "[config]", config_types) {
  Config c{};
  std::string key{"the_key"};
  REQUIRE_THROWS(c.get<TestType>(key, Config::must_find));
}
