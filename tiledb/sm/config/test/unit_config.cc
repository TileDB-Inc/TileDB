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
#include "../config.h"

using tiledb::sm::Config;

/*
 * These are the types for which `Config::get` is explicitly specialized.
 */
using config_types = std::
    tuple<bool, int, uint32_t, int64_t, uint64_t, float, double, std::string>;
using config_types_integral = std::tuple<int, uint32_t, int64_t, uint64_t>;
using config_types_floating = std::tuple<float, double>;

TEST_CASE("Config::get - not found", "[config]") {
  Config c{};  // empty
  std::string key{"this_key_is_not_present"};
  auto x{c.get(key)};
  CHECK(!x.has_value());
}

TEMPLATE_LIST_TEST_CASE("Config::get - not found", "[config]", config_types) {
  Config c{};  // empty
  std::string key{"this_key_is_not_present"};
  auto x{c.get<TestType>(key)};
  CHECK(!x.has_value());
}

TEST_CASE("Config::get<bool> - found true", "[config]") {
  Config c{};
  std::string key{"the_key"};
  c.set(key, "true");
  auto x{c.get<bool>(key)};
  REQUIRE(x.has_value());
  CHECK(x.value());
}

TEST_CASE("Config::get<bool> - found false", "[config]") {
  Config c{};
  std::string key{"the_key"};
  c.set(key, "false");
  auto x{c.get<bool>(key)};
  REQUIRE(x.has_value());
  CHECK(!x.value());
}

TEMPLATE_LIST_TEST_CASE(
    "Config::get<T> - found 1", "[config]", config_types_integral) {
  Config c{};
  std::string key{"the_key"};
  c.set(key, "1");
  auto x{c.get<TestType>(key)};
  REQUIRE(x.has_value());
  CHECK(x.value() == 1);
}

TEMPLATE_LIST_TEST_CASE(
    "Config::get<T> - found 1.0", "[config]", config_types_floating) {
  Config c{};
  std::string key{"the_key"};
  c.set(key, "1.0");
  auto x{c.get<TestType>(key)};
  REQUIRE(x.has_value());
  CHECK(x.value() == 1.0);
}

TEST_CASE("Config::get<std::string> - found and matched", "[config]") {
  Config c{};
  std::string key{"the_key"};
  std::string value{"xyzzy"};
  c.set(key, value);
  auto x{c.get<std::string>(key)};
  REQUIRE(x.has_value());
  CHECK(x.value() == value);
}
