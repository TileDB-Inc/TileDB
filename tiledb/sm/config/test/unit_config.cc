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
using config_types_integral = std::tuple<int, uint32_t, int64_t, uint64_t>;
using config_types_floating = std::tuple<float, double>;

template <typename T>
struct TestConfig {
  static void check_expected(
      T expected, const Config& c, const std::string& key) {
    SECTION(" must_find = true") {
      REQUIRE_NOTHROW(c.get<T>(key, Config::must_find));
      auto found_value{c.get<T>(key, Config::must_find)};
      CHECK(found_value == expected);
    }
    SECTION(" must_find = false") {
      REQUIRE_NOTHROW(c.get<T>(key));
      auto found_value{c.get<T>(key)};
      CHECK(found_value.has_value());
      CHECK(found_value.value() == expected);
    }
  }
};

TEMPLATE_LIST_TEST_CASE(
    "Config::get<T> - not found", "[config]", config_types) {
  Config c{};
  std::string key{"the_key"};
  SECTION(" must_find = true") {
    REQUIRE_THROWS(c.get<TestType>(key, Config::must_find));
  }
  SECTION(" must_find = false") {
    REQUIRE_NOTHROW(c.get<TestType>(key));
    auto found_value{c.get<TestType>(key)};
    CHECK(!found_value.has_value());
  }
}

TEST_CASE("Config::get<bool> - found bool", "[config]") {
  Config c{};
  std::string key{"the_key"};
  bool expected_value = GENERATE(true, false);
  CHECK(c.set(key, expected_value ? "true" : "false").ok());
  TestConfig<bool>::check_expected(expected_value, c, key);
}

TEMPLATE_LIST_TEST_CASE(
    "Config::get<T> - found integral", "[config]", config_types_integral) {
  Config c{};
  std::string key{"the_key"};
  TestType expected_value = 1;
  CHECK(c.set(key, "1").ok());
  TestConfig<TestType>::check_expected(expected_value, c, key);
}

TEMPLATE_LIST_TEST_CASE(
    "Config::get<T> - found floating", "[config]", config_types_floating) {
  Config c{};
  std::string key{"the_key"};
  TestType expected_value = 1.0;
  CHECK(c.set(key, "1.0").ok());
  TestConfig<TestType>::check_expected(expected_value, c, key);
}

TEST_CASE("Config::get<std::string> - found and matched", "[config]") {
  Config c{};
  std::string key{"the_key"};
  std::string expected_value = GENERATE("test", "true", "0", "1.5");
  CHECK(c.set(key, expected_value).ok());
  TestConfig<std::string>::check_expected(expected_value, c, key);
}

TEST_CASE("Config::set_params - set and unset", "[config]") {
  Config c{};
  std::string key{"the_key"};
  std::string value{"the_value"};

  // Set the parameter
  CHECK(c.set(key, value).ok());
  auto set_params = c.set_params();
  CHECK(set_params.find(key) != set_params.end());

  // Unset the parameter
  CHECK(c.unset(key).ok());
  set_params = c.set_params();
  CHECK(set_params.find(key) == set_params.end());
}
