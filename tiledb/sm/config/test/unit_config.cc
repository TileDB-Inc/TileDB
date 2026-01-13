/**
 * @file tiledb/sm/config/test/unit_config.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022-2025 TileDB, Inc.
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
#include "tiledb/sm/rest/rest_profile.h"

#include "test/support/src/temporary_local_directory.h"

using tiledb::sm::Config;
using tiledb::sm::utils::parse::convert;

/*
 * These are the types for which `Config::get` is explicitly specialized.
 */
using config_types = std::tuple<
    bool,
    int,
    uint32_t,
    int64_t,
    uint64_t,
    float,
    double,
    std::string_view>;
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

TEST_CASE("Config::get<std::string_view> - found and matched", "[config]") {
  Config c{};
  std::string key{"the_key"};
  std::string expected_value = GENERATE("test", "true", "0", "1.5");
  CHECK(c.set(key, expected_value).ok());
  TestConfig<std::string_view>::check_expected(expected_value, c, key);
}

TEST_CASE("Config::set_profile - failures", "[config]") {
  std::string profile_name = "test_profile";
  std::string profile_dir = "non_existent_directory";
  Config c{};
  CHECK(c.set("profile_name", profile_name).ok());
  CHECK(c.set("profile_dir", profile_dir).ok());

  // Set a profile that does not exist. This will throw an exception.
  bool found;
  CHECK_THROWS(c.get("rest.server_address", &found));
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

TEST_CASE("Config::set_profile - found", "[config]") {
  Config c{};
  CHECK(c.set("rest.server_address", "http://test_server:8080").ok());

  // Create a profile
  std::string profile_name = "test_profile";
  tiledb::sm::TemporaryLocalDirectory tempdir_;
  std::string profile_dir(tempdir_.path());
  auto profile = tiledb::sm::RestProfile(profile_name, profile_dir);

  // Set some parameters in the profile
  profile.set_param("rest.username", "test_user");
  profile.set_param("rest.password", "test_password");

  // Save the profile
  profile.save_to_file();

  // Set the profile in the config
  CHECK(c.set("profile_name", profile_name).ok());
  CHECK(c.set("profile_dir", profile_dir).ok());

  // Check that the config has the profile's parameters
  bool found;
  auto username = c.get("rest.username", &found);
  REQUIRE(found);
  CHECK(username == "test_user");

  // Check that we can retrieve the config's parameters unrelated to the profile
  auto server_address = c.get("rest.server_address", &found);
  REQUIRE(found);
  CHECK(server_address == "http://test_server:8080");

  // In case that the config has alread been set with a different
  // `rest.username`
  CHECK(c.set("rest.username", "another_user").ok());
  // Check that the config's parameters have priority over the profile's
  // parameters
  auto another_username = c.get("rest.username", &found);
  REQUIRE(found);
  CHECK(another_username == "another_user");

  // Check that removing the config's parameter restores the profile's parameter
  CHECK(c.unset("rest.username").ok());
  auto restored_username = c.get("rest.username", &found);
  REQUIRE(found);
  CHECK(restored_username == "test_user");
}

TEST_CASE("Config::get_with_source - various sources", "[config]") {
  Config c{};

  // Test default value
  auto [source, value] = c.get_with_source("rest.retry_count");
  CHECK(source == tiledb::sm::ConfigSource::DEFAULT);
  CHECK(value == "25");

  // Test user-set value
  CHECK(c.set("rest.retry_count", "50").ok());
  auto [source2, value2] = c.get_with_source("rest.retry_count");
  CHECK(source2 == tiledb::sm::ConfigSource::USER_SET);
  CHECK(value2 == "50");

  // Test non-existent parameter
  auto [source3, value3] = c.get_with_source("nonexistent.param");
  CHECK(source3 == tiledb::sm::ConfigSource::NONE);
  CHECK(value3 == "");
}

TEST_CASE(
    "Config::get_effective_rest_auth_method - REST authentication",
    "[config]") {
  Config c{};

  SECTION("No authentication configured") {
    auto method = c.get_effective_rest_auth_method();
    CHECK(method == tiledb::sm::RestAuthMethod::NONE);
  }

  SECTION("With user-set token") {
    CHECK(c.set("rest.token", "my-token").ok());
    auto method = c.get_effective_rest_auth_method();
    CHECK(method == tiledb::sm::RestAuthMethod::TOKEN);
  }

  SECTION("With user-set username/password") {
    CHECK(c.set("rest.username", "user").ok());
    CHECK(c.set("rest.password", "pass").ok());
    auto method = c.get_effective_rest_auth_method();
    CHECK(method == tiledb::sm::RestAuthMethod::USERNAME_PASSWORD);
  }

  SECTION("Only username configured - invalid") {
    CHECK(c.set("rest.username", "user").ok());
    auto method = c.get_effective_rest_auth_method();
    CHECK(method == tiledb::sm::RestAuthMethod::INVALID);
  }

  SECTION("Only password configured - invalid") {
    CHECK(c.set("rest.password", "pass").ok());
    auto method = c.get_effective_rest_auth_method();
    CHECK(method == tiledb::sm::RestAuthMethod::INVALID);
  }

  SECTION(
      "Priority: Both token and username/password with same priority - prefer "
      "token") {
    CHECK(c.set("rest.token", "my-token").ok());
    CHECK(c.set("rest.username", "user").ok());
    CHECK(c.set("rest.password", "pass").ok());
    auto method = c.get_effective_rest_auth_method();
    CHECK(method == tiledb::sm::RestAuthMethod::TOKEN);
  }

  SECTION(
      "Priority: Profile with token, user-set username/password - prefer "
      "user-set") {
    // Create a profile with token configured
    tiledb::sm::TemporaryLocalDirectory tempdir_;
    std::string profile_dir(tempdir_.path());
    std::string profile_name = "test_profile";
    auto profile = tiledb::sm::RestProfile(profile_name, profile_dir);
    profile.set_param("rest.token", "profile-token");
    profile.save_to_file();

    // Set profile in config
    CHECK(c.set("profile_name", profile_name).ok());
    CHECK(c.set("profile_dir", profile_dir).ok());

    // User explicitly sets username/password in config
    CHECK(c.set("rest.username", "user").ok());
    CHECK(c.set("rest.password", "pass").ok());

    // Should return USERNAME_PASSWORD because user-set has higher priority than
    // profile
    auto method = c.get_effective_rest_auth_method();
    CHECK(method == tiledb::sm::RestAuthMethod::USERNAME_PASSWORD);
  }
}
