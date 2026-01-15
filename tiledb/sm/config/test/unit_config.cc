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

#include "test/support/src/helpers.h"
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

  // Test environment variable (ENVIRONMENT source)
  {
    auto env_token = setenv_local("TILEDB_REST_TOKEN", "env-test-token");
    Config c2{};  // Create new config to pick up environment variable
    auto [source_env, value_env] = c2.get_with_source("rest.token");
    CHECK(source_env == tiledb::sm::ConfigSource::ENVIRONMENT);
    CHECK(value_env == "env-test-token");
  }  // env_token goes out of scope, automatically restores old value

  // Test user-set overrides environment variable
  {
    auto env_username = setenv_local("TILEDB_REST_USERNAME", "env-username");
    Config c3{};
    // First check environment variable is picked up
    auto [source_env2, value_env2] = c3.get_with_source("rest.username");
    CHECK(source_env2 == tiledb::sm::ConfigSource::ENVIRONMENT);
    CHECK(value_env2 == "env-username");
    // Now override with user-set value
    CHECK(c3.set("rest.username", "user-set-username").ok());
    auto [source_override, value_override] =
        c3.get_with_source("rest.username");
    CHECK(source_override == tiledb::sm::ConfigSource::USER_SET);
    CHECK(value_override == "user-set-username");
  }

  // Test profile source
  tiledb::sm::TemporaryLocalDirectory tempdir_;
  std::string profile_dir(tempdir_.path());
  std::string profile_name = "test_profile";
  auto profile = tiledb::sm::RestProfile(profile_name, profile_dir);
  profile.set_param("rest.token", "profile-token");
  profile.save_to_file();

  Config c4{};
  CHECK(c4.set("profile_name", profile_name).ok());
  CHECK(c4.set("profile_dir", profile_dir).ok());
  auto [source_profile, value_profile] = c4.get_with_source("rest.token");
  CHECK(source_profile == tiledb::sm::ConfigSource::PROFILE);
  CHECK(value_profile == "profile-token");

  // Test priority: user-set > environment > profile
  {
    // Start with profile value
    Config c5{};
    CHECK(c5.set("profile_name", profile_name).ok());
    CHECK(c5.set("profile_dir", profile_dir).ok());
    auto [src1, val1] = c5.get_with_source("rest.token");
    CHECK(src1 == tiledb::sm::ConfigSource::PROFILE);
    CHECK(val1 == "profile-token");
    // Environment variable overrides profile
    auto env_token2 = setenv_local("TILEDB_REST_TOKEN", "env-token-2");
    Config c6{};
    CHECK(c6.set("profile_name", profile_name).ok());
    CHECK(c6.set("profile_dir", profile_dir).ok());
    auto [src2, val2] = c6.get_with_source("rest.token");
    CHECK(src2 == tiledb::sm::ConfigSource::ENVIRONMENT);
    CHECK(val2 == "env-token-2");
    // User-set overrides environment
    CHECK(c6.set("rest.token", "user-token").ok());
    auto [src3, val3] = c6.get_with_source("rest.token");
    CHECK(src3 == tiledb::sm::ConfigSource::USER_SET);
    CHECK(val3 == "user-token");
  }

  // Test non-existent parameter
  auto [source_none, value_none] = c.get_with_source("nonexistent.param");
  CHECK(source_none == tiledb::sm::ConfigSource::NONE);
  CHECK(value_none == "");
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

  SECTION("With environment variable token") {
    auto env_token = setenv_local("TILEDB_REST_TOKEN", "env-token");
    auto method = c.get_effective_rest_auth_method();
    CHECK(method == tiledb::sm::RestAuthMethod::TOKEN);
  }

  SECTION("With user-set username/password") {
    CHECK(c.set("rest.username", "user").ok());
    CHECK(c.set("rest.password", "pass").ok());
    auto method = c.get_effective_rest_auth_method();
    CHECK(method == tiledb::sm::RestAuthMethod::USERNAME_PASSWORD);
  }

  SECTION("Only username configured - throws exception") {
    CHECK(c.set("rest.username", "user").ok());
    CHECK_THROWS_WITH(
        c.get_effective_rest_auth_method(),
        Catch::Matchers::ContainsSubstring("rest.password is missing"));
  }

  SECTION("Only password configured - throws exception") {
    CHECK(c.set("rest.password", "pass").ok());
    CHECK_THROWS_WITH(
        c.get_effective_rest_auth_method(),
        Catch::Matchers::ContainsSubstring("rest.username is missing"));
  }

  SECTION("Username and password at different priority levels - throws") {
    // Set username via config (USER_SET priority)
    CHECK(c.set("rest.username", "user").ok());
    // Set password via environment variable (ENVIRONMENT priority)
    auto env_pass = setenv_local("TILEDB_REST_PASSWORD", "env-pass");
    CHECK_THROWS_WITH(
        c.get_effective_rest_auth_method(),
        Catch::Matchers::ContainsSubstring("set at different priority levels"));
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

  SECTION("Token with partial username at same level - prefer token") {
    // This scenario occurs in REST tests where TILEDB_REST_USERNAME is set
    // for logging/display purposes, but authentication uses TILEDB_REST_TOKEN
    CHECK(c.set("rest.token", "my-token").ok());
    CHECK(c.set("rest.username", "user").ok());
    // Password is not set, but token is available so it should be used
    auto method = c.get_effective_rest_auth_method();
    CHECK(method == tiledb::sm::RestAuthMethod::TOKEN);
  }

  SECTION("Token at higher priority than partial username - use token") {
    // Token at USER_SET, partial username at ENVIRONMENT
    CHECK(c.set("rest.token", "my-token").ok());
    auto env_username = setenv_local("TILEDB_REST_USERNAME", "env-user");
    // Password not set, token has higher priority so should be used
    auto method = c.get_effective_rest_auth_method();
    CHECK(method == tiledb::sm::RestAuthMethod::TOKEN);
  }

  SECTION(
      "Username and password at different levels with higher priority token - "
      "use token") {
    // Token has highest priority, so username/password at different levels
    // shouldn't cause an error
    CHECK(c.set("rest.token", "my-token").ok());
    CHECK(c.set("rest.username", "user").ok());
    auto env_pass = setenv_local("TILEDB_REST_PASSWORD", "env-pass");
    // Username at USER_SET, password at ENVIRONMENT, token at USER_SET
    // Should use token without error
    auto method = c.get_effective_rest_auth_method();
    CHECK(method == tiledb::sm::RestAuthMethod::TOKEN);
  }

  SECTION("Username from config with token from profile - use token") {
    // Even if partial username has higher priority, token should be used
    tiledb::sm::TemporaryLocalDirectory tempdir_;
    std::string profile_dir(tempdir_.path());
    std::string profile_name = "test_profile";
    auto profile = tiledb::sm::RestProfile(profile_name, profile_dir);
    profile.set_param("rest.token", "profile-token");
    profile.save_to_file();

    CHECK(c.set("profile_name", profile_name).ok());
    CHECK(c.set("profile_dir", profile_dir).ok());
    CHECK(c.set("rest.username", "user").ok());
    // Username at USER_SET, token at PROFILE - should use token
    auto method = c.get_effective_rest_auth_method();
    CHECK(method == tiledb::sm::RestAuthMethod::TOKEN);
  }
}
