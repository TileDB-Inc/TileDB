/**
 * @file unit_rest_profile.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2025 TileDB, Inc.
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
 * Tests the `RestProfile` class.
 */

#include "test/support/src/temporary_local_directory.h"
#include "tiledb/sm/rest/rest_profile.h"

#include <test/support/tdb_catch.h>
#include <iostream>

using namespace tiledb::common;
using namespace tiledb::sm;

std::string cloudtoken_ = "abc123def456";  // Token used by in-test cloud.json.

/* Tracks the expected name and parameter values for a RestProfile. */
struct expected_values_t {
  std::string name = RestProfile::DEFAULT_NAME;
  std::string password = RestProfile::DEFAULT_PASSWORD;
  std::string payer_namespace = RestProfile::DEFAULT_PAYER_NAMESPACE;
  std::string token = cloudtoken_;
  std::string server_address = RestProfile::DEFAULT_SERVER_ADDRESS;
  std::string username = RestProfile::DEFAULT_USERNAME;
};

struct RestProfileFx {
 public:
  TemporaryLocalDirectory tempdir_;  // A temporary working directory.
  std::string homedir_;              // The temporary in-test $HOME directory.
  std::string cloudpath_;            // The in-test path to the cloud.json file.

  RestProfileFx()
      : tempdir_(TemporaryLocalDirectory("unit_rest_profile"))
      , homedir_(tempdir_.path())
      , cloudpath_(homedir_ + ".tiledb/cloud.json") {
    // Fstream cannot create directories, only files, so create the .tiledb dir.
    std::filesystem::create_directories(homedir_ + ".tiledb");

    // Write to the cloud path.
    json j = {
        {"api_key",
         {json::object_t::value_type("X-TILEDB-REST-API-KEY", cloudtoken_)}},
        {"host", RestProfile::DEFAULT_SERVER_ADDRESS},
        {"username", RestProfile::DEFAULT_USERNAME},
        {"password", RestProfile::DEFAULT_PASSWORD},
        {"verify_ssl", "true"}};

    {
      std::ofstream file(cloudpath_);
      file << std::setw(2) << j << std::endl;
    }
  }

  ~RestProfileFx() = default;

  /** Returns a new RestProfile with the given name, at the in-test $HOME. */
  RestProfile create_profile(
      const std::string& name = RestProfile::DEFAULT_NAME) {
    return RestProfile(name, homedir_);
  }

  /** Returns true iff the profile's parameter values match the expected. */
  bool is_valid(RestProfile p, expected_values_t e) {
    if (p.name() == e.name && p.get_param("rest.password") == e.password &&
        p.get_param("rest.payer_namespace") == e.payer_namespace &&
        p.get_param("rest.token") == e.token &&
        p.get_param("rest.server_address") == e.server_address &&
        p.get_param("rest.username") == e.username)
      return true;
    return false;
  }

  /**
   * Returns the RestProfile at the given name from the local file,
   * as a json object.
   */
  json profile_from_file_to_json(
      std::string_view filepath, std::string_view name) {
    json data;
    if (std::filesystem::exists(filepath)) {
      std::ifstream file(filepath);
      file >> data;
      file.close();
      return data[std::string(name)];
    } else {
      return data;
    }
  }
};

TEST_CASE_METHOD(
    RestProfileFx, "REST Profile: Default profile", "[rest_profile][default]") {
  // Remove cloud.json to ensure the RestProfile constructor doesn't inherit it.
  std::filesystem::remove(cloudpath_);
  CHECK(!std::filesystem::exists(cloudpath_));

  // Create and validate a default RestProfile.
  RestProfile profile(create_profile());
  profile.save_to_file();

  // Set the expected token value; expected_values_t uses cloudtoken_ by
  // default.
  expected_values_t expected;
  expected.token = RestProfile::DEFAULT_TOKEN;
  CHECK(is_valid(profile, expected));
}

TEST_CASE_METHOD(
    RestProfileFx,
    "REST Profile: Default profile, empty directory",
    "[rest_profile][default][empty_directory]") {
  // Remove the .tiledb directory to ensure the cloud.json file isn't inherited.
  std::filesystem::remove_all(homedir_ + ".tiledb");

  // Create and validate a default RestProfile.
  RestProfile profile(create_profile());
  profile.save_to_file();
  expected_values_t expected;
  expected.token = RestProfile::DEFAULT_TOKEN;
  CHECK(is_valid(profile, expected));
}

TEST_CASE_METHOD(
    RestProfileFx,
    "REST Profile: Default profile inherited from cloudpath",
    "[rest_profile][default][inherited]") {
  // Create and validate a default RestProfile.
  RestProfile profile(create_profile());
  profile.save_to_file();
  expected_values_t expected;
  CHECK(is_valid(profile, expected));
}

TEST_CASE_METHOD(
    RestProfileFx,
    "REST Profile: Save/Remove",
    "[rest_profile][save][remove]") {
  // Create a default RestProfile.
  RestProfile p(create_profile());
  std::string filepath = homedir_ + ".tiledb/profiles.json";
  CHECK(profile_from_file_to_json(filepath, p.name()).empty());

  // Save and validate.
  p.save_to_file();
  expected_values_t e;
  CHECK(is_valid(p, e));
  CHECK(!profile_from_file_to_json(filepath, p.name()).empty());

  // Remove the profile and validate that the local json object is removed.
  p.remove_from_file();
  CHECK(profile_from_file_to_json(filepath, p.name()).empty());
}

TEST_CASE_METHOD(
    RestProfileFx,
    "REST Profile: Non-default profile",
    "[rest_profile][non-default]") {
  expected_values_t e{
      "non-default",
      "password",
      "payer_namespace",
      "token",
      "server_address",
      "username"};

  // Set and validate non-default parameters.
  RestProfile p(create_profile(e.name));
  p.set_param("rest.password", e.password);
  p.set_param("rest.payer_namespace", e.payer_namespace);
  p.set_param("rest.token", e.token);
  p.set_param("rest.server_address", e.server_address);
  p.set_param("rest.username", e.username);
  p.save_to_file();
  CHECK(is_valid(p, e));
}

TEST_CASE_METHOD(
    RestProfileFx, "REST Profile: to_json", "[rest_profile][to_json]") {
  // Create a default RestProfile.
  RestProfile p(create_profile());
  p.save_to_file();

  // Validate.
  expected_values_t e;
  json j = p.to_json();
  CHECK(j["rest.password"] == e.password);
  CHECK(j["rest.payer_namespace"] == e.payer_namespace);
  CHECK(j["rest.token"] == e.token);
  CHECK(j["rest.server_address"] == e.server_address);
  CHECK(j["rest.username"] == e.username);
}

TEST_CASE_METHOD(
    RestProfileFx,
    "REST Profile: Get/Set invalid parameters",
    "[rest_profile][get_set_invalid]") {
  RestProfile p(create_profile());

  // Try to get a parameter with an invalid name.
  REQUIRE_THROWS_WITH(
      p.get_param("username"),
      Catch::Matchers::ContainsSubstring("Failed to retrieve parameter"));

  // Try to set a parameter with an invalid name.
  REQUIRE_THROWS_WITH(
      p.set_param("username", "failed_username"),
      Catch::Matchers::ContainsSubstring(
          "Failed to set parameter of invalid name"));

  // Set username and try to save without setting password.
  p.set_param("rest.username", "username");
  REQUIRE_THROWS_WITH(
      p.save_to_file(),
      Catch::Matchers::ContainsSubstring("invalid username/password pairing"));
  // Set password and save valid profile
  p.set_param("rest.password", "password");
  p.save_to_file();

  // Validate.
  expected_values_t e;
  e.username = "username";
  e.password = "password";
  CHECK(is_valid(p, e));
}

TEST_CASE_METHOD(
    RestProfileFx,
    "REST Profile: Multiple profiles, same name",
    "[rest_profile][multiple][same_name]") {
  std::string payer_namespace = "payer_namespace";
  std::string token = "token";

  // Create and validate a RestProfile with default name.
  RestProfile p(create_profile());
  p.set_param("rest.payer_namespace", payer_namespace);
  p.save_to_file();
  expected_values_t e;
  e.payer_namespace = payer_namespace;
  CHECK(is_valid(p, e));

  // Create a second profile, ensuring the payer_namespace is inherited.
  RestProfile p2(create_profile());
  CHECK(p2.get_param("rest.payer_namespace") == payer_namespace);

  // Set a non-default token on the second profile.
  p2.set_param("rest.token", token);
  REQUIRE_THROWS_WITH(
      p2.save_to_file(),
      Catch::Matchers::ContainsSubstring("profile already exists"));
  p.remove_from_file();
  p2.save_to_file();
  e.token = token;
  CHECK(is_valid(p2, e));

  // Ensure the first profile is now out of date.
  CHECK(p.get_param("rest.token") == cloudtoken_);
}

TEST_CASE_METHOD(
    RestProfileFx,
    "REST Profile: Multiple profiles, different name",
    "[rest_profile][multiple][different_name]") {
  std::string name = "non-default";
  std::string payer_namespace = "payer_namespace";

  // Create and validate a RestProfile with default name.
  RestProfile p(create_profile());
  p.set_param("rest.payer_namespace", payer_namespace);
  p.save_to_file();
  expected_values_t e;
  e.payer_namespace = payer_namespace;
  CHECK(is_valid(p, e));

  // Create a second profile with non-default name and ensure the
  // payer_namespace and cloudtoken_ are NOT inherited.
  RestProfile p2(create_profile(name));
  CHECK(p2.get_param("rest.payer_namespace") != payer_namespace);
  p2.save_to_file();
  e.name = name;
  e.payer_namespace = RestProfile::DEFAULT_PAYER_NAMESPACE;
  e.token = RestProfile::DEFAULT_TOKEN;
  CHECK(is_valid(p2, e));

  // Ensure the default profile is unchanged.
  CHECK(p.name() == RestProfile::DEFAULT_NAME);
}
