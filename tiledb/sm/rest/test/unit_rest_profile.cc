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
  std::string homedir_;    // The temporary in-test $HOME directory.
  std::string cloudpath_;  // The in-test path to the cloud.json file.

  RestProfileFx()
      : homedir_(TemporaryLocalDirectory("unit_rest_profile").path())
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

  /** Returns true iff the profile's parameter values match the expected. */
  bool is_valid(RestProfile p, expected_values_t e) {
    if (p.name() == e.name && p.get("rest.password") == e.password &&
        p.get("rest.payer_namespace") == e.payer_namespace &&
        p.get("rest.token") == e.token &&
        p.get("rest.server_address") == e.server_address &&
        p.get("rest.username") == e.username)
      return true;
    return false;
  }

  /**
   * Returns the RestProfile at the given name from the local file,
   * as a json object.
   */
  json profile_from_file_to_json(std::string filepath, std::string name) {
    json data;
    if (std::filesystem::exists(filepath)) {
      std::ifstream file(filepath);
      file >> data;
      file.close();
      return data[name];
    } else {
      return data;
    }
  }
};

TEST_CASE_METHOD(
    RestProfileFx, "REST Profile: Default profile", "[rest_profile][default]") {
  // Remove cloud.json to ensure the RestProfile constructor doesn't inherit it.
  std::filesystem::remove(cloudpath_);

  // Create and validate a default RestProfile.
  RestProfile profile(RestProfile::DEFAULT_NAME, homedir_);
  profile.save();

  // Set the expected token value; expected_values_t uses cloudtoken_ by
  // default.
  expected_values_t expected;
  expected.token = RestProfile::DEFAULT_TOKEN;
  CHECK(is_valid(profile, expected));
}

TEST_CASE_METHOD(
    RestProfileFx,
    "REST Profile: Default profile inherited from cloudpath",
    "[rest_profile][default][inherited]") {
  // Create and validate a default RestProfile.
  RestProfile profile(RestProfile::DEFAULT_NAME, homedir_);
  profile.save();
  expected_values_t expected;
  CHECK(is_valid(profile, expected));
}

TEST_CASE_METHOD(
    RestProfileFx,
    "REST Profile: Save/Remove",
    "[rest_profile][save][remove]") {
  // Create a default RestProfile.
  RestProfile p(RestProfile::DEFAULT_NAME, homedir_);
  std::string filepath = homedir_ + ".tiledb/profiles.json";
  CHECK(profile_from_file_to_json(filepath, p.name()).empty());

  // Save and validate.
  p.save();
  expected_values_t e;
  CHECK(is_valid(p, e));
  CHECK(!profile_from_file_to_json(filepath, p.name()).empty());

  // Remove the profile and validate that the local json object is removed.
  p.remove();
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
  RestProfile p(e.name, homedir_);
  p.set("rest.password", e.password);
  p.set("rest.payer_namespace", e.payer_namespace);
  p.set("rest.token", e.token);
  p.set("rest.server_address", e.server_address);
  p.set("rest.username", e.username);
  p.save();
  CHECK(is_valid(p, e));
}

TEST_CASE_METHOD(
    RestProfileFx, "REST Profile: to_json", "[rest_profile][to_json]") {
  // Create a default RestProfile.
  RestProfile p(RestProfile::DEFAULT_NAME, homedir_);
  p.save();

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
  RestProfile p(RestProfile::DEFAULT_NAME, homedir_);

  // Try to get a parameter with an invalid name.
  REQUIRE_THROWS_WITH(
      p.get("username"),
      Catch::Matchers::ContainsSubstring("Failed to retrieve parameter"));

  // Try to set a parameter with an invalid name.
  REQUIRE_THROWS_WITH(
      p.set("username", "failed_username"),
      Catch::Matchers::ContainsSubstring(
          "Failed to set parameter of invalid name"));

  // Set username and try to save without setting password.
  p.set("rest.username", "username");
  REQUIRE_THROWS_WITH(
      p.save(),
      Catch::Matchers::ContainsSubstring("invalid username/password pairing"));
  // Set password and save valid profile
  p.set("rest.password", "password");
  p.save();

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
  RestProfile p(RestProfile::DEFAULT_NAME, homedir_);
  p.set("rest.payer_namespace", payer_namespace);
  p.save();
  expected_values_t e;
  e.payer_namespace = payer_namespace;
  CHECK(is_valid(p, e));

  // Create a second profile, ensuring the payer_namespace is inherited.
  RestProfile p2(RestProfile::DEFAULT_NAME, homedir_);
  CHECK(p2.get("rest.payer_namespace") == payer_namespace);

  // Set a non-default token on the second profile.
  p2.set("rest.token", token);
  p2.save();
  e.token = token;
  CHECK(is_valid(p2, e));

  // Ensure the first profile is now out of date.
  CHECK(p.get("rest.token") == cloudtoken_);
}

TEST_CASE_METHOD(
    RestProfileFx,
    "REST Profile: Multiple profiles, different name",
    "[rest_profile][multiple][different_name]") {
  std::string name = "non-default";
  std::string payer_namespace = "payer_namespace";

  // Create and validate a RestProfile with default name.
  RestProfile p(RestProfile::DEFAULT_NAME, homedir_);
  p.set("rest.payer_namespace", payer_namespace);
  p.save();
  expected_values_t e;
  e.payer_namespace = payer_namespace;
  CHECK(is_valid(p, e));

  // Create a second profile with non-default name and ensure the
  // payer_namespace and cloudtoken_ are NOT inherited.
  RestProfile p2(name, homedir_);
  CHECK(p2.get("rest.payer_namespace") != payer_namespace);
  p2.save();
  e.name = name;
  e.payer_namespace = RestProfile::DEFAULT_PAYER_NAMESPACE;
  e.token = RestProfile::DEFAULT_TOKEN;
  CHECK(is_valid(p2, e));

  // Ensure the default profile is unchanged.
  CHECK(p.name() == RestProfile::DEFAULT_NAME);
}
