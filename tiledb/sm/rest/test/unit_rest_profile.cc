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
#include <filesystem>
#include <iostream>

#include <nlohmann/json.hpp>

using namespace tiledb::common;
using namespace tiledb::sm;

using json = nlohmann::json;

struct RestProfileFx {
 public:
  std::string dir_;       // The temporary in-test directory.
  std::string filepath_;  // The in-test path to the profiles.json file.

  RestProfileFx()
      : dir_(TemporaryLocalDirectory("unit_rest_profile").path())
      , filepath_(dir_ + "profiles.json") {
    // Fstream cannot create directories, only files, so create the dir.
    std::filesystem::create_directories(dir_);
  }

  ~RestProfileFx() = default;

  /** Returns a new RestProfile with the given name, at the in-test directory.
   */
  RestProfile create_profile(
      const std::string& name = RestProfile::DEFAULT_PROFILE_NAME) {
    return RestProfile(name, dir_);
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
      return data[std::string(name)];
    }
    return json{};
  }
};

TEST_CASE_METHOD(
    RestProfileFx, "REST Profile: Default profile", "[rest_profile][default]") {
  // Remove cloud.json to ensure the RestProfile constructor doesn't inherit it.
  std::filesystem::remove(filepath_);
  CHECK(!std::filesystem::exists(filepath_));

  // Create and validate a default RestProfile.
  RestProfile profile(create_profile());
  profile.save_to_file();

  // Check that the profile is created on disk.
  CHECK(std::filesystem::exists(filepath_));
}

TEST_CASE_METHOD(
    RestProfileFx,
    "REST Profile: Default profile, empty directory",
    "[rest_profile][default][empty_directory]") {
  // Remove the dir_ to ensure the cloud.json file isn't inherited.
  std::filesystem::remove_all(dir_);
  CHECK(!std::filesystem::exists(dir_));

  // Create and validate a default RestProfile.
  RestProfile profile(create_profile());
  profile.save_to_file();

  // Check that the directory is created and the profile is saved.
  CHECK(std::filesystem::exists(dir_));
  CHECK(std::filesystem::exists(filepath_));
}

TEST_CASE_METHOD(
    RestProfileFx,
    "REST Profile: Save/Remove",
    "[rest_profile][save][load][remove]") {
  // Ensure the profile file does not exist before the test.
  CHECK(!std::filesystem::exists(filepath_));

  // Create a default RestProfile.
  RestProfile p(create_profile());
  // Set parameters.
  p.set_param("rest.token", "custom_token");
  p.set_param("rest.server_address", "https://custom.server");
  // Save profile to file.
  p.save_to_file();

  // Load profile.
  RestProfile loaded_profile(create_profile());
  loaded_profile.load_from_file();

  // Check that the values of the profile are as expected.
  CHECK(*loaded_profile.get_param("rest.token") == "custom_token");
  CHECK(
      *loaded_profile.get_param("rest.server_address") ==
      "https://custom.server");

  // Validate that the local json object is created.
  CHECK(!profile_from_file_to_json(filepath_, std::string(p.name())).empty());

  // Remove the profile and validate that the local json object is removed.
  RestProfile::remove_profile(std::nullopt, dir_);
  CHECK(profile_from_file_to_json(filepath_, std::string(p.name())).empty());
}

TEST_CASE_METHOD(
    RestProfileFx, "REST Profile: to_json", "[rest_profile][to_json]") {
  // Create a default RestProfile.
  RestProfile p(create_profile());
  p.set_param("rest.username", "test_user");
  p.set_param("rest.password", "test_password");
  p.set_param("rest.server_address", "https://test.server");
  p.save_to_file();

  // Validate.
  json j = p.to_json();
  CHECK(j["rest.password"] == "test_password");
  CHECK(j["rest.server_address"] == "https://test.server");
  CHECK(j["rest.username"] == "test_user");
}

TEST_CASE_METHOD(
    RestProfileFx,
    "REST Profile: Get/Set invalid parameters",
    "[rest_profile][get_set_invalid]") {
  RestProfile p(create_profile());

  // Try to get a parameter with an empty name.
  const std::string* value = p.get_param("");
  REQUIRE(value == nullptr);

  // Try to set a parameter with an empty name.
  REQUIRE_THROWS_WITH(
      p.set_param("", "value"),
      Catch::Matchers::ContainsSubstring(
          "Failed to retrieve parameter; parameter name must not be empty."));

  // Try to set a parameter with an empty value.
  p.set_param("rest.username", "");
}

TEST_CASE_METHOD(
    RestProfileFx,
    "REST Profile: Multiple profiles, same name",
    "[rest_profile][load][same_profile_name]") {
  // Create and save a RestProfile with default profile name.
  RestProfile p1(create_profile());
  p1.set_param("rest.token", "token1");
  p1.save_to_file();

  // Create a second profile, again with the default name.
  RestProfile p2(create_profile());
  p2.set_param("rest.token", "token2");
  REQUIRE_THROWS_WITH(
      p2.save_to_file(),
      Catch::Matchers::Equals("RestProfile: Failed to save 'default'; This "
                              "profile has already been saved and must be "
                              "explicitly removed in order to be replaced."));
  RestProfile::remove_profile(std::nullopt, dir_);
  p2.save_to_file();

  // Ensure the first profile is now removed and the second is saved.
  RestProfile p(create_profile());
  p.load_from_file();
  CHECK(*p.get_param("rest.token") == "token2");
}

TEST_CASE_METHOD(
    RestProfileFx,
    "REST Profile: Multiple profiles, different name",
    "[rest_profile][multiple][different_name]") {
  // Create and save a RestProfile.
  RestProfile p1(create_profile("named_profile1"));
  p1.set_param("rest.token", "token1");
  p1.save_to_file();

  // Create a second RestProfile with a different name.
  RestProfile p2(create_profile("named_profile2"));
  p2.set_param("rest.token", "token2");
  p2.save_to_file();

  // Ensure the first profile is unchanged.
  RestProfile p1_check(create_profile("named_profile1"));
  p1_check.load_from_file();
  CHECK(*p1_check.get_param("rest.token") == "token1");
  // Ensure the second profile is saved correctly.
  RestProfile p2_check(create_profile("named_profile2"));
  p2_check.load_from_file();
  CHECK(*p2_check.get_param("rest.token") == "token2");
}
