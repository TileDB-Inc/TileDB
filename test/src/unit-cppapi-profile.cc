/**
 * @file   unit-cppapi-profile.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2025 TileDB Inc.
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
 * Tests the C++ API for profile-related functionality.
 */

#include <test/support/src/helpers.h>
#include <test/support/tdb_catch.h>

#include "test/support/src/temporary_local_directory.h"
#include "tiledb/sm/cpp_api/profile_experimental.h"
#include "tiledb/sm/rest/rest_profile.h"

using namespace tiledb;
using namespace tiledb::test;

struct ProfileCPPFx {
  const std::string name_;
  tiledb::sm::TemporaryLocalDirectory tempdir_;

  ProfileCPPFx()
      : name_(tiledb::sm::RestProfile::DEFAULT_PROFILE_NAME)
      , tempdir_("unit_cppapi_profile") {
  }

  bool profile_exists(std::string filepath, std::string name) {
    json data;
    if (std::filesystem::exists(filepath)) {
      std::ifstream file(filepath);
      file >> data;
      file.close();
      return data.find(name) != data.end();
    } else {
      return false;
    }
  }
};

struct expected_values_t {
  std::string profile_name = sm::RestProfile::DEFAULT_PROFILE_NAME;
  std::string password = sm::RestProfile::DEFAULT_PASSWORD;
  std::string payer_namespace = sm::RestProfile::DEFAULT_PAYER_NAMESPACE;
  std::string token = sm::RestProfile::DEFAULT_TOKEN;
  std::string server_address = sm::RestProfile::DEFAULT_SERVER_ADDRESS;
  std::string username = sm::RestProfile::DEFAULT_USERNAME;
};

bool is_expected(const Profile& p, const expected_values_t& e) {
  return p.name() == e.profile_name &&
         p.get_param("rest.username") == e.username &&
         p.get_param("rest.password") == e.password &&
         p.get_param("rest.payer_namespace") == e.payer_namespace &&
         p.get_param("rest.server_address") == e.server_address &&
         p.get_param("rest.token") == e.token;
}

TEST_CASE_METHOD(
    ProfileCPPFx,
    "C++ API: Profile get_name validation",
    "[cppapi][profile][get_name]") {
  SECTION("default, explicitly passed") {
    Profile p(name_, tempdir_.path());
    REQUIRE(p.name() == name_);
  }
  SECTION("default, inherited from nullptr") {
    Profile p(std::nullopt, tempdir_.path());
    REQUIRE(p.name() == name_);
  }
  SECTION("non-default") {
    const char* name = "non_default";
    Profile p(name, tempdir_.path());
    REQUIRE(p.name() == name);
  }
}

TEST_CASE_METHOD(
    ProfileCPPFx,
    "C++ API: Profile get_dir validation",
    "[cppapi][profile][get_dir]") {
  SECTION("explicitly passed") {
    Profile p(name_, tempdir_.path());
    REQUIRE(p.dir() == tempdir_.path());
  }
  SECTION("inherited from nullptr") {
    Profile p(name_, std::nullopt);
    REQUIRE(
        p.dir() == tiledb::common::filesystem::home_directory() +
                       tiledb::sm::constants::rest_profile_foldername + "/");
  }
}

TEST_CASE_METHOD(
    ProfileCPPFx,
    "C++ API: Profile set_param validation",
    "[cppapi][profile][set_param]") {
  SECTION("valid") {
    Profile p(name_, tempdir_.path());
    p.set_param("rest.username", "test_user");
    p.set_param("rest.password", "test_password");
  }
  SECTION("valid empty value") {
    Profile p(name_, tempdir_.path());
    p.set_param("rest.username", "");
  }
  SECTION("invalid empty key") {
    Profile p(name_, tempdir_.path());
    REQUIRE_THROWS(p.set_param("", "test_user"));
  }
}

TEST_CASE_METHOD(
    ProfileCPPFx,
    "C++ API: Profile get_param validation",
    "[cppapi][profile][get_param]") {
  SECTION("valid") {
    Profile p(name_, tempdir_.path());
    p.set_param("rest.username", "test_user");
    REQUIRE(p.get_param("rest.username") == "test_user");
  }
  SECTION("invalid empty key") {
    Profile p(name_, tempdir_.path());
    REQUIRE_THROWS(p.get_param(""));
  }
}

TEST_CASE_METHOD(
    ProfileCPPFx,
    "C++ API: Profile save validation",
    "[cppapi][profile][save]") {
  SECTION("rest.username and rest.password not set") {
    Profile p(name_, tempdir_.path());
    // check that the profiles file was not there before
    REQUIRE(!std::filesystem::exists(
        tempdir_.path() + tiledb::sm::constants::rest_profile_filename));
    // save the profile
    p.save();
    // check that the profiles file is created
    REQUIRE(std::filesystem::exists(
        tempdir_.path() + tiledb::sm::constants::rest_profile_filename));
    // check that the profile is saved
    REQUIRE(profile_exists(
        tempdir_.path() + tiledb::sm::constants::rest_profile_filename, name_));
  }
  SECTION("rest.username and rest.password set") {
    Profile p(name_, tempdir_.path());
    p.set_param("rest.username", "test_user");
    p.set_param("rest.password", "test_password");
    // check that the profiles file was not there before
    REQUIRE(!std::filesystem::exists(
        tempdir_.path() + tiledb::sm::constants::rest_profile_filename));
    // save the profile
    p.save();
    // check that the profiles file is created
    REQUIRE(std::filesystem::exists(
        tempdir_.path() + tiledb::sm::constants::rest_profile_filename));
    // check that the profile is saved
    REQUIRE(profile_exists(
        tempdir_.path() + tiledb::sm::constants::rest_profile_filename, name_));
  }
  SECTION("rest.username set and rest.password not set") {
    Profile p(name_, tempdir_.path());
    p.set_param("rest.username", "test_user");
    REQUIRE_THROWS(p.save());
  }
  SECTION("rest.username not set and rest.password set") {
    Profile p(name_, tempdir_.path());
    p.set_param("rest.password", "test_password");
    REQUIRE_THROWS(p.save());
  }
}

TEST_CASE_METHOD(
    ProfileCPPFx,
    "C++ API: Profile load validation",
    "[cppapi][profile][load]") {
  SECTION("success") {
    Profile p(name_, tempdir_.path());
    expected_values_t expected;
    // check that the profiles file was not there before
    REQUIRE(!std::filesystem::exists(
        tempdir_.path() + tiledb::sm::constants::rest_profile_filename));
    // save some parameters
    p.set_param("rest.username", "test_user");
    p.set_param("rest.password", "test_password");
    // save the profile
    p.save();
    // check that the profiles file is created
    REQUIRE(std::filesystem::exists(
        tempdir_.path() + tiledb::sm::constants::rest_profile_filename));

    // load the profile again
    Profile p2 = Profile::load(name_, tempdir_.path());
    // check that the parameters are loaded correctly
    expected.username = "test_user";
    expected.password = "test_password";
    is_expected(p2, expected);
  }
  SECTION("profiles file is present") {
    // check that the profiles file is not there
    REQUIRE(!std::filesystem::exists(
        tempdir_.path() + tiledb::sm::constants::rest_profile_filename));
    // attempt to load the profile
    REQUIRE_THROWS(Profile::load(name_, tempdir_.path()));
  }
  SECTION("another profile is saved - profiles file is present") {
    Profile p1(name_, tempdir_.path());
    Profile p2("another_profile", tempdir_.path());
    // check that the profiles file was not there before
    REQUIRE(!std::filesystem::exists(
        tempdir_.path() + tiledb::sm::constants::rest_profile_filename));
    // save the other profile
    p1.save();
    // check that the profiles file is created
    REQUIRE(std::filesystem::exists(
        tempdir_.path() + tiledb::sm::constants::rest_profile_filename));
    // check that the other profile is saved
    REQUIRE(profile_exists(
        tempdir_.path() + tiledb::sm::constants::rest_profile_filename,
        p1.name()));
  }
}

TEST_CASE_METHOD(
    ProfileCPPFx,
    "C++ API: Profile remove validation",
    "[cppapi][profile][remove]") {
  SECTION("success") {
    Profile p(name_, tempdir_.path());
    // check that the profiles file was not there before
    REQUIRE(!std::filesystem::exists(
        tempdir_.path() + tiledb::sm::constants::rest_profile_filename));
    // save the profile
    p.save();
    // check that the profiles file is created
    REQUIRE(std::filesystem::exists(
        tempdir_.path() + tiledb::sm::constants::rest_profile_filename));
    // remove the profile
    Profile::remove(name_, tempdir_.path());
    // check that the profiles file is still there
    REQUIRE(std::filesystem::exists(
        tempdir_.path() + tiledb::sm::constants::rest_profile_filename));
    // check that the profile is removed
    REQUIRE(!profile_exists(
        tempdir_.path() + tiledb::sm::constants::rest_profile_filename, name_));
  }
  SECTION("profiles file is present") {
    Profile p(name_, tempdir_.path());
    // check that the profiles file was not there before
    REQUIRE(!std::filesystem::exists(
        tempdir_.path() + tiledb::sm::constants::rest_profile_filename));
    // attempt to remove the profile
    REQUIRE_THROWS(p.remove());
  }
  SECTION("another profile is saved - profiles file is present") {
    Profile p1(name_, tempdir_.path());
    Profile p2("another_profile", tempdir_.path());
    // check that the profiles file was not there before
    REQUIRE(!std::filesystem::exists(
        tempdir_.path() + tiledb::sm::constants::rest_profile_filename));
    // save the other profile
    p2.save();
    // check that the profiles file is created
    REQUIRE(std::filesystem::exists(
        tempdir_.path() + tiledb::sm::constants::rest_profile_filename));
    // check that the other profile is saved
    REQUIRE(profile_exists(
        tempdir_.path() + tiledb::sm::constants::rest_profile_filename,
        p2.name()));
    // attempt remove the tested profile
    REQUIRE_THROWS(p1.remove());
    // check that the other profile still exists
    REQUIRE(profile_exists(
        tempdir_.path() + tiledb::sm::constants::rest_profile_filename,
        p2.name()));
    // check that the tested profile still does not exist
    REQUIRE(!profile_exists(
        tempdir_.path() + tiledb::sm::constants::rest_profile_filename,
        p1.name()));
  }
}

TEST_CASE_METHOD(
    ProfileCPPFx,
    "C++ API: Profile dump validation",
    "[cppapi][profile][dump]") {
  SECTION("success") {
    Profile p(name_, tempdir_.path());
    p.set_param("rest.username", "test_user");
    p.set_param("rest.password", "test_password");
    std::string dump_str = p.dump();

    // check that the dump string contains the expected values
    REQUIRE(dump_str.find("rest.username") != std::string::npos);
    REQUIRE(dump_str.find("test_user") != std::string::npos);
    REQUIRE(dump_str.find("rest.password") != std::string::npos);
    REQUIRE(dump_str.find("test_password") != std::string::npos);
    REQUIRE(dump_str.find("rest.payer_namespace") != std::string::npos);
    REQUIRE(dump_str.find("rest.server_address") != std::string::npos);
    REQUIRE(dump_str.find("https://api.tiledb.com") != std::string::npos);
    REQUIRE(dump_str.find("rest.token") != std::string::npos);
  }
}

TEST_CASE_METHOD(
    ProfileCPPFx,
    "C++ API: Profile default constructor validation",
    "[cppapi][profile][default_constructor]") {
  SECTION("Default constructor") {
    expected_values_t expected;

    Profile p1;
    REQUIRE(p1.name() == expected.profile_name);
    REQUIRE(
        p1.dir() == tiledb::common::filesystem::home_directory() +
                        tiledb::sm::constants::rest_profile_foldername + "/");

    Profile p2(std::nullopt, std::nullopt);
    REQUIRE(p2.name() == expected.profile_name);
    REQUIRE(
        p2.dir() == tiledb::common::filesystem::home_directory() +
                        tiledb::sm::constants::rest_profile_foldername + "/");
  }
}
