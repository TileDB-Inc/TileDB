/**
 * @file   unit-cppapi-config.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2023 TileDB, Inc.
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
 * Util Tests for C++ API.
 */

#include <thread>

#include <test/support/tdb_catch.h>
#include "test/support/src/helpers.h"
#include "test/support/src/temporary_local_directory.h"
#include "tiledb/api/c_api/config/config_api_internal.h"
#include "tiledb/sm/c_api/tiledb_serialization.h"
#include "tiledb/sm/cpp_api/tiledb"
#include "tiledb/sm/cpp_api/tiledb_experimental"

using namespace tiledb::sm;

class tiledb::sm::WhiteboxConfig {
 public:
  WhiteboxConfig(tiledb::sm::Config config)
      : config_(config){};

  const std::map<std::string, std::string>& get_all_params() const {
    return config_.param_values();
  }

  tiledb::sm::Config config_;
};

TEST_CASE("C++ API: Config", "[cppapi][config]") {
  tiledb::Config config;
  config["foo"] = "bar";
  std::string result1 = config["foo"];
  CHECK(result1 == "bar");

  auto readInvalidKey = [&config]() { std::string result2 = config["bar"]; };
  REQUIRE_THROWS_AS(readInvalidKey(), tiledb::TileDBError);

  bool contains = config.contains("foo");
  CHECK(contains == true);

  contains = config.contains("bar");
  CHECK(contains == false);
}

TEST_CASE("C++ API: Config iterator", "[cppapi][config]") {
  tiledb::Config config;
  std::vector<std::string> names;
  for (auto it = config.begin("vfs"), ite = config.end(); it != ite; ++it) {
    names.push_back(it->first);
  }
  // Check number of VFS params in default config object.
  CHECK(names.size() == 67);
}

TEST_CASE("C++ API: Config Environment Variables", "[cppapi][config]") {
  tiledb::Config config;
  auto readInvalidKey = [&config]() { std::string result1 = config["foo"]; };
  REQUIRE_THROWS_AS(readInvalidKey(), tiledb::TileDBError);

  setenv_local("TILEDB_FOO", "bar");
  std::string result1 = config["foo"];
  CHECK(result1 == "bar");

  setenv_local("TILEDB_FOO", "bar2");
  std::string result2 = config["foo"];
  CHECK(result2 == "bar2");

  config["config.env_var_prefix"] = "TILEDB_TEST_";
  auto readInvalidKey2 = [&config]() { std::string result2 = config["foo"]; };
  REQUIRE_THROWS_AS(readInvalidKey2(), tiledb::TileDBError);

  setenv_local("TILEDB_TEST_FOO", "bar3");
  std::string result3 = config["foo"];
  CHECK(result3 == "bar3");
}

TEST_CASE(
    "C++ API: Config Environment Variables Default Override",
    "[cppapi][config]") {
  tiledb::Config config;
  const std::string key = "sm.io_concurrency_level";

  unsigned int threads = std::thread::hardware_concurrency();
  const std::string result1 = config[key];
  CHECK(result1 == std::to_string(threads));

  const std::string value2 = std::to_string(threads + 1);
  setenv_local("TILEDB_SM_IO_CONCURRENCY_LEVEL", value2.c_str());
  const std::string result2 = config[key];
  CHECK(result2 == value2);

  // Check iterator
  for (auto& c : config) {
    if (c.first == key) {
      CHECK(c.second == value2);
    }
  }

  const std::string value3 = std::to_string(threads + 2);
  config[key] = value3;
  const std::string result3 = config[key];
  CHECK(result3 == value3);
}

TEST_CASE(
    "C++ API: Config Environment Variables with Profile", "[cppapi][config]") {
  tiledb::Config config;
  const std::string key = "rest.server_address";
  const std::string config_value = "test_config_localhost:8080";
  const std::string profile_value = "test_profile_localhost:8080";

  // Set the config value
  config[key] = config_value;
  // Check the config value
  CHECK(config.get(key) == config_value);

  // Create a profile
  const std::string profile_name = "test_profile";
  tiledb::sm::TemporaryLocalDirectory tempdir_;
  const std::string profile_homedir = tempdir_.path();
  auto profile = tiledb::Profile(profile_name, profile_homedir);

  // Set the profile value
  profile.set_param(key, profile_value);
  // Check the profile value
  CHECK(profile.get_param(key) == profile_value);
  // Save the profile to disk
  profile.save();
  // Set the profile in the config
  config.set_profile(profile_name, profile_homedir);
  // Check the config value after setting the profile
  CHECK(config.get(key) == config_value);
  // Unset the config value
  config.unset(key);
  // Check the config value after unsetting
  CHECK(config.get(key) == profile_value);
}

TEST_CASE("C++ API: Config Equality", "[cppapi][config]") {
  // Check for equality
  tiledb::Config config1;
  config1["foo"] = "bar";
  tiledb::Config config2;
  config2["foo"] = "bar";
  bool config_equal = config1 == config2;
  CHECK(config_equal);

  // Check for inequality
  config2["foo"] = "bar2";
  bool config_not_equal = config1 != config2;
  CHECK(config_not_equal);
}

#ifdef TILEDB_SERIALIZATION
TEST_CASE("C++ API: Config Serialization", "[cppapi][config][serialization]") {
  // this variable is parameterized below, but we initialize
  // here to avoid warning/error on MSVC
  //   C4701: potentially uninitialized local variable 'format'
  tiledb_serialization_type_t format = tiledb_serialization_type_t::TILEDB_JSON;
  SECTION("- json") {
    format = tiledb_serialization_type_t::TILEDB_JSON;
  }

  SECTION("- capnp") {
    format = tiledb_serialization_type_t::TILEDB_CAPNP;
  }
  // Check for equality
  tiledb::Config config1;
  config1["foo"] = "bar";

  tiledb::Context ctx;

  // Serialize the query (client-side).
  tiledb_buffer_t* buff1;
  int32_t rc = tiledb_serialize_config(
      ctx.ptr().get(), config1.ptr().get(), format, 1, &buff1);
  CHECK(rc == TILEDB_OK);

  tiledb_config_t* config2_ptr;
  rc = tiledb_deserialize_config(
      ctx.ptr().get(), buff1, format, 0, &config2_ptr);
  CHECK(rc == TILEDB_OK);
  tiledb::Config config2(&config2_ptr);

  auto cfg1 = config1.ptr().get()->config();
  auto cfg2 = config2.ptr().get()->config();
  // Check that the deserialized config already contains the values set in
  // environment variables
  bool config_equal = cfg1.get_all_params_from_config_or_env() ==
                      WhiteboxConfig(cfg2).get_all_params();
  CHECK(config_equal);

  // Check for inequality
  CHECK(config2.get("foo") == std::string("bar"));

  tiledb_buffer_free(&buff1);
}
#endif  // TILEDB_SERIALIZATION
