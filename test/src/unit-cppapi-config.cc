/**
 * @file   unit-cppapi_config.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2021 TileDB, Inc.
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

#include "catch.hpp"
#include "tiledb/sm/cpp_api/tiledb"

int setenv_local(const char* __name, const char* __value) {
#ifdef _WIN32
  return _putenv_s(__name, __value);
#else
  return ::setenv(__name, __value, 1);
#endif
}

TEST_CASE("C++ API: Config", "[cppapi][config]") {
  tiledb::Config config;
  config["foo"] = "bar";
  std::string result1 = config["foo"];
  CHECK(result1 == "bar");

  auto readInvalidKey = [&config]() { std::string result2 = config["bar"]; };
  REQUIRE_THROWS_AS(readInvalidKey(), tiledb::TileDBError);
}

TEST_CASE("C++ API: Config iterator", "[cppapi], [cppapi-config]") {
  tiledb::Config config;
  std::vector<std::string> names;
  for (auto it = config.begin("vfs"), ite = config.end(); it != ite; ++it) {
    names.push_back(it->first);
  }
  // Check number of VFS params in default config object.
  CHECK(names.size() == 54);
}

TEST_CASE(
    "C++ API: Config Environment Variables", "[cppapi], [cppapi-config]") {
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
    "[cppapi], [cppapi-config]") {
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

TEST_CASE("C++ API: Config Equality", "[cppapi], [cppapi-config]") {
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
