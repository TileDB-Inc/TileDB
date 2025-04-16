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

const std::string name_ = tiledb::sm::RestProfile::DEFAULT_NAME;
tiledb::sm::TemporaryLocalDirectory tempdir_("unit_cppapi_profile");

TEST_CASE(
    "C++ API: Profile get_name validation", "[cppapi][profile][get_name]") {
  const std::string homedir_ = tempdir_.path();
  SECTION("default, explicitly passed") {
    Profile p(name_, homedir_);
    REQUIRE(p.get_name() == name_);
  }
  SECTION("default, inherited from nullptr") {
    Profile p(std::nullopt, homedir_);
    REQUIRE(p.get_name() == name_);
  }
  SECTION("non-default") {
    const char* name = "non_default";
    Profile p(name, homedir_);
    REQUIRE(p.get_name() == name);
  }
}

TEST_CASE(
    "C++ API: Profile get_homedir validation",
    "[cppapi][profile][get_homedir]") {
  auto homedir_ = tempdir_.path().c_str();
  SECTION("explicitly passed") {
    Profile p(name_, homedir_);
    REQUIRE(p.get_homedir() == homedir_);
  }
  SECTION("inherited from nullptr") {
    Profile p(name_, std::nullopt);
    REQUIRE(p.get_homedir() == tiledb::common::filesystem::home_directory());
  }
  SECTION("non-default") {
    const char* homedir = "non_default";
    Profile p(name_, homedir);
    REQUIRE(p.get_homedir() == "non_default");
  }
}
