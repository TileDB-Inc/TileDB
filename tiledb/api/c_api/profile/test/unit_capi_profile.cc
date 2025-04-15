/**
 * @file tiledb/api/c_api/profile/test/unit_capi_profile.cc
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
 * Validates the arguments for the profile C API.
 */

#define CATCH_CONFIG_MAIN
#include <test/support/tdb_catch.h>

#include "../../../c_api_test_support/testsupport_capi_profile.h"
#include "../profile_api_experimental.h"
#include "../profile_api_internal.h"
#include "test/support/src/temporary_local_directory.h"

using namespace tiledb::api::test_support;

const char* name_ = tiledb::sm::RestProfile::DEFAULT_NAME.c_str();
tiledb::sm::TemporaryLocalDirectory tempdir_("unit_capi_profile");

TEST_CASE(
    "C API: tiledb_profile_alloc argument validation", "[capi][profile]") {
  capi_return_t rc;
  tiledb_error_t* err = nullptr;
  tiledb_profile_t* profile;
  auto homedir_ = tempdir_.path().c_str();
  SECTION("success") {
    rc = tiledb_profile_alloc(name_, homedir_, &profile, &err);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
    REQUIRE_NOTHROW(tiledb_profile_free(&profile));
    CHECK(profile == nullptr);
  }
  SECTION("empty name") {
    rc = tiledb_profile_alloc("", homedir_, &profile, &err);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null name") {
    rc = tiledb_profile_alloc(nullptr, homedir_, &profile, &err);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
    // Ensure nullptr resolves to default name internally.
    tiledb_string_t* name;
    rc = tiledb_profile_get_name(profile, &name, &err);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
    REQUIRE(strcmp(name, name_) == 0);
    REQUIRE_NOTHROW(tiledb_profile_free(&profile));
    CHECK(profile == nullptr);
  }
  SECTION("empty homedir") {
    rc = tiledb_profile_alloc(name_, "", &profile, &err);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null homedir") {
    // Normal expected use-case. Internally resolves to default homedir.
    rc = tiledb_profile_alloc(name_, nullptr, &profile, &err);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
    tiledb_string_t* homedir;
    rc = tiledb_profile_get_homedir(profile, &homedir, &err);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
    // Validate homedir is non-empty. The path may not be resolved when using
    // the default homedir (per the RestProfile::homedir() invariant).
    // #TODO REQUIRE(homedir[0] != '\0');
    REQUIRE_NOTHROW(tiledb_profile_free(&profile));
    CHECK(profile == nullptr);
  }
  SECTION("null profile") {
    rc = tiledb_profile_alloc(name_, homedir_, nullptr, &err);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE("C API: tiledb_profile_free argument validation", "[capi][profile]") {
  tiledb_profile_t* profile;
  tiledb_error_t* err = nullptr;
  auto rc =
      tiledb_profile_alloc(name_, tempdir_.path().c_str(), &profile, &err);
  REQUIRE(tiledb_status(rc) == TILEDB_OK);
  SECTION("success") {
    REQUIRE_NOTHROW(tiledb_profile_free(&profile));
    CHECK(profile == nullptr);
  }
  SECTION("null profile") {
    auto rc = tiledb_profile_free(nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE(
    "C API: tiledb_profile_get_name argument validation", "[capi][profile]") {
  capi_return_t rc;
  tiledb_error_t* err = nullptr;
  ordinary_profile x{name_, tempdir_.path().c_str()};
  tiledb_string_t* name;
  SECTION("success") {
    rc = tiledb_profile_get_name(x.profile, &name, &err);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
    REQUIRE(strcmp(name, name_) == 0);
  }
  SECTION("null profile") {
    rc = tiledb_profile_get_name(nullptr, &name, &err);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null name") {
    rc = tiledb_profile_get_name(x.profile, nullptr, &err);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE(
    "C API: tiledb_profile_get_homedir argument validation",
    "[capi][profile]") {
  capi_return_t rc;
  tiledb_error_t* err = nullptr;
  ordinary_profile x{name_, tempdir_.path().c_str()};
  tiledb_string_t* homedir;
  SECTION("success") {
    rc = tiledb_profile_get_homedir(x.profile, &homedir, &err);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
    REQUIRE(homedir[0] != '\0');  // non-empty.
  }
  SECTION("null profile") {
    rc = tiledb_profile_get_homedir(nullptr, &homedir, &err);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null homedir") {
    rc = tiledb_profile_get_homedir(x.profile, nullptr, &err);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}
