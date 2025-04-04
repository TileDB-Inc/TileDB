/**
 * @file tiledb/api/c_api/rest/test/unit_capi_rest_profile.cc
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
 * Validates the arguments for the RestProfile C API.
 */

#define CATCH_CONFIG_MAIN
#include <test/support/tdb_catch.h>

#include "test/support/src/temporary_local_directory.h"

#include "../rest_profile_api_external.h"
#include "../rest_profile_api_internal.h"

const char* name_ = tiledb::sm::RestProfile::DEFAULT_NAME.c_str();
tiledb::sm::TemporaryLocalDirectory tempdir_("unit_capi_rest_profile");

/**
 * No TEST_CASE "C API: tiledb_rest_profile_alloc argument validation" here;
 * The user-facing API is not to be used in-test, to avoid async changes to
 * the local, on-disk file. Instead, `tiledb_rest_profile_alloc_test` is used
 * throughout this test, including in ordinary_rest_profile.
 */

TEST_CASE(
    "C API: tiledb_rest_profile_alloc_test argument validation",
    "[capi][rest_profile]") {
  capi_return_t rc;
  tiledb_rest_profile_t* profile;
  auto homedir = tempdir_.path().c_str();
  SECTION("success") {
    rc = tiledb_rest_profile_alloc_test(name_, homedir, &profile);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
    REQUIRE_NOTHROW(tiledb_rest_profile_free(&profile));
    CHECK(profile == nullptr);
  }
  SECTION("empty name") {
    rc = tiledb_rest_profile_alloc_test("", homedir, &profile);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null name") {
    rc = tiledb_rest_profile_alloc_test(nullptr, homedir, &profile);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("empty homedir") {
    rc = tiledb_rest_profile_alloc_test(name_, "", &profile);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null homedir") {
    rc = tiledb_rest_profile_alloc_test(name_, nullptr, &profile);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null profile") {
    rc = tiledb_rest_profile_alloc_test(name_, homedir, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE(
    "C API: tiledb_rest_profile_free argument validation",
    "[capi][rest_profile]") {
  tiledb_rest_profile_t* profile;
  auto rc =
      tiledb_rest_profile_alloc_test(name_, tempdir_.path().c_str(), &profile);
  REQUIRE(tiledb_status(rc) == TILEDB_OK);
  SECTION("success") {
    REQUIRE_NOTHROW(tiledb_rest_profile_free(&profile));
    CHECK(profile == nullptr);
  }
  SECTION("null profile") {
    /*
     * `tiledb_rest_profile_free` is a void function, otherwise we would check
     * for an error.
     */
    REQUIRE_NOTHROW(tiledb_rest_profile_free(nullptr));
  }
}
