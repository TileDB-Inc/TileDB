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

#include "../profile_api_internal.h"
#include "test/support/src/temporary_local_directory.h"
#include "tiledb/api/c_api_test_support/testsupport_capi_profile.h"

using namespace tiledb::api::test_support;

struct CAPINProfileFx {
  const char* name_;
  tiledb::sm::TemporaryLocalDirectory tempdir_;

  CAPINProfileFx()
      : name_(tiledb::sm::RestProfile::DEFAULT_NAME.c_str())
      , tempdir_("unit_capi_profile") {
  }
};

TEST_CASE_METHOD(
    CAPINProfileFx,
    "C API: tiledb_profile_alloc argument validation",
    "[capi][profile]") {
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
    REQUIRE_NOTHROW(tiledb_profile_free(&profile));
    CHECK(profile == nullptr);
  }
  SECTION("null profile") {
    rc = tiledb_profile_alloc(name_, homedir_, nullptr, &err);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE_METHOD(
    CAPINProfileFx,
    "C API: tiledb_profile_free argument validation",
    "[capi][profile]") {
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
    REQUIRE_NOTHROW(tiledb_profile_free(nullptr));
  }
}

TEST_CASE_METHOD(
    CAPINProfileFx,
    "C API: tiledb_profile_get_name argument validation",
    "[capi][profile]") {
  capi_return_t rc;
  tiledb_error_t* err = nullptr;
  ordinary_profile x{name_, tempdir_.path().c_str()};
  tiledb_string_t* name;
  SECTION("success") {
    rc = tiledb_profile_get_name(x.profile, &name, &err);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
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

TEST_CASE_METHOD(
    CAPINProfileFx,
    "C API: tiledb_profile_get_homedir argument validation",
    "[capi][profile]") {
  capi_return_t rc;
  tiledb_error_t* err = nullptr;
  ordinary_profile x{name_, tempdir_.path().c_str()};
  tiledb_string_t* homedir;
  SECTION("success") {
    rc = tiledb_profile_get_homedir(x.profile, &homedir, &err);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
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

TEST_CASE_METHOD(
    CAPINProfileFx,
    "C API: tiledb_profile_set_param argument validation",
    "[capi][profile][set_param]") {
  capi_return_t rc;
  tiledb_profile_t* profile;
  tiledb_error_t* err = nullptr;
  tiledb_profile_alloc(name_, tempdir_.path().c_str(), &profile, &err);
  REQUIRE(profile != nullptr);

  SECTION("success") {
    rc = tiledb_profile_set_param(profile, "rest.username", "test_user", &err);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null param") {
    rc = tiledb_profile_set_param(profile, nullptr, "test_user", &err);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null value") {
    rc = tiledb_profile_set_param(profile, "rest.username", nullptr, &err);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null param and value") {
    rc = tiledb_profile_set_param(profile, nullptr, nullptr, &err);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null profile") {
    rc = tiledb_profile_set_param(nullptr, "rest.username", "test_user", &err);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }

  tiledb_profile_free(&profile);
}

TEST_CASE_METHOD(
    CAPINProfileFx,
    "C API: tiledb_profile_get_param argument validation",
    "[capi][profile][get_param]") {
  capi_return_t rc;
  tiledb_profile_t* profile;
  tiledb_error_t* err = nullptr;
  tiledb_profile_alloc(name_, tempdir_.path().c_str(), &profile, &err);
  REQUIRE(profile != nullptr);

  tiledb_string_t* value;
  SECTION("success") {
    rc = tiledb_profile_set_param(profile, "rest.username", "test_user", &err);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
    rc = tiledb_profile_get_param(profile, "rest.username", &value, &err);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null param") {
    rc = tiledb_profile_get_param(profile, nullptr, &value, &err);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null value") {
    rc = tiledb_profile_get_param(profile, "rest.username", nullptr, &err);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }

  tiledb_profile_free(&profile);
}

TEST_CASE_METHOD(
    CAPINProfileFx,
    "C API: tiledb_profile_save argument validation",
    "[capi][profile][save]") {
  capi_return_t rc;
  tiledb_profile_t* profile;
  tiledb_error_t* err = nullptr;
  tiledb_profile_alloc(name_, tempdir_.path().c_str(), &profile, &err);
  REQUIRE(profile != nullptr);
  SECTION("success") {
    rc = tiledb_profile_save(profile, &err);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null profile") {
    rc = tiledb_profile_save(nullptr, &err);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }

  tiledb_profile_free(&profile);
}

TEST_CASE_METHOD(
    CAPINProfileFx,
    "C API: tiledb_profile_load argument validation",
    "[capi][profile][load]") {
  capi_return_t rc;
  tiledb_profile_t* profile;
  tiledb_error_t* err = nullptr;
  tiledb_profile_alloc(name_, tempdir_.path().c_str(), &profile, &err);
  REQUIRE(profile != nullptr);
  rc = tiledb_profile_save(profile, &err);
  REQUIRE(tiledb_status(rc) == TILEDB_OK);
  tiledb_profile_t* loaded_profile;
  // use the same name and homedir
  tiledb_profile_alloc(name_, tempdir_.path().c_str(), &loaded_profile, &err);
  REQUIRE(loaded_profile != nullptr);
  SECTION("success") {
    rc = tiledb_profile_load(loaded_profile, &err);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null profile") {
    rc = tiledb_profile_load(nullptr, &err);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE_METHOD(
    CAPINProfileFx,
    "C API: tiledb_profile_remove argument validation",
    "[capi][profile][remove]") {
  capi_return_t rc;
  tiledb_profile_t* profile;
  tiledb_error_t* err = nullptr;
  tiledb_profile_alloc(name_, tempdir_.path().c_str(), &profile, &err);
  REQUIRE(profile != nullptr);
  SECTION("success") {
    rc = tiledb_profile_save(profile, &err);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
    rc = tiledb_profile_remove(profile, &err);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null profile") {
    rc = tiledb_profile_remove(nullptr, &err);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  tiledb_profile_free(&profile);
}

TEST_CASE_METHOD(
    CAPINProfileFx,
    "C API: tiledb_profile_dump_str argument validation",
    "[capi][profile][dump]") {
  capi_return_t rc;
  tiledb_profile_t* profile;
  tiledb_error_t* err = nullptr;
  tiledb_profile_alloc(name_, tempdir_.path().c_str(), &profile, &err);
  REQUIRE(profile != nullptr);
  tiledb_string_t* dump_ascii;
  SECTION("success") {
    rc = tiledb_profile_dump_str(profile, &dump_ascii, &err);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
    tiledb_string_free(&dump_ascii);
  }
  SECTION("null profile") {
    rc = tiledb_profile_dump_str(nullptr, &dump_ascii, &err);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null dump_ascii") {
    rc = tiledb_profile_dump_str(profile, nullptr, &err);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }

  tiledb_profile_free(&profile);
}
