/**
 * @file tiledb/api/c_api_test_support/testsupport_capi_profile.h
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
 * This file defines test support for the profile section of the C API.
 */

#ifndef TILEDB_TESTSUPPORT_CAPI_PROFILE_H
#define TILEDB_TESTSUPPORT_CAPI_PROFILE_H

#include "test/support/src/temporary_local_directory.h"
#include "tiledb/api/c_api/profile/profile_api_experimental.h"
#include "tiledb/api/c_api/profile/profile_api_internal.h"

namespace tiledb::api::test_support {

class ordinary_profile_exception : public StatusException {
 public:
  explicit ordinary_profile_exception(const std::string& message = "")
      : StatusException("error creating test profile; ", message) {
  }
};

struct ordinary_profile {
  tiledb_profile_handle_t* profile{nullptr};

  /** Constructor. */
  ordinary_profile(const char* name, const char* dir) {
    tiledb_error_t* err = nullptr;
    int rc = tiledb_profile_alloc(name, dir, &profile, &err);
    if (rc != TILEDB_OK) {
      throw std::runtime_error("error creating test profile");
    }
    if (profile == nullptr) {
      throw std::logic_error(
          "tiledb_profile_alloc_test returned OK but without profile");
    }
  }

  /** Constructor. */
  ordinary_profile(const char* name)
      : ordinary_profile(name, nullptr) {
  }

  /** Constructor. */
  ordinary_profile()
      : ordinary_profile(nullptr, nullptr) {
  }

  /** Copy and move constructors are deleted. */
  ordinary_profile(const ordinary_profile&) = delete;
  ordinary_profile& operator=(const ordinary_profile&) = delete;
  ordinary_profile(ordinary_profile&&) = delete;
  ordinary_profile& operator=(ordinary_profile&&) = delete;

  /** Destructor. */
  ~ordinary_profile() {
    tiledb_profile_free(&profile);
  }
};

}  // namespace tiledb::api::test_support

#endif  // TILEDB_TESTSUPPORT_CAPI_PROFILE_H
