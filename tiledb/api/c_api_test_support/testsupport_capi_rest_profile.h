/**
 * @file tiledb/api/c_api_test_support/testsupport_capi_rest_profile.h
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
 * This file defines test support for the rest_profile section of the C API.
 */

#ifndef TILEDB_TESTSUPPORT_CAPI_REST_PROFILE_H
#define TILEDB_TESTSUPPORT_CAPI_REST_PROFILE_H

#include "test/support/src/temporary_local_directory.h"
#include "tiledb/api/c_api/rest/rest_profile_api_external.h"
#include "tiledb/api/c_api/rest/rest_profile_api_internal.h"

namespace tiledb::api::test_support {

class ordinary_rest_profile_exception : public StatusException {
 public:
  explicit ordinary_rest_profile_exception(const std::string& message = "")
      : StatusException("error creating test rest_profile; ", message) {
  }
};

struct ordinary_rest_profile {
  tiledb_rest_profile_handle_t* rest_profile{nullptr};
  ordinary_rest_profile(const char* name, const char* homedir) {
    int rc = tiledb_rest_profile_alloc_test(name, homedir, &rest_profile);
    if (rc != TILEDB_OK) {
      throw std::runtime_error("error creating test rest_profile");
    }
    if (rest_profile == nullptr) {
      throw std::logic_error(
          "tiledb_rest_profile_alloc_test returned OK but without "
          "rest_profile");
    }
  }
  ~ordinary_rest_profile() {
    tiledb_rest_profile_free(&rest_profile);
  }
};

}  // namespace tiledb::api::test_support

#endif  // TILEDB_TESTSUPPORT_CAPI_REST_PROFILE_H
