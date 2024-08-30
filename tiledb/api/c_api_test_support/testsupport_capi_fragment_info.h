/**
 * @file tiledb/api/c_api_test_support/testsupport_capi_fragment_info.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2024 TileDB, Inc.
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
 * This file defines test support for the fragment info section of the C API.
 */

#ifndef TILEDB_TESTSUPPORT_CAPI_FRAGMENT_INFO_H
#define TILEDB_TESTSUPPORT_CAPI_FRAGMENT_INFO_H

#include "testsupport_capi_context.h"
#include "tiledb/api/c_api/fragment_info/fragment_info_api_external.h"
#include "tiledb/api/c_api/fragment_info/fragment_info_api_internal.h"

namespace tiledb::api::test_support {

/**
 * Ordinary fragment info base class
 */
class ordinary_fragment_info {
 public:
  ordinary_context context{};
  tiledb_fragment_info_handle_t* fragment_info{nullptr};

  ordinary_fragment_info(const char* uri = "unit_capi_fragment_info") {
    auto rc = tiledb_fragment_info_alloc(context.context, uri, &fragment_info);
    if (rc != TILEDB_OK) {
      throw std::runtime_error("error creating test fragment_info");
    }
    if (fragment_info == nullptr) {
      throw std::logic_error(
          "tiledb_fragment_info_alloc returned OK but without fragment_info");
    }
  }

  ~ordinary_fragment_info() {
    tiledb_fragment_info_free(&fragment_info);
  }

  [[nodiscard]] tiledb_ctx_handle_t* ctx() const {
    return context.context;
  }
};

}  // namespace tiledb::api::test_support

#endif  // TILEDB_TESTSUPPORT_CAPI_FRAGMENT_INFO_H
