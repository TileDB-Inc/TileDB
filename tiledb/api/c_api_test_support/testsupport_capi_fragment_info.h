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
#include "tiledb/api/c_api/array_schema/array_schema_api_internal.h"
#include "tiledb/api/c_api/attribute/attribute_api_internal.h"
#include "tiledb/api/c_api/dimension/dimension_api_internal.h"
#include "tiledb/api/c_api/domain/domain_api_internal.h"
#include "tiledb/api/c_api/fragment_info/fragment_info_api_external.h"
#include "tiledb/api/c_api/fragment_info/fragment_info_api_internal.h"

#include "tiledb/sm/c_api/tiledb.h"
#include "tiledb/sm/c_api/tiledb_struct_def.h"

namespace tiledb::api::test_support {

class ordinary_fragment_info_exception : public StatusException {
 public:
  explicit ordinary_fragment_info_exception(const std::string& message = "")
      : StatusException("error creating test fragment info", message) {
  }
};

/**
 * Base class for an ordinary fragment info object.
 *
 * Note that this base class is considered "empty", as it does not create a
 * schema object to write fragments to.
 * As such, the fragment info object should not loaded.
 */
class ordinary_fragment_info_without_fragments {
 public:
  ordinary_context context{};
  tiledb_fragment_info_handle_t* fragment_info{nullptr};

  ordinary_fragment_info_without_fragments(
      const char* uri = "unit_capi_fragment_info") {
    auto rc = tiledb_fragment_info_alloc(context.context, uri, &fragment_info);
    if (rc != TILEDB_OK) {
      throw ordinary_fragment_info_exception();
    }
    if (fragment_info == nullptr) {
      throw ordinary_fragment_info_exception(
          "tiledb_fragment_info_alloc returned OK but without fragment_info");
    }
  }

  ~ordinary_fragment_info_without_fragments() {
    tiledb_fragment_info_free(&fragment_info);
  }

  [[nodiscard]] tiledb_ctx_handle_t* ctx() const {
    return context.context;
  }
};

/**
 * An ordinary fragment info object with valid fragments which have been loaded.
 *
 * @section Maturity Notes
 *
 * Use of `storage_manager_stub` in the CAPI handle object libraries prevents
 * complete linking of all objects needed to properly allocate a `fragment_info`
 * handle object in an RAII-compliant fashion. As such, this object shall
 * rely on already-written fragments from an array in the
 * `TILEDB_TEST_INPUTS_DIR` until the stub library is eliminated.
 */
struct ordinary_fragment_info
    : public ordinary_fragment_info_without_fragments {
 private:
  std::string array_uri;

 public:
  ordinary_fragment_info(bool is_var = false)
      : array_uri{
            is_var ? std::string(TILEDB_TEST_INPUTS_DIR) +
                         "/arrays/zero_var_chunks_v10" :
                     std::string(TILEDB_TEST_INPUTS_DIR) +
                         "/arrays/non_split_coords_v1_4_0"} {
    // Create fragment info object
    int rc = tiledb_fragment_info_alloc(ctx(), uri(), &fragment_info);
    if (rc != TILEDB_OK) {
      throw ordinary_fragment_info_exception();
    }
    if (fragment_info == nullptr) {
      throw ordinary_fragment_info_exception(
          "tiledb_fragment_info_alloc returned OK but without fragment_info");
    }

    rc = tiledb_fragment_info_load(ctx(), fragment_info);
    if (rc != TILEDB_OK) {
      throw ordinary_fragment_info_exception(
          "tiledb_fragment_info_load failed");
    }
  }

  [[nodiscard]] const char* uri() {
    return array_uri.c_str();
  }
};

}  // namespace tiledb::api::test_support

#endif  // TILEDB_TESTSUPPORT_CAPI_FRAGMENT_INFO_H
