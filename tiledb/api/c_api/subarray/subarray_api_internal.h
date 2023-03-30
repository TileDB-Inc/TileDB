/**
 * @file tiledb/api/c_api/subarray/subarray_api_internal.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2023 TileDB, Inc.
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
 * This file declares the internal subarray section of the C API for TileDB.
 */

#ifndef TILEDB_CAPI_SUBARRAY_API_INTERNAL_H
#define TILEDB_CAPI_SUBARRAY_API_INTERNAL_H

#include "../../c_api_support/handle/handle.h"
#include "../context/context_api_internal.h"
#include "tiledb/sm/subarray/subarray.h"

struct tiledb_subarray_handle_t
    : public tiledb::api::CAPIHandle<tiledb_subarray_handle_t> {
  /**
   * Type name
   */
  static constexpr std::string_view object_type_name{"subarray"};

 private:
  tiledb::sm::Subarray subarray_;
  tiledb::sm::Subarray* subarray_addr_;

 public:
  explicit tiledb_subarray_handle_t(
      tiledb::sm::Context* ctx, tiledb::sm::Array* array)
      : subarray_(
            array,
            (tiledb::sm::stats::Stats*)nullptr,
            ctx->resources().logger(),  // ctx->storage_manager()->logger(),
            true,
            ctx->storage_manager())
      , subarray_addr_(&subarray_) {
  }

  explicit tiledb_subarray_handle_t(const tiledb::sm::Subarray& subarray)
      : subarray_(subarray)
      , subarray_addr_(&subarray_) {
  }

  [[nodiscard]] inline tiledb::sm::Subarray& subarray() {
    return *subarray_addr_;
  }

  [[nodiscard]] inline const tiledb::sm::Subarray& subarray() const {
    return *subarray_addr_;
  }
};

namespace tiledb::api {

/**
 * Returns if the argument is a valid subarray: non-null, valid as a handle
 *
 * @param subarray A subarray of unknown validity
 */
inline void ensure_subarray_is_valid(const tiledb_subarray_handle_t* subarray) {
  ensure_handle_is_valid(subarray);
}

}  // namespace tiledb::api

#endif  // TILEDB_CAPI_SUBARRAY_API_INTERNAL_H
