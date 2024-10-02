/**
 * @file tiledb/api/c_api/subarray/subarray_api_internal.h
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
 * This file declares the internals of the subarray section of the C API.
 */

#ifndef TILEDB_CAPI_SUBARRAY_INTERNAL_H
#define TILEDB_CAPI_SUBARRAY_INTERNAL_H

#include "subarray_api_external.h"

#include "tiledb/api/c_api/context/context_api_internal.h"
#include "tiledb/api/c_api_support/handle/handle.h"
#include "tiledb/common/common.h"
#include "tiledb/sm/subarray/subarray.h"
#include "tiledb/sm/subarray/subarray_partitioner.h"

struct tiledb_subarray_t {
  tiledb::sm::Subarray* subarray_ = nullptr;
  bool is_allocated_ = false;
};

namespace tiledb::api {

/**
 * Returns after successfully validating a subarray object.
 *
 * @param subarray Possibly-valid pointer to a subarray object.
 */
inline void ensure_subarray_is_valid(const tiledb_subarray_t* subarray) {
  // ensure_handle_is_valid(subarray);
  if (subarray == nullptr || subarray->subarray_ == nullptr ||
      subarray->subarray_->array() == nullptr) {
    throw CAPIException("Invalid TileDB subarray object");
  }
}

}  // namespace tiledb::api

#endif  // TILEDB_CAPI_SUBARRAY_INTERNAL_H
