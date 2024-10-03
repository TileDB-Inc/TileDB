/**
 * @file tiledb/api/c_api_test_support/testsupport_capi_subarray.h
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
 * This file defines test support for the subarray section of the C API.
 */

#ifndef TILEDB_TESTSUPPORT_CAPI_SUBARRAY_H
#define TILEDB_TESTSUPPORT_CAPI_SUBARRAY_H

#include "testsupport_capi_array.h"
#include "tiledb/api/c_api/subarray/subarray_api_external.h"
#include "tiledb/api/c_api/subarray/subarray_api_internal.h"

namespace tiledb::api::test_support {

struct ordinary_subarray {
  ordinary_array array{};
  tiledb_subarray_handle_t* subarray{nullptr};

  ordinary_subarray() {
    array.open();
    auto rc = tiledb_subarray_alloc(array.ctx(), array.array, &subarray);
    if (rc != TILEDB_OK) {
      throw std::runtime_error("error creating test subarray");
    }
    if (subarray == nullptr) {
      throw std::logic_error(
          "tiledb_subarray_alloc returned OK but without subarray");
    }
  }
  ~ordinary_subarray() {
    tiledb_subarray_free(&subarray);
  }

  [[nodiscard]] tiledb_ctx_handle_t* ctx() const {
    return array.ctx();
  }
};

/**
 * Ordinary subarray with var-sized dimensions.
 */
struct ordinary_subarray_var {
  ordinary_array array{true};  // Make the dimensions var-sized.
  tiledb_subarray_handle_t* subarray{nullptr};

  ordinary_subarray_var() {
    array.open();
    auto rc = tiledb_subarray_alloc(array.ctx(), array.array, &subarray);
    if (rc != TILEDB_OK) {
      throw std::runtime_error("error creating test subarray");
    }
    if (subarray == nullptr) {
      throw std::logic_error(
          "tiledb_subarray_alloc returned OK but without subarray");
    }
  }
  ~ordinary_subarray_var() {
    tiledb_subarray_free(&subarray);
  }

  [[nodiscard]] tiledb_ctx_handle_t* ctx() const {
    return array.ctx();
  }
};

}  // namespace tiledb::api::test_support

#endif  // TILEDB_TESTSUPPORT_CAPI_SUBARRAY_H
