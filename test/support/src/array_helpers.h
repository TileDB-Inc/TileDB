/**
 * @file   array_schema.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2024 TileDB, Inc.
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
 * This file declares some array test suite helper functions.
 */

#ifndef TILEDB_TEST_ARRAY_HELPERS_H
#define TILEDB_TEST_ARRAY_HELPERS_H

#include "test/support/src/error_helpers.h"
#include "tiledb/sm/c_api/tiledb.h"

#include <string>

namespace tiledb::test {

/**
 * RAII to make sure an array is deleted before exiting a scope.
 *
 * This is useful within rapidcheck properties which may
 * want to use the same temp directory and array name
 * for each instance of a test.
 */
struct DeleteArrayGuard {
  DeleteArrayGuard(tiledb_ctx_t* ctx, const char* array_uri)
      : ctx_(ctx)
      , array_uri_(array_uri) {
  }

  DeleteArrayGuard(DeleteArrayGuard&& movefrom)
      : ctx_(movefrom.ctx_)
      , array_uri_(movefrom.array_uri_) {
    movefrom.release();
  }

  ~DeleteArrayGuard() {
    del();
  }

  capi_return_t del() {
    if (ctx_ && array_uri_) {
      return tiledb_array_delete(ctx_, array_uri_);
    } else {
      return TILEDB_OK;
    }
  }

  void release() {
    ctx_ = nullptr;
    array_uri_ = nullptr;
  }

  tiledb_ctx_t* ctx_;
  const char* array_uri_;
};

/**
 * RAII to make sure we close our arrays so that keeping the same array URI
 * open from one test to the next doesn't muck things up
 * (this is especially important for rapidcheck)
 */
struct CApiArray {
  tiledb_ctx_t* ctx_;
  tiledb_array_t* array_;

  CApiArray()
      : ctx_(nullptr)
      , array_(nullptr) {
  }

  CApiArray(tiledb_ctx_t* ctx, const char* uri, tiledb_query_type_t mode)
      : ctx_(ctx)
      , array_(nullptr) {
    throw_if_error(ctx, tiledb_array_alloc(ctx, uri, &array_));
    throw_if_error(ctx, tiledb_array_open(ctx, array_, mode));
  }

  CApiArray(CApiArray&& from)
      : ctx_(from.ctx_)
      , array_(from.movefrom()) {
  }

  ~CApiArray() {
    if (array_) {
      // yes this may std::terminate but hey it's test code
      throw_if_error(ctx_, tiledb_array_close(ctx_, array_));
      tiledb_array_free(&array_);
    }
  }

  tiledb_array_t* movefrom() {
    auto array = array_;
    array_ = nullptr;
    return array;
  }

  operator tiledb_array_t*() const {
    return array_;
  }

  tiledb_array_t* operator->() const {
    return array_;
  }
};

/**
 * Encapsulates memory budget configuration parameters for the sparse global
 * order reader
 */
struct SparseGlobalOrderReaderMemoryBudget {
  std::string total_budget_;
  std::string ratio_tile_ranges_;
  std::string ratio_array_data_;
  std::string ratio_coords_;

  SparseGlobalOrderReaderMemoryBudget()
      : total_budget_("1048576")
      , ratio_tile_ranges_("0.1")
      , ratio_array_data_("0.1")
      , ratio_coords_("0.5") {
  }

  /**
   * Apply this memory budget to `config`.
   *
   * @return an error if one occurred, or nullptr if successful
   */
  tiledb_error_t* apply(tiledb_config_t* config);
};

}  // namespace tiledb::test

#endif
