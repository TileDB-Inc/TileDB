/**
 * @file tiledb/api/c_api/query_condition/query_condition_api_internal.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2026 TileDB, Inc.
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
 * This file declares the internal query condition section of the C API for
 * TileDB.
 */

#ifndef TILEDB_CAPI_QUERY_CONDITION_INTERNAL_H
#define TILEDB_CAPI_QUERY_CONDITION_INTERNAL_H

#include "query_condition_api_external.h"

#include "tiledb/api/c_api_support/handle/handle.h"
#include "tiledb/common/common.h"
#include "tiledb/sm/enums/query_status.h"
#include "tiledb/sm/query/query.h"

/** Handle `struct` for API query objects. */
struct tiledb_query_condition_handle_t : public tiledb::api::CAPIHandle {
  /** Type name */
  static constexpr std::string_view object_type_name{"query_condition"};

 private:
  shared_ptr<tiledb::sm::QueryCondition> query_condition_;

 public:
  /**
   * Constructs a blank query condition handle.
   */
  tiledb_query_condition_handle_t()
      : query_condition_(make_shared<tiledb::sm::QueryCondition>(HERE())) {
  }

  /**
   * Constructs a query condition handle from a shared pointer to an existing
   * internal query condition object.
   */
  tiledb_query_condition_handle_t(
      shared_ptr<tiledb::sm::QueryCondition> query_condition)
      : query_condition_(query_condition) {
  }

  /**
   * Constructs a query condition handle from an internal query condition object
   * constructed in-place.
   */
  tiledb_query_condition_handle_t(std::in_place_t, auto&&... args)
      : query_condition_(make_shared<tiledb::sm::QueryCondition>(
            HERE(), std::forward<decltype(args)>(args)...)) {
  }

  /** Returns a raw pointer to the underlying QueryCondition. */
  [[nodiscard]] tiledb::sm::QueryCondition* query_condition() const {
    return query_condition_.get();
  }
};

#endif
