/**
 * @file tiledb/api/c_api/query/query_api_internal.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2023-2024 TileDB, Inc.
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
 * This file declares the internal query section of the C API for TileDB.
 */

#ifndef TILEDB_CAPI_QUERY_INTERNAL_H
#define TILEDB_CAPI_QUERY_INTERNAL_H

#include "query_api_external.h"

#include "tiledb/api/c_api_support/handle/handle.h"
#include "tiledb/common/common.h"
#include "tiledb/sm/enums/query_status.h"
#include "tiledb/sm/query/query.h"

/** Handle `struct` for API query objects. */
struct tiledb_query_handle_t : public tiledb::api::CAPIHandle {
  /** Type name */
  static constexpr std::string_view object_type_name{"query"};

 private:
  shared_ptr<tiledb::sm::Query> query_;

 public:
  /**
   * Constructor for creating a new query.
   */
  tiledb_query_handle_t(
      tiledb::sm::ContextResources& resources,
      tiledb::sm::CancellationSource cancellation_source,
      tiledb::sm::StorageManager* storage_manager,
      shared_ptr<tiledb::sm::Array> array)
      : query_(make_shared<tiledb::sm::Query>(
            HERE(), resources, cancellation_source, storage_manager, array)) {
  }

  /**
   * Constructor from an existing shared_ptr<Query>.
   */
  explicit tiledb_query_handle_t(shared_ptr<tiledb::sm::Query> query)
      : query_(query) {
  }

  /** Returns a raw pointer to the underlying Query. */
  [[nodiscard]] tiledb::sm::Query* query() const {
    return query_.get();
  }

  /** Returns the shared_ptr to the underlying Query. */
  [[nodiscard]] shared_ptr<tiledb::sm::Query> query_shared() const {
    return query_;
  }
};

namespace tiledb::api {

/**
 * Validation function for a TileDB query
 * Throws in case the argument is nullptr
 *
 * @param query A C api query pointer
 */
inline void ensure_query_is_valid(const tiledb_query_t* query) {
  ensure_handle_is_valid(query);
}

/**
 * Validation function for a TileDB query
 * Throws in case the query is >= INITIALIZED with regards to its lifetime
 *
 * @param query A sm::Query pointer
 */
inline void ensure_query_is_not_initialized(const tiledb::sm::Query& query) {
  if (query.status() != sm::QueryStatus::UNINITIALIZED) {
    throw CAPIStatusException(
        "argument `query` is at a too late state of its lifetime");
  }
}

/**
 * Validation function for a TileDB query
 * Throws in case the query is >= INITIALIZED with regards to its lifetime
 *
 * @param query A C api query pointer
 */
inline void ensure_query_is_not_initialized(tiledb_query_t* query) {
  ensure_query_is_valid(query);
  ensure_query_is_not_initialized(*query->query());
}

}  // namespace tiledb::api

#endif
