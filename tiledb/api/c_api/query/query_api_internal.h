/**
 * @file tiledb/api/c_api/query_aggregate/query_aggregate_api_internal.h
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
 * This file declares the internal query section of the C API for TileDB.
 */

#ifndef TILEDB_CAPI_QUERY_INTERNAL_H
#define TILEDB_CAPI_QUERY_INTERNAL_H

#include "tiledb/api/c_api_support/handle/handle.h"
#include "tiledb/sm/c_api/tiledb_struct_def.h"

namespace tiledb::api {

/**
 * Validation function for a TileDB query
 * Throws in case the argument or its internal state is nullptr
 *
 * @param query A C api query pointer
 */
inline void ensure_query_is_valid(tiledb_query_t* query) {
  if (!query || !query->query_) {
    throw CAPIStatusException("argument `query` may not be nullptr");
  }
}

}  // namespace tiledb::api

#endif
