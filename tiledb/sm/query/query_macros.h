/**
 * @file   query_macros.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2018-2024 TileDB, Inc.
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
 * This file defines macros shared by the query-related files.
 */

#ifndef TILEDB_QUERY_MACROS_H
#define TILEDB_QUERY_MACROS_H

using namespace tiledb::common;

namespace tiledb::sm {

#ifndef RETURN_CANCEL_OR_ERROR
/**
 * Returns an error status if the given Status is not Status::Ok, or
 * if the Context that owns this Query has requested cancellation.
 */
#define RETURN_CANCEL_OR_ERROR(s)                   \
  do {                                              \
    Status _s = (s);                                \
    if (!_s.ok()) {                                 \
      return _s;                                    \
    }                                               \
    process_external_cancellation();                \
    if (cancelled()) {                              \
      return Status_QueryError("Query cancelled."); \
    }                                               \
  } while (false)
#endif

#ifndef RETURN_CANCEL_OR_ERROR_TUPLE
/**
 * Returns an error status if the given Status is not Status::Ok, or
 * if the Context that owns this Query has requested cancellation.
 */
#define RETURN_CANCEL_OR_ERROR_TUPLE(s)                             \
  do {                                                              \
    Status _s = (s);                                                \
    if (!_s.ok()) {                                                 \
      return {_s, std::nullopt};                                    \
    }                                                               \
    process_external_cancellation();                                \
    if (cancelled()) {                                              \
      return {Status_QueryError("Query cancelled."), std::nullopt}; \
    }                                                               \
  } while (false)
#endif

}  // namespace tiledb::sm

#endif  // TILEDB_QUERY_MACROS_H
