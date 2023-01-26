/**
 * @file   query_experimental.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022 TileDB, Inc.
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
 * This file declares the C++ experimental API for the query.
 */

#ifndef TILEDB_CPP_API_QUERY_EXPERIMENTAL_H
#define TILEDB_CPP_API_QUERY_EXPERIMENTAL_H
#include "context.h"
#include "tiledb.h"

namespace tiledb {
class QueryExperimental {
 public:
  /**
   * Sets the update value.
   *
   * Note that more than one update value may be set on a query.
   *
   * @param ctx TileDB context.
   * @param query Query object.
   * @param field_name The attribute name.
   * @param update_value The value to set.
   * @param update_value_size The byte size of `update_value`.
   * @return Reference to this Query
   */
  static void add_update_value_to_query(
      const Context& ctx,
      Query& query,
      const char* field_name,
      const void* update_value,
      uint64_t update_value_size) {
    ctx.handle_error(tiledb_query_add_update_value(
        ctx.ptr().get(),
        query.ptr().get(),
        field_name,
        update_value,
        update_value_size));
  }

  /**
   * Get the number of relevant fragments from the subarray. Should only be
   * called after size estimation was asked for.
   *
   * @param ctx TileDB context.
   * @param query Query object.
   * @return Number of relevant fragments.
   */
  static uint64_t get_relevant_fragment_num(Context& ctx, const Query& query) {
    uint64_t relevant_fragment_num = 0;
    ctx.handle_error(tiledb_query_get_relevant_fragment_num(
        ctx.ptr().get(), query.ptr().get(), &relevant_fragment_num));
    return relevant_fragment_num;
  }
};
}  // namespace tiledb

#endif  // TILEDB_CPP_API_QUERY_EXPERIMENTAL_H