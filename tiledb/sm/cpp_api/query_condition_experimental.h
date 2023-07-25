/**
 * @file query_condition_experimental.h
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
 * This file declares the experimental C++ API for the query condition.
 */

#ifndef TILEDB_CPP_API_QUERY_CONDITION_EXPERIMENTAL_H
#define TILEDB_CPP_API_QUERY_CONDITION_EXPERIMENTAL_H

#include "query_condition.h"

namespace tiledb {
class QueryConditionExperimental {
 public:
  /**
   * Set whether or not to use the associated enumeration.
   *
   * @param ctx TileDB Context.
   * @param cond TileDB Query Condition.
   * @param use_enumeration Whether to use the associated enumeration.
   */
  static void set_use_enumeration(
      const Context& ctx, QueryCondition& cond, bool use_enumeration) {
    ctx.handle_error(tiledb_query_condition_set_use_enumeration(
        ctx.ptr().get(), cond.ptr().get(), use_enumeration ? 1 : 0));
  }
};

}  // namespace tiledb

#endif
