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
  /* ********************************* */
  /*          STATIC FUNCTIONS         */
  /* ********************************* */

  /**
   * Factory function for creating a new set membership query condition.
   *
   * **Example:**
   * @code{.cpp}
   * tiledb::Context ctx;
   * std::vector<int> values = {1, 2, 3, 4, 5};
   * auto a1 = tiledb::QueryConditionExperimental::create(ctx, "a1", values);
   * @endcode
   *
   * @param ctx The TileDB context.
   * @param field_name The field name.
   * @param values The set membership values to use.
   * @param op The query condition operator to use. Currently limited to
   *        TILEDB_IN and TILEDB_NOT_IN.
   */
  template <typename T, impl::enable_trivial<T>* = nullptr>
  static QueryCondition create(
      const Context& ctx,
      const std::string& field_name,
      const std::vector<T>& values,
      tiledb_query_condition_op_t op) {
    std::vector<uint64_t> offsets;
    offsets.push_back(0);
    for (size_t i = 1; i < values.size(); i++) {
      offsets.push_back(i * sizeof(T));
    }

    tiledb_query_condition_t* qc;
    ctx.handle_error(tiledb_query_condition_alloc_set_membership(
        ctx.ptr().get(),
        field_name.c_str(),
        values.data(),
        values.size() * sizeof(T),
        offsets.data(),
        offsets.size() * sizeof(uint64_t),
        op,
        &qc));

    return QueryCondition(ctx, qc);
  }

  /**
   * Factory function for creating a new set membership query condition.
   *
   * **Example:**
   * @code{.cpp}
   * tiledb::Context ctx;
   * std::vector<std::string> values = {"foo", "bar", "baz"};
   * auto a1 = tiledb::QueryConditionExperimental::create(ctx, "a1", values);
   * @endcode
   *
   * @param ctx The TileDB context.
   * @param field_name The field name.
   * @param values The set membership values to use.
   * @param op The query condition operator to use. Currently limited to
   *        TILEDB_IN and TILEDB_NOT_IN.
   */
  template <typename T, impl::enable_trivial<T>* = nullptr>
  static QueryCondition create(
      const Context& ctx,
      const std::string& field_name,
      const std::vector<std::basic_string<T>>& values,
      tiledb_query_condition_op_t op) {
    std::vector<uint8_t> data;
    std::vector<uint64_t> offsets;

    uint64_t data_size = 0;
    for (auto& val : values) {
      data_size += val.size();
    }

    data.resize(data_size);
    uint64_t curr_offset = 0;
    for (auto& val : values) {
      offsets.push_back(curr_offset);
      memcpy(data.data() + curr_offset, val.data(), val.size());
      curr_offset += val.size();
    }

    tiledb_query_condition_t* qc;
    ctx.handle_error(tiledb_query_condition_alloc_set_membership(
        ctx.ptr().get(),
        field_name.c_str(),
        data.data(),
        data.size(),
        offsets.data(),
        offsets.size() * sizeof(uint64_t),
        op,
        &qc));

    return QueryCondition(ctx, qc);
  }

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

#endif  // TILEDB_CPP_API_QUERY_CONDITION_EXPERIMENTAL_H
