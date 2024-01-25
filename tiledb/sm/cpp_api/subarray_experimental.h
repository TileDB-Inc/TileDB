/**
 * @file   subarray_experimental.h
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
 * This file declares the experimental C++ API for the subarray.
 */

#ifndef TILEDB_CPP_API_SUBARRAY_EXPERIMENTAL_H
#define TILEDB_CPP_API_SUBARRAY_EXPERIMENTAL_H

#include "array_schema_experimental.h"
#include "dimension_label_experimental.h"
#include "subarray.h"
#include "tiledb_experimental.h"

namespace tiledb {
class SubarrayExperimental {
 public:
  /**
   * Adds a 1D range to a subarray dimension label, specified by its name, in
   * the form (start, end, stride). The datatype of the range must be the same
   * as the label datatype.
   *
   * @tparam T The datatype of the labels.
   * @param ctx TileDB context.
   * @param subarray Target subarray.
   * @param label_name Name of the target dimension label to add the range to.
   * @param start The range start to add.
   * @param end The range end to add.
   * @param stride The range stride to add.
   */
  template <class T>
  static void add_label_range(
      const Context& ctx,
      Subarray& subarray,
      const std::string& label_name,
      T start,
      T end,
      T stride = 0) {
    impl::type_check<T>(ArraySchemaExperimental::dimension_label(
                            ctx, subarray.schema_, label_name)
                            .label_type());
    ctx.handle_error(tiledb_subarray_add_label_range(
        ctx.ptr().get(),
        subarray.ptr().get(),
        label_name.c_str(),
        &start,
        &end,
        (stride == 0) ? nullptr : &stride));
  }

  /**
   * Adds a 1D string range to a subarray dimension label, specified by its
   * name, in the form (start, end). Only applicable to string labels.
   *
   * @param ctx TileDB context.
   * @param subarray Target subarray.
   * @param label_name Name of the target dimension label to add the range to.
   * @param start The range start to add.
   * @param end The range end to add.
   */
  static void add_label_range(
      const Context& ctx,
      Subarray& subarray,
      const std::string& label_name,
      const std::string& start,
      const std::string& end) {
    impl::type_check<char>(ArraySchemaExperimental::dimension_label(
                               ctx, subarray.schema_, label_name)
                               .label_type());
    ctx.handle_error(tiledb_subarray_add_label_range_var(
        ctx.ptr().get(),
        subarray.ptr().get(),
        label_name.c_str(),
        start.c_str(),
        start.size(),
        end.c_str(),
        end.size()));
  }

  /**
   * Retrieves the number of ranges for a dimension label name.
   *
   * @param ctx TileDB context.
   * @param subarray Target subarray.
   * @param label_name Name of the target dimension label.
   * @returns The number of ranges.
   */
  static uint64_t label_range_num(
      const Context& ctx, Subarray& subarray, const std::string& label_name) {
    uint64_t range_num;
    ctx.handle_error(tiledb_subarray_get_label_range_num(
        ctx.ptr().get(), subarray.ptr().get(), label_name.c_str(), &range_num));
    return range_num;
  }

  /**
   * Retrieves a range from a given dimension label name and range id. The
   * template datatype must be the same as that of the underlying labels.
   *
   * @param ctx TileDB context.
   * @param subarray Target subarray.
   * @param label_name The name of the target dimension label.
   * @param range_idx The range index.
   * @returns A triplet of the form (start, end, stride).
   */
  template <class T>
  static std::array<T, 3> label_range(
      const Context& ctx,
      Subarray& subarray,
      const std::string& label_name,
      uint64_t range_idx) {
    impl::type_check<T>(ArraySchemaExperimental::dimension_label(
                            ctx, subarray.schema_, label_name)
                            .label_type());
    const void *start, *end, *stride;
    ctx.handle_error(tiledb_subarray_get_label_range(
        ctx.ptr().get(),
        subarray.ptr().get(),
        label_name.c_str(),
        range_idx,
        &start,
        &end,
        &stride));
    std::array<T, 3> ret = {
        {*(const T*)start,
         *(const T*)end,
         (stride == nullptr) ? (const T)0 : *(const T*)stride}};
    return ret;
  }

  /**
   * Retrieves a string range from a given dimension label name and range id.
   * The underlying labels must be have a string datatype.
   *
   * @param ctx TileDB context.
   * @param subarray Target subarray.
   * @param label_name The name of the target dimension label.
   * @param range_idx The range index.
   * @returns A pair of the form (start, end).
   */
  static std::array<std::string, 2> label_range(
      const Context& ctx,
      Subarray& subarray,
      const std::string& label_name,
      uint64_t range_idx) {
    impl::type_check<char>(ArraySchemaExperimental::dimension_label(
                               ctx, subarray.schema_, label_name)
                               .label_type());
    uint64_t start_size, end_size;
    ctx.handle_error(tiledb_subarray_get_label_range_var_size(
        ctx.ptr().get(),
        subarray.ptr().get(),
        label_name.c_str(),
        range_idx,
        &start_size,
        &end_size));

    std::string start;
    start.resize(start_size);
    std::string end;
    end.resize(end_size);

    ctx.handle_error(tiledb_subarray_get_label_range_var(
        ctx.ptr().get(),
        subarray.ptr().get(),
        label_name.c_str(),
        range_idx,
        &start[0],
        &end[0]));
    std::array<std::string, 2> ret = {{std::move(start), std::move(end)}};
    return ret;
  }
};
}  // namespace tiledb

#endif
