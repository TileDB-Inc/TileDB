/**
 * @file   aggregate_with_count.h
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
 * This file defines class AggregateWithCount.
 *
 * TODO: Add more benchmark coverage for this class (sc-33758).
 */

#ifndef TILEDB_AGGREGATE_WITH_COUNT_H
#define TILEDB_AGGREGATE_WITH_COUNT_H

#include "tiledb/sm/query/readers/aggregators/aggregate_buffer.h"
#include "tiledb/sm/query/readers/aggregators/field_info.h"

namespace tiledb::sm {

/** Convert type to a value type. **/
template <typename T>
struct type_data {
  using type = T;
  typedef T value_type;
};

template <>
struct type_data<std::string> {
  using type = std::string;
  typedef std::string_view value_type;
};

template <typename T>
class AggregateWithCount {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  AggregateWithCount(const FieldInfo field_info)
      : field_info_(field_info) {
  }

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /**
   * Aggregate the input data.
   *
   * @tparam AGG_T Aggregate value type.
   * @tparam BITMAP_T Bitmap type.
   * @tparam AggPolicy Aggregation policy.
   * @param input_data Input data for the aggregation.
   *
   * NOTE: Count of cells returned is used to infer the validity from the
   * caller.
   * @return {Aggregate value, count of cells}.
   */
  template <typename AGG_T, typename BITMAP_T, class AggPolicy>
  tuple<AGG_T, uint64_t> aggregate(AggregateBuffer& input_data) {
    typedef typename type_data<T>::value_type VALUE_T;
    AggPolicy agg_policy;
    AGG_T res;
    if constexpr (std::is_same<AGG_T, std::string_view>::value) {
      res = "";
    } else {
      res = 0;
    }
    uint64_t count{0};

    // Run different loops for bitmap versus no bitmap and nullable versus non
    // nullable. The bitmap tells us which cells was already filtered out by
    // ranges or query conditions.
    if (input_data.has_bitmap()) {
      auto bitmap_values = input_data.bitmap_data_as<BITMAP_T>();

      if (field_info_.is_nullable_) {
        auto validity_values = input_data.validity_data();

        // Process for nullable values with bitmap.
        for (uint64_t c = input_data.min_cell(); c < input_data.max_cell();
             c++) {
          if (validity_values[c] != 0 && bitmap_values[c] != 0) {
            AGG_T value = input_data.value_at<VALUE_T>(c);
            for (BITMAP_T i = 0; i < bitmap_values[c]; i++) {
              agg_policy.op(value, res, count);
              count++;
            }
          }
        }
      } else {
        // Process for non nullable values with bitmap.
        for (uint64_t c = input_data.min_cell(); c < input_data.max_cell();
             c++) {
          AGG_T value = input_data.value_at<VALUE_T>(c);

          for (BITMAP_T i = 0; i < bitmap_values[c]; i++) {
            agg_policy.op(value, res, count);
            count++;
          }
        }
      }
    } else {
      if (field_info_.is_nullable_) {
        auto validity_values = input_data.validity_data();

        // Process for nullable values with no bitmap.
        for (uint64_t c = input_data.min_cell(); c < input_data.max_cell();
             c++) {
          if (validity_values[c] != 0) {
            AGG_T value = input_data.value_at<VALUE_T>(c);
            agg_policy.op(value, res, count);
            count++;
          }
        }
      } else {
        // Process for non nullable values with no bitmap.
        for (uint64_t c = input_data.min_cell(); c < input_data.max_cell();
             c++) {
          AGG_T value = input_data.value_at<VALUE_T>(c);
          agg_policy.op(value, res, count);
          count++;
        }
      }
    }

    return {res, count};
  }

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** Field information. */
  const FieldInfo field_info_;
};

}  // namespace tiledb::sm

#endif  // TILEDB_AGGREGATE_WITH_COUNT_H
