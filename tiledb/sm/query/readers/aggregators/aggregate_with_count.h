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
 */

#ifndef TILEDB_AGGREGATE_WITH_COUNT_H
#define TILEDB_AGGREGATE_WITH_COUNT_H

#include "tiledb/sm/query/readers/aggregators/aggregate_buffer.h"
#include "tiledb/sm/query/readers/aggregators/field_info.h"
#include "tiledb/sm/query/readers/aggregators/no_op.h"

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

template <typename T, typename AGG_T, class AggPolicy, class ValidityPolicy>
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
   * @tparam BITMAP_T Bitmap type.
   * @param input_data Input data for the aggregation.
   *
   * NOTE: Count of cells returned is used to infer the validity from the
   * caller.
   * @return {Aggregate value, count of cells}.
   */
  template <typename BITMAP_T>
  tuple<AGG_T, uint64_t> aggregate(AggregateBuffer& input_data) {
    AggPolicy agg_policy;
    ValidityPolicy val_policy;
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
      if (field_info_.is_dense_dim_) {
        // Process for dense dimension values with bitmap.
        for (uint64_t c = 0; c < input_data.size(); c++) {
          auto bitmap_val = input_data.bitmap_at<BITMAP_T>(c);
          auto value = dense_dim_value_at(input_data, c);
          for (BITMAP_T i = 0; i < bitmap_val; i++) {
            agg_policy.op(value, res, count);
            count++;
          }
        }
      } else if (field_info_.is_nullable_) {
        // Process for nullable values with bitmap.
        for (uint64_t c = 0; c < input_data.size(); c++) {
          auto bitmap_val = input_data.bitmap_at<BITMAP_T>(c);
          if (val_policy.op(input_data.validity_at(c)) && bitmap_val != 0) {
            auto value = value_at(input_data, c);
            for (BITMAP_T i = 0; i < bitmap_val; i++) {
              agg_policy.op(value, res, count);
              count++;
            }
          }
        }
      } else {
        // Process for non nullable values with bitmap.
        for (uint64_t c = 0; c < input_data.size(); c++) {
          auto bitmap_val = input_data.bitmap_at<BITMAP_T>(c);
          auto value = value_at(input_data, c);
          for (BITMAP_T i = 0; i < bitmap_val; i++) {
            agg_policy.op(value, res, count);
            count++;
          }
        }
      }
    } else {
      if (field_info_.is_dense_dim_) {
        // Process for dense dimension values with no bitmap.
        for (uint64_t c = 0; c < input_data.size(); c++) {
          auto value = dense_dim_value_at(input_data, c);
          agg_policy.op(value, res, count);
          count++;
        }
      } else if (field_info_.is_nullable_) {
        // Process for nullable values with no bitmap.
        for (uint64_t c = 0; c < input_data.size(); c++) {
          if (val_policy.op(input_data.validity_at(c))) {
            auto value = value_at(input_data, c);
            agg_policy.op(value, res, count);
            count++;
          }
        }
      } else {
        // Process for non nullable values with no bitmap.
        for (uint64_t c = 0; c < input_data.size(); c++) {
          auto value = value_at(input_data, c);
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

  /* ********************************* */
  /*           PRIVATE METHODS         */
  /* ********************************* */

  /**
   * Returns the value at the specified cell if needed.
   *
   * @param input_data Input data.
   * @param c Cell index.
   * @return Value.
   */
  inline AGG_T value_at(AggregateBuffer& input_data, uint64_t c) {
    typedef typename type_data<T>::value_type VALUE_T;
    if constexpr (!std::is_same<AggPolicy, NoOp>::value) {
      return input_data.value_at<VALUE_T>(c);
    }

    return AGG_T();
  }

  /**
   * Returns the dense dimension value at the specified cell if needed.
   *
   * @param input_data Input data.
   * @param c Cell index.
   * @return Value.
   */
  inline AGG_T dense_dim_value_at(AggregateBuffer& input_data, uint64_t c) {
    typedef typename type_data<T>::value_type VALUE_T;
    if constexpr (
        !std::is_same<AggPolicy, NoOp>::value &&
        !std::is_same<AGG_T, std::string_view>::value) {
      return input_data.value_at<VALUE_T>(0) + c * field_info_.is_slab_dim_;
    }

    return AGG_T();
  }
};

}  // namespace tiledb::sm

#endif  // TILEDB_AGGREGATE_WITH_COUNT_H
