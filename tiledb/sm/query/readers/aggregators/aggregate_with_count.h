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
#include "tiledb/sm/query/readers/aggregators/safe_sum.h"

namespace tiledb {
namespace sm {

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
   * Add the sum of cells for the input data.
   *
   * @tparam SUM_T Sum type.
   * @tparam BITMAP_T Bitmap type.
   * @tparam AggPolicy Aggregation policy.
   * @param input_data Input data for the sum.
   *
   * @return {Sum for the cells, number of cells, optional validity value}.
   */
  template <typename SUM_T, typename BITMAP_T, class AggPolicy>
  tuple<SUM_T, uint64_t, optional<uint8_t>> aggregate(
      AggregateBuffer& input_data) {
    SUM_T sum{0};
    uint64_t count{0};
    optional<uint8_t> validity{nullopt};
    auto values = input_data.fixed_data_as<T>();

    // Run different loops for bitmap versus no bitmap and nullable versus non
    // nullable. The bitmap tells us which cells was already filtered out by
    // ranges or query conditions.
    if (input_data.has_bitmap()) {
      auto bitmap_values = input_data.bitmap_data_as<BITMAP_T>();

      if (field_info_.is_nullable_) {
        validity = 0;
        auto validity_values = input_data.validity_data();

        // Process for nullable sums with bitmap.
        for (uint64_t c = input_data.min_cell(); c < input_data.max_cell();
             c++) {
          if (validity_values[c] != 0 && bitmap_values[c] != 0) {
            validity = 1;

            auto value = static_cast<SUM_T>(values[c]);
            for (BITMAP_T i = 0; i < bitmap_values[c]; i++) {
              count++;
              AggPolicy::op(value, sum);
            }
          }
        }
      } else {
        // Process for non nullable sums with bitmap.
        for (uint64_t c = input_data.min_cell(); c < input_data.max_cell();
             c++) {
          auto value = static_cast<SUM_T>(values[c]);

          for (BITMAP_T i = 0; i < bitmap_values[c]; i++) {
            count++;
            AggPolicy::op(value, sum);
          }
        }
      }
    } else {
      if (field_info_.is_nullable_) {
        validity = 0;
        auto validity_values = input_data.validity_data();

        // Process for nullable sums with no bitmap.
        for (uint64_t c = input_data.min_cell(); c < input_data.max_cell();
             c++) {
          if (validity_values[c] != 0) {
            validity = 1;

            auto value = static_cast<SUM_T>(values[c]);
            count++;
            AggPolicy::op(value, sum);
          }
        }
      } else {
        // Process for non nullable sums with no bitmap.
        for (uint64_t c = input_data.min_cell(); c < input_data.max_cell();
             c++) {
          auto value = static_cast<SUM_T>(values[c]);
          count++;
          AggPolicy::op(value, sum);
        }
      }
    }

    return {sum, count, validity};
  }

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** Field information. */
  const FieldInfo field_info_;
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_AGGREGATE_WITH_COUNT_H
