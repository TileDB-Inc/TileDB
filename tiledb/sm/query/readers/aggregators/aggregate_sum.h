/**
 * @file   aggregate_sum.h
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
 * This file defines class AggregateSum.
 */

#ifndef TILEDB_AGGREGATE_SUM_H
#define TILEDB_AGGREGATE_SUM_H

#include "tiledb/sm/query/readers/aggregators/aggregate_buffer.h"
#include "tiledb/sm/query/readers/aggregators/field_info.h"

namespace tiledb {
namespace sm {

#define SUM_TYPE_DATA(T, SUM_T) \
  template <>                   \
  struct sum_type_data<T> {     \
    using type = T;             \
    typedef SUM_T sum_type;     \
  };

/** Convert basic type to a sum type. **/
template <typename T>
struct sum_type_data;

SUM_TYPE_DATA(int8_t, int64_t);
SUM_TYPE_DATA(uint8_t, uint64_t);
SUM_TYPE_DATA(int16_t, int64_t);
SUM_TYPE_DATA(uint16_t, uint64_t);
SUM_TYPE_DATA(int32_t, int64_t);
SUM_TYPE_DATA(uint32_t, uint64_t);
SUM_TYPE_DATA(int64_t, int64_t);
SUM_TYPE_DATA(uint64_t, uint64_t);
SUM_TYPE_DATA(float, double);
SUM_TYPE_DATA(double, double);

/**
 * Sum function that prevent wrap arounds on overflow.
 *
 * @param value Value to add to the sum.
 * @param sum Computed sum.
 */
template <typename SUM_T>
void safe_sum(SUM_T value, SUM_T& sum);

/**
 * Sum function for atomics that prevent wrap arounds on overflow.
 *
 * @param value Value to add to the sum.
 * @param sum Computed sum.
 */
template <typename SUM_T>
void safe_sum(SUM_T value, std::atomic<SUM_T>& sum) {
  SUM_T cur_sum = sum;
  SUM_T new_sum;
  do {
    new_sum = cur_sum;
    safe_sum(value, new_sum);
  } while (!std::atomic_compare_exchange_weak(&sum, &cur_sum, new_sum));
}

template <typename T>
class AggregateSum {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  AggregateSum(const FieldInfo field_info)
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
   * @param input_data Input data for the sum.
   *
   * @return {Sum for the cells, number of cells, optional validity value}.
   */
  template <typename SUM_T, typename BITMAP_T>
  tuple<SUM_T, uint64_t, optional<uint8_t>> sum(AggregateBuffer& input_data) {
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
              safe_sum(value, sum);
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
            safe_sum(value, sum);
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
            safe_sum(value, sum);
          }
        }
      } else {
        // Process for non nullable sums with no bitmap.
        for (uint64_t c = input_data.min_cell(); c < input_data.max_cell();
             c++) {
          auto value = static_cast<SUM_T>(values[c]);
          count++;
          safe_sum(value, sum);
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

#endif  // TILEDB_AGGREGATE_SUM_H
