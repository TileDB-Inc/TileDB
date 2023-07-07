/**
 * @file   sum_aggregator.h
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
 * This file defines class SumAggregator.
 */

#ifndef TILEDB_SUM_AGGREGATOR_H
#define TILEDB_SUM_AGGREGATOR_H

#include "tiledb/common/status.h"
#include "tiledb/sm/enums/layout.h"
#include "tiledb/sm/query/readers/aggregators/iaggregator.h"

using namespace tiledb::common;

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

class QueryBuffer;

/**
 * Sum function that prevent wrap arounds on overflow.
 *
 * @param value Value to add to the sum.
 * @param sum Computed sum.
 */
template <typename SUM_T>
void safe_sum(SUM_T value, SUM_T& sum);
template <typename T>
class SumAggregator : public IAggregator {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  SumAggregator() = delete;

  /**
   * Constructor.
   *
   * @param field_name Filed name that is aggregated.
   * @param var_sized Is this a var sized attribute?
   * @param is_nullable Is this a nullable attribute?
   * @param cell_val_num Number of values per cell.
   */
  SumAggregator(
      const std::string field_name,
      const bool var_sized,
      const bool is_nullable,
      const unsigned cell_val_num);

  DISABLE_COPY_AND_COPY_ASSIGN(SumAggregator);
  DISABLE_MOVE_AND_MOVE_ASSIGN(SumAggregator);

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /** Returns the field name for the aggregator. */
  std::string field_name() override {
    return field_name_;
  }

  /** Returns if the aggregation is var sized or not. */
  bool var_sized() override {
    return false;
  };

  /** Returns if the aggregate needs to be recomputed on overflow. */
  bool need_recompute_on_overflow() override {
    return true;
  }

  /**
   * Validate the result buffer.
   *
   * @param output_field_name Name for the output buffer.
   * @param buffers Query buffers.
   */
  void validate_output_buffer(
      std::string output_field_name,
      std::unordered_map<std::string, QueryBuffer>& buffers) override;

  /**
   * Aggregate data using the aggregator.
   *
   * @param input_data Input data for aggregation.
   */
  void aggregate_data(AggregateBuffer& input_data) override;

  /**
   * Copy final data to the user buffer.
   *
   * @param output_field_name Name for the output buffer.
   * @param buffers Query buffers.
   */
  void copy_to_user_buffer(
      std::string output_field_name,
      std::unordered_map<std::string, QueryBuffer>& buffers) override;

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** Is the sum nullable? */
  const bool is_nullable_;

  /** Field name. */
  const std::string field_name_;

  /** Mutex protecting `sum_` and `sum_overflowed_`. */
  std::mutex sum_mtx_;

  /** Computed sum. */
  typename sum_type_data<T>::sum_type sum_;

  /** Computed validity value. */
  uint8_t validity_value_;

  /** Has the sum overflowed. */
  bool sum_overflowed_;

  /* ********************************* */
  /*           PRIVATE METHODS         */
  /* ********************************* */

  /**
   * Add the sum of cells for the input data.
   *
   * @tparam SUM_T Sum type.
   * @tparam BITMAP_T Bitmap type.
   * @param input_data Input data for the sum.
   *
   * @return {Computed sum for the cells, validity value}.
   */
  template <typename SUM_T, typename BITMAP_T>
  tuple<SUM_T, uint8_t> sum(AggregateBuffer& input_data);
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_SUM_AGGREGATOR_H
