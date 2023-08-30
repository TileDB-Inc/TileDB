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

#include "tiledb/sm/query/readers/aggregators/aggregate_with_count.h"
#include "tiledb/sm/query/readers/aggregators/field_info.h"
#include "tiledb/sm/query/readers/aggregators/iaggregator.h"
#include "tiledb/sm/query/readers/aggregators/sum_type.h"

namespace tiledb {
namespace sm {

class QueryBuffer;

template <typename T>
class SumAggregator : public OutputBufferValidator, public IAggregator {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  SumAggregator() = delete;

  /**
   * Constructor.
   *
   * @param field_info Field info.
   */
  SumAggregator(FieldInfo field_info);

  DISABLE_COPY_AND_COPY_ASSIGN(SumAggregator);
  DISABLE_MOVE_AND_MOVE_ASSIGN(SumAggregator);

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /** Returns the field name for the aggregator. */
  std::string field_name() override {
    return field_info_.name_;
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

  /** Field information. */
  const FieldInfo field_info_;

  /** AggregateWithCount to do summation of AggregateBuffer data. */
  AggregateWithCount<T> summator_;

  /** Computed sum. */
  std::atomic<typename sum_type_data<T>::sum_type> sum_;

  /** Computed validity value. */
  optional<uint8_t> validity_value_;

  /** Has the sum overflowed. */
  std::atomic<bool> sum_overflowed_;
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_SUM_AGGREGATOR_H
