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
#include "tiledb/sm/query/readers/aggregators/safe_sum.h"
#include "tiledb/sm/query/readers/aggregators/sum_type.h"
#include "tiledb/sm/query/readers/aggregators/validity_policies.h"

namespace tiledb::sm {

class QueryBuffer;

template <typename T>
class SumWithCountAggregator : public InputFieldValidator,
                               public OutputBufferValidator,
                               public IAggregator {
 public:
  using SUM_T = typename sum_type_data<T>::sum_type;

  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  SumWithCountAggregator() = delete;

  /**
   * Constructor.
   *
   * @param field_info Field info.
   */
  SumWithCountAggregator(FieldInfo field_info);

  DISABLE_COPY_AND_COPY_ASSIGN(SumWithCountAggregator);
  DISABLE_MOVE_AND_MOVE_ASSIGN(SumWithCountAggregator);

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /** Returns the field name for the aggregator. */
  std::string field_name() override {
    return field_info_.name_;
  }

  /** Returns if the aggregation is var sized or not. */
  bool aggregation_var_sized() override {
    return false;
  };

  /** Returns if the aggregation is nullable or not. */
  bool aggregation_nullable() override {
    return field_info_.is_nullable_;
  }

  /** Returns if the aggregation is for validity only data. */
  bool aggregation_validity_only() override {
    return false;
  }

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
   * Aggregate a tile with fragment metadata.
   *
   * @param tile_metadata Tile metadata for aggregation.
   */
  void aggregate_tile_with_frag_md(TileMetadata& tile_metadata) override;

  /**
   * Copy final validity value to the user buffer.
   *
   * @param result_buffer Query buffer to copy to.
   */
  void copy_validity_value_to_user_buffers(QueryBuffer& result_buffer);

 protected:
  /* ********************************* */
  /*        PROTECTED FUNCTIONS        */
  /* ********************************* */

  /**
   * Update the sum.
   *
   * @param sum Sum.
   * @param count Count of values summed.
   */
  void update_sum(SUM_T sum, uint64_t count);

  /* ********************************* */
  /*        PROTECTED ATTRIBUTES       */
  /* ********************************* */

  /** Field information. */
  const FieldInfo field_info_;

  /** AggregateWithCount to do summation of AggregateBuffer data. */
  AggregateWithCount<T, SUM_T, SafeSum, NonNull> aggregate_with_count_;

  /** Computed sum. */
  std::atomic<SUM_T> sum_;

  /** Count of values. */
  std::atomic<uint64_t> count_;

  /** Computed validity value. */
  optional<uint8_t> validity_value_;

  /** Has the sum overflowed. */
  std::atomic<bool> sum_overflowed_;
};

template <typename T>
class SumAggregator : public SumWithCountAggregator<T> {
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
  SumAggregator(FieldInfo field_info)
      : SumWithCountAggregator<T>(field_info) {
  }

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /**
   * Copy final data to the user buffer.
   *
   * @param output_field_name Name for the output buffer.
   * @param buffers Query buffers.
   */
  void copy_to_user_buffer(
      std::string output_field_name,
      std::unordered_map<std::string, QueryBuffer>& buffers) override;

  /** Returns name of the aggregate, e.g. COUNT, MIN, SUM. */
  std::string aggregate_name() override {
    return constants::aggregate_sum_str;
  }

  /** Returns the TileDB datatype of the output field for the aggregate. */
  Datatype output_datatype() override {
    return sum_type_data<T>::tiledb_datatype;
  }
};

template <typename T>
class MeanAggregator : public SumWithCountAggregator<T> {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  MeanAggregator() = delete;

  /**
   * Constructor.
   *
   * @param field_info Field info.
   */
  MeanAggregator(FieldInfo field_info)
      : SumWithCountAggregator<T>(field_info) {
  }

  /* ********************************* */
  /*                API                */
  /* ********************************* */
  /**
   * Copy final data to the user buffer.
   *
   * @param output_field_name Name for the output buffer.
   * @param buffers Query buffers.
   */
  void copy_to_user_buffer(
      std::string output_field_name,
      std::unordered_map<std::string, QueryBuffer>& buffers) override;

  /** Returns name of the aggregate, e.g. COUNT, MIN, SUM. */
  std::string aggregate_name() override {
    return constants::aggregate_mean_str;
  }

  /** Returns the TileDB datatype of the output field for the aggregate. */
  Datatype output_datatype() override {
    return Datatype::FLOAT64;
  }
};

}  // namespace tiledb::sm

#endif  // TILEDB_SUM_AGGREGATOR_H
