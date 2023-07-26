/**
 * @file   min_max_aggregator.h
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
 * This file defines class MinMaxAggregator.
 */

#ifndef TILEDB_MIN_MAX_AGGREGATOR_H
#define TILEDB_MIN_MAX_AGGREGATOR_H

#include "tiledb/common/status.h"
#include "tiledb/sm/enums/layout.h"
#include "tiledb/sm/query/readers/aggregators/field_info.h"
#include "tiledb/sm/query/readers/aggregators/iaggregator.h"

#include <functional>

using namespace tiledb::common;

namespace tiledb {
namespace sm {

class QueryBuffer;

/** Convert type to a value type. **/
template <typename T>
struct min_max_type_data {
  using type = T;
  typedef T value_type;
};

template <>
struct min_max_type_data<std::string> {
  using type = std::string;
  typedef std::string_view value_type;
};

/**
 * Comparator aggregator base class to handle partial specialization of some
 * types.
 */
template <typename T>
class ComparatorAggregatorBase {
 protected:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  ComparatorAggregatorBase() = delete;
  ComparatorAggregatorBase(const FieldInfo&& field_info) = delete;

  /**
   * Constructor.
   *
   * @param field_info Field info.
   */
  ComparatorAggregatorBase(const FieldInfo& field_info)
      : field_info_(field_info)
      , value_(nullopt)
      , validity_value_(
            field_info_.is_nullable_ ? std::make_optional(0) : nullopt) {
  }

  DISABLE_COPY_AND_COPY_ASSIGN(ComparatorAggregatorBase);
  DISABLE_MOVE_AND_MOVE_ASSIGN(ComparatorAggregatorBase);

  /* ********************************* */
  /*        PROTECTED METHODS          */
  /* ********************************* */

  /**
   * Get the value at a certain cell index for the input buffers.
   *
   * @tparam VALUE_T Value type.
   * @param fixed_data Fixed data buffer.
   * @param var_data Var data buffer.
   * @param cell_idx Cell index.
   *
   * @return Value.
   */
  template <typename VALUE_T>
  VALUE_T value_at(
      const void* fixed_data,
      const char* var_data,
      const uint64_t cell_idx) const;

  /**
   * Get the last value for the input buffers.
   *
   * @tparam VALUE_T Value type.
   * @param fixed_data Fixed data buffer.
   * @param var_data Var data buffer.
   * @param input_data Aggregate buffer.
   *
   * @return Value.
   */
  template <typename VALUE_T>
  VALUE_T last_var_value(
      const void* fixed_data,
      const char* var_data,
      const AggregateBuffer& input_data) const;

  /**
   * Copy final data to the user buffer.
   *
   * @param output_field_name Name for the output buffer.
   * @param buffers Query buffers.
   */
  void copy_to_user_buffer(
      std::string output_field_name,
      std::unordered_map<std::string, QueryBuffer>& buffers) const;

 protected:
  /* ********************************* */
  /*       PROTECTED ATTRIBUTES        */
  /* ********************************* */

  /** Field information. */
  const FieldInfo field_info_;

  /** Computed min/max. */
  optional<T> value_;

  /** Computed validity value. */
  optional<uint8_t> validity_value_;
};

template <typename T, typename Op>
class ComparatorAggregator : public ComparatorAggregatorBase<T>,
                             public IAggregator {
 protected:
  using VALUE_T = typename min_max_type_data<T>::value_type;

  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  ComparatorAggregator() = delete;
  ComparatorAggregator(const FieldInfo&& field_info) = delete;

  /**
   * Constructor.
   *
   * @param field_info Field info.
   */
  ComparatorAggregator(const FieldInfo& field_info);

  DISABLE_COPY_AND_COPY_ASSIGN(ComparatorAggregator);
  DISABLE_MOVE_AND_MOVE_ASSIGN(ComparatorAggregator);

  /* ********************************* */
  /*                API                */
  /* ********************************* */

 public:
  /** Returns the field name for the aggregator. */
  std::string field_name() override {
    return ComparatorAggregatorBase<T>::field_info_.name_;
  }

  /** Returns if the aggregation is var sized or not. */
  bool var_sized() override {
    return ComparatorAggregatorBase<T>::field_info_.var_sized_;
  };

  /** Returns if the aggregate needs to be recomputed on overflow. */
  bool need_recompute_on_overflow() override {
    return false;
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

  /** Mutex protecting `value_`. */
  std::mutex value_mtx_;

  /** Operation function to determine value. */
  Op op_;

  /* ********************************* */
  /*           PRIVATE METHODS         */
  /* ********************************* */

  /**
   * Potentially update the min/max value with the value at cell index 'c'.
   *
   * @param value Value to possibly update.
   * @param fixed_data Fixed data.
   * @param var_data Var data.
   * @param c Cell index.
   */
  inline void update_min_max(
      optional<VALUE_T>& value,
      const void* fixed_data,
      const char* var_data,
      const uint64_t c) {
    auto cmp = ComparatorAggregatorBase<T>::template value_at<VALUE_T>(
        fixed_data, var_data, c);
    if (!value.has_value()) {
      value = cmp;
    } else if (op_(cmp, value.value())) {
      value = cmp;
    }
  }

  /**
   * Potentially update the min/max value with the last var value.
   *
   * @param value Value to possibly update.
   * @param fixed_data Fixed data.
   * @param var_data Var data.
   * @param c Cell index.
   */
  inline void update_last_var_min_max(
      optional<VALUE_T>& value,
      const void* fixed_data,
      const char* var_data,
      const AggregateBuffer& input_data) {
    auto cmp = ComparatorAggregatorBase<T>::template last_var_value<VALUE_T>(
        fixed_data, var_data, input_data);
    if (!value.has_value()) {
      value = cmp;
    } else if (op_(cmp, value.value())) {
      value = cmp;
    }
  }

  /**
   * Get the min/max value of cells for the input data.
   *
   * @tparam BITMAP_T Bitmap type.
   * @param input_data Input data for the min/max.
   *
   * @return {Computed min/max for the cells}.
   */
  template <typename BITMAP_T>
  optional<T> min_max(AggregateBuffer& input_data);
};

template <typename T>
class MinAggregator
    : public ComparatorAggregator<
          T,
          std::less<typename min_max_type_data<T>::value_type>> {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  MinAggregator() = delete;

  /**
   * Constructor.
   *
   * @param field info Field info.
   */
  MinAggregator(const FieldInfo field_info)
      : ComparatorAggregator<
            T,
            std::less<typename min_max_type_data<T>::value_type>>(field_info){};

  DISABLE_COPY_AND_COPY_ASSIGN(MinAggregator);
  DISABLE_MOVE_AND_MOVE_ASSIGN(MinAggregator);
};

template <typename T>
class MaxAggregator
    : public ComparatorAggregator<
          T,
          std::greater<typename min_max_type_data<T>::value_type>> {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  MaxAggregator() = delete;

  /**
   * Constructor.
   *
   * @param field info Field info.
   */
  MaxAggregator(const FieldInfo field_info)
      : ComparatorAggregator<
            T,
            std::greater<typename min_max_type_data<T>::value_type>>(
            field_info){};

  DISABLE_COPY_AND_COPY_ASSIGN(MaxAggregator);
  DISABLE_MOVE_AND_MOVE_ASSIGN(MaxAggregator);
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_MIN_MAX_AGGREGATOR_H
