/**
 * @file sum_aggregator.cc
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
 * This file implements class SumAggregator.
 */

#include "tiledb/sm/query/readers/aggregators/sum_aggregator.h"

#include "tiledb/sm/query/query_buffer.h"
#include "tiledb/sm/query/readers/aggregators/aggregate_buffer.h"

namespace tiledb {
namespace sm {

class SumAggregatorStatusException : public StatusException {
 public:
  explicit SumAggregatorStatusException(const std::string& message)
      : StatusException("SumAggregator", message) {
  }
};

template <typename T>
SumAggregator<T>::SumAggregator(const FieldInfo field_info)
    : OutputBufferValidator(field_info)
    , field_info_(field_info)
    , summator_(field_info)
    , sum_(0)
    , validity_value_(
          field_info_.is_nullable_ ? std::make_optional(0) : nullopt)
    , sum_overflowed_(false) {
  if (field_info_.var_sized_) {
    throw SumAggregatorStatusException(
        "Sum aggregates must not be requested for var sized attributes.");
  }

  if (field_info_.cell_val_num_ != 1) {
    throw SumAggregatorStatusException(
        "Sum aggregates must not be requested for attributes with more than "
        "one value.");
  }
}

template <typename T>
void SumAggregator<T>::validate_output_buffer(
    std::string output_field_name,
    std::unordered_map<std::string, QueryBuffer>& buffers) {
  if (buffers.count(output_field_name) == 0) {
    throw SumAggregatorStatusException("Result buffer doesn't exist.");
  }

  validate_output_buffer_arithmetic(buffers[output_field_name]);
}

template <typename T>
void SumAggregator<T>::aggregate_data(AggregateBuffer& input_data) {
  // Return if a previous aggregation has overflowed.
  if (sum_overflowed_) {
    return;
  }

  try {
    tuple<typename sum_type_data<T>::sum_type, uint64_t, optional<uint8_t>> res{
        0, 0, nullopt};

    if (input_data.is_count_bitmap()) {
      res =
          summator_.template sum<typename sum_type_data<T>::sum_type, uint64_t>(
              input_data);
    } else {
      res =
          summator_.template sum<typename sum_type_data<T>::sum_type, uint8_t>(
              input_data);
    }

    safe_sum(std::get<0>(res), sum_);
    if (field_info_.is_nullable_ && std::get<2>(res).value() == 1) {
      validity_value_ = 1;
    }
  } catch (std::overflow_error& e) {
    sum_overflowed_ = true;
  }
}

template <typename T>
void SumAggregator<T>::copy_to_user_buffer(
    std::string output_field_name,
    std::unordered_map<std::string, QueryBuffer>& buffers) {
  auto& result_buffer = buffers[output_field_name];
  auto s =
      static_cast<typename sum_type_data<T>::sum_type*>(result_buffer.buffer_);
  if (sum_overflowed_) {
    *s = std::numeric_limits<typename sum_type_data<T>::sum_type>::max();
  } else {
    *s = sum_;
  }

  if (result_buffer.buffer_size_) {
    *result_buffer.buffer_size_ = sizeof(typename sum_type_data<T>::sum_type);
  }

  if (field_info_.is_nullable_) {
    auto v = static_cast<uint8_t*>(result_buffer.validity_vector_.buffer());
    if (sum_overflowed_) {
      *v = 0;
    } else {
      *v = validity_value_.value();
    }

    if (result_buffer.validity_vector_.buffer_size()) {
      *result_buffer.validity_vector_.buffer_size() = 1;
    }
  }
}

// Explicit template instantiations
template SumAggregator<int8_t>::SumAggregator(const FieldInfo);
template SumAggregator<int16_t>::SumAggregator(const FieldInfo);
template SumAggregator<int32_t>::SumAggregator(const FieldInfo);
template SumAggregator<int64_t>::SumAggregator(const FieldInfo);
template SumAggregator<uint8_t>::SumAggregator(const FieldInfo);
template SumAggregator<uint16_t>::SumAggregator(const FieldInfo);
template SumAggregator<uint32_t>::SumAggregator(const FieldInfo);
template SumAggregator<uint64_t>::SumAggregator(const FieldInfo);
template SumAggregator<float>::SumAggregator(const FieldInfo);
template SumAggregator<double>::SumAggregator(const FieldInfo);

}  // namespace sm
}  // namespace tiledb
