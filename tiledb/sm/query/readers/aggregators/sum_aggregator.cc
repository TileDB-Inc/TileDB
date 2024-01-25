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

namespace tiledb::sm {

class SumAggregatorStatusException : public StatusException {
 public:
  explicit SumAggregatorStatusException(const std::string& message)
      : StatusException("SumAggregator", message) {
  }
};

template <typename T>
SumWithCountAggregator<T>::SumWithCountAggregator(const FieldInfo field_info)
    : OutputBufferValidator(field_info)
    , field_info_(field_info)
    , aggregate_with_count_(field_info)
    , sum_(0)
    , validity_value_(
          field_info_.is_nullable_ ? std::make_optional(0) : nullopt)
    , sum_overflowed_(false) {
  ensure_field_numeric(field_info_);
}

template <typename T>
void SumWithCountAggregator<T>::validate_output_buffer(
    std::string output_field_name,
    std::unordered_map<std::string, QueryBuffer>& buffers) {
  if (buffers.count(output_field_name) == 0) {
    throw SumAggregatorStatusException("Result buffer doesn't exist.");
  }

  ensure_output_buffer_arithmetic(buffers[output_field_name]);
}

template <typename T>
void SumWithCountAggregator<T>::aggregate_data(AggregateBuffer& input_data) {
  // Return if a previous aggregation has overflowed.
  if (sum_overflowed_) {
    return;
  }

  tuple<typename sum_type_data<T>::sum_type, uint64_t> res;
  try {
    if (input_data.is_count_bitmap()) {
      res = aggregate_with_count_.template aggregate<uint64_t>(input_data);
    } else {
      res = aggregate_with_count_.template aggregate<uint8_t>(input_data);
    }
  } catch (std::overflow_error& e) {
    sum_overflowed_ = true;
  }

  const auto sum = std::get<0>(res);
  const auto count = std::get<1>(res);
  update_sum(sum, count);
}

template <typename T>
void SumWithCountAggregator<T>::aggregate_tile_with_frag_md(
    TileMetadata& tile_metadata) {
  const auto sum = tile_metadata.sum_as<SUM_T>();
  const auto count = tile_metadata.count() - tile_metadata.null_count();
  update_sum(sum, count);
}

template <typename T>
void SumWithCountAggregator<T>::copy_validity_value_to_user_buffers(
    QueryBuffer& result_buffer) {
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

template <typename T>
void SumWithCountAggregator<T>::update_sum(SUM_T sum, uint64_t count) {
  try {
    SafeSum().safe_sum(sum, sum_);
    count_ += count;

    // Here we know that if the count is greater than 0, it means at least one
    // valid item was found, which means the result is valid.
    if (field_info_.is_nullable_ && count) {
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
  if (SumWithCountAggregator<T>::sum_overflowed_) {
    *s = std::numeric_limits<typename sum_type_data<T>::sum_type>::max();
  } else {
    *s = SumWithCountAggregator<T>::sum_;
  }

  if (result_buffer.buffer_size_) {
    *result_buffer.buffer_size_ = sizeof(typename sum_type_data<T>::sum_type);
  }

  SumWithCountAggregator<T>::copy_validity_value_to_user_buffers(result_buffer);
}

template <typename T>
void MeanAggregator<T>::copy_to_user_buffer(
    std::string output_field_name,
    std::unordered_map<std::string, QueryBuffer>& buffers) {
  auto& result_buffer = buffers[output_field_name];
  auto s = static_cast<double*>(result_buffer.buffer_);

  if (SumWithCountAggregator<T>::sum_overflowed_) {
    *s = std::numeric_limits<double>::max();
  } else {
    *s = static_cast<double>(SumWithCountAggregator<T>::sum_) /
         SumWithCountAggregator<T>::count_;
  }

  if (result_buffer.buffer_size_) {
    *result_buffer.buffer_size_ = sizeof(double);
  }

  SumWithCountAggregator<T>::copy_validity_value_to_user_buffers(result_buffer);
}

// Explicit template instantiations
template SumWithCountAggregator<int8_t>::SumWithCountAggregator(
    const FieldInfo);
template SumWithCountAggregator<int16_t>::SumWithCountAggregator(
    const FieldInfo);
template SumWithCountAggregator<int32_t>::SumWithCountAggregator(
    const FieldInfo);
template SumWithCountAggregator<int64_t>::SumWithCountAggregator(
    const FieldInfo);
template SumWithCountAggregator<uint8_t>::SumWithCountAggregator(
    const FieldInfo);
template SumWithCountAggregator<uint16_t>::SumWithCountAggregator(
    const FieldInfo);
template SumWithCountAggregator<uint32_t>::SumWithCountAggregator(
    const FieldInfo);
template SumWithCountAggregator<uint64_t>::SumWithCountAggregator(
    const FieldInfo);
template SumWithCountAggregator<float>::SumWithCountAggregator(const FieldInfo);
template SumWithCountAggregator<double>::SumWithCountAggregator(
    const FieldInfo);

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

template MeanAggregator<int8_t>::MeanAggregator(const FieldInfo);
template MeanAggregator<int16_t>::MeanAggregator(const FieldInfo);
template MeanAggregator<int32_t>::MeanAggregator(const FieldInfo);
template MeanAggregator<int64_t>::MeanAggregator(const FieldInfo);
template MeanAggregator<uint8_t>::MeanAggregator(const FieldInfo);
template MeanAggregator<uint16_t>::MeanAggregator(const FieldInfo);
template MeanAggregator<uint32_t>::MeanAggregator(const FieldInfo);
template MeanAggregator<uint64_t>::MeanAggregator(const FieldInfo);
template MeanAggregator<float>::MeanAggregator(const FieldInfo);
template MeanAggregator<double>::MeanAggregator(const FieldInfo);

}  // namespace tiledb::sm
