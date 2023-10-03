/**
 * @file mean_aggregator.cc
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
 * This file implements class MeanAggregator.
 */

#include "tiledb/sm/query/readers/aggregators/mean_aggregator.h"

#include "tiledb/sm/query/query_buffer.h"
#include "tiledb/sm/query/readers/aggregators/aggregate_buffer.h"
#include "tiledb/sm/query/readers/aggregators/safe_sum.h"

namespace tiledb::sm {

class MeanAggregatorStatusException : public StatusException {
 public:
  explicit MeanAggregatorStatusException(const std::string& message)
      : StatusException("MeanAggregator", message) {
  }
};

template <typename T>
MeanAggregator<T>::MeanAggregator(const FieldInfo field_info)
    : OutputBufferValidator(field_info)
    , field_info_(field_info)
    , aggregate_with_count_(field_info)
    , sum_(0)
    , count_(0)
    , validity_value_(
          field_info_.is_nullable_ ? std::make_optional(0) : nullopt)
    , sum_overflowed_(false) {
  // TODO: These argument validation can be merged for mean/sum and possibly
  // other aggregates. (sc-33763).
  if (field_info_.var_sized_) {
    throw MeanAggregatorStatusException(
        "Mean aggregates are not supported for var sized attributes.");
  }

  if (field_info_.cell_val_num_ != 1) {
    throw MeanAggregatorStatusException(
        "Mean aggregates are not supported for attributes with cell_val_num "
        "greater than one.");
  }
}

template <typename T>
void MeanAggregator<T>::validate_output_buffer(
    std::string output_field_name,
    std::unordered_map<std::string, QueryBuffer>& buffers) {
  if (buffers.count(output_field_name) == 0) {
    throw MeanAggregatorStatusException("Result buffer doesn't exist.");
  }

  ensure_output_buffer_arithmetic(buffers[output_field_name]);
}

template <typename T>
void MeanAggregator<T>::aggregate_data(AggregateBuffer& input_data) {
  // TODO: While this could be shared with the sum implementation, it isn't
  // because it will be improved soon... Means should always be able to be
  // computed with no overflows. The simple formula is 2*(a/2 + b/2) + a%2 +
  // b%2, but we need benchmarks to see what the performance hit would be to run
  // this more complex formula (sc-32789).

  // Return if a previous aggregation has overflowed.
  if (sum_overflowed_) {
    return;
  }

  try {
    tuple<typename sum_type_data<T>::sum_type, uint64_t> res;

    // TODO: This is duplicated across aggregates but will go away with
    // sc-33104.
    if (input_data.is_count_bitmap()) {
      res = aggregate_with_count_.template aggregate<
          typename sum_type_data<T>::sum_type,
          uint64_t,
          SafeSum>(input_data);
    } else {
      res = aggregate_with_count_.template aggregate<
          typename sum_type_data<T>::sum_type,
          uint8_t,
          SafeSum>(input_data);
    }

    SafeSum().safe_sum(std::get<0>(res), sum_);
    count_ += std::get<1>(res);
    if (field_info_.is_nullable_ && std::get<1>(res) > 0) {
      validity_value_ = 1;
    }
  } catch (std::overflow_error& e) {
    sum_overflowed_ = true;
  }
}

template <typename T>
void MeanAggregator<T>::copy_to_user_buffer(
    std::string output_field_name,
    std::unordered_map<std::string, QueryBuffer>& buffers) {
  auto& result_buffer = buffers[output_field_name];
  auto s = static_cast<double*>(result_buffer.buffer_);

  if (sum_overflowed_) {
    *s = std::numeric_limits<double>::max();
  } else {
    *s = static_cast<double>(sum_) / count_;
  }

  if (result_buffer.buffer_size_) {
    *result_buffer.buffer_size_ = sizeof(double);
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
