/**
 * @file min_max_aggregator.cc
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
 * This file implements class MinMaxAggregator.
 */

#include "tiledb/sm/query/readers/aggregators/min_max_aggregator.h"

#include "tiledb/sm/query/query_buffer.h"
#include "tiledb/sm/query/readers/aggregators/aggregate_buffer.h"

namespace tiledb::sm {

class MinMaxAggregatorStatusException : public StatusException {
 public:
  explicit MinMaxAggregatorStatusException(const std::string& message)
      : StatusException("MinMaxAggregator", message) {
  }
};

template <typename T>
void ComparatorAggregatorBase<T>::copy_to_user_buffer(
    std::string output_field_name,
    std::unordered_map<std::string, QueryBuffer>& buffers) const {
  auto& result_buffer = buffers[output_field_name];

  *static_cast<T*>(result_buffer.buffer_) = value_.value_or(0);
  if (result_buffer.buffer_size_) {
    *result_buffer.buffer_size_ = sizeof(T);
  }

  if (field_info_.is_nullable_) {
    *static_cast<uint8_t*>(result_buffer.validity_vector_.buffer()) =
        validity_value_.value();

    if (result_buffer.validity_vector_.buffer_size()) {
      *result_buffer.validity_vector_.buffer_size() = 1;
    }
  }
}

template <>
void ComparatorAggregatorBase<std::string>::copy_to_user_buffer(
    std::string output_field_name,
    std::unordered_map<std::string, QueryBuffer>& buffers) const {
  auto& result_buffer = buffers[output_field_name];

  if (field_info_.var_sized_) {
    // For var sized string, we need to set the offset value of 0 and the var
    // data buffer.
    *static_cast<uint64_t*>(result_buffer.buffer_) = 0;
    if (result_buffer.buffer_size_) {
      *result_buffer.buffer_size_ = constants::cell_var_offset_size;
    }

    if (value_.has_value()) {
      if (result_buffer.original_buffer_var_size_ < value_.value().size()) {
        throw MinMaxAggregatorStatusException(
            "Min/max buffer not big enough for " + field_info_.name_ +
            ". Required: " + std::to_string(value_.value().size()));
      }

      memcpy(
          result_buffer.buffer_var_,
          value_.value().data(),
          value_.value().size());
    }
  } else {
    if (value_.has_value()) {
      // For fixed size strings, just set the fixed buffer.
      memcpy(
          result_buffer.buffer_, value_.value().data(), value_.value().size());
    }
  }

  // Set the var buffer size.
  if (result_buffer.buffer_var_size_) {
    *result_buffer.buffer_var_size_ =
        value_.has_value() ? value_.value().size() : 0;
  }

  if (field_info_.is_nullable_) {
    *static_cast<uint8_t*>(result_buffer.validity_vector_.buffer()) =
        validity_value_.value();

    if (result_buffer.validity_vector_.buffer_size()) {
      *result_buffer.validity_vector_.buffer_size() = 1;
    }
  }
}

template <typename T, typename Op>
ComparatorAggregator<T, Op>::ComparatorAggregator(const FieldInfo& field_info)
    : ComparatorAggregatorBase<T>(field_info)
    , OutputBufferValidator(field_info)
    , aggregate_with_count_(field_info) {
  if constexpr (!std::is_same<T, std::string>::value) {
    ensure_field_numeric(field_info);
  }
}

template <typename T, typename Op>
void ComparatorAggregator<T, Op>::validate_output_buffer(
    std::string output_field_name,
    std::unordered_map<std::string, QueryBuffer>& buffers) {
  if (buffers.count(output_field_name) == 0) {
    throw MinMaxAggregatorStatusException("Result buffer doesn't exist.");
  }

  ensure_output_buffer_var<T>(buffers[output_field_name]);
}

template <typename T, typename Op>
void ComparatorAggregator<T, Op>::aggregate_data(AggregateBuffer& input_data) {
  tuple<VALUE_T, uint64_t> res;
  if (input_data.is_count_bitmap()) {
    res = aggregate_with_count_.template aggregate<uint64_t>(input_data);
  } else {
    res = aggregate_with_count_.template aggregate<uint8_t>(input_data);
  }

  const auto value = std::get<0>(res);
  const auto count = std::get<1>(res);
  update_value(value, count);
}

template <typename T, typename Op>
void ComparatorAggregator<T, Op>::aggregate_tile_with_frag_md(
    TileMetadata& tile_metadata) {
  const auto value = tile_metadata_value(tile_metadata);
  const auto count = tile_metadata.count() - tile_metadata.null_count();
  update_value(value, count);
}

template <typename T, typename Op>
void ComparatorAggregator<T, Op>::copy_to_user_buffer(
    std::string output_field_name,
    std::unordered_map<std::string, QueryBuffer>& buffers) {
  ComparatorAggregatorBase<T>::copy_to_user_buffer(output_field_name, buffers);
}

template <typename T, typename Op>
void ComparatorAggregator<T, Op>::update_value(VALUE_T value, uint64_t count) {
  {
    // This might be called on multiple threads, the final result stored in
    // value_ should be computed in a thread safe manner.
    std::unique_lock lock(value_mtx_);
    if (count > 0 &&
        (ComparatorAggregatorBase<T>::value_ == std::nullopt ||
         op_(value, ComparatorAggregatorBase<T>::value_.value()))) {
      ComparatorAggregatorBase<T>::value_ = value;
    }

    // Here we know that if the count is greater than 0, it means at least one
    // valid item was found, which means the result is valid.
    if (ComparatorAggregatorBase<T>::field_info_.is_nullable_ && count > 0) {
      ComparatorAggregatorBase<T>::validity_value_ = 1;
    }
  }
}

// Explicit template instantiations
template ComparatorAggregator<int8_t, std::less<int8_t>>::ComparatorAggregator(
    const FieldInfo&);
template ComparatorAggregator<int16_t, std::less<int16_t>>::
    ComparatorAggregator(const FieldInfo&);
template ComparatorAggregator<int32_t, std::less<int32_t>>::
    ComparatorAggregator(const FieldInfo&);
template ComparatorAggregator<int64_t, std::less<int64_t>>::
    ComparatorAggregator(const FieldInfo&);
template ComparatorAggregator<uint8_t, std::less<uint8_t>>::
    ComparatorAggregator(const FieldInfo&);
template ComparatorAggregator<uint16_t, std::less<uint16_t>>::
    ComparatorAggregator(const FieldInfo&);
template ComparatorAggregator<uint32_t, std::less<uint32_t>>::
    ComparatorAggregator(const FieldInfo&);
template ComparatorAggregator<uint64_t, std::less<uint64_t>>::
    ComparatorAggregator(const FieldInfo&);
template ComparatorAggregator<float, std::less<float>>::ComparatorAggregator(
    const FieldInfo&);
template ComparatorAggregator<double, std::less<double>>::ComparatorAggregator(
    const FieldInfo&);
template ComparatorAggregator<std::string, std::less<std::string_view>>::
    ComparatorAggregator(const FieldInfo&);
template ComparatorAggregator<int8_t, std::greater<int8_t>>::
    ComparatorAggregator(const FieldInfo&);
template ComparatorAggregator<int16_t, std::greater<int16_t>>::
    ComparatorAggregator(const FieldInfo&);
template ComparatorAggregator<int32_t, std::greater<int32_t>>::
    ComparatorAggregator(const FieldInfo&);
template ComparatorAggregator<int64_t, std::greater<int64_t>>::
    ComparatorAggregator(const FieldInfo&);
template ComparatorAggregator<uint8_t, std::greater<uint8_t>>::
    ComparatorAggregator(const FieldInfo&);
template ComparatorAggregator<uint16_t, std::greater<uint16_t>>::
    ComparatorAggregator(const FieldInfo&);
template ComparatorAggregator<uint32_t, std::greater<uint32_t>>::
    ComparatorAggregator(const FieldInfo&);
template ComparatorAggregator<uint64_t, std::greater<uint64_t>>::
    ComparatorAggregator(const FieldInfo&);
template ComparatorAggregator<float, std::greater<float>>::ComparatorAggregator(
    const FieldInfo&);
template ComparatorAggregator<double, std::greater<double>>::
    ComparatorAggregator(const FieldInfo&);
template ComparatorAggregator<std::string, std::greater<std::string_view>>::
    ComparatorAggregator(const FieldInfo&);
template MinAggregator<int8_t>::MinAggregator(const FieldInfo);
template MinAggregator<int16_t>::MinAggregator(const FieldInfo);
template MinAggregator<int32_t>::MinAggregator(const FieldInfo);
template MinAggregator<int64_t>::MinAggregator(const FieldInfo);
template MinAggregator<uint8_t>::MinAggregator(const FieldInfo);
template MinAggregator<uint16_t>::MinAggregator(const FieldInfo);
template MinAggregator<uint32_t>::MinAggregator(const FieldInfo);
template MinAggregator<uint64_t>::MinAggregator(const FieldInfo);
template MinAggregator<float>::MinAggregator(const FieldInfo);
template MinAggregator<double>::MinAggregator(const FieldInfo);
template MinAggregator<std::string>::MinAggregator(const FieldInfo);
template MaxAggregator<int8_t>::MaxAggregator(const FieldInfo);
template MaxAggregator<int16_t>::MaxAggregator(const FieldInfo);
template MaxAggregator<int32_t>::MaxAggregator(const FieldInfo);
template MaxAggregator<int64_t>::MaxAggregator(const FieldInfo);
template MaxAggregator<uint8_t>::MaxAggregator(const FieldInfo);
template MaxAggregator<uint16_t>::MaxAggregator(const FieldInfo);
template MaxAggregator<uint32_t>::MaxAggregator(const FieldInfo);
template MaxAggregator<uint64_t>::MaxAggregator(const FieldInfo);
template MaxAggregator<float>::MaxAggregator(const FieldInfo);
template MaxAggregator<double>::MaxAggregator(const FieldInfo);
template MaxAggregator<std::string>::MaxAggregator(const FieldInfo);

}  // namespace tiledb::sm
