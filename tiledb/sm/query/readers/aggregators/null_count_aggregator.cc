/**
 * @file null_count_aggregator.cc
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
 * This file implements class NullCountAggregator.
 */

#include "tiledb/sm/query/readers/aggregators/null_count_aggregator.h"

#include "tiledb/sm/query/query_buffer.h"
#include "tiledb/sm/query/readers/aggregators/aggregate_buffer.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

class NullCountAggregatorStatusException : public StatusException {
 public:
  explicit NullCountAggregatorStatusException(const std::string& message)
      : StatusException("NullCountAggregator", message) {
  }
};

template <typename T>
NullCountAggregator<T>::NullCountAggregator(const FieldInfo field_info)
    : field_info_(field_info)
    , null_count_(0) {
  if (!field_info_.is_nullable_) {
    throw NullCountAggregatorStatusException(
        "NullCount aggregates must only be requested for nullable attributes.");
  }
}

template <typename T>
void NullCountAggregator<T>::validate_output_buffer(
    std::string output_field_name,
    std::unordered_map<std::string, QueryBuffer>& buffers) {
  if (buffers.count(output_field_name) == 0) {
    throw NullCountAggregatorStatusException("Result buffer doesn't exist.");
  }

  auto& result_buffer = buffers[output_field_name];
  if (result_buffer.buffer_ == nullptr) {
    throw NullCountAggregatorStatusException(
        "NullCount aggregates must have a fixed size buffer.");
  }

  if (result_buffer.buffer_var_ != nullptr) {
    throw NullCountAggregatorStatusException(
        "NullCount aggregates must not have a var buffer.");
  }

  if (result_buffer.original_buffer_size_ != 8) {
    throw NullCountAggregatorStatusException(
        "NullCount aggregates fixed size buffer should be for one element "
        "only.");
  }

  bool exists_validity = result_buffer.validity_vector_.buffer();
  if (exists_validity) {
    throw NullCountAggregatorStatusException(
        "NullCount aggregates must not have a validity buffer.");
  }
}

template <typename T>
void NullCountAggregator<T>::aggregate_data(AggregateBuffer& input_data) {
  if (input_data.is_count_bitmap()) {
    null_count_ += null_count<uint64_t>(input_data);
  } else {
    null_count_ += null_count<uint8_t>(input_data);
  }
}

template <typename T>
void NullCountAggregator<T>::copy_to_user_buffer(
    std::string output_field_name,
    std::unordered_map<std::string, QueryBuffer>& buffers) {
  auto& result_buffer = buffers[output_field_name];
  *static_cast<uint64_t*>(result_buffer.buffer_) = null_count_;

  if (result_buffer.buffer_size_) {
    *result_buffer.buffer_size_ = sizeof(uint64_t);
  }
}

template <typename T>
template <typename BITMAP_T>
uint64_t NullCountAggregator<T>::null_count(AggregateBuffer& input_data) {
  uint64_t null_count{0};

  // Run different loops for bitmap versus no bitmap. The bitmap tells us which
  // cells was already filtered out by ranges or query conditions.
  if (input_data.has_bitmap()) {
    auto bitmap_values = input_data.bitmap_data_as<BITMAP_T>();
    auto validity_values = input_data.validity_data();

    // Process with bitmap.
    for (uint64_t c = input_data.min_cell(); c < input_data.max_cell(); c++) {
      if (validity_values[c] == 0) {
        null_count += bitmap_values[c];
      }
    }
  } else {
    auto validity_values = input_data.validity_data();

    // Process with no bitmap.
    for (uint64_t c = input_data.min_cell(); c < input_data.max_cell(); c++) {
      if (validity_values[c] == 0) {
        null_count++;
      }
    }
  }

  return null_count;
}

// Explicit template instantiations
template NullCountAggregator<int8_t>::NullCountAggregator(const FieldInfo);
template NullCountAggregator<int16_t>::NullCountAggregator(const FieldInfo);
template NullCountAggregator<int32_t>::NullCountAggregator(const FieldInfo);
template NullCountAggregator<int64_t>::NullCountAggregator(const FieldInfo);
template NullCountAggregator<uint8_t>::NullCountAggregator(const FieldInfo);
template NullCountAggregator<uint16_t>::NullCountAggregator(const FieldInfo);
template NullCountAggregator<uint32_t>::NullCountAggregator(const FieldInfo);
template NullCountAggregator<uint64_t>::NullCountAggregator(const FieldInfo);
template NullCountAggregator<float>::NullCountAggregator(const FieldInfo);
template NullCountAggregator<double>::NullCountAggregator(const FieldInfo);
template NullCountAggregator<std::string>::NullCountAggregator(const FieldInfo);

}  // namespace sm
}  // namespace tiledb
