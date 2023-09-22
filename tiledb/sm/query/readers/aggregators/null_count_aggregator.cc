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

namespace tiledb::sm {

class NullCountAggregatorStatusException : public StatusException {
 public:
  explicit NullCountAggregatorStatusException(const std::string& message)
      : StatusException("NullCountAggregator", message) {
  }
};

NullCountAggregator::NullCountAggregator(const FieldInfo field_info)
    : OutputBufferValidator(field_info)
    , field_info_(field_info)
    , null_count_(0) {
  if (!field_info_.is_nullable_) {
    throw NullCountAggregatorStatusException(
        "NullCount aggregates must only be requested for nullable attributes.");
  }
}

void NullCountAggregator::validate_output_buffer(
    std::string output_field_name,
    std::unordered_map<std::string, QueryBuffer>& buffers) {
  if (buffers.count(output_field_name) == 0) {
    throw NullCountAggregatorStatusException("Result buffer doesn't exist.");
  }

  ensure_output_buffer_count(buffers[output_field_name]);
}

void NullCountAggregator::aggregate_data(AggregateBuffer& input_data) {
  if (input_data.is_count_bitmap()) {
    null_count_ += null_count<uint64_t>(input_data);
  } else {
    null_count_ += null_count<uint8_t>(input_data);
  }
}

void NullCountAggregator::copy_to_user_buffer(
    std::string output_field_name,
    std::unordered_map<std::string, QueryBuffer>& buffers) {
  auto& result_buffer = buffers[output_field_name];
  *static_cast<uint64_t*>(result_buffer.buffer_) = null_count_;

  if (result_buffer.buffer_size_) {
    *result_buffer.buffer_size_ = sizeof(uint64_t);
  }
}

template <typename BITMAP_T>
uint64_t NullCountAggregator::null_count(AggregateBuffer& input_data) {
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

}  // namespace tiledb::sm
