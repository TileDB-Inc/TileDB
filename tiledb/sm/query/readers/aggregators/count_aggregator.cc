/**
 * @file count_aggregator.cc
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
 * This file implements class CountAggregator.
 */

#include "tiledb/sm/query/readers/aggregators/count_aggregator.h"

#include "tiledb/sm/query/query_buffer.h"
#include "tiledb/sm/query/readers/aggregators/aggregate_buffer.h"

namespace tiledb {
namespace sm {

class CountAggregatorStatusException : public StatusException {
 public:
  explicit CountAggregatorStatusException(const std::string& message)
      : StatusException("CountAggregator", message) {
  }
};

CountAggregator::CountAggregator()
    : count_(0) {
}

void CountAggregator::validate_output_buffer(
    std::string output_field_name,
    std::unordered_map<std::string, QueryBuffer>& buffers) {
  if (buffers.count(output_field_name) == 0) {
    throw CountAggregatorStatusException("Result buffer doesn't exist.");
  }

  auto& result_buffer = buffers[output_field_name];
  if (result_buffer.buffer_ == nullptr) {
    throw CountAggregatorStatusException(
        "Count aggregates must have a fixed size buffer.");
  }

  if (result_buffer.buffer_var_ != nullptr) {
    throw CountAggregatorStatusException(
        "Count aggregates must not have a var buffer.");
  }

  if (result_buffer.original_buffer_size_ != 8) {
    throw CountAggregatorStatusException(
        "Count aggregates fixed size buffer should be for one element only.");
  }

  bool exists_validity = result_buffer.validity_vector_.buffer();
  if (exists_validity) {
    throw CountAggregatorStatusException(
        "Count aggregates must not have a validity buffer.");
  }
}

template <class BitmapType>
uint64_t count_cells(AggregateBuffer& input_data) {
  uint64_t ret = 0;

  auto bitmap_data = input_data.bitmap_data_as<BitmapType>();
  for (uint64_t c = input_data.min_cell(); c < input_data.max_cell(); c++) {
    ret += bitmap_data[c];
  }

  return ret;
}

void CountAggregator::aggregate_data(AggregateBuffer& input_data) {
  // Run different loops for bitmap versus no bitmap. The bitmap tells us which
  // cells was already filtered out by ranges or query conditions.
  if (input_data.has_bitmap()) {
    if (input_data.is_count_bitmap()) {
      count_ += count_cells<uint64_t>(input_data);
    } else {
      count_ += count_cells<uint8_t>(input_data);
    }
  } else {
    count_ += input_data.max_cell() - input_data.min_cell();
  }
}

void CountAggregator::copy_to_user_buffer(
    std::string output_field_name,
    std::unordered_map<std::string, QueryBuffer>& buffers) {
  auto& result_buffer = buffers[output_field_name];
  *static_cast<uint64_t*>(result_buffer.buffer_) = count_;

  if (result_buffer.buffer_size_) {
    *result_buffer.buffer_size_ = sizeof(uint64_t);
  }
}

}  // namespace sm
}  // namespace tiledb
