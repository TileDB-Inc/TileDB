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

namespace tiledb::sm {

class CountAggregatorStatusException : public StatusException {
 public:
  explicit CountAggregatorStatusException(const std::string& message)
      : StatusException("CountAggregator", message) {
  }
};

template <class ValidityPolicy>
CountAggregatorBase<ValidityPolicy>::CountAggregatorBase(FieldInfo field_info)
    : OutputBufferValidator(field_info)
    , aggregate_with_count_(field_info)
    , count_(0) {
}

template <class ValidityPolicy>
void CountAggregatorBase<ValidityPolicy>::validate_output_buffer(
    std::string output_field_name,
    std::unordered_map<std::string, QueryBuffer>& buffers) {
  if (buffers.count(output_field_name) == 0) {
    throw CountAggregatorStatusException("Result buffer doesn't exist.");
  }

  ensure_output_buffer_count(buffers[output_field_name]);
}

template <class ValidityPolicy>
void CountAggregatorBase<ValidityPolicy>::aggregate_data(
    AggregateBuffer& input_data) {
  tuple<uint8_t, uint64_t> res;

  if (input_data.is_count_bitmap()) {
    res = aggregate_with_count_.template aggregate<uint64_t>(input_data);
  } else {
    res = aggregate_with_count_.template aggregate<uint8_t>(input_data);
  }

  count_ += std::get<1>(res);
}

template <class ValidityPolicy>
void CountAggregatorBase<ValidityPolicy>::aggregate_tile_with_frag_md(
    TileMetadata& tile_metadata) {
  uint64_t count;
  if constexpr (std::is_same<ValidityPolicy, NonNull>::value) {
    count = tile_metadata.count();
  } else {
    count = tile_metadata.null_count();
  }

  count_ += count;
}

template <class ValidityPolicy>
void CountAggregatorBase<ValidityPolicy>::copy_to_user_buffer(
    std::string output_field_name,
    std::unordered_map<std::string, QueryBuffer>& buffers) {
  auto& result_buffer = buffers[output_field_name];
  *static_cast<uint64_t*>(result_buffer.buffer_) = count_;

  if (result_buffer.buffer_size_) {
    *result_buffer.buffer_size_ = sizeof(uint64_t);
  }
}

NullCountAggregator::NullCountAggregator(FieldInfo field_info)
    : CountAggregatorBase(field_info)
    , field_info_(field_info) {
  ensure_field_nullable(field_info);
}

// Explicit template instantiations
template CountAggregatorBase<Null>::CountAggregatorBase(FieldInfo);
template CountAggregatorBase<NonNull>::CountAggregatorBase(FieldInfo);

}  // namespace tiledb::sm
