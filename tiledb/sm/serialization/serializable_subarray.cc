/**
 * @file serializable_subarray.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022 TileDB, Inc.
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
 * This file defines the class SerializableSubarray.
 */

// clang-format off
#ifdef TILEDB_SERIALIZATION
#include <capnp/compat/json.h>
#include <capnp/message.h>
#include <capnp/serialize.h>
#include "tiledb/sm/serialization/capnp_utils.h"
#include "serializable_subarray.h"
#include "stats.h"
#endif
// clang-format on

#include "tiledb/common/common.h"
#include "tiledb/sm/enums/layout.h"
#include "tiledb/sm/subarray/range_subset.h"
#include "tiledb/sm/subarray/subarray.h"

namespace tiledb {
namespace sm {
namespace serialization {

#ifdef TILEDB_SERIALIZATION

extern shared_ptr<Logger> dummy_logger;

std::string SerializableSubarray::to_json() {
  ::capnp::MallocMessageBuilder message;
  auto builder = message.initRoot<capnp::Subarray>();

  to_capnp(builder);
  ::capnp::JsonCodec json;
  kj::String capnp_json = json.encode(builder);

  return std::string(capnp_json.cStr(), capnp_json.size());
}

void SerializableSubarray::to_capnp(capnp::Subarray::Builder& builder) {
  builder.setLayout(layout_str(layout_));

  const uint32_t dim_num = array_schema_.dim_num();
  auto ranges_builder = builder.initRanges(dim_num);
  for (uint32_t i = 0; i < dim_num; i++) {
    const auto datatype{array_schema_.dimension_ptr(i)->type()};
    auto range_builder = ranges_builder[i];
    const auto& ranges = range_subset_[i].ranges();
    range_builder.setType(datatype_str(datatype));

    range_builder.setHasDefaultRange(is_default_[i]);
    auto range_sizes = range_builder.initBufferSizes(ranges.size());
    auto range_start_sizes = range_builder.initBufferStartSizes(ranges.size());
    // This will copy all of the ranges into one large byte vector
    // Future improvement is to do this in a zero copy manner
    // (kj::ArrayBuilder?)
    auto capnpVector = kj::Vector<uint8_t>();
    uint64_t range_idx = 0;
    for (auto& range : ranges) {
      capnpVector.addAll(kj::ArrayPtr<uint8_t>(
          const_cast<uint8_t*>(reinterpret_cast<const uint8_t*>(range.data())),
          range.size()));
      range_sizes.set(range_idx, range.size());
      range_start_sizes.set(range_idx, range.start_size());
      ++range_idx;
    }
    range_builder.setBuffer(capnpVector.asPtr());
  }

  // If stats object exists set its cap'n proto object
  if (stats_ != nullptr) {
    auto stats_builder = builder.initStats();
    throw_if_not_ok(
        stats_to_capnp(*const_cast<Stats*>(stats_), &stats_builder));
  }

  if (relevant_fragments_.relevant_fragments_size() > 0) {
    auto relevant_fragments_builder = builder.initRelevantFragments(
        relevant_fragments_.relevant_fragments_size());
    for (size_t i = 0; i < relevant_fragments_.relevant_fragments_size(); ++i) {
      relevant_fragments_builder.set(i, relevant_fragments_[i]);
    }
  }
}

Subarray SerializableSubarray::from_capnp(
    const Array* array,
    stats::Stats* query_stats,
    const Layout query_layout,
    const capnp::Subarray::Reader& reader) {
  auto ranges_reader = reader.getRanges();
  uint32_t dim_num = ranges_reader.size();
  std::vector<bool> is_default(dim_num);
  std::vector<std::vector<Range>> ranges(dim_num);
  for (uint32_t i = 0; i < dim_num; i++) {
    auto range_reader = ranges_reader[i];
    Datatype type = Datatype::UINT8;

    if (!datatype_enum(range_reader.getType(), &type).ok()) {
      throw std::runtime_error("Invalid data type string");
    }

    auto data = range_reader.getBuffer();
    auto data_ptr = data.asBytes();
    if (range_reader.hasBufferSizes()) {
      auto buffer_sizes = range_reader.getBufferSizes();
      auto buffer_start_sizes = range_reader.getBufferStartSizes();
      size_t range_count = buffer_sizes.size();
      ranges[i].resize(range_count);
      uint64_t offset = 0;
      for (size_t j = 0; j < range_count; j++) {
        uint64_t range_size = buffer_sizes[j];
        uint64_t range_start_size = buffer_start_sizes[j];
        if (range_start_size != 0) {
          ranges[i][j] =
              Range(data_ptr.begin() + offset, range_size, range_start_size);
        } else {
          ranges[i][j] = Range(data_ptr.begin() + offset, range_size);
        }
        offset += range_size;
      }
    } else {
      // Handle 1.7 style ranges where there is a single range with no sizes
      ranges[i].resize(1);
      ranges[i][0] = Range(data_ptr.begin(), data.size());
    }
    is_default[i] = range_reader.getHasDefaultRange();
  }

  // If cap'n proto object has stats set it on c++ object
  stats::Stats* stats = nullptr;
  if (reader.hasStats()) {
    throw_if_not_ok(stats_from_capnp(reader.getStats(), stats));
  }

  std::vector<unsigned> relevant_fragments;
  if (reader.hasRelevantFragments()) {
    auto rel_fragments = reader.getRelevantFragments();
    size_t count = rel_fragments.size();
    std::vector<unsigned> rf;
    relevant_fragments.reserve(count);
    for (size_t i = 0; i < count; i++) {
      relevant_fragments.emplace_back(relevant_fragments[i]);
    }
  }

  return Subarray(
      array,
      query_layout,
      stats,
      dummy_logger,
      ranges,
      move(is_default),
      move(relevant_fragments));
}

#endif

}  // namespace serialization
}  // namespace sm
}  // namespace tiledb
