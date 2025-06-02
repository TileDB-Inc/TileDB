/**
 * @file   ordered_dim_label_reader.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022-2024 TileDB, Inc.
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
 * This file implements class OrderedDimLabelReader.
 */
#include "tiledb/sm/query/readers/ordered_dim_label_reader.h"

#include "tiledb/common/assert.h"
#include "tiledb/common/logger.h"
#include "tiledb/sm/array/array.h"
#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/array_schema/dimension.h"
#include "tiledb/sm/fragment/fragment_metadata.h"
#include "tiledb/sm/misc/parallel_functions.h"
#include "tiledb/sm/query/legacy/cell_slab_iter.h"
#include "tiledb/sm/query/query_macros.h"
#include "tiledb/sm/query/readers/filtered_data.h"
#include "tiledb/sm/query/readers/result_tile.h"
#include "tiledb/sm/stats/global_stats.h"
#include "tiledb/sm/subarray/subarray.h"
#include "tiledb/type/apply_with_type.h"

#include <numeric>

using namespace tiledb;
using namespace tiledb::common;
using namespace tiledb::sm::stats;

namespace tiledb::sm {

class OrderedDimLabelReaderException : public StatusException {
 public:
  explicit OrderedDimLabelReaderException(const std::string& message)
      : StatusException("OrderedDimLabelReader", message) {
  }
};

/* ****************************** */
/*          CONSTRUCTORS          */
/* ****************************** */

OrderedDimLabelReader::OrderedDimLabelReader(
    stats::Stats* stats,
    shared_ptr<Logger> logger,
    StrategyParams& params,
    bool increasing_labels)
    : ReaderBase(
          stats, logger->clone("OrderedDimLabelReader", ++logger_id_), params)
    , ranges_(
          subarray_.get_attribute_ranges(array_schema_.attributes()[0]->name()))
    , label_name_(array_schema_.attributes()[0]->name())
    , label_type_(array_schema_.attributes()[0]->type())
    , label_var_size_(array_schema_.attributes()[0]->var_size())
    , increasing_labels_(increasing_labels)
    , index_dim_(array_schema_.domain().dimension_ptr(0))
    , result_tiles_(fragment_metadata_.size()) {
  // Sanity checks.
  if (!params.default_channel_aggregates().empty()) {
    throw OrderedDimLabelReaderException(
        "Cannot initialize reader; Reader cannot process aggregates");
  }

  if (!params.skip_checks_serialization() && buffers_.empty()) {
    throw OrderedDimLabelReaderException(
        "Cannot initialize ordered dim label reader; Buffers not set");
  }

  if (!params.skip_checks_serialization() && buffers_.size() != 1) {
    throw OrderedDimLabelReaderException(
        "Cannot initialize ordered dim label reader with " +
        std::to_string(buffers_.size()) + " buffers; Only one buffer allowed");
  }

  for (const auto& b : buffers_) {
    if (b.first != index_dim_->name()) {
      throw OrderedDimLabelReaderException(
          "Cannot initialize ordered dim label reader; Wrong buffer set");
    }

    if (*b.second.buffer_size_ !=
        ranges_.size() * 2 * datatype_size(index_dim_->type())) {
      throw OrderedDimLabelReaderException(
          "Cannot initialize ordered dim label reader; Wrong buffer size");
    }

    if (b.second.buffer_var_size_ != 0) {
      throw OrderedDimLabelReaderException(
          "Cannot initialize ordered dim label reader; Wrong buffer var size");
    }
  }

  if (subarray_.is_set()) {
    throw OrderedDimLabelReaderException(
        "Cannot initialize ordered dim label reader; Subarray is set");
  }

  if (condition_.has_value()) {
    throw OrderedDimLabelReaderException(
        "Ordered dimension label reader cannot process query condition");
  }

  memory_budget_ =
      config_.get<uint64_t>("sm.mem.total_budget", Config::must_find);
}

/* ****************************** */
/*               API              */
/* ****************************** */

bool OrderedDimLabelReader::incomplete() const {
  return false;
}

QueryStatusDetailsReason OrderedDimLabelReader::status_incomplete_reason()
    const {
  return QueryStatusDetailsReason::REASON_NONE;
}

void OrderedDimLabelReader::refresh_config() {
}

Status OrderedDimLabelReader::dowork() {
  auto timer_se = stats_->start_timer("dowork");

  get_dim_attr_stats();
  reset_buffer_sizes();

  // Load tile offsets and tile var sizes. This will update `tile_offsets_`,
  // `tile_var_offsets_`, `tile_validity_offsets_` and `tile_var_sizes_` in
  // `fragment_metadata_`.
  std::vector<std::string> names = {label_name_};
  load_tile_offsets(subarray_.relevant_fragments(), names);
  load_tile_var_sizes(subarray_.relevant_fragments(), names);

  // Load the dimension labels min/max values.
  load_label_min_max_values();

  // Do the read.
  label_read();

  return Status::Ok();
}

void OrderedDimLabelReader::reset() {
}

std::string OrderedDimLabelReader::name() {
  return "OrderedDimLabelReader";
}

/* ********************************* */
/*           PRIVATE METHODS         */
/* ********************************* */

void OrderedDimLabelReader::label_read() {
  // Do the label read depending on the index type.
  auto type{index_dim_->type()};

  auto g = [&](auto T) {
    if constexpr (tiledb::type::TileDBIntegral<decltype(T)>) {
      label_read<decltype(T)>();
    } else {
      throw OrderedDimLabelReaderException(
          "Cannot read ordered label array; Unsupported domain type");
    }
  };
  apply_with_type(g, type);
}

template <typename IndexType>
void OrderedDimLabelReader::label_read() {
  // Sanity checks.
  iassert(std::is_integral<IndexType>::value);

  // Handle empty array.
  if (fragment_metadata_.empty()) {
    throw OrderedDimLabelReaderException(
        "Cannot read dim label; Dimension label is empty");
  }

  // Precompute data.
  auto&& [non_empty_domain, non_empty_domains, frag_first_array_tile_idx] =
      cache_dimension_label_data<IndexType>();
  non_empty_domain_ = std::move(non_empty_domain);
  non_empty_domains_ = std::move(non_empty_domains);
  frag_first_array_tile_idx_ = std::move(frag_first_array_tile_idx);
  compute_array_tile_indexes_for_ranges<IndexType>();

  // Validate order of the label data.
  validate_attribute_order<IndexType>(
      label_type_,
      label_name_,
      increasing_labels_,
      non_empty_domain_,
      non_empty_domains_,
      frag_first_array_tile_idx_);

  // Save the offsets into the user buffer if we make more than one iteration
  // because of memory budgetting.
  uint64_t buffer_offset = 0;
  do {
    stats_->add_counter("loop_num", 1);

    // Create result tiles.
    uint64_t max_range = create_result_tiles<IndexType>();
    std::vector<ResultTile*> result_tiles;
    for (auto& result_tile_map : result_tiles_) {
      for (auto& result_tile : result_tile_map) {
        result_tiles.push_back(&result_tile.second);
      }
    }
    std::sort(result_tiles.begin(), result_tiles.end(), result_tile_cmp);

    // Read/unfilter tiles.
    throw_if_not_ok(
        read_and_unfilter_attribute_tiles({label_name_}, result_tiles));

    // Compute/copy results.
    throw_if_not_ok(
        parallel_for(&resources_.compute_tp(), 0, max_range, [&](uint64_t r) {
          compute_and_copy_range_indexes<IndexType>(buffer_offset, r);
          return Status::Ok();
        }));

    // Truncate ranges_ for the next iteration.
    for (auto& rt_map : result_tiles_) {
      rt_map.clear();
    }
    ranges_.erase(ranges_.begin(), ranges_.begin() + max_range);
    per_range_array_tile_indexes_.erase(
        per_range_array_tile_indexes_.begin(),
        per_range_array_tile_indexes_.begin() + max_range);

    // Move the offset into the user buffer for the next iteration.
    buffer_offset += max_range;
  } while (ranges_.size() > 0);
}

template <typename IndexType>
void OrderedDimLabelReader::compute_array_tile_indexes_for_ranges() {
  auto timer_se = stats_->start_timer("compute_array_tile_indexes_for_ranges");

  // Save the minimum/maximum tile indexes (in the full domain) to be used
  // later.
  auto tile_extent = index_dim_->tile_extent().rvalue_as<IndexType>();
  const IndexType* dim_dom = index_dim_->domain().typed_data<IndexType>();
  auto array_non_empty_domain = non_empty_domain_.typed_data<IndexType>();
  auto tile_idx_min =
      index_dim_->tile_idx(array_non_empty_domain[0], dim_dom[0], tile_extent);
  auto tile_idx_max =
      index_dim_->tile_idx(array_non_empty_domain[1], dim_dom[0], tile_extent);

  // Compute the tile (by index) where the label values include each range
  // start and end. This is stored in the following value, by fragment/range.
  std::vector<std::vector<FragmentRangeTileIndexes>>
      per_range_array_tile_indexes(ranges_.size());
  for (uint64_t r = 0; r < ranges_.size(); r++) {
    per_range_array_tile_indexes[r].resize(fragment_metadata_.size());
  }
  throw_if_not_ok(parallel_for_2d(
      &resources_.compute_tp(),
      0,
      fragment_metadata_.size(),
      0,
      ranges_.size(),
      [&](uint64_t f, uint64_t r) {
        per_range_array_tile_indexes[r][f] =
            get_array_tile_indexes_for_range(f, r);
        return Status::Ok();
      }));

  // Compute the tile indexes (min/max) that can potentially contain the label
  // value for each range start/end.
  per_range_array_tile_indexes_.resize(ranges_.size());
  throw_if_not_ok(parallel_for(
      &resources_.compute_tp(), 0, ranges_.size(), [&](uint64_t r) {
        per_range_array_tile_indexes_[r] = RangeTileIndexes(
            tile_idx_min, tile_idx_max, per_range_array_tile_indexes[r]);
        return Status::Ok();
      }));
}

void OrderedDimLabelReader::load_label_min_max_values() {
  auto timer_se = stats_->start_timer("load_label_min_max_values");
  const auto encryption_key = array_->encryption_key();

  // Load min/max data for all fragments.
  throw_if_not_ok(parallel_for(
      &resources_.compute_tp(),
      0,
      fragment_metadata_.size(),
      [&](const uint64_t i) {
        auto& fragment = fragment_metadata_[i];
        std::vector<std::string> names = {label_name_};
        fragment->loaded_metadata()->load_tile_min_values(
            *encryption_key, names);
        fragment->loaded_metadata()->load_tile_max_values(
            *encryption_key, names);
        return Status::Ok();
      }));
}

template <typename LabelType>
OrderedDimLabelReader::FragmentRangeTileIndexes
OrderedDimLabelReader::get_array_tile_indexes_for_range(
    unsigned f, uint64_t r) {
  const auto tile_num = fragment_metadata_[f]->tile_num();
  uint64_t start_index = increasing_labels_ ? 0 : tile_num - 1,
           end_index = increasing_labels_ ? tile_num - 1 : 0;

  const auto start_range = get_range_as<LabelType>(r, 0);
  const auto end_range = get_range_as<LabelType>(r, 1);

  // Check if either the start or end range is fully excluded from the fragment.
  IndexValueType start_val_type = IndexValueType::CONTAINED,
                 end_val_type = IndexValueType::CONTAINED;

  if (increasing_labels_) {
    const auto min =
        fragment_metadata_[f]->get_tile_min_as<LabelType>(label_name_, 0);
    const auto max = fragment_metadata_[f]->get_tile_max_as<LabelType>(
        label_name_, tile_num - 1);

    if (start_range < min) {
      start_val_type = IndexValueType::LT;
    } else if (start_range > max) {
      start_val_type = IndexValueType::GT;
    }

    if (end_range < min) {
      end_val_type = IndexValueType::LT;
    } else if (end_range > max) {
      end_val_type = IndexValueType::GT;
    }
  } else {
    const auto min = fragment_metadata_[f]->get_tile_min_as<LabelType>(
        label_name_, tile_num - 1);
    const auto max =
        fragment_metadata_[f]->get_tile_max_as<LabelType>(label_name_, 0);
    if (start_range > max) {
      start_val_type = IndexValueType::LT;
    } else if (start_range < min) {
      start_val_type = IndexValueType::GT;
    }

    if (end_range > max) {
      end_val_type = IndexValueType::LT;
    } else if (end_range < min) {
      end_val_type = IndexValueType::GT;
    }
  }

  // If the start range is included, find in which tile.
  if (start_val_type == IndexValueType::CONTAINED) {
    if (increasing_labels_) {
      for (; start_index < tile_num; start_index++) {
        const auto max = fragment_metadata_[f]->get_tile_max_as<LabelType>(
            label_name_, start_index);
        if (max >= start_range) {
          break;
        }
      }
    } else {
      for (;; start_index--) {
        const auto max = fragment_metadata_[f]->get_tile_max_as<LabelType>(
            label_name_, start_index);
        if (start_index == 0 || max >= start_range) {
          break;
        }
      }
    }
  }

  // If the end range is included, find in which tile.
  if (end_val_type == IndexValueType::CONTAINED) {
    if (increasing_labels_) {
      for (;; end_index--) {
        const auto min = fragment_metadata_[f]->get_tile_min_as<LabelType>(
            label_name_, end_index);
        if (end_index == 0 || min <= end_range) {
          break;
        }
      }
    } else {
      for (; end_index < tile_num; end_index++) {
        const auto min = fragment_metadata_[f]->get_tile_min_as<LabelType>(
            label_name_, end_index);
        if (min <= end_range) {
          break;
        }
      }
    }
  }

  return FragmentRangeTileIndexes(
      start_index + frag_first_array_tile_idx_[f],
      start_val_type,
      end_index + frag_first_array_tile_idx_[f],
      end_val_type);
}

OrderedDimLabelReader::FragmentRangeTileIndexes
OrderedDimLabelReader::get_array_tile_indexes_for_range(
    unsigned f, uint64_t r) {
  auto g = [&](auto T) {
    if constexpr (std::is_same_v<decltype(T), char>) {
      return get_array_tile_indexes_for_range<std::string_view>(f, r);
    } else if constexpr (tiledb::type::TileDBFundamental<decltype(T)>) {
      return get_array_tile_indexes_for_range<decltype(T)>(f, r);
    }
  };
  return apply_with_type(g, label_type_);
}

uint64_t OrderedDimLabelReader::label_tile_size(unsigned f, uint64_t t) const {
  uint64_t tile_size = fragment_metadata_[f]->tile_size(label_name_, t);
  if (label_var_size_) {
    tile_size +=
        fragment_metadata_[f]->loaded_metadata()->tile_var_size(label_name_, t);
  }

  return tile_size;
}

template <typename IndexType>
bool OrderedDimLabelReader::tile_overwritten(
    unsigned frag_idx,
    uint64_t tile_idx,
    const IndexType& domain_low,
    const IndexType& tile_extent) const {
  // Compute the first and last index for this tile.
  IndexType tile_range[2]{
      index_dim_->tile_coord_low(tile_idx, domain_low, tile_extent),
      index_dim_->tile_coord_high(tile_idx, domain_low, tile_extent)};
  Range r(tile_range, 2 * sizeof(IndexType));

  // Use the non empty domains for all fragments to see if the tile is
  // covered.
  auto fragment_num = (unsigned)fragment_metadata_.size();
  for (unsigned f = frag_idx + 1; f < fragment_num; ++f) {
    if (index_dim_->covered(r, fragment_metadata_[f]->non_empty_domain()[0])) {
      return true;
    }
  }

  return false;
}

template <typename IndexType>
uint64_t OrderedDimLabelReader::create_result_tiles() {
  auto timer_se = stats_->start_timer("create_result_tiles");

  uint64_t max_range = 0;
  uint64_t total_mem_used = 0;
  const IndexType* dim_dom = index_dim_->domain().typed_data<IndexType>();
  auto tile_extent = index_dim_->tile_extent().rvalue_as<IndexType>();

  // Set of covered tiles, per fragment. The unordered set is for tile
  // indexes.
  std::vector<std::unordered_set<uint64_t>> covered_tiles(
      fragment_metadata_.size());

  // Process ranges one by one.
  for (uint64_t r = 0; r < ranges_.size(); r++) {
    // Add tiles for each fragment.
    for (unsigned f = 0; f < fragment_metadata_.size(); f++) {
      // Add the tiles for the start/end range.
      for (uint8_t range_index = 0; range_index < 2; range_index++) {
        for (uint64_t tile_idx =
                 per_range_array_tile_indexes_[r].min(range_index);
             tile_idx <= per_range_array_tile_indexes_[r].max(range_index);
             tile_idx++) {
          if ((tile_idx >= frag_first_array_tile_idx_[f]) &&
              (tile_idx < frag_first_array_tile_idx_[f] +
                              fragment_metadata_[f]->tile_num()) &&
              (result_tiles_[f].count(tile_idx) == 0) &&
              (covered_tiles[f].count(tile_idx) == 0)) {
            // Make sure the tile can fit in the budget.
            uint64_t frag_tile_idx = tile_idx - frag_first_array_tile_idx_[f];
            uint64_t tile_size = label_tile_size(f, frag_tile_idx);
            bool covered = tile_overwritten<IndexType>(
                f, tile_idx, dim_dom[0], tile_extent);
            if (covered) {
              covered_tiles[f].emplace(tile_idx);
            } else if (total_mem_used + tile_size < memory_budget_) {
              total_mem_used += tile_size;
              result_tiles_[f].emplace(
                  std::piecewise_construct,
                  std::forward_as_tuple(tile_idx),
                  std::forward_as_tuple(
                      f,
                      frag_tile_idx,
                      *fragment_metadata_[f].get(),
                      query_memory_tracker_));
            } else {
              if (r == 0) {
                throw OrderedDimLabelReaderException(
                    "Can't process a single range requiring " +
                    std::to_string(tile_size) +
                    " bytes, increase memory budget(" +
                    std::to_string(memory_budget_) + ")");
              }
              return r;
            }
          }
        }
      }
    }
    max_range = r;
  }

  return max_range + 1;
}

template <typename LabelType>
LabelType OrderedDimLabelReader::get_label_value(
    const unsigned f, const uint64_t tile_idx, const uint64_t cell_idx) {
  auto& rt = result_tiles_[f].at(tile_idx);
  return rt.attribute_value<LabelType>(label_name_, cell_idx);
}

template <typename IndexType, typename LabelType>
LabelType OrderedDimLabelReader::get_value_at(
    const IndexType& index,
    const IndexType& domain_low,
    const IndexType& tile_extent) {
  // Start with the most recent fragment.
  for (auto f = static_cast<unsigned>(fragment_metadata_.size() - 1);; f--) {
    const IndexType* non_empty_domain =
        static_cast<const IndexType*>(non_empty_domains_[f]);
    // If the value is in the non empty domain for the fragment, get it.
    if (index >= non_empty_domain[0] && index <= non_empty_domain[1]) {
      // Get the tile index in the current fragment.
      const auto tile_idx =
          index_dim_->tile_idx(index, domain_low, tile_extent);

      // Get the cell index in the current tile.
      const auto cell_idx =
          index - index_dim_->tile_coord_low(tile_idx, domain_low, tile_extent);

      // Finally get the data.
      return get_label_value<LabelType>(f, tile_idx, cell_idx);
    }

    // We should always find the value before the last fragment.
    if (f == 0) {
      throw OrderedDimLabelReaderException("Couldn't find value");
    }
  }
}

template <typename LabelType>
LabelType OrderedDimLabelReader::get_range_as(uint64_t r, uint8_t range_index) {
  return ranges_[r].typed_data<LabelType>()[range_index];
}

template <>
std::string_view OrderedDimLabelReader::get_range_as<std::string_view>(
    uint64_t r, uint8_t range_index) {
  if (range_index == 0) {
    return ranges_[r].start_str();
  } else {
    return ranges_[r].end_str();
  }
}

template <typename IndexType, typename LabelType, typename Op>
IndexType OrderedDimLabelReader::search_for_range(
    uint64_t r,
    uint8_t range_index,
    const IndexType& domain_low,
    const IndexType& tile_extent) {
  // Get the value we are looking for.
  LabelType value = get_range_as<LabelType>(r, range_index);

  // Minimum index to look into.
  auto non_empty_domain = non_empty_domain_.typed_data<IndexType>();
  auto t_min = per_range_array_tile_indexes_[r].min(range_index);
  auto left_index = std::max(
      index_dim_->tile_coord_low(t_min, domain_low, tile_extent),
      non_empty_domain[0]);

  // Maximum index to look into.
  auto t_max = per_range_array_tile_indexes_[r].max(range_index);
  auto right_index = std::min(
      index_dim_->tile_coord_high(t_max, domain_low, tile_extent),
      non_empty_domain[1]);

  // Run a binary search.
  Op cmp;
  while (left_index + 1 < right_index) {
    // Check against mid.
    IndexType mid = left_index + (right_index - left_index) / 2;
    if (cmp(get_value_at<IndexType, LabelType>(mid, domain_low, tile_extent),
            value)) {
      right_index = mid;
    } else {
      left_index = mid;
    }
  }

  // Do one last comparison to decide to return left or right. If finding the
  // smaller range value, check if the left bound is within the value range.
  // If finding the larger range value, check if the right value is within the
  // value range.
  IndexType bound;
  if (increasing_labels_) {
    bound = range_index == 0 ? left_index : right_index;
  } else {
    bound = range_index == 0 ? right_index : left_index;
  }

  return (cmp(
             get_value_at<IndexType, LabelType>(bound, domain_low, tile_extent),
             value)) ?
             left_index :
             right_index;
}

template <typename IndexType, typename LabelType>
void OrderedDimLabelReader::compute_and_copy_range_indexes(
    IndexType* dest, uint64_t r) {
  // For easy reference.
  auto tile_extent = index_dim_->tile_extent().rvalue_as<IndexType>();
  const IndexType* dim_dom = index_dim_->domain().typed_data<IndexType>();
  auto non_empty_domain = non_empty_domain_.typed_data<IndexType>();

  // Set the results.
  if (increasing_labels_) {
    dest[0] =
        search_for_range<IndexType, LabelType, std::greater_equal<LabelType>>(
            r, 0, dim_dom[0], tile_extent);

    // If the result is the last index, make sure the range includes it.
    if (dest[0] == non_empty_domain[1]) {
      LabelType value = get_range_as<LabelType>(r, 0);
      if (get_value_at<IndexType, LabelType>(dest[0], dim_dom[0], tile_extent) <
          value) {
        throw OrderedDimLabelReaderException("Range contained no values");
      }
    }

    dest[1] = search_for_range<IndexType, LabelType, std::greater<LabelType>>(
        r, 1, dim_dom[0], tile_extent);

    // If the result is the first index, make sure the range includes it.
    if (dest[1] == non_empty_domain[0]) {
      LabelType value = get_range_as<LabelType>(r, 1);
      if (get_value_at<IndexType, LabelType>(dest[1], dim_dom[0], tile_extent) >
          value) {
        throw OrderedDimLabelReaderException("Range contained no values");
      }
    }
  } else {
    dest[0] =
        search_for_range<IndexType, LabelType, std::less_equal<LabelType>>(
            r, 1, dim_dom[0], tile_extent);

    // If the result is the last index, make sure the range includes it.
    if (dest[0] == non_empty_domain[1]) {
      LabelType value = get_range_as<LabelType>(r, 1);
      if (get_value_at<IndexType, LabelType>(dest[0], dim_dom[0], tile_extent) >
          value) {
        throw OrderedDimLabelReaderException("Range contained no values");
      }
    }

    dest[1] = search_for_range<IndexType, LabelType, std::less<LabelType>>(
        r, 0, dim_dom[0], tile_extent);

    // If the result is the first index, make sure the range includes it.
    if (dest[1] == non_empty_domain[0]) {
      LabelType value = get_range_as<LabelType>(r, 0);
      if (get_value_at<IndexType, LabelType>(dest[1], dim_dom[0], tile_extent) <
          value) {
        throw OrderedDimLabelReaderException("Range contained no values");
      }
    }
  }

  // If the range provided contained no values, throw an error.
  if (dest[0] > dest[1]) {
    throw OrderedDimLabelReaderException("Range contained no values");
  }
}

template <typename IndexType>
void OrderedDimLabelReader::compute_and_copy_range_indexes(
    uint64_t buffer_offset, uint64_t r) {
  auto timer_se = stats_->start_timer("compute_and_copy_range_indexes");

  auto dest = static_cast<IndexType*>(buffers_[index_dim_->name()].buffer_) +
              (buffer_offset + r) * 2;

  auto g = [&](auto T) {
    if constexpr (std::is_same_v<decltype(T), char>) {
      compute_and_copy_range_indexes<IndexType, std::string_view>(dest, r);
    } else if constexpr (tiledb::type::TileDBFundamental<decltype(T)>) {
      compute_and_copy_range_indexes<IndexType, decltype(T)>(dest, r);
    }
  };
  apply_with_type(g, label_type_);
}

}  // namespace tiledb::sm
