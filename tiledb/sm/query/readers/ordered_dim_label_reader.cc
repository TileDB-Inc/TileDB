/**
 * @file   ordered_dim_label_reader.cc
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
 * This file implements class OrderedDimLabelReader.
 */

#include "tiledb/common/logger.h"

#include "tiledb/sm/array/array.h"
#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/array_schema/dimension.h"
#include "tiledb/sm/fragment/fragment_metadata.h"
#include "tiledb/sm/misc/parallel_functions.h"
#include "tiledb/sm/misc/utils.h"
#include "tiledb/sm/query/legacy/cell_slab_iter.h"
#include "tiledb/sm/query/query_macros.h"
#include "tiledb/sm/query/readers/ordered_dim_label_reader.h"
#include "tiledb/sm/query/readers/result_tile.h"
#include "tiledb/sm/stats/global_stats.h"
#include "tiledb/sm/storage_manager/storage_manager.h"
#include "tiledb/sm/subarray/subarray.h"

#include <numeric>

using namespace tiledb;
using namespace tiledb::common;
using namespace tiledb::sm::stats;

namespace tiledb {
namespace sm {

class OrderedDimLabelReaderStatusException : public StatusException {
 public:
  explicit OrderedDimLabelReaderStatusException(const std::string& message)
      : StatusException("OrderedDimLabelReader", message) {
  }
};

/* ****************************** */
/*          CONSTRUCTORS          */
/* ****************************** */

OrderedDimLabelReader::OrderedDimLabelReader(
    stats::Stats* stats,
    shared_ptr<Logger> logger,
    StorageManager* storage_manager,
    Array* array,
    Config& config,
    std::unordered_map<std::string, QueryBuffer>& buffers,
    Subarray& subarray,
    Layout layout,
    QueryCondition& condition,
    bool increasing_labels,
    bool skip_checks_serialization)
    : ReaderBase(
          stats,
          logger->clone("OrderedDimLabelReader", ++logger_id_),
          storage_manager,
          array,
          config,
          buffers,
          subarray,
          layout,
          condition)
    , ranges_(
          subarray.get_attribute_ranges(array_schema_.attributes()[0]->name()))
    , label_name_(array_schema_.attributes()[0]->name())
    , label_type_(array_schema_.attributes()[0]->type())
    , label_var_size_(array_schema_.attributes()[0]->var_size())
    , increasing_labels_(increasing_labels)
    , index_dim_(array_schema_.domain().dimension_ptr(0))
    , non_empty_domains_(fragment_metadata_.size())
    , tile_idx_min_(std::numeric_limits<uint64_t>::max())
    , tile_idx_max_(std::numeric_limits<uint64_t>::min())
    , result_tiles_(fragment_metadata_.size())
    , domain_tile_idx_(fragment_metadata_.size()) {
  // Sanity checks.
  if (storage_manager_ == nullptr) {
    throw OrderedDimLabelReaderStatusException(
        "Cannot initialize ordered dim label reader; Storage manager not set");
  }

  if (!skip_checks_serialization && buffers_.empty()) {
    throw OrderedDimLabelReaderStatusException(
        "Cannot initialize ordered dim label reader; Buffers not set");
  }

  if (buffers_.size() != 1) {
    throw OrderedDimLabelReaderStatusException(
        "Cannot initialize ordered dim label reader; Only one buffer allowed");
  }

  for (const auto& b : buffers_) {
    if (b.first != index_dim_->name()) {
      throw OrderedDimLabelReaderStatusException(
          "Cannot initialize ordered dim label reader; Wrong buffer set");
    }

    if (*b.second.buffer_size_ !=
        ranges_.size() * 2 * datatype_size(index_dim_->type())) {
      throw OrderedDimLabelReaderStatusException(
          "Cannot initialize ordered dim label reader; Wrong buffer size");
    }

    if (b.second.buffer_var_size_ != 0) {
      throw OrderedDimLabelReaderStatusException(
          "Cannot initialize ordered dim label reader; Wrong buffer var size");
    }
  }

  if (!skip_checks_serialization && subarray_.is_set()) {
    throw OrderedDimLabelReaderStatusException(
        "Cannot initialize ordered dim label reader; Subarray is set");
  }

  if (!condition_.empty()) {
    throw OrderedDimLabelReaderStatusException(
        "Ordered dimension laber reader cannot process query condition");
  }

  bool found = false;
  if (!config_.get<uint64_t>("sm.mem.total_budget", &memory_budget_, &found)
           .ok()) {
    throw OrderedDimLabelReaderStatusException("Cannot get setting");
  }
  assert(found);

  uint64_t tile_cache_size = 0;
  if (!config_.get<uint64_t>("sm.tile_cache_size", &tile_cache_size, &found)
           .ok()) {
    throw OrderedDimLabelReaderStatusException("Cannot get setting");
  }
  assert(found);
  disable_cache_ = tile_cache_size == 0;
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

Status OrderedDimLabelReader::initialize_memory_budget() {
  return Status::Ok();
}

Status OrderedDimLabelReader::dowork() {
  auto timer_se = stats_->start_timer("dowork");

  get_dim_attr_stats();
  reset_buffer_sizes();

  // Load tile offsets and tile var sizes. This will update `tile_offsets_`,
  // `tile_var_offsets_`, `tile_validity_offsets_` and `tile_var_sizes_` in
  // `fragment_metadata_`.
  std::vector<std::string> names = {label_name_};
  RETURN_NOT_OK(load_tile_offsets(subarray_, names));
  RETURN_NOT_OK(load_tile_var_sizes(subarray_, names));

  // Load the dimension labels min/max values.
  load_label_min_max_values();

  // Do the read.
  label_read();

  return Status::Ok();
}

void OrderedDimLabelReader::reset() {
}

/* ********************************* */
/*           PRIVATE METHODS         */
/* ********************************* */

void OrderedDimLabelReader::label_read() {
  // Do the label read depending on the index type.
  auto type{index_dim_->type()};
  switch (type) {
    case Datatype::INT8:
      return label_read<int8_t>();
    case Datatype::UINT8:
      return label_read<uint8_t>();
    case Datatype::INT16:
      return label_read<int16_t>();
    case Datatype::UINT16:
      return label_read<uint16_t>();
    case Datatype::INT32:
      return label_read<int>();
    case Datatype::UINT32:
      return label_read<unsigned>();
    case Datatype::INT64:
      return label_read<int64_t>();
    case Datatype::UINT64:
      return label_read<uint64_t>();
    case Datatype::DATETIME_YEAR:
    case Datatype::DATETIME_MONTH:
    case Datatype::DATETIME_WEEK:
    case Datatype::DATETIME_DAY:
    case Datatype::DATETIME_HR:
    case Datatype::DATETIME_MIN:
    case Datatype::DATETIME_SEC:
    case Datatype::DATETIME_MS:
    case Datatype::DATETIME_US:
    case Datatype::DATETIME_NS:
    case Datatype::DATETIME_PS:
    case Datatype::DATETIME_FS:
    case Datatype::DATETIME_AS:
    case Datatype::TIME_HR:
    case Datatype::TIME_MIN:
    case Datatype::TIME_SEC:
    case Datatype::TIME_MS:
    case Datatype::TIME_US:
    case Datatype::TIME_NS:
    case Datatype::TIME_PS:
    case Datatype::TIME_FS:
    case Datatype::TIME_AS:
      return label_read<int64_t>();
    default:
      throw OrderedDimLabelReaderStatusException(
          "Cannot read ordered label array; Unsupported domain type");
  }
}

template <typename IndexType>
void OrderedDimLabelReader::label_read() {
  // Sanity checks.
  assert(std::is_integral<IndexType>::value);

  // Precompute data.
  compute_non_empty_domain<IndexType>();
  compute_array_tile_indexes_for_ranges<IndexType>();

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
        if (!result_tile.second.covered()) {
          result_tiles.push_back(&result_tile.second);
        }
      }
    }

    // Read/unfilter tiles.
    throw_if_not_ok(read_attribute_tiles({label_name_}, result_tiles));
    throw_if_not_ok(unfilter_tiles(label_name_, result_tiles));

    // Compute/copy results.
    throw_if_not_ok(parallel_for(
        storage_manager_->compute_tp(), 0, max_range, [&](uint64_t r) {
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
void OrderedDimLabelReader::compute_non_empty_domain() {
  auto timer_se = stats_->start_timer("compute_non_empty_domain");

  IndexType min = std::numeric_limits<IndexType>::max();
  IndexType max = std::numeric_limits<IndexType>::min();

  for (unsigned f = 0; f < fragment_metadata_.size(); f++) {
    non_empty_domains_[f] = fragment_metadata_[f]->non_empty_domain()[0].data();
    auto frag_non_empty_domain =
        static_cast<const IndexType*>(non_empty_domains_[f]);
    min = std::min(min, frag_non_empty_domain[0]);
    max = std::max(max, frag_non_empty_domain[1]);
  }

  non_empty_domain_ = Range(&min, &max, sizeof(IndexType));

  // Save the minimum/maximum tile indexes (in the full domain) to be used
  // later.
  auto tile_extent = index_dim_->tile_extent().rvalue_as<IndexType>();
  const IndexType* dim_dom =
      static_cast<const IndexType*>(index_dim_->domain().data());
  tile_idx_min_ = index_dim_->tile_idx(min, dim_dom[0], tile_extent);
  tile_idx_max_ = index_dim_->tile_idx(max, dim_dom[0], tile_extent);
}

template <typename IndexType>
void OrderedDimLabelReader::compute_array_tile_indexes_for_ranges() {
  auto timer_se = stats_->start_timer("compute_array_tile_indexes_for_ranges");

  // Compute the tile index (inside of the full domain) of the first tile for
  // each fragments.
  auto tile_extent = index_dim_->tile_extent().rvalue_as<IndexType>();
  const IndexType* dim_dom =
      static_cast<const IndexType*>(index_dim_->domain().data());
  throw_if_not_ok(parallel_for(
      storage_manager_->compute_tp(),
      0,
      fragment_metadata_.size(),
      [&](uint64_t f) {
        const IndexType* non_empty_domain =
            static_cast<const IndexType*>(non_empty_domains_[f]);
        domain_tile_idx_[f] = index_dim_->tile_idx<IndexType>(
            non_empty_domain[0], dim_dom[0], tile_extent);
        return Status::Ok();
      }));

  // Compute the tile (by index) where the label values include each range
  // start and end. This is stored in the following value, by fragment/range.
  std::vector<std::vector<FragmentRangeTileIndexes>>
      per_range_array_tile_indexes(ranges_.size());
  for (uint64_t r = 0; r < ranges_.size(); r++) {
    per_range_array_tile_indexes[r].resize(fragment_metadata_.size());
  }
  throw_if_not_ok(parallel_for_2d(
      storage_manager_->compute_tp(),
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
      storage_manager_->compute_tp(), 0, ranges_.size(), [&](uint64_t r) {
        per_range_array_tile_indexes_[r] = RangeTileIndexes(
            tile_idx_min_, tile_idx_max_, per_range_array_tile_indexes[r]);
        return Status::Ok();
      }));
}

void OrderedDimLabelReader::load_label_min_max_values() {
  auto timer_se = stats_->start_timer("load_label_min_max_values");
  const auto encryption_key = array_->encryption_key();

  // Load min/max data for all fragments.
  throw_if_not_ok(parallel_for(
      storage_manager_->compute_tp(),
      0,
      fragment_metadata_.size(),
      [&](const uint64_t i) {
        auto& fragment = fragment_metadata_[i];
        std::vector<std::string> names_min = {label_name_};
        RETURN_NOT_OK(fragment->load_tile_min_values(
            *encryption_key, std::move(names_min)));
        std::vector<std::string> names_max = {label_name_};
        RETURN_NOT_OK(fragment->load_tile_max_values(
            *encryption_key, std::move(names_max)));
        return Status::Ok();
      }));
}

template <typename LabelType>
OrderedDimLabelReader::FragmentRangeTileIndexes
OrderedDimLabelReader::get_array_tile_indexes_for_range(
    unsigned f, uint64_t r) {
  const auto tile_num = fragment_metadata_[f]->tile_num();
  uint64_t start_index = 0, end_index = tile_num - 1;

  const auto start_range = get_range_as<LabelType>(r, 0);
  const auto end_range = get_range_as<LabelType>(r, 1);

  // Check if either the start or end range is fully excluded from the fragment.
  IndexValueType start_val_type = IndexValueType::CONTAINED,
                 end_val_type = IndexValueType::CONTAINED;

  if (increasing_labels_) {
    // Start with the minimum of the first tile.
    const auto min =
        fragment_metadata_[f]->get_tile_min_as<LabelType>(label_name_, 0);
    if (start_range < min) {
      start_val_type = IndexValueType::LT;
    }
    if (end_range < min) {
      end_val_type = IndexValueType::LT;
    }

    // Now with the maximum of the last tile.
    const auto max = fragment_metadata_[f]->get_tile_max_as<LabelType>(
        label_name_, tile_num - 1);
    if (start_range > max) {
      start_val_type = IndexValueType::GT;
    }
    if (end_range > max) {
      end_val_type = IndexValueType::GT;
    }
  } else {
    // Start with the minimum of the first tile.
    const auto max =
        fragment_metadata_[f]->get_tile_max_as<LabelType>(label_name_, 0);
    if (start_range > max) {
      start_val_type = IndexValueType::LT;
    }
    if (end_range > max) {
      end_val_type = IndexValueType::LT;
    }

    // Now with the maximum of the last tile.
    const auto min = fragment_metadata_[f]->get_tile_min_as<LabelType>(
        label_name_, tile_num - 1);
    if (start_range < min) {
      start_val_type = IndexValueType::GT;
    }
    if (end_range < min) {
      end_val_type = IndexValueType::GT;
    }
  }

  // If the start range is included, find in which tile.
  if (start_val_type == IndexValueType::CONTAINED) {
    for (; start_index < tile_num; start_index++) {
      if (increasing_labels_) {
        const auto max = fragment_metadata_[f]->get_tile_max_as<LabelType>(
            label_name_, start_index);
        if (max >= start_range) {
          break;
        }
      } else {
        const auto min = fragment_metadata_[f]->get_tile_min_as<LabelType>(
            label_name_, start_index);
        if (min <= start_range) {
          break;
        }
      }
    }
  }

  // If the end range is included, find in which tile.
  if (end_val_type == IndexValueType::CONTAINED) {
    for (;; end_index--) {
      if (increasing_labels_) {
        const auto min = fragment_metadata_[f]->get_tile_min_as<LabelType>(
            label_name_, end_index);
        if (end_index == 0 || min <= end_range) {
          break;
        }
      } else {
        const auto max = fragment_metadata_[f]->get_tile_max_as<LabelType>(
            label_name_, end_index);
        if (end_index == 0 || max >= end_range) {
          break;
        }
      }
    }
  }

  return FragmentRangeTileIndexes(
      start_index + domain_tile_idx_[f],
      start_val_type,
      end_index + domain_tile_idx_[f],
      end_val_type);
}

OrderedDimLabelReader::FragmentRangeTileIndexes
OrderedDimLabelReader::get_array_tile_indexes_for_range(
    unsigned f, uint64_t r) {
  switch (label_type_) {
    case Datatype::INT8:
      return get_array_tile_indexes_for_range<int8_t>(f, r);
    case Datatype::UINT8:
      return get_array_tile_indexes_for_range<uint8_t>(f, r);
    case Datatype::INT16:
      return get_array_tile_indexes_for_range<int16_t>(f, r);
    case Datatype::UINT16:
      return get_array_tile_indexes_for_range<uint16_t>(f, r);
    case Datatype::INT32:
      return get_array_tile_indexes_for_range<int32_t>(f, r);
    case Datatype::UINT32:
      return get_array_tile_indexes_for_range<uint32_t>(f, r);
    case Datatype::INT64:
      return get_array_tile_indexes_for_range<int64_t>(f, r);
    case Datatype::UINT64:
      return get_array_tile_indexes_for_range<uint64_t>(f, r);
    case Datatype::FLOAT32:
      return get_array_tile_indexes_for_range<float>(f, r);
    case Datatype::FLOAT64:
      return get_array_tile_indexes_for_range<double>(f, r);
    case Datatype::DATETIME_YEAR:
    case Datatype::DATETIME_MONTH:
    case Datatype::DATETIME_WEEK:
    case Datatype::DATETIME_DAY:
    case Datatype::DATETIME_HR:
    case Datatype::DATETIME_MIN:
    case Datatype::DATETIME_SEC:
    case Datatype::DATETIME_MS:
    case Datatype::DATETIME_US:
    case Datatype::DATETIME_NS:
    case Datatype::DATETIME_PS:
    case Datatype::DATETIME_FS:
    case Datatype::DATETIME_AS:
    case Datatype::TIME_HR:
    case Datatype::TIME_MIN:
    case Datatype::TIME_SEC:
    case Datatype::TIME_MS:
    case Datatype::TIME_US:
    case Datatype::TIME_NS:
    case Datatype::TIME_PS:
    case Datatype::TIME_FS:
    case Datatype::TIME_AS:
      return get_array_tile_indexes_for_range<int64_t>(f, r);
    case Datatype::STRING_ASCII:
      return get_array_tile_indexes_for_range<std::string_view>(f, r);
    default:
      throw OrderedDimLabelReaderStatusException("Invalid dimension type");
  }
}

uint64_t OrderedDimLabelReader::label_tile_size(unsigned f, uint64_t t) const {
  uint64_t tile_size = fragment_metadata_[f]->tile_size(label_name_, t);
  if (label_var_size_) {
    auto&& [st, size] = fragment_metadata_[f]->tile_var_size(label_name_, t);
    throw_if_not_ok(st);
    tile_size += *size;
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

  // Use the non empty domains for all fragments to see if the tile is covered.
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
  const IndexType* dim_dom =
      static_cast<const IndexType*>(index_dim_->domain().data());
  auto tile_extent = index_dim_->tile_extent().rvalue_as<IndexType>();

  // Process ranges one by one.
  for (uint64_t r = 0; r < ranges_.size(); r++) {
    // Add tiles for each fragments.
    for (unsigned f = 0; f < fragment_metadata_.size(); f++) {
      // Add the tiles for the start/end range.
      for (uint8_t range_index = 0; range_index < 2; range_index++) {
        for (uint64_t i = per_range_array_tile_indexes_[r].min(range_index);
             i <= per_range_array_tile_indexes_[r].max(range_index);
             i++) {
          if ((i >= domain_tile_idx_[f]) &&
              (i < domain_tile_idx_[f] + fragment_metadata_[f]->tile_num()) &&
              (result_tiles_[f].count(i) == 0)) {
            // Make sure the tile can fit in the budget.
            uint64_t frag_tile_idx = i - domain_tile_idx_[f];
            uint64_t tile_size = label_tile_size(f, frag_tile_idx);
            bool covered =
                tile_overwritten<IndexType>(f, i, dim_dom[0], tile_extent);
            if (covered || total_mem_used + tile_size < memory_budget_) {
              total_mem_used += tile_size;
              result_tiles_[f].emplace(
                  std::piecewise_construct,
                  std::forward_as_tuple(i),
                  std::forward_as_tuple(
                      f, frag_tile_idx, array_schema_, covered));
            } else {
              if (r == 0) {
                throw OrderedDimLabelReaderStatusException(
                    "Can't process a single range, increase memory budget");
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
  const auto label_data =
      rt.tile_tuple(label_name_)->fixed_tile().template data_as<LabelType>();
  return label_data[cell_idx];
}

template <>
std::string_view OrderedDimLabelReader::get_label_value<std::string_view>(
    const unsigned f, const uint64_t tile_idx, const uint64_t cell_idx) {
  auto& rt = result_tiles_[f][tile_idx];
  auto tile_tuple = rt.tile_tuple(label_name_);
  auto offsets_data = tile_tuple->fixed_tile().template data_as<uint64_t>();
  auto& var_tile = tile_tuple->var_tile();
  auto offset = offsets_data[cell_idx];

  auto size = static_cast<size_t>(cell_idx) == rt.cell_num() - 1 ?
                  var_tile.size() - offset :
                  offsets_data[cell_idx + 1] - offset;
  return std::string_view(&var_tile.template data_as<char>()[offset], size);
}

template <typename IndexType, typename LabelType>
LabelType OrderedDimLabelReader::get_value_at(
    const IndexType& index,
    const IndexType& domain_low,
    const IndexType& tile_extent) {
  // Start with the most recent fragment.
  unsigned f = static_cast<unsigned>(fragment_metadata_.size() - 1);
  while (true) {
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
      throw OrderedDimLabelReaderStatusException("Couldn't find value");
    }

    // Try the next fragment.
    f--;
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
IndexType OrderedDimLabelReader::search_for_range_fixed(
    uint64_t r,
    uint8_t range_index,
    const IndexType& domain_low,
    const IndexType& tile_extent) {
  // Get the value we are looking for.
  LabelType value = get_range_as<LabelType>(r, range_index);

  // Minimum index to look into.
  auto non_empty_domain =
      static_cast<const IndexType*>(non_empty_domain_.data());
  auto t_min = per_range_array_tile_indexes_[r].min(range_index);
  auto left = std::max(
      index_dim_->tile_coord_low(t_min, domain_low, tile_extent),
      non_empty_domain[0]);

  // Maximum index to look into.
  auto t_max = per_range_array_tile_indexes_[r].max(range_index);
  auto right = std::min(
      index_dim_->tile_coord_high(t_max, domain_low, tile_extent),
      non_empty_domain[1]);

  // Run a binary search.
  Op cmp;
  while (left != right - 1) {
    // Check against mid.
    IndexType mid = left + (right - left) / 2;
    if (cmp(get_value_at<IndexType, LabelType>(mid, domain_low, tile_extent),
            value)) {
      right = mid;
    } else {
      left = mid;
    }
  }

  // Do one last comparison to decide to return left or right.
  auto bound = range_index == 0 ? left : right;
  return (cmp(
             get_value_at<IndexType, LabelType>(bound, domain_low, tile_extent),
             value)) ?
             left :
             right;
}

template <typename IndexType, typename LabelType>
void OrderedDimLabelReader::compute_and_copy_range_indexes(
    IndexType* dest, uint64_t r) {
  // For easy reference.
  auto tile_extent = index_dim_->tile_extent().rvalue_as<IndexType>();
  const IndexType* dim_dom =
      static_cast<const IndexType*>(index_dim_->domain().data());

  // Set the results.
  if (increasing_labels_) {
    dest[0] = search_for_range_fixed<
        IndexType,
        LabelType,
        std::greater_equal<LabelType>>(r, 0, dim_dom[0], tile_extent);
    dest[1] =
        search_for_range_fixed<IndexType, LabelType, std::greater<LabelType>>(
            r, 1, dim_dom[0], tile_extent);
  } else {
    dest[0] = search_for_range_fixed<
        IndexType,
        LabelType,
        std::less_equal<LabelType>>(r, 0, dim_dom[0], tile_extent);
    dest[1] =
        search_for_range_fixed<IndexType, LabelType, std::less<LabelType>>(
            r, 1, dim_dom[0], tile_extent);
  }
}

template <typename IndexType>
void OrderedDimLabelReader::compute_and_copy_range_indexes(
    uint64_t buffer_offset, uint64_t r) {
  auto timer_se = stats_->start_timer("compute_and_copy_range_indexes");

  auto dest = static_cast<IndexType*>(buffers_[index_dim_->name()].buffer_) +
              (buffer_offset + r) * 2;
  switch (label_type_) {
    case Datatype::INT8:
      compute_and_copy_range_indexes<IndexType, int8_t>(dest, r);
      break;
    case Datatype::UINT8:
      compute_and_copy_range_indexes<IndexType, uint8_t>(dest, r);
      break;
    case Datatype::INT16:
      compute_and_copy_range_indexes<IndexType, int16_t>(dest, r);
      break;
    case Datatype::UINT16:
      compute_and_copy_range_indexes<IndexType, uint16_t>(dest, r);
      break;
    case Datatype::INT32:
      compute_and_copy_range_indexes<IndexType, int32_t>(dest, r);
      break;
    case Datatype::UINT32:
      compute_and_copy_range_indexes<IndexType, uint32_t>(dest, r);
      break;
    case Datatype::INT64:
      compute_and_copy_range_indexes<IndexType, int64_t>(dest, r);
      break;
    case Datatype::UINT64:
      compute_and_copy_range_indexes<IndexType, uint64_t>(dest, r);
      break;
    case Datatype::FLOAT32:
      compute_and_copy_range_indexes<IndexType, float>(dest, r);
      break;
    case Datatype::FLOAT64:
      compute_and_copy_range_indexes<IndexType, double>(dest, r);
      break;
    case Datatype::DATETIME_YEAR:
    case Datatype::DATETIME_MONTH:
    case Datatype::DATETIME_WEEK:
    case Datatype::DATETIME_DAY:
    case Datatype::DATETIME_HR:
    case Datatype::DATETIME_MIN:
    case Datatype::DATETIME_SEC:
    case Datatype::DATETIME_MS:
    case Datatype::DATETIME_US:
    case Datatype::DATETIME_NS:
    case Datatype::DATETIME_PS:
    case Datatype::DATETIME_FS:
    case Datatype::DATETIME_AS:
    case Datatype::TIME_HR:
    case Datatype::TIME_MIN:
    case Datatype::TIME_SEC:
    case Datatype::TIME_MS:
    case Datatype::TIME_US:
    case Datatype::TIME_NS:
    case Datatype::TIME_PS:
    case Datatype::TIME_FS:
    case Datatype::TIME_AS:
      compute_and_copy_range_indexes<IndexType, int64_t>(dest, r);
      break;
    case Datatype::STRING_ASCII:
      compute_and_copy_range_indexes<IndexType, std::string_view>(dest, r);
      break;
    default:
      throw OrderedDimLabelReaderStatusException("Invalid label type");
  }
}

}  // namespace sm
}  // namespace tiledb
