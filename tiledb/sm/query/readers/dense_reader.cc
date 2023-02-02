/**
 * @file   dense_reader.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2022 TileDB, Inc.
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
 * This file implements class DenseReader.
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
#include "tiledb/sm/query/readers/dense_reader.h"
#include "tiledb/sm/query/readers/filtered_data.h"
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

class DenseReaderStatusException : public StatusException {
 public:
  explicit DenseReaderStatusException(const std::string& message)
      : StatusException("DenseReader", message) {
  }
};

/* ****************************** */
/*          CONSTRUCTORS          */
/* ****************************** */

DenseReader::DenseReader(
    stats::Stats* stats,
    shared_ptr<Logger> logger,
    StorageManager* storage_manager,
    Array* array,
    Config& config,
    std::unordered_map<std::string, QueryBuffer>& buffers,
    Subarray& subarray,
    Layout layout,
    QueryCondition& condition,
    bool skip_checks_serialization)
    : ReaderBase(
          stats,
          logger->clone("DenseReader", ++logger_id_),
          storage_manager,
          array,
          config,
          buffers,
          subarray,
          layout,
          condition) {
  elements_mode_ = false;

  // Sanity checks.
  if (storage_manager_ == nullptr) {
    throw DenseReaderStatusException(
        "Cannot initialize dense reader; Storage manager not set");
  }

  if (!skip_checks_serialization && buffers_.empty()) {
    throw DenseReaderStatusException(
        "Cannot initialize dense reader; Buffers not set");
  }

  if (!skip_checks_serialization && !subarray_.is_set()) {
    throw DenseReaderStatusException(
        "Cannot initialize reader; Dense reads must have a subarray set");
  }

  // Check subarray.
  check_subarray();

  // Initialize the read state.
  init_read_state();

  // Check the validity buffer sizes.
  check_validity_buffer_sizes();
}

/* ****************************** */
/*               API              */
/* ****************************** */

bool DenseReader::incomplete() const {
  return read_state_.overflowed_ || !read_state_.done();
}

QueryStatusDetailsReason DenseReader::status_incomplete_reason() const {
  return incomplete() ? QueryStatusDetailsReason::REASON_USER_BUFFER_SIZE :
                        QueryStatusDetailsReason::REASON_NONE;
}

Status DenseReader::initialize_memory_budget() {
  return Status::Ok();
}

const DenseReader::ReadState* DenseReader::read_state() const {
  return &read_state_;
}

DenseReader::ReadState* DenseReader::read_state() {
  return &read_state_;
}

Status DenseReader::complete_read_loop() {
  if (offsets_extra_element_) {
    RETURN_NOT_OK(add_extra_offset());
  }

  return Status::Ok();
}

Status DenseReader::dowork() {
  auto timer_se = stats_->start_timer("dowork");

  // Check that the query condition is valid.
  RETURN_NOT_OK(condition_.check(array_schema_));

  get_dim_attr_stats();

  // Get next partition.
  if (!read_state_.unsplittable_)
    RETURN_NOT_OK(read_state_.next());

  // Loop until you find results, or unsplittable, or done.
  do {
    stats_->add_counter("loop_num", 1);

    read_state_.overflowed_ = false;
    reset_buffer_sizes();

    // Perform read.
    if (offsets_bitsize_ == 64) {
      RETURN_NOT_OK(dense_read<uint64_t>());
    } else {
      RETURN_NOT_OK(dense_read<uint32_t>());
    }

    // In the case of overflow, we need to split the current partition
    // without advancing to the next partition.
    if (read_state_.overflowed_) {
      zero_out_buffer_sizes();
      RETURN_NOT_OK(read_state_.split_current());

      if (read_state_.unsplittable_) {
        return complete_read_loop();
      }
    } else {
      read_state_.unsplittable_ = false;
      return complete_read_loop();
    }
  } while (true);

  return Status::Ok();
}

void DenseReader::reset() {
}

template <class OffType>
Status DenseReader::dense_read() {
  auto type{array_schema_.domain().dimension_ptr(0)->type()};
  switch (type) {
    case Datatype::INT8:
      return dense_read<int8_t, OffType>();
    case Datatype::UINT8:
      return dense_read<uint8_t, OffType>();
    case Datatype::INT16:
      return dense_read<int16_t, OffType>();
    case Datatype::UINT16:
      return dense_read<uint16_t, OffType>();
    case Datatype::INT32:
      return dense_read<int, OffType>();
    case Datatype::UINT32:
      return dense_read<unsigned, OffType>();
    case Datatype::INT64:
      return dense_read<int64_t, OffType>();
    case Datatype::UINT64:
      return dense_read<uint64_t, OffType>();
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
      return dense_read<int64_t, OffType>();
    default:
      return LOG_STATUS(Status_ReaderError(
          "Cannot read dense array; Unsupported domain type"));
  }

  return Status::Ok();
}

template <class DimType, class OffType>
Status DenseReader::dense_read() {
  // Sanity checks.
  assert(std::is_integral<DimType>::value);

  // For easy reference.
  const auto dim_num = array_schema_.dim_num();
  auto& subarray = read_state_.partitioner_.current();
  RETURN_NOT_OK(subarray.compute_tile_coords<DimType>());
  const auto& domain{array_schema_.domain()};

  // Cache tile extents.
  std::vector<DimType> tile_extents(dim_num);
  for (uint32_t d = 0; d < dim_num; d++) {
    tile_extents[d] =
        *reinterpret_cast<const DimType*>(domain.tile_extent(d).data());
  }

  // Compute result space tiles. The result space tiles hold all the
  // relevant result tiles of the dense fragments.
  std::map<const DimType*, ResultSpaceTile<DimType>> result_space_tiles;
  compute_result_space_tiles<DimType>(
      subarray, read_state_.partitioner_.subarray(), result_space_tiles);

  std::vector<ResultTile*> result_tiles;
  for (const auto& result_space_tile : result_space_tiles) {
    for (const auto& result_tile : result_space_tile.second.result_tiles()) {
      result_tiles.push_back(const_cast<ResultTile*>(&result_tile.second));
    }
  }
  std::sort(result_tiles.begin(), result_tiles.end(), result_tile_cmp);

  // Compute subarrays for each tile.
  const auto& tile_coords = subarray.tile_coords();
  stats_->add_counter("num_tiles", tile_coords.size());
  TileSubarrays tile_subarrays{tile_coords.size()};
  const auto& layout =
      layout_ == Layout::GLOBAL_ORDER ? array_schema_.cell_order() : layout_;
  auto status = parallel_for(
      storage_manager_->compute_tp(),
      0,
      tile_subarrays.size(),
      [&](uint64_t t) {
        subarray.crop_to_tile(
            &tile_subarrays[t], (const DimType*)&tile_coords[t][0], layout);
        return Status::Ok();
      });
  RETURN_NOT_OK(status);

  // Compute tile offsets for global order or range info for row/col major.
  std::vector<uint64_t> tile_offsets;
  std::vector<RangeInfo<DimType>> range_info(dim_num);

  if (layout_ == Layout::GLOBAL_ORDER) {
    tile_offsets.reserve(tile_coords.size());

    uint64_t tile_offset = 0;
    for (uint64_t i = 0; i < tile_subarrays.size(); i++) {
      tile_offsets.emplace_back(tile_offset);
      tile_offset += tile_subarrays[i].cell_num();
    }
  } else {
    for (uint32_t d = 0; d < dim_num; d++) {
      auto& ranges = subarray.ranges_for_dim(d);

      // Compute the 1D offset for every range in this dimension.
      range_info[d].cell_offsets_.reserve(ranges.size());
      range_info[d].mins_.reserve(ranges.size());
      uint64_t offset = 0;
      for (uint64_t r = 0; r < ranges.size(); r++) {
        range_info[d].mins_.emplace_back(
            *static_cast<const DimType*>(ranges[r].start_fixed()));
        range_info[d].cell_offsets_.emplace_back(offset);

        // Increment the offset with the number of cells in this 1D range.
        auto range = static_cast<const DimType*>(ranges[r].data());
        offset += range[1] - range[0] + 1;
      }

      // Sets the initial multiplier, will be adjusted in the next step.
      range_info[d].multiplier_ = offset;
    }
  }

  // Compute the correct multipliers.
  uint64_t mult = 1;
  if (subarray.layout() == Layout::COL_MAJOR) {
    for (int32_t d = 0; d < static_cast<int32_t>(dim_num); d++) {
      auto saved = mult;
      mult *= range_info[d].multiplier_;
      range_info[d].multiplier_ = saved;
    }
  } else {
    for (int32_t d = static_cast<int32_t>(dim_num) - 1; d >= 0; d--) {
      auto saved = mult;
      mult *= range_info[d].multiplier_;
      range_info[d].multiplier_ = saved;
    }
  }

  // Compute parallelization parameters.
  uint64_t num_range_threads = 1;
  const auto num_threads = storage_manager_->compute_tp()->concurrency_level();
  if (tile_coords.size() < num_threads) {
    // Ceil the division between thread_num and tile_num.
    num_range_threads = 1 + ((num_threads - 1) / tile_coords.size());
  }

  // Compute attribute names to load and copy.
  std::vector<std::string> names;
  std::vector<std::string> fixed_names;
  std::vector<std::string> var_names;
  auto condition_names = condition_.field_names();
  for (auto& name : condition_names) {
    names.emplace_back(name);
  }

  for (const auto& it : buffers_) {
    const auto& name = it.first;

    if (name == constants::coords || array_schema_.is_dim(name)) {
      continue;
    }

    if (condition_names.count(name) == 0) {
      names.emplace_back(name);
    }

    if (array_schema_.var_size(name)) {
      var_names.emplace_back(name);
    } else {
      fixed_names.emplace_back(name);
    }
  }

  // Pre-load all attribute offsets into memory for attributes
  // in query condition to be read.
  RETURN_CANCEL_OR_ERROR(load_tile_var_sizes(
      read_state_.partitioner_.subarray().relevant_fragments(), var_names));
  RETURN_CANCEL_OR_ERROR(load_tile_offsets(
      read_state_.partitioner_.subarray().relevant_fragments(), names));

  auto&& [st, qc_result] = apply_query_condition<DimType, OffType>(
      subarray,
      tile_extents,
      result_tiles,
      tile_subarrays,
      tile_offsets,
      range_info,
      result_space_tiles,
      num_range_threads);
  RETURN_CANCEL_OR_ERROR(st);

  // For `qc_coords_mode` just fill in the coordinates and skip attribute
  // processing.
  if (qc_coords_mode_) {
    fill_dense_coords<DimType>(subarray, qc_result);
    return Status::Ok();
  }

  // Process attributes.
  std::vector<std::string> to_read(1);
  for (auto& buff : buffers_) {
    auto& name = buff.first;
    if (name == constants::coords || array_schema_.is_dim(name)) {
      continue;
    }

    if (condition_.field_names().count(name) == 0) {
      // Read and unfilter tiles.
      to_read[0] = name;
      RETURN_CANCEL_OR_ERROR(
          read_and_unfilter_attribute_tiles(to_read, result_tiles));
    }

    // Copy attribute data to users buffers.
    status = copy_attribute<DimType, OffType>(
        name,
        tile_extents,
        subarray,
        tile_subarrays,
        tile_offsets,
        range_info,
        result_space_tiles,
        *qc_result,
        num_range_threads);
    RETURN_CANCEL_OR_ERROR(status);

    clear_tiles(name, result_tiles);
  }

  if (read_state_.overflowed_) {
    return Status::Ok();
  }

  // Fill coordinates if the user requested them.
  if (!read_state_.overflowed_ && has_coords()) {
    fill_dense_coords<DimType>(subarray, nullopt);
  }

  return Status::Ok();
}

void DenseReader::init_read_state() {
  auto timer_se = stats_->start_timer("init_state");

  // Check subarray.
  if (subarray_.layout() == Layout::GLOBAL_ORDER &&
      subarray_.range_num() != 1) {
    throw DenseReaderStatusException(
        "Cannot initialize read state; Multi-range subarrays do not support "
        "global order");
  }

  // Get config values.
  bool found = false;
  uint64_t memory_budget = 0;
  if (!config_.get<uint64_t>("sm.memory_budget", &memory_budget, &found).ok()) {
    throw DenseReaderStatusException("Cannot get setting");
  }
  assert(found);

  uint64_t memory_budget_var = 0;
  if (!config_.get<uint64_t>("sm.memory_budget_var", &memory_budget_var, &found)
           .ok()) {
    throw DenseReaderStatusException("Cannot get setting");
  }
  assert(found);

  offsets_format_mode_ = config_.get("sm.var_offsets.mode", &found);
  assert(found);
  if (offsets_format_mode_ != "bytes" && offsets_format_mode_ != "elements") {
    throw DenseReaderStatusException(
        "Cannot initialize reader; Unsupported offsets format in "
        "configuration");
  }
  elements_mode_ = offsets_format_mode_ == "elements";

  if (!config_
           .get<bool>(
               "sm.var_offsets.extra_element", &offsets_extra_element_, &found)
           .ok()) {
    throw DenseReaderStatusException("Cannot get setting");
  }
  assert(found);

  if (!config_
           .get<uint32_t>("sm.var_offsets.bitsize", &offsets_bitsize_, &found)
           .ok()) {
    throw DenseReaderStatusException("Cannot get setting");
  }
  if (offsets_bitsize_ != 32 && offsets_bitsize_ != 64) {
    throw DenseReaderStatusException(
        "Cannot initialize reader; Unsupported offsets bitsize in "
        "configuration");
  }
  assert(found);

  if (!config_
           .get<bool>("sm.query.dense.qc_coords_mode", &qc_coords_mode_, &found)
           .ok()) {
    throw DenseReaderStatusException("Cannot get setting");
  }
  assert(found);

  if (qc_coords_mode_ && condition_.empty()) {
    throw DenseReaderStatusException(
        "sm.query.dense.qc_coords_mode requires a query condition");
  }

  // Consider the validity memory budget to be identical to `sm.memory_budget`
  // because the validity vector is currently a bytemap. When converted to a
  // bitmap, this can be budgeted as `sm.memory_budget` / 8.
  uint64_t memory_budget_validity = memory_budget;

  // Create read state.
  read_state_.partitioner_ = SubarrayPartitioner(
      &config_,
      subarray_,
      memory_budget,
      memory_budget_var,
      memory_budget_validity,
      storage_manager_->compute_tp(),
      stats_,
      logger_);
  read_state_.overflowed_ = false;
  read_state_.unsplittable_ = false;

  // Set result size budget
  for (const auto& a : buffers_) {
    auto attr_name = a.first;
    auto buffer_size = a.second.buffer_size_;
    auto buffer_var_size = a.second.buffer_var_size_;
    auto buffer_validity_size = a.second.validity_vector_.buffer_size();
    if (!array_schema_.var_size(attr_name)) {
      if (!array_schema_.is_nullable(attr_name)) {
        if (!read_state_.partitioner_
                 .set_result_budget(attr_name.c_str(), *buffer_size)
                 .ok()) {
          throw DenseReaderStatusException("Cannot set result budget");
        }
      } else {
        if (!read_state_.partitioner_
                 .set_result_budget_nullable(
                     attr_name.c_str(), *buffer_size, *buffer_validity_size)
                 .ok()) {
          throw DenseReaderStatusException("Cannot set result budget");
        }
      }
    } else {
      if (!array_schema_.is_nullable(attr_name)) {
        if (!read_state_.partitioner_
                 .set_result_budget(
                     attr_name.c_str(), *buffer_size, *buffer_var_size)
                 .ok()) {
          throw DenseReaderStatusException("Cannot set result budget");
        }
      } else {
        if (!read_state_.partitioner_
                 .set_result_budget_nullable(
                     attr_name.c_str(),
                     *buffer_size,
                     *buffer_var_size,
                     *buffer_validity_size)
                 .ok()) {
          throw DenseReaderStatusException("Cannot set result budget");
        }
      }
    }
  }

  read_state_.unsplittable_ = false;
  read_state_.overflowed_ = false;
  read_state_.initialized_ = true;
}

/** Apply the query condition. */
template <class DimType, class OffType>
tuple<Status, optional<std::vector<uint8_t>>>
DenseReader::apply_query_condition(
    Subarray& subarray,
    const std::vector<DimType>& tile_extents,
    std::vector<ResultTile*>& result_tiles,
    DynamicArray<Subarray>& tile_subarrays,
    std::vector<uint64_t>& tile_offsets,
    const std::vector<RangeInfo<DimType>>& range_info,
    std::map<const DimType*, ResultSpaceTile<DimType>>& result_space_tiles,
    const uint64_t num_range_threads) {
  auto timer_se = stats_->start_timer("apply_query_condition");
  std::vector<uint8_t> qc_result;
  if (!condition_.empty()) {
    // For easy reference.
    const auto& tile_coords = subarray.tile_coords();
    const auto cell_num = subarray.cell_num();
    const auto dim_num = array_schema_.dim_num();
    auto stride = array_schema_.domain().stride<DimType>(layout_);
    const auto cell_order = array_schema_.cell_order();
    const auto global_order = layout_ == Layout::GLOBAL_ORDER;

    // Compute the result of the query condition.
    std::vector<std::string> qc_names;
    qc_names.reserve(condition_.field_names().size());
    for (auto& name : condition_.field_names()) {
      qc_names.emplace_back(name);
    }

    // Read and unfilter query condition attributes.
    {
      RETURN_CANCEL_OR_ERROR_TUPLE(
          read_and_unfilter_attribute_tiles(qc_names, result_tiles));
    }

    if (stride == UINT64_MAX) {
      stride = 1;
    }

    // Initialize the result buffer.
    qc_result.resize(cell_num, 1);

    // Process all tiles in parallel.
    auto status = parallel_for_2d(
        storage_manager_->compute_tp(),
        0,
        tile_coords.size(),
        0,
        num_range_threads,
        [&](uint64_t t, uint64_t range_thread_idx) {
          // Find out result space tile and tile subarray.
          const DimType* tc = (DimType*)&tile_coords[t][0];
          auto it = result_space_tiles.find(tc);
          assert(it != result_space_tiles.end());

          // Iterate over all coordinates, retrieved in cell slab.
          const auto& frag_domains = it->second.frag_domains();
          TileCellSlabIter<DimType> iter(
              range_thread_idx,
              num_range_threads,
              subarray,
              tile_subarrays[t],
              tile_extents,
              it->second.start_coords(),
              range_info,
              cell_order);

          // Compute cell offset and destination pointer.
          uint64_t cell_offset =
              global_order ? tile_offsets[t] + iter.global_offset() : 0;
          auto dest_ptr = qc_result.data() + cell_offset;

          while (!iter.end()) {
            // Compute destination pointer for row/col major orders.
            if (!global_order) {
              cell_offset = iter.dest_offset_row_col();
              dest_ptr = qc_result.data() + cell_offset;
            }

            for (int32_t i = static_cast<int32_t>(frag_domains.size()) - 1;
                 i >= 0;
                 --i) {
              // If the cell slab overlaps this fragment domain range, apply
              // clause.
              auto&& [overlaps, start, end] = cell_slab_overlaps_range(
                  dim_num,
                  frag_domains[i].domain(),
                  iter.cell_slab_coords(),
                  iter.cell_slab_length());
              if (overlaps) {
                // Re-initialize the bitmap to 1 in case of overlapping domains.
                if (i != static_cast<int32_t>(frag_domains.size()) - 1) {
                  for (uint64_t c = start; c <= end; c++) {
                    dest_ptr[c] = 1;
                  }
                }

                RETURN_NOT_OK(condition_.apply_dense(
                    *(fragment_metadata_[frag_domains[i].fid()]
                          ->array_schema()
                          .get()),
                    it->second.result_tile(frag_domains[i].fid()),
                    start,
                    end - start + 1,
                    iter.pos_in_tile(),
                    stride,
                    dest_ptr));
              }
            }

            // Adjust the destination pointers for global order.
            if (global_order) {
              dest_ptr += iter.cell_slab_length();
            }

            ++iter;
          }

          return Status::Ok();
        });
    RETURN_NOT_OK_TUPLE(status, nullopt);
  }

  return {Status::Ok(), qc_result};
}

template <class OffType>
uint64_t DenseReader::fix_offsets_buffer(
    const std::string& name,
    const bool nullable,
    const uint64_t cell_num,
    std::vector<void*>& var_data) {
  // For easy reference.
  const auto& fill_value = array_schema_.attribute(name)->fill_value();
  const auto fill_value_size = (OffType)fill_value.size();
  auto offsets_buffer = (OffType*)buffers_[name].buffer_;

  // Switch offsets from sizes to real offsets.
  uint64_t offset = 0;
  for (uint64_t i = 0; i < cell_num; ++i) {
    auto tmp = offsets_buffer[i];

    // The maximum value is used as a sentinel to request the fill value.
    if (tmp == std::numeric_limits<OffType>::max()) {
      tmp = fill_value_size;

      // Set the pointer for the var data.
      var_data[i] = (void*)fill_value.data();
    }
    offsets_buffer[i] = offset;
    offset += tmp;
  }

  // Set the output offset buffer sizes.
  *buffers_[name].buffer_size_ = cell_num * sizeof(OffType);

  if (nullable) {
    *buffers_[name].validity_vector_.buffer_size() = cell_num;
  }

  // Return the buffer size.
  return offset;
}

template <class DimType, class OffType>
Status DenseReader::copy_attribute(
    const std::string& name,
    const std::vector<DimType>& tile_extents,
    const Subarray& subarray,
    const DynamicArray<Subarray>& tile_subarrays,
    const std::vector<uint64_t>& tile_offsets,
    const std::vector<RangeInfo<DimType>>& range_info,
    std::map<const DimType*, ResultSpaceTile<DimType>>& result_space_tiles,
    const std::vector<uint8_t>& qc_result,
    const uint64_t num_range_threads) {
  auto timer_se = stats_->start_timer("copy_attribute");

  // For easy reference
  const auto& tile_coords = subarray.tile_coords();
  const auto cell_num = subarray.cell_num();
  const auto global_order = layout_ == Layout::GLOBAL_ORDER;

  if (array_schema_.var_size(name)) {
    // Make sure the user offset buffers are big enough.
    const auto required_size =
        (cell_num + offsets_extra_element_) * sizeof(OffType);
    if (required_size > *buffers_[name].buffer_size_) {
      read_state_.overflowed_ = true;
      return Status::Ok();
    }

    // Vector to hold pointers to the var data.
    std::vector<void*> var_data(cell_num);

    // Process offsets.
    {
      auto timer_se = stats_->start_timer("copy_offset_tiles");
      auto status = parallel_for_2d(
          storage_manager_->compute_tp(),
          0,
          tile_coords.size(),
          0,
          num_range_threads,
          [&](uint64_t t, uint64_t range_thread_idx) {
            // Find out result space tile and tile subarray.
            const DimType* tc = (DimType*)&tile_coords[t][0];
            auto it = result_space_tiles.find(tc);
            assert(it != result_space_tiles.end());

            // Copy the tile offsets.
            return copy_offset_tiles<DimType, OffType>(
                name,
                tile_extents,
                it->second,
                subarray,
                tile_subarrays[t],
                global_order ? tile_offsets[t] : 0,
                var_data,
                range_info,
                qc_result,
                range_thread_idx,
                num_range_threads);
          });
      RETURN_NOT_OK(status);
    }

    uint64_t var_buffer_size;
    {
      // We have the cell lengths in the users buffer, convert to offsets.
      auto timer_se = stats_->start_timer("fix_offset_tiles");
      const bool nullable = array_schema_.is_nullable(name);
      var_buffer_size =
          fix_offsets_buffer<OffType>(name, nullable, cell_num, var_data);

      // Make sure the user var buffer is big enough.
      uint64_t required_var_size = var_buffer_size;
      if (elements_mode_) {
        required_var_size *= datatype_size(array_schema_.type(name));
      }

      // Exit early in case of overflow.
      if (read_state_.overflowed_ ||
          required_var_size > *buffers_[name].buffer_var_size_) {
        read_state_.overflowed_ = true;
        return Status::Ok();
      }

      *buffers_[name].buffer_var_size_ = required_var_size;
    }

    {
      auto timer_se = stats_->start_timer("copy_var_tiles");
      // Process var data in parallel.
      auto status = parallel_for_2d(
          storage_manager_->compute_tp(),
          0,
          tile_coords.size(),
          0,
          num_range_threads,
          [&](uint64_t t, uint64_t range_thread_idx) {
            // Find out result space tile and tile subarray.
            const DimType* tc = (DimType*)&tile_coords[t][0];
            auto it = result_space_tiles.find(tc);
            assert(it != result_space_tiles.end());

            return copy_var_tiles<DimType, OffType>(
                name,
                tile_extents,
                it->second,
                subarray,
                tile_subarrays[t],
                global_order ? tile_offsets[t] : 0,
                var_data,
                range_info,
                t == tile_coords.size() - 1,
                var_buffer_size,
                range_thread_idx,
                num_range_threads);

            return Status::Ok();
          });
      RETURN_NOT_OK(status);
    }
  } else {
    // Make sure the user fixed buffer is big enough.
    const auto required_size = cell_num * array_schema_.cell_size(name);
    if (required_size > *buffers_[name].buffer_size_) {
      read_state_.overflowed_ = true;
      return Status::Ok();
    }

    {
      auto timer_se = stats_->start_timer("copy_fixed_tiles");
      // Process values in parallel.
      auto status = parallel_for_2d(
          storage_manager_->compute_tp(),
          0,
          tile_coords.size(),
          0,
          num_range_threads,
          [&](uint64_t t, uint64_t range_thread_idx) {
            // Find out result space tile and tile subarray.
            const DimType* tc = (DimType*)&tile_coords[t][0];
            auto it = result_space_tiles.find(tc);
            assert(it != result_space_tiles.end());

            // Copy the tile fixed values.
            RETURN_NOT_OK(copy_fixed_tiles(
                name,
                tile_extents,
                it->second,
                subarray,
                tile_subarrays[t],
                global_order ? tile_offsets[t] : 0,
                range_info,
                qc_result,
                range_thread_idx,
                num_range_threads));

            return Status::Ok();
          });
      RETURN_NOT_OK(status);
    }

    // Set the output size for the fixed buffer.
    *buffers_[name].buffer_size_ = required_size;

    if (array_schema_.is_nullable(name)) {
      *buffers_[name].validity_vector_.buffer_size() = cell_num;
    }
  }

  return Status::Ok();
}

template <class DimType>
tuple<bool, uint64_t, uint64_t> DenseReader::cell_slab_overlaps_range(
    const unsigned dim_num,
    const NDRange& ndrange,
    const std::vector<DimType>& coords,
    const uint64_t length) {
  const unsigned slab_dim = (layout_ == Layout::COL_MAJOR) ? 0 : dim_num - 1;
  const DimType slab_start = coords[slab_dim];
  const DimType slab_end = slab_start + length - 1;

  // Check if there is any overlap.
  for (unsigned d = 0; d < dim_num; ++d) {
    auto dom = static_cast<const DimType*>(ndrange[d].data());
    if (d == slab_dim) {
      if (slab_end < dom[0] || slab_start > dom[1]) {
        return {false, 0, 0};
      }
    } else if (coords[d] < dom[0] || coords[d] > dom[1]) {
      return {false, 0, 0};
    }
  }

  // Compute the normalized start and end coordinates for the slab.
  auto dom = static_cast<const DimType*>(ndrange[slab_dim].data());
  auto start = std::max(slab_start, dom[0]) - slab_start;
  auto end = std::min(slab_end, dom[1]) - slab_start;
  return {true, start, end};
}

template <class DimType>
Status DenseReader::copy_fixed_tiles(
    const std::string& name,
    const std::vector<DimType>& tile_extents,
    ResultSpaceTile<DimType>& result_space_tile,
    const Subarray& subarray,
    const Subarray& tile_subarray,
    const uint64_t global_cell_offset,
    const std::vector<RangeInfo<DimType>>& range_info,
    const std::vector<uint8_t>& qc_result,
    const uint64_t range_thread_idx,
    const uint64_t num_range_threads) {
  // For easy reference
  const auto dim_num = array_schema_.dim_num();
  const auto cell_order = array_schema_.cell_order();
  auto stride = array_schema_.domain().stride<DimType>(layout_);
  const auto& frag_domains = result_space_tile.frag_domains();
  auto dst_buf = static_cast<uint8_t*>(buffers_[name].buffer_);
  auto dst_val_buf = buffers_[name].validity_vector_.buffer();
  const auto attribute = array_schema_.attribute(name);
  const auto cell_size = array_schema_.cell_size(name);
  const auto nullable = attribute->nullable();
  const auto& fill_value = attribute->fill_value();
  const auto& fill_value_nullable = attribute->fill_value_validity();

  // Cache tile tuples.
  std::vector<ResultTile::TileTuple*> tile_tuples(frag_domains.size());
  for (uint32_t fd = 0; fd < frag_domains.size(); ++fd) {
    tile_tuples[fd] =
        result_space_tile.result_tile(frag_domains[fd].fid())->tile_tuple(name);
    assert(tile_tuples[fd] != nullptr);
  }

  if (stride == UINT64_MAX) {
    stride = 1;
  }

  // Iterate over all coordinates, retrieved in cell slab.
  TileCellSlabIter<DimType> iter(
      range_thread_idx,
      num_range_threads,
      subarray,
      tile_subarray,
      tile_extents,
      result_space_tile.start_coords(),
      range_info,
      cell_order);

  // Initialise for global order, will be adjusted later for row/col major.
  uint64_t cell_offset = global_cell_offset + iter.global_offset();
  while (!iter.end()) {
    // Compute cell offset for row/col major orders.
    if (layout_ != Layout::GLOBAL_ORDER) {
      cell_offset = iter.dest_offset_row_col();
    }

    // Iterate through all fragment domains and copy data.
    for (int32_t fd = (int32_t)frag_domains.size() - 1; fd >= 0; --fd) {
      // If the cell slab overlaps this fragment domain range, copy data.
      auto&& [overlaps, start, end] = cell_slab_overlaps_range(
          dim_num,
          frag_domains[fd].domain(),
          iter.cell_slab_coords(),
          iter.cell_slab_length());
      if (overlaps) {
        // Calculate the destination pointers.
        auto dest_ptr = dst_buf + cell_offset * cell_size;
        auto dest_validity_ptr = dst_val_buf + cell_offset;

        // Get the tile buffers.
        const auto& tile = tile_tuples[fd]->fixed_tile();

        auto src_offset = iter.pos_in_tile() + start * stride;

        // If the subarray and tile are in the same order, copy the whole
        // slab.
        if (stride == 1) {
          std::memcpy(
              dest_ptr + cell_size * start,
              tile.data_as<char>() + cell_size * src_offset,
              cell_size * (end - start + 1));

          if (nullable) {
            const auto& tile_nullable = tile_tuples[fd]->validity_tile();
            std::memcpy(
                dest_validity_ptr + start,
                tile_nullable.data_as<char>() + src_offset,
                (end - start + 1));
          }
        } else {
          // Go cell by cell.
          auto src = tile.data_as<char>() + cell_size * src_offset;
          auto dest = dest_ptr + cell_size * start;
          for (uint64_t i = 0; i < end - start + 1; ++i) {
            std::memcpy(dest, src, cell_size);
            src += cell_size * stride;
            dest += cell_size;
          }

          if (nullable) {
            const auto& tile_nullable = tile_tuples[fd]->validity_tile();
            auto src_validity = tile_nullable.data_as<char>() + src_offset;
            auto dest_validity = dest_validity_ptr + start;
            for (uint64_t i = 0; i < end - start + 1; ++i) {
              memcpy(dest_validity, src_validity, 1);
              src_validity += stride;
              dest_validity++;
            }
          }
        }

        end = end + 1;
      }

      // Fill the non written cells for the first fragment domain with the fill
      // value.

      // Calculate the destination pointers.
      auto dest_ptr = dst_buf + cell_offset * cell_size;
      auto dest_validity_ptr = dst_val_buf + cell_offset;

      // Do the filling.
      if (fd == (int32_t)frag_domains.size() - 1) {
        auto buff = dest_ptr;
        for (uint64_t i = 0; i < start; ++i) {
          std::memcpy(buff, fill_value.data(), fill_value.size());
          buff += fill_value.size();
        }

        buff = dest_ptr + end * fill_value.size();
        for (uint64_t i = 0; i < iter.cell_slab_length() - end; ++i) {
          std::memcpy(buff, fill_value.data(), fill_value.size());
          buff += fill_value.size();
        }

        if (nullable) {
          std::memset(dest_validity_ptr, fill_value_nullable, start);
          std::memset(
              dest_validity_ptr + end,
              fill_value_nullable,
              iter.cell_slab_length() - end);
        }
      }
    }

    // Check if we need to fill the whole slab or apply query condition.

    // Calculate the destination pointers.
    auto dest_ptr = dst_buf + cell_offset * cell_size;
    auto dest_validity_ptr = dst_val_buf + cell_offset;

    // Need to fill the whole slab.
    if (frag_domains.size() == 0) {
      auto buff = dest_ptr;
      for (uint64_t i = 0; i < iter.cell_slab_length(); ++i) {
        std::memcpy(buff, fill_value.data(), fill_value.size());
        buff += fill_value.size();
      }

      if (nullable) {
        std::memset(
            dest_validity_ptr, fill_value_nullable, iter.cell_slab_length());
      }
    }

    // Apply query condition results to this slab.
    if (!condition_.empty()) {
      for (uint64_t c = 0; c < iter.cell_slab_length(); c++) {
        if (!(qc_result[c + cell_offset] & 0x1)) {
          memcpy(
              dest_ptr + c * cell_size, fill_value.data(), fill_value.size());

          if (nullable) {
            std::memset(dest_validity_ptr + c, fill_value_nullable, 1);
          }
        }
      }
    }

    // Adjust the cell offset for global order.
    if (layout_ == Layout::GLOBAL_ORDER) {
      cell_offset += iter.cell_slab_length();
    }

    ++iter;
  }

  return Status::Ok();
}

template <class DimType, class OffType>
Status DenseReader::copy_offset_tiles(
    const std::string& name,
    const std::vector<DimType>& tile_extents,
    ResultSpaceTile<DimType>& result_space_tile,
    const Subarray& subarray,
    const Subarray& tile_subarray,
    const uint64_t global_cell_offset,
    std::vector<void*>& var_data,
    const std::vector<RangeInfo<DimType>>& range_info,
    const std::vector<uint8_t>& qc_result,
    const uint64_t range_thread_idx,
    const uint64_t num_range_threads) {
  // For easy reference
  const auto dim_num = array_schema_.dim_num();
  const auto cell_order = array_schema_.cell_order();
  const auto cell_num_per_tile = array_schema_.domain().cell_num_per_tile();
  auto stride = array_schema_.domain().stride<DimType>(layout_);
  const auto& frag_domains = result_space_tile.frag_domains();
  auto dst_buf = static_cast<uint8_t*>(buffers_[name].buffer_);
  auto dst_val_buf = buffers_[name].validity_vector_.buffer();
  const auto attribute = array_schema_.attribute(name);
  const auto data_type_size = datatype_size(array_schema_.type(name));
  const auto nullable = attribute->nullable();

  // Cache tile tuples.
  std::vector<ResultTile::TileTuple*> tile_tuples(frag_domains.size());
  for (uint32_t fd = 0; fd < frag_domains.size(); ++fd) {
    tile_tuples[fd] =
        result_space_tile.result_tile(frag_domains[fd].fid())->tile_tuple(name);
    assert(tile_tuples[fd] != nullptr);
  }

  if (stride == UINT64_MAX) {
    stride = 1;
  }

  // Iterate over all coordinates, retrieved in cell slabs
  TileCellSlabIter<DimType> iter(
      range_thread_idx,
      num_range_threads,
      subarray,
      tile_subarray,
      tile_extents,
      result_space_tile.start_coords(),
      range_info,
      cell_order);

  // Initialise for global order, will be adjusted later for row/col major.
  uint64_t cell_offset = global_cell_offset + iter.global_offset();
  while (!iter.end()) {
    // Compute cell offset for row/col major orders.
    if (layout_ != Layout::GLOBAL_ORDER) {
      cell_offset = iter.dest_offset_row_col();
    }

    // Get the source cell offset.
    uint64_t src_cell = iter.pos_in_tile();

    // Iterate through all fragment domains and copy data.
    for (int32_t fd = (int32_t)frag_domains.size() - 1; fd >= 0; --fd) {
      // If the cell slab overlaps this fragment domain range, copy data.
      auto&& [overlaps, start, end] = cell_slab_overlaps_range(
          dim_num,
          frag_domains[fd].domain(),
          iter.cell_slab_coords(),
          iter.cell_slab_length());
      if (overlaps) {
        // Calculate the destination pointers.
        auto dest_ptr = dst_buf + cell_offset * sizeof(OffType);
        auto var_data_buff = var_data.data() + cell_offset;
        auto dest_validity_ptr = dst_val_buf + cell_offset;

        // Get the tile buffers.
        const auto& t_var = tile_tuples[fd]->var_tile();

        // Setup variables for the copy.
        auto src_buff =
            static_cast<uint64_t*>(tile_tuples[fd]->fixed_tile().data()) +
            start * stride + src_cell;
        auto div = elements_mode_ ? data_type_size : 1;
        auto dest = (OffType*)dest_ptr + start;

        // Copy the data cell by cell, last copy was taken out to take
        // advantage of vectorization.
        uint64_t i = 0;
        for (; i < end - start; ++i) {
          auto i_src = i * stride;
          dest[i] = (src_buff[i_src + 1] - src_buff[i_src]) / div;
          var_data_buff[i + start] = t_var.data_as<char>() + src_buff[i_src];
        }

        // Copy the last value.
        if (start + src_cell + (end - start) * stride >=
            cell_num_per_tile - 1) {
          dest[i] = (t_var.size() - src_buff[i * stride]) / div;
        } else {
          auto i_src = i * stride;
          dest[i] = (src_buff[i_src + 1] - src_buff[i_src]) / div;
        }
        var_data_buff[i + start] = t_var.data_as<char>() + src_buff[i * stride];

        if (nullable) {
          auto src_buff_validity =
              static_cast<uint8_t*>(tile_tuples[fd]->validity_tile().data()) +
              start + src_cell;

          for (i = 0; i < end - start; ++i) {
            dest_validity_ptr[start + i] = src_buff_validity[i * stride];
          }

          // Copy last validity value.
          dest_validity_ptr[start + i] = src_buff_validity[i * stride];
        }

        end = end + 1;
      }

      // Fill the non written cells for the first fragment domain with max
      // value.

      // Calculate the destination pointers.
      auto dest_ptr = dst_buf + cell_offset * sizeof(OffType);
      auto dest_validity_ptr = dst_val_buf + cell_offset;
      const auto& fill_value_nullable = attribute->fill_value_validity();

      // Do the filling.
      if (fd == (int32_t)frag_domains.size() - 1) {
        memset(dest_ptr, 0xFF, start * sizeof(OffType));
        memset(
            dest_ptr + end * sizeof(OffType),
            0xFF,
            (iter.cell_slab_length() - end) * sizeof(OffType));

        if (nullable) {
          std::memset(dest_validity_ptr, fill_value_nullable, start);
          std::memset(
              dest_validity_ptr + end,
              fill_value_nullable,
              iter.cell_slab_length() - end);
        }
      }
    }

    // Check if we need to fill the whole slab or apply query condition.

    // Calculate the destination pointers.
    auto dest_ptr = dst_buf + cell_offset * sizeof(OffType);
    auto dest_validity_ptr = dst_val_buf + cell_offset;
    const auto& fill_value_nullable = attribute->fill_value_validity();

    // Need to fill the whole slab.
    if (frag_domains.size() == 0) {
      memset(dest_ptr, 0xFF, iter.cell_slab_length() * sizeof(OffType));

      if (nullable) {
        std::memset(
            dest_validity_ptr, fill_value_nullable, iter.cell_slab_length());
      }
    }

    if (!condition_.empty()) {
      // Apply query condition results to this slab.
      for (uint64_t c = 0; c < iter.cell_slab_length(); c++) {
        if (!(qc_result[c + cell_offset] & 0x1)) {
          memset(dest_ptr + c * sizeof(OffType), 0xFF, sizeof(OffType));
        }

        if (nullable) {
          std::memset(dest_validity_ptr + c, fill_value_nullable, 1);
        }
      }
    }

    // Adjust the cell offset for global order.
    if (layout_ == Layout::GLOBAL_ORDER) {
      cell_offset += iter.cell_slab_length();
    }

    ++iter;
  }

  return Status::Ok();
}

template <class DimType, class OffType>
Status DenseReader::copy_var_tiles(
    const std::string& name,
    const std::vector<DimType>& tile_extents,
    ResultSpaceTile<DimType>& result_space_tile,
    const Subarray& subarray,
    const Subarray& tile_subarray,
    const uint64_t global_cell_offset,
    std::vector<void*>& var_data,
    const std::vector<RangeInfo<DimType>>& range_info,
    bool last_tile,
    uint64_t var_buffer_size,
    const uint64_t range_thread_idx,
    const uint64_t num_range_threads) {
  // For easy reference
  const auto cell_order = array_schema_.cell_order();
  auto dst_buf = static_cast<uint8_t*>(buffers_[name].buffer_var_);
  auto offsets_buf = static_cast<OffType*>(buffers_[name].buffer_);
  const auto data_type_size = datatype_size(array_schema_.type(name));

  // Iterate over all coordinates, retrieved in cell slabs
  TileCellSlabIter<DimType> iter(
      range_thread_idx,
      num_range_threads,
      subarray,
      tile_subarray,
      tile_extents,
      result_space_tile.start_coords(),
      range_info,
      cell_order);

  // Initialise for global order, will be adjusted later for row/col major.
  uint64_t cell_offset = global_cell_offset + iter.global_offset();
  while (!iter.end()) {
    // Compute cell offset for row/col major orders.
    if (layout_ != Layout::GLOBAL_ORDER) {
      cell_offset = iter.dest_offset_row_col();
    }

    auto cell_slab_length = iter.cell_slab_length();
    ++iter;

    // Setup variables for the copy.
    auto mult = elements_mode_ ? data_type_size : 1;
    uint64_t size = 0;
    uint64_t offset = 0;
    uint64_t i = 0;

    // Copy the data cell by cell, last copy was taken out to take advantage
    // of vectorization.
    for (; i < cell_slab_length - 1; i++) {
      offset = offsets_buf[cell_offset + i] * mult;
      size = offsets_buf[cell_offset + i + 1] * mult - offset;
      std::memcpy(dst_buf + offset, var_data[cell_offset + i], size);
    }

    // Do the last copy.
    offset = offsets_buf[cell_offset + i] * mult;
    if (last_tile && iter.last_slab() && i == cell_slab_length - 1) {
      size = var_buffer_size * mult - offset;
    } else {
      size = offsets_buf[cell_offset + i + 1] * mult - offset;
    }
    std::memcpy(dst_buf + offset, var_data[cell_offset + i], size);

    // Adjust cell offset for global order.
    if (layout_ == Layout::GLOBAL_ORDER) {
      cell_offset += cell_slab_length;
    }
  }

  return Status::Ok();
}

Status DenseReader::add_extra_offset() {
  // Add extra offset element for all var size offset buffers.
  for (const auto& it : buffers_) {
    const auto& name = it.first;
    if (!array_schema_.var_size(name)) {
      continue;
    }

    // Do not apply offset for empty results because we will
    // write backwards and corrupt memory we don't own.
    if (*it.second.buffer_size_ == 0) {
      continue;
    }

    auto buffer = static_cast<unsigned char*>(it.second.buffer_);
    if (offsets_format_mode_ == "bytes") {
      memcpy(
          buffer + *it.second.buffer_size_,
          it.second.buffer_var_size_,
          offsets_bytesize());
    } else if (offsets_format_mode_ == "elements") {
      auto elements =
          *it.second.buffer_var_size_ / datatype_size(array_schema_.type(name));
      memcpy(buffer + *it.second.buffer_size_, &elements, offsets_bytesize());
    } else {
      return LOG_STATUS(Status_ReaderError(
          "Cannot add extra offset to buffer; Unsupported offsets format"));
    }

    *it.second.buffer_size_ += offsets_bytesize();
  }

  return Status::Ok();
}

template <class T>
void DenseReader::fill_dense_coords(
    const Subarray& subarray, const optional<std::vector<uint8_t>> qc_results) {
  auto timer_se = stats_->start_timer("fill_dense_coords");

  // Count the number of cells.
  auto cell_num = subarray.cell_num();
  if (qc_results.has_value()) {
    auto func = [](uint64_t res, const uint8_t v) { return res + v; };
    cell_num = std::accumulate(
        qc_results.value().begin(), qc_results.value().end(), 0, func);
  }

  // Prepare buffers.
  std::vector<unsigned> dim_idx;
  std::vector<QueryBuffer*> buffers;
  auto coords_it = buffers_.find(constants::coords);
  auto dim_num = array_schema_.dim_num();
  if (coords_it != buffers_.end()) {
    // Check for overflow.
    if (coords_it->second.original_buffer_size_ <
        cell_num * array_schema_.cell_size(constants::coords)) {
      read_state_.overflowed_ = true;
      return;
    }
    buffers.emplace_back(&(coords_it->second));
    dim_idx.emplace_back(dim_num);
  } else {
    for (unsigned d = 0; d < dim_num; ++d) {
      const auto dim{array_schema_.dimension_ptr(d)};
      auto it = buffers_.find(dim->name());
      if (it != buffers_.end()) {
        // Check for overflow.
        if (it->second.original_buffer_size_ <
            cell_num * array_schema_.cell_size(dim->name())) {
          read_state_.overflowed_ = true;
          return;
        }
        buffers.emplace_back(&(it->second));
        dim_idx.emplace_back(d);
      }
    }
  }

  uint64_t qc_results_index = 0;
  std::vector<uint64_t> offsets(buffers.size(), 0);
  if (layout_ == Layout::GLOBAL_ORDER) {
    fill_dense_coords_global<T>(
        subarray, qc_results, qc_results_index, dim_idx, buffers, offsets);
  } else {
    assert(layout_ == Layout::ROW_MAJOR || layout_ == Layout::COL_MAJOR);
    fill_dense_coords_row_col<T>(
        subarray, qc_results, qc_results_index, dim_idx, buffers, offsets);
  }

  // Update buffer sizes.
  for (size_t i = 0; i < buffers.size(); ++i) {
    *(buffers[i]->buffer_size_) = offsets[i];
  }
}

template <class T>
void DenseReader::fill_dense_coords_global(
    const Subarray& subarray,
    const optional<std::vector<uint8_t>> qc_results,
    uint64_t& qc_results_index,
    const std::vector<unsigned>& dim_idx,
    const std::vector<QueryBuffer*>& buffers,
    std::vector<uint64_t>& offsets) {
  auto tile_coords = subarray.tile_coords();
  auto cell_order = array_schema_.cell_order();

  for (const auto& tc : tile_coords) {
    auto tile_subarray = subarray.crop_to_tile((const T*)&tc[0], cell_order);
    fill_dense_coords_row_col<T>(
        tile_subarray, qc_results, qc_results_index, dim_idx, buffers, offsets);
  }
}

template <class T>
void DenseReader::fill_dense_coords_row_col(
    const Subarray& subarray,
    const optional<std::vector<uint8_t>> qc_results,
    uint64_t& qc_results_index,
    const std::vector<unsigned>& dim_idx,
    const std::vector<QueryBuffer*>& buffers,
    std::vector<uint64_t>& offsets) {
  auto cell_order = array_schema_.cell_order();

  // Iterate over all coordinates, retrieved in cell slabs.
  CellSlabIter<T> iter(&subarray);
  if (!iter.begin().ok()) {
    throw DenseReaderStatusException("Cannot begin iteration");
  }
  while (!iter.end()) {
    auto cell_slab = iter.cell_slab();
    auto coords_num = cell_slab.length_;

    // Copy slab.
    if (layout_ == Layout::ROW_MAJOR ||
        (layout_ == Layout::GLOBAL_ORDER && cell_order == Layout::ROW_MAJOR))
      fill_dense_coords_row_slab(
          &cell_slab.coords_[0],
          qc_results,
          qc_results_index,
          coords_num,
          dim_idx,
          buffers,
          offsets);
    else
      fill_dense_coords_col_slab(
          &cell_slab.coords_[0],
          qc_results,
          qc_results_index,
          coords_num,
          dim_idx,
          buffers,
          offsets);

    ++iter;
  }
}

template <class T>
void DenseReader::fill_dense_coords_row_slab(
    const T* start,
    const optional<std::vector<uint8_t>> qc_results,
    uint64_t& qc_results_index,
    uint64_t num,
    const std::vector<unsigned>& dim_idx,
    const std::vector<QueryBuffer*>& buffers,
    std::vector<uint64_t>& offsets) const {
  // For easy reference.
  auto dim_num = array_schema_.dim_num();

  // Special zipped coordinates.
  if (dim_idx.size() == 1 && dim_idx[0] == dim_num) {
    auto c_buff = (char*)buffers[0]->buffer_;
    auto offset = &offsets[0];

    // Fill coordinates.
    for (uint64_t i = 0; i < num; ++i) {
      if (!qc_results.has_value() || qc_results.value()[qc_results_index]) {
        // First dim-1 dimensions are copied as they are.
        if (dim_num > 1) {
          auto bytes_to_copy = (dim_num - 1) * sizeof(T);
          std::memcpy(c_buff + *offset, start, bytes_to_copy);
          *offset += bytes_to_copy;
        }

        // Last dimension is incremented by `i`.
        auto new_coord = start[dim_num - 1] + i;
        std::memcpy(c_buff + *offset, &new_coord, sizeof(T));
        *offset += sizeof(T);
      }
      qc_results_index++;
    }
  } else {  // Set of separate coordinate buffers.
    for (uint64_t i = 0; i < num; ++i) {
      for (size_t b = 0; b < buffers.size(); ++b) {
        if (!qc_results.has_value() || qc_results.value()[qc_results_index]) {
          auto c_buff = (char*)buffers[b]->buffer_;
          auto offset = &offsets[b];

          // First dim-1 dimensions are copied as they are.
          if (dim_num > 1 && dim_idx[b] < dim_num - 1) {
            std::memcpy(c_buff + *offset, &start[dim_idx[b]], sizeof(T));
            *offset += sizeof(T);
          } else {
            // Last dimension is incremented by `i`.
            auto new_coord = start[dim_num - 1] + i;
            std::memcpy(c_buff + *offset, &new_coord, sizeof(T));
            *offset += sizeof(T);
          }
        }
      }
      qc_results_index++;
    }
  }
}

template <class T>
void DenseReader::fill_dense_coords_col_slab(
    const T* start,
    const optional<std::vector<uint8_t>> qc_results,
    uint64_t& qc_results_index,
    uint64_t num,
    const std::vector<unsigned>& dim_idx,
    const std::vector<QueryBuffer*>& buffers,
    std::vector<uint64_t>& offsets) const {
  // For easy reference.
  auto dim_num = array_schema_.dim_num();

  // Special zipped coordinates.
  if (dim_idx.size() == 1 && dim_idx[0] == dim_num) {
    auto c_buff = (char*)buffers[0]->buffer_;
    auto offset = &offsets[0];

    // Fill coordinates.
    for (uint64_t i = 0; i < num; ++i) {
      if (!qc_results.has_value() || qc_results.value()[qc_results_index]) {
        // First dimension is incremented by `i`.
        auto new_coord = start[0] + i;
        std::memcpy(c_buff + *offset, &new_coord, sizeof(T));
        *offset += sizeof(T);

        // Last dim-1 dimensions are copied as they are.
        if (dim_num > 1) {
          auto bytes_to_copy = (dim_num - 1) * sizeof(T);
          std::memcpy(c_buff + *offset, &start[1], bytes_to_copy);
          *offset += bytes_to_copy;
        }
      }
      qc_results_index++;
    }
  } else {  // Separate coordinate buffers.
    for (uint64_t i = 0; i < num; ++i) {
      for (size_t b = 0; b < buffers.size(); ++b) {
        if (!qc_results.has_value() || qc_results.value()[qc_results_index]) {
          auto c_buff = (char*)buffers[b]->buffer_;
          auto offset = &offsets[b];

          // First dimension is incremented by `i`.
          if (dim_idx[b] == 0) {
            auto new_coord = start[0] + i;
            std::memcpy(c_buff + *offset, &new_coord, sizeof(T));
            *offset += sizeof(T);
          } else {  // Last dim-1 dimensions are copied as they are
            std::memcpy(c_buff + *offset, &start[dim_idx[b]], sizeof(T));
            *offset += sizeof(T);
          }
        }
      }
      qc_results_index++;
    }
  }
}

}  // namespace sm
}  // namespace tiledb
