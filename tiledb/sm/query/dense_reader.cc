/**
 * @file   dense_reader.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2021 TileDB, Inc.
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
#include "tiledb/sm/query/dense_reader.h"
#include "tiledb/sm/query/query_macros.h"
#include "tiledb/sm/query/result_tile.h"
#include "tiledb/sm/stats/global_stats.h"
#include "tiledb/sm/storage_manager/storage_manager.h"
#include "tiledb/sm/subarray/cell_slab_iter.h"
#include "tiledb/sm/subarray/subarray.h"

using namespace tiledb;
using namespace tiledb::common;
using namespace tiledb::sm::stats;

namespace tiledb {
namespace sm {

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
    QueryCondition& condition)
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

Status DenseReader::init() {
  // Sanity checks.
  if (storage_manager_ == nullptr)
    return LOG_STATUS(Status_DenseReaderError(
        "Cannot initialize dense reader; Storage manager not set"));
  if (buffers_.empty())
    return LOG_STATUS(Status_DenseReaderError(
        "Cannot initialize dense reader; Buffers not set"));
  if (!subarray_.is_set())
    return LOG_STATUS(Status_ReaderError(
        "Cannot initialize reader; Dense reads must have a subarray set"));

  // Check subarray.
  RETURN_NOT_OK(check_subarray());

  // Initialize the read state.
  RETURN_NOT_OK(init_read_state());

  // Check the validity buffer sizes.
  RETURN_NOT_OK(check_validity_buffer_sizes());

  return Status::Ok();
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

  // Compute subarrays for each tile.
  const auto& tile_coords = subarray.tile_coords();
  std::vector<Subarray> tile_subarrays(tile_coords.size());
  const auto& layout =
      layout_ == Layout::GLOBAL_ORDER ? array_schema_.cell_order() : layout_;
  auto status = parallel_for(
      storage_manager_->compute_tp(),
      0,
      tile_subarrays.size(),
      [&](uint64_t t) {
        tile_subarrays[t] =
            subarray.crop_to_tile((const DimType*)&tile_coords[t][0], layout);

        return Status::Ok();
      });
  RETURN_NOT_OK(status);

  // Compute tile offsets for global order or range info for row/col major.
  std::vector<uint64_t> tile_offsets;
  std::vector<RangeInfo> range_info(dim_num);

  if (layout_ == Layout::GLOBAL_ORDER) {
    tile_offsets.reserve(tile_coords.size());

    uint64_t tile_offset = 0;
    for (auto& tile_subarray : tile_subarrays) {
      tile_offsets.emplace_back(tile_offset);
      tile_offset += tile_subarray.cell_num();
    }
  } else {
    for (uint32_t d = 0; d < dim_num; d++) {
      auto& ranges = subarray.ranges_for_dim(d);

      // Compute the 1D offset for every range in this dimension.
      range_info[d].cell_offsets_.reserve(ranges.size());
      uint64_t offset = 0;
      for (uint64_t r = 0; r < ranges.size(); r++) {
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
  RETURN_CANCEL_OR_ERROR(
      load_tile_offsets(read_state_.partitioner_.subarray(), names));

  // Read and unfilter tiles.
  RETURN_CANCEL_OR_ERROR(read_attribute_tiles(names, result_tiles));

  for (const auto& name : names) {
    RETURN_CANCEL_OR_ERROR(unfilter_tiles(name, result_tiles));
  }

  // Compute the result of the query condition.
  auto&& [st, qc_result] = apply_query_condition<DimType, OffType>(
      subarray, tile_subarrays, tile_offsets, range_info, result_space_tiles);
  RETURN_CANCEL_OR_ERROR(st);

  // Copy attribute data to users buffers.
  status = copy_attributes<DimType, OffType>(
      fixed_names,
      var_names,
      subarray,
      tile_subarrays,
      tile_offsets,
      range_info,
      result_space_tiles,
      *qc_result);
  RETURN_CANCEL_OR_ERROR(status);

  if (read_state_.overflowed_)
    Status::Ok();

  // Fill coordinates if the user requested them.
  if (!read_state_.overflowed_ && has_coords()) {
    auto&& [st, overflowed] = fill_dense_coords<DimType>(subarray);
    RETURN_CANCEL_OR_ERROR(st);
    read_state_.overflowed_ = *overflowed;
  }

  return Status::Ok();
}

Status DenseReader::init_read_state() {
  auto timer_se = stats_->start_timer("init_state");

  // Check subarray.
  if (subarray_.layout() == Layout::GLOBAL_ORDER && subarray_.range_num() != 1)
    return LOG_STATUS(
        Status_ReaderError("Cannot initialize read "
                           "state; Multi-range "
                           "subarrays do not "
                           "support global order"));

  // Get config values.
  bool found = false;
  uint64_t memory_budget = 0;
  RETURN_NOT_OK(
      config_.get<uint64_t>("sm.memory_budget", &memory_budget, &found));
  assert(found);

  uint64_t memory_budget_var = 0;
  RETURN_NOT_OK(config_.get<uint64_t>(
      "sm.memory_budget_var", &memory_budget_var, &found));
  assert(found);

  offsets_format_mode_ = config_.get("sm.var_offsets.mode", &found);
  assert(found);
  if (offsets_format_mode_ != "bytes" && offsets_format_mode_ != "elements") {
    return LOG_STATUS(
        Status_ReaderError("Cannot initialize reader; Unsupported offsets "
                           "format in configuration"));
  }
  elements_mode_ = offsets_format_mode_ == "elements";

  RETURN_NOT_OK(config_.get<bool>(
      "sm.var_offsets.extra_element", &offsets_extra_element_, &found));
  assert(found);

  RETURN_NOT_OK(config_.get<uint32_t>(
      "sm.var_offsets.bitsize", &offsets_bitsize_, &found));
  if (offsets_bitsize_ != 32 && offsets_bitsize_ != 64) {
    return LOG_STATUS(
        Status_ReaderError("Cannot initialize reader; Unsupported offsets "
                           "bitsize in configuration"));
  }
  assert(found);

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

  // Set result size budget.
  for (const auto& a : buffers_) {
    auto attr_name = a.first;
    auto buffer_size = a.second.buffer_size_;
    auto buffer_var_size = a.second.buffer_var_size_;
    auto buffer_validity_size = a.second.validity_vector_.buffer_size();
    if (!array_schema_.var_size(attr_name)) {
      if (!array_schema_.is_nullable(attr_name)) {
        RETURN_NOT_OK(read_state_.partitioner_.set_result_budget(
            attr_name.c_str(), *buffer_size));
      } else {
        RETURN_NOT_OK(read_state_.partitioner_.set_result_budget_nullable(
            attr_name.c_str(), *buffer_size, *buffer_validity_size));
      }
    } else {
      if (!array_schema_.is_nullable(attr_name)) {
        RETURN_NOT_OK(read_state_.partitioner_.set_result_budget(
            attr_name.c_str(), *buffer_size, *buffer_var_size));
      } else {
        RETURN_NOT_OK(read_state_.partitioner_.set_result_budget_nullable(
            attr_name.c_str(),
            *buffer_size,
            *buffer_var_size,
            *buffer_validity_size));
      }
    }
  }

  read_state_.unsplittable_ = false;
  read_state_.overflowed_ = false;
  read_state_.initialized_ = true;

  return Status::Ok();
}

/** Apply the query condition. */
template <class DimType, class OffType>
tuple<Status, optional<std::vector<uint8_t>>>
DenseReader::apply_query_condition(
    Subarray& subarray,
    std::vector<Subarray>& tile_subarrays,
    std::vector<uint64_t>& tile_offsets,
    const std::vector<RangeInfo>& range_info,
    std::map<const DimType*, ResultSpaceTile<DimType>>& result_space_tiles) {
  auto timer_se = stats_->start_timer("apply_query_condition");
  std::vector<uint8_t> qc_result;
  if (!condition_.clauses().empty()) {
    // For easy reference.
    const auto& tile_coords = subarray.tile_coords();
    const auto cell_num = subarray.cell_num();
    const auto dim_num = array_schema_.dim_num();
    auto stride = array_schema_.domain().stride<DimType>(layout_);
    const auto& domain{array_schema_.domain()};
    const auto cell_order = array_schema_.cell_order();
    const auto global_order = layout_ == Layout::GLOBAL_ORDER;

    if (stride == UINT64_MAX) {
      stride = 1;
    }

    // Initialize the result buffer.
    qc_result.resize(cell_num, 1);

    // Process all tiles in parallel.
    auto status = parallel_for(
        storage_manager_->compute_tp(), 0, tile_coords.size(), [&](uint64_t t) {
          // Find out result space tile and tile subarray.
          const DimType* tc = (DimType*)&tile_coords[t][0];
          auto it = result_space_tiles.find(tc);
          assert(it != result_space_tiles.end());

          const auto& frag_domains = it->second.frag_domains();
          uint64_t cell_offset = global_order ? tile_offsets[t] : 0;
          auto dest_ptr = qc_result.data() + cell_offset;

          // Iterate over all coordinates, retrieved in cell slab.
          CellSlabIter<DimType> iter(&tile_subarrays[t]);
          RETURN_NOT_OK(iter.begin());
          while (!iter.end()) {
            auto cell_slab = iter.cell_slab();

            // Compute destination pointer for row/col major orders.
            if (!global_order) {
              cell_offset = get_dest_cell_offset_row_col(
                  dim_num,
                  subarray,
                  tile_subarrays[t],
                  cell_slab.coords_.data(),
                  iter.range_coords(),
                  range_info);
              dest_ptr = qc_result.data() + cell_offset;
            }

            // Get the source cell offset.
            uint64_t src_cell = get_cell_pos_in_tile(
                cell_order,
                dim_num,
                domain,
                it->second,
                cell_slab.coords_.data());

            for (int32_t i = (int32_t)frag_domains.size() - 1; i >= 0; --i) {
              // If the cell slab overlaps this fragment domain range, apply
              // clause.
              auto&& [overlaps, start, end] = cell_slab_overlaps_range(
                  dim_num,
                  frag_domains[i].second,
                  cell_slab.coords_.data(),
                  cell_slab.length_);
              if (overlaps) {
                RETURN_NOT_OK(condition_.apply_dense(
                    *(fragment_metadata_[frag_domains[i].first]
                          ->array_schema()
                          .get()),
                    it->second.result_tile(frag_domains[i].first),
                    start,
                    end - start + 1,
                    src_cell,
                    stride,
                    dest_ptr));
              }
            }

            // Adjust the destination pointers for global order.
            if (global_order) {
              dest_ptr += cell_slab.length_;
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

  if (nullable)
    *buffers_[name].validity_vector_.buffer_size() = cell_num;

  // Return the buffer size.
  return offset;
}

template <class DimType, class OffType>
Status DenseReader::copy_attributes(
    const std::vector<std::string>& fixed_names,
    const std::vector<std::string>& var_names,
    const Subarray& subarray,
    const std::vector<Subarray>& tile_subarrays,
    const std::vector<uint64_t>& tile_offsets,
    const std::vector<RangeInfo>& range_info,
    std::map<const DimType*, ResultSpaceTile<DimType>>& result_space_tiles,
    const std::vector<uint8_t>& qc_result) {
  auto timer_se = stats_->start_timer("copy_attributes");

  // For easy reference
  const auto& tile_coords = subarray.tile_coords();
  const auto cell_num = subarray.cell_num();
  const auto global_order = layout_ == Layout::GLOBAL_ORDER;

  if (!var_names.empty()) {
    // Make sure the user offset buffers are big enough.
    for (auto& name : var_names) {
      const auto required_size =
          (cell_num + offsets_extra_element_) * sizeof(OffType);
      if (required_size > *buffers_[name].buffer_size_) {
        read_state_.overflowed_ = true;
        return Status::Ok();
      }
    }

    // Vector to hold pointers to the var data.
    std::vector<std::vector<void*>> var_data(var_names.size());
    for (auto& var_data_buff : var_data) {
      var_data_buff.resize(cell_num);
    }

    // Make some vectors to prevent map lookups.
    std::vector<uint8_t*> dst_off_bufs;
    std::vector<uint8_t*> dst_var_bufs;
    std::vector<uint8_t*> dst_val_bufs;
    std::vector<const Attribute*> attributes;
    std::vector<uint64_t> data_type_sizes;
    dst_off_bufs.reserve(var_names.size());
    dst_var_bufs.reserve(var_names.size());
    dst_val_bufs.reserve(var_names.size());
    attributes.reserve(var_names.size());
    data_type_sizes.reserve(var_names.size());

    for (auto& name : var_names) {
      dst_off_bufs.push_back((uint8_t*)buffers_[name].buffer_);
      dst_var_bufs.push_back((uint8_t*)buffers_[name].buffer_var_);
      dst_val_bufs.push_back(buffers_[name].validity_vector_.buffer());
      attributes.push_back(array_schema_.attribute(name));
      data_type_sizes.push_back(datatype_size(array_schema_.type(name)));
    }

    // Process offsets in parallel.
    {
      auto timer_se = stats_->start_timer("copy_offset_tiles");
      auto status = parallel_for(
          storage_manager_->compute_tp(),
          0,
          tile_coords.size(),
          [&](uint64_t t) {
            // Find out result space tile and tile subarray.
            const DimType* tc = (DimType*)&tile_coords[t][0];
            auto it = result_space_tiles.find(tc);
            assert(it != result_space_tiles.end());

            // Copy the tile offsets.
            return copy_offset_tiles<DimType, OffType>(
                var_names,
                dst_off_bufs,
                dst_val_bufs,
                attributes,
                data_type_sizes,
                it->second,
                subarray,
                tile_subarrays[t],
                global_order ? tile_offsets[t] : 0,
                var_data,
                range_info,
                qc_result);
          });
      RETURN_NOT_OK(status);
    }

    // We have the cell lengths in the users buffer, convert to offsets.
    std::vector<uint64_t> var_buffer_sizes(var_names.size());
    {
      auto timer_se = stats_->start_timer("fix_offset_tiles");
      auto status = parallel_for(
          storage_manager_->compute_tp(), 0, var_names.size(), [&](uint64_t n) {
            const auto& name = var_names[n];
            const bool nullable = array_schema_.is_nullable(name);
            var_buffer_sizes[n] = fix_offsets_buffer<OffType>(
                name, nullable, cell_num, var_data[n]);

            // Make sure the user var buffer is big enough.
            uint64_t required_var_size = var_buffer_sizes[n];
            if (elements_mode_)
              required_var_size *= datatype_size(array_schema_.type(name));

            // Exit early in case of overflow.
            if (read_state_.overflowed_ ||
                required_var_size > *buffers_[name].buffer_var_size_) {
              read_state_.overflowed_ = true;
              return Status::Ok();
            }

            *buffers_[name].buffer_var_size_ = required_var_size;
            return Status::Ok();
          });
      RETURN_NOT_OK(status);
    }

    if (read_state_.overflowed_) {
      return Status::Ok();
    }

    {
      auto timer_se = stats_->start_timer("copy_var_tiles");
      // Process var data in parallel.
      auto status = parallel_for(
          storage_manager_->compute_tp(),
          0,
          tile_coords.size(),
          [&](uint64_t t) {
            return copy_var_tiles<DimType, OffType>(
                var_names,
                dst_var_bufs,
                dst_off_bufs,
                data_type_sizes,
                subarray,
                tile_subarrays[t],
                global_order ? tile_offsets[t] : 0,
                var_data,
                range_info,
                t == tile_coords.size() - 1,
                var_buffer_sizes);

            return Status::Ok();
          });
      RETURN_NOT_OK(status);
    }
  }

  if (!fixed_names.empty()) {
    // Make sure the user fixed buffers are big enough.
    for (auto& name : fixed_names) {
      const auto required_size = cell_num * array_schema_.cell_size(name);
      if (required_size > *buffers_[name].buffer_size_) {
        read_state_.overflowed_ = true;
        return Status::Ok();
      }
    }

    // Make some vectors to prevent map lookups.
    std::vector<uint8_t*> dst_bufs;
    std::vector<uint8_t*> dst_val_bufs;
    std::vector<const Attribute*> attributes;
    std::vector<uint64_t> cell_sizes;
    dst_bufs.reserve(fixed_names.size());
    dst_val_bufs.reserve(fixed_names.size());
    attributes.reserve(fixed_names.size());
    cell_sizes.reserve(fixed_names.size());

    for (auto& name : fixed_names) {
      dst_bufs.push_back((uint8_t*)buffers_[name].buffer_);
      dst_val_bufs.push_back(buffers_[name].validity_vector_.buffer());
      attributes.push_back(array_schema_.attribute(name));
      cell_sizes.push_back(array_schema_.cell_size(name));
    }

    {
      auto timer_se = stats_->start_timer("copy_fixed_tiles");
      // Process values in parallel.
      auto status = parallel_for(
          storage_manager_->compute_tp(),
          0,
          tile_coords.size(),
          [&](uint64_t t) {
            // Find out result space tile and tile subarray.
            const DimType* tc = (DimType*)&tile_coords[t][0];
            auto it = result_space_tiles.find(tc);
            assert(it != result_space_tiles.end());

            // Copy the tile fixed values.
            RETURN_NOT_OK(copy_fixed_tiles(
                fixed_names,
                dst_bufs,
                dst_val_bufs,
                attributes,
                cell_sizes,
                it->second,
                subarray,
                tile_subarrays[t],
                global_order ? tile_offsets[t] : 0,
                range_info,
                qc_result));

            return Status::Ok();
          });
      RETURN_NOT_OK(status);
    }

    // Set the output size for the fixed buffer.
    for (auto& name : fixed_names) {
      const auto required_size = cell_num * array_schema_.cell_size(name);

      // Set the output size for the fixed buffer.
      *buffers_[name].buffer_size_ = required_size;

      if (array_schema_.is_nullable(name))
        *buffers_[name].validity_vector_.buffer_size() = cell_num;
    }
  }

  return Status::Ok();
}

template <class DimType>
uint64_t DenseReader::get_cell_pos_in_tile(
    const Layout& cell_order,
    const int32_t dim_num,
    const Domain& domain,
    const ResultSpaceTile<DimType>& result_space_tile,
    const DimType* const coords) {
  uint64_t pos = 0;
  uint64_t mult = 1;

  const auto& start = result_space_tile.start_coords();
  if (cell_order == Layout::COL_MAJOR) {
    for (int32_t d = 0; d < dim_num; d++) {
      pos += mult * (coords[d] - start[d]);
      mult *= *(const DimType*)domain.tile_extent(d).data();
    }
  } else {
    for (auto d = dim_num - 1; d >= 0; d--) {
      pos += mult * (coords[d] - start[d]);
      mult *= *(const DimType*)domain.tile_extent(d).data();
    }
  }

  return pos;
}

template <class DimType>
tuple<bool, uint64_t, uint64_t> DenseReader::cell_slab_overlaps_range(
    const unsigned dim_num,
    const NDRange& ndrange,
    const DimType* coords,
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
uint64_t DenseReader::get_dest_cell_offset_row_col(
    const int32_t dim_num,
    const Subarray& subarray,
    const Subarray& tile_subarray,
    const DimType* const coords,
    const DimType* const range_coords,
    const std::vector<RangeInfo>& range_info) {
  uint64_t ret = 0;
  std::vector<uint64_t> converted_range_coords(dim_num);
  if (subarray.range_num() > 1) {
    tile_subarray.get_original_range_coords(
        range_coords, &converted_range_coords);
  }

  if (subarray.layout() == Layout::COL_MAJOR) {
    for (int32_t d = 0; d < dim_num; d++) {
      auto r = converted_range_coords[d];
      auto min = *static_cast<const DimType*>(
          subarray.ranges_for_dim((uint32_t)d)[r].start_fixed());
      ret += range_info[d].multiplier_ *
             (coords[d] - min + range_info[d].cell_offsets_[r]);
    }
  } else {
    for (int32_t d = dim_num - 1; d >= 0; d--) {
      auto r = converted_range_coords[d];
      auto min = *static_cast<const DimType*>(
          subarray.ranges_for_dim((uint32_t)d)[r].start_fixed());
      ret += range_info[d].multiplier_ *
             (coords[d] - min + range_info[d].cell_offsets_[r]);
    }
  }

  return ret;
}

template <class DimType>
Status DenseReader::copy_fixed_tiles(
    const std::vector<std::string>& names,
    const std::vector<uint8_t*>& dst_bufs,
    const std::vector<uint8_t*>& dst_val_bufs,
    const std::vector<const Attribute*>& attributes,
    const std::vector<uint64_t>& cell_sizes,
    ResultSpaceTile<DimType>& result_space_tile,
    const Subarray& subarray,
    const Subarray& tile_subarray,
    const uint64_t global_cell_offset,
    const std::vector<RangeInfo>& range_info,
    const std::vector<uint8_t>& qc_result) {
  // For easy reference
  const auto dim_num = array_schema_.dim_num();
  const auto& domain{array_schema_.domain()};
  const auto cell_order = array_schema_.cell_order();
  auto stride = array_schema_.domain().stride<DimType>(layout_);
  const auto& frag_domains = result_space_tile.frag_domains();

  if (stride == UINT64_MAX) {
    stride = 1;
  }

  // Initialise for global order, will be adjusted later for row/col major.
  uint64_t cell_offset = global_cell_offset;

  // Iterate over all coordinates, retrieved in cell slab.
  CellSlabIter<DimType> iter(&tile_subarray);
  RETURN_CANCEL_OR_ERROR(iter.begin());
  while (!iter.end()) {
    auto cell_slab = iter.cell_slab();

    // Compute cell offset for row/col major orders.
    if (layout_ != Layout::GLOBAL_ORDER) {
      cell_offset = get_dest_cell_offset_row_col(
          dim_num,
          subarray,
          tile_subarray,
          cell_slab.coords_.data(),
          iter.range_coords(),
          range_info);
    }

    // Get the source cell offset.
    uint64_t src_cell = get_cell_pos_in_tile(
        cell_order,
        dim_num,
        domain,
        result_space_tile,
        cell_slab.coords_.data());

    // Iterate through all fragment domains and copy data.
    for (int32_t fd = (int32_t)frag_domains.size() - 1; fd >= 0; --fd) {
      // If the cell slab overlaps this fragment domain range, copy data.
      auto&& [overlaps, start, end] = cell_slab_overlaps_range(
          dim_num,
          frag_domains[fd].second,
          cell_slab.coords_.data(),
          cell_slab.length_);
      if (overlaps) {
        for (uint64_t n = 0; n < names.size(); n++) {
          // Calculate the destination pointers.
          const auto cell_size = cell_sizes[n];
          auto dest_ptr = dst_bufs[n] + cell_offset * cell_size;
          auto dest_validity_ptr = dst_val_bufs[n] + cell_offset;

          // Get the tile buffers.
          const auto tile_tuple =
              result_space_tile.result_tile(frag_domains[fd].first)
                  ->tile_tuple(names[n]);
          assert(tile_tuple != nullptr);
          const Tile* const tile = &std::get<0>(*tile_tuple);
          const Tile* const tile_nullable = &std::get<2>(*tile_tuple);

          auto src_offset = src_cell + start * stride;

          // If the subarray and tile are in the same order, copy the whole
          // slab.
          if (stride == 1) {
            std::memcpy(
                dest_ptr + cell_size * start,
                tile->data_as<char>() + cell_size * src_offset,
                cell_size * (end - start + 1));

            if (attributes[n]->nullable()) {
              std::memcpy(
                  dest_validity_ptr + start,
                  tile_nullable->data_as<char>() + src_offset,
                  (end - start + 1));
            }
          } else {
            // Go cell by cell.
            const auto nullable = attributes[n]->nullable();
            auto src = tile->data_as<char>() + cell_size * src_offset;
            auto src_validity =
                nullable ? tile_nullable->data_as<char>() + src_offset :
                           nullptr;
            auto dest = dest_ptr + cell_size * start;
            auto dest_validity = dest_validity_ptr + start;
            for (uint64_t i = 0; i < end - start + 1; ++i) {
              std::memcpy(dest, src, cell_size);
              src += cell_size * stride;
              dest += cell_size;

              if (nullable) {
                memcpy(dest_validity, src_validity, 1);
                src_validity += stride;
                dest_validity++;
              }
            }
          }
        }

        end = end + 1;
      }

      // Fill the non written cells for the first fragment domain with the fill
      // value.
      for (uint64_t n = 0; n < names.size(); n++) {
        // Calculate the destination pointers.
        const auto cell_size = cell_sizes[n];
        auto dest_ptr = dst_bufs[n] + cell_offset * cell_size;
        auto dest_validity_ptr = dst_val_bufs[n] + cell_offset;
        const auto& fill_value = attributes[n]->fill_value();
        const auto& fill_value_nullable = attributes[n]->fill_value_validity();

        // Do the filling.
        if (fd == (int32_t)frag_domains.size() - 1) {
          auto buff = dest_ptr;
          for (uint64_t i = 0; i < start; ++i) {
            std::memcpy(buff, fill_value.data(), fill_value.size());
            buff += fill_value.size();
          }

          buff = dest_ptr + end * fill_value.size();
          for (uint64_t i = 0; i < cell_slab.length_ - end; ++i) {
            std::memcpy(buff, fill_value.data(), fill_value.size());
            buff += fill_value.size();
          }

          if (attributes[n]->nullable()) {
            std::memset(dest_validity_ptr, fill_value_nullable, start);
            std::memset(
                dest_validity_ptr + end,
                fill_value_nullable,
                cell_slab.length_ - end);
          }
        }
      }
    }

    // Check if we need to fill the whole slab or apply query condition.
    for (uint64_t n = 0; n < names.size(); n++) {
      // Calculate the destination pointers.
      const auto cell_size = cell_sizes[n];
      auto dest_ptr = dst_bufs[n] + cell_offset * cell_size;
      auto dest_validity_ptr = dst_val_bufs[n] + cell_offset;
      const auto& fill_value = attributes[n]->fill_value();
      const auto& fill_value_nullable = attributes[n]->fill_value_validity();

      // Need to fill the whole slab.
      if (frag_domains.size() == 0) {
        auto buff = dest_ptr;
        for (uint64_t i = 0; i < cell_slab.length_; ++i) {
          std::memcpy(buff, fill_value.data(), fill_value.size());
          buff += fill_value.size();
        }

        if (attributes[n]->nullable()) {
          std::memset(
              dest_validity_ptr, fill_value_nullable, cell_slab.length_);
        }
      }

      // Apply query condition results to this slab.
      if (!condition_.empty()) {
        for (uint64_t c = 0; c < cell_slab.length_; c++) {
          if (!(qc_result[c + cell_offset] & 0x1)) {
            memcpy(
                dest_ptr + c * cell_size, fill_value.data(), fill_value.size());

            if (attributes[n]->nullable()) {
              std::memset(dest_validity_ptr + c, fill_value_nullable, 1);
            }
          }
        }
      }
    }

    // Adjust the cell offset for global order.
    if (layout_ == Layout::GLOBAL_ORDER) {
      cell_offset += cell_slab.length_;
    }

    ++iter;
  }

  return Status::Ok();
}

template <class DimType, class OffType>
Status DenseReader::copy_offset_tiles(
    const std::vector<std::string>& names,
    const std::vector<uint8_t*>& dst_bufs,
    const std::vector<uint8_t*>& dst_val_bufs,
    const std::vector<const Attribute*>& attributes,
    const std::vector<uint64_t>& data_type_sizes,
    ResultSpaceTile<DimType>& result_space_tile,
    const Subarray& subarray,
    const Subarray& tile_subarray,
    const uint64_t global_cell_offset,
    std::vector<std::vector<void*>>& var_data,
    const std::vector<RangeInfo>& range_info,
    const std::vector<uint8_t>& qc_result) {
  // For easy reference
  const auto& domain{array_schema_.domain()};
  const auto dim_num = array_schema_.dim_num();
  const auto cell_order = array_schema_.cell_order();
  const auto cell_num_per_tile = array_schema_.domain().cell_num_per_tile();
  auto stride = array_schema_.domain().stride<DimType>(layout_);
  const auto& frag_domains = result_space_tile.frag_domains();

  if (stride == UINT64_MAX) {
    stride = 1;
  }

  // Initialise for global order, will be adjusted later for row/col major.
  uint64_t cell_offset = global_cell_offset;

  // Iterate over all coordinates, retrieved in cell slabs
  CellSlabIter<DimType> iter(&tile_subarray);
  RETURN_CANCEL_OR_ERROR(iter.begin());
  while (!iter.end()) {
    auto cell_slab = iter.cell_slab();

    // Compute cell offset for row/col major orders.
    if (layout_ != Layout::GLOBAL_ORDER) {
      cell_offset = get_dest_cell_offset_row_col(
          dim_num,
          subarray,
          tile_subarray,
          cell_slab.coords_.data(),
          iter.range_coords(),
          range_info);
    }

    // Get the source cell offset.
    uint64_t src_cell = get_cell_pos_in_tile(
        cell_order,
        dim_num,
        domain,
        result_space_tile,
        cell_slab.coords_.data());

    // Iterate through all fragment domains and copy data.
    for (int32_t fd = (int32_t)frag_domains.size() - 1; fd >= 0; --fd) {
      // If the cell slab overlaps this fragment domain range, copy data.
      auto&& [overlaps, start, end] = cell_slab_overlaps_range(
          dim_num,
          frag_domains[fd].second,
          cell_slab.coords_.data(),
          cell_slab.length_);
      if (overlaps) {
        for (uint64_t n = 0; n < names.size(); n++) {
          // Calculate the destination pointers.
          auto dest_ptr = dst_bufs[n] + cell_offset * sizeof(OffType);
          auto var_data_buff = var_data[n].data() + cell_offset;
          auto dest_validity_ptr = dst_val_bufs[n] + cell_offset;

          // Get the tile buffers.
          const auto tile_tuple =
              result_space_tile.result_tile(frag_domains[fd].first)
                  ->tile_tuple(names[n]);
          assert(tile_tuple != nullptr);
          const Tile* const t_var = &std::get<1>(*tile_tuple);

          // Setup variables for the copy.
          auto src_buff = (uint64_t*)std::get<0>(*tile_tuple).data() +
                          start * stride + src_cell;
          auto src_buff_validity =
              attributes[n]->nullable() ?
                  (uint8_t*)std::get<2>(*tile_tuple).data() + start + src_cell :
                  nullptr;
          auto div = elements_mode_ ? data_type_sizes[n] : 1;
          auto dest = (OffType*)dest_ptr + start;

          // Copy the data cell by cell, last copy was taken out to take
          // advantage of vectorization.
          uint64_t i = 0;
          for (; i < end - start; ++i) {
            auto i_src = i * stride;
            dest[i] = (src_buff[i_src + 1] - src_buff[i_src]) / div;
            var_data_buff[i + start] = t_var->data_as<char>() + src_buff[i_src];
          }

          if (attributes[n]->nullable()) {
            i = 0;
            for (; i < end - start; ++i) {
              dest_validity_ptr[start + i] = src_buff_validity[i * stride];
            }
          }

          // Copy the last value.
          if (start + src_cell + (end - start) * stride >=
              cell_num_per_tile - 1) {
            dest[i] = (t_var->size() - src_buff[i * stride]) / div;
          } else {
            auto i_src = i * stride;
            dest[i] = (src_buff[i_src + 1] - src_buff[i_src]) / div;
          }
          var_data_buff[i + start] =
              t_var->data_as<char>() + src_buff[i * stride];

          if (attributes[n]->nullable())
            dest_validity_ptr[start + i] = src_buff_validity[i * stride];
        }

        end = end + 1;
      }

      // Fill the non written cells for the first fragment domain with max
      // value.
      for (uint64_t n = 0; n < names.size(); n++) {
        // Calculate the destination pointers.
        auto dest_ptr = dst_bufs[n] + cell_offset * sizeof(OffType);
        auto dest_validity_ptr = dst_val_bufs[n] + cell_offset;
        const auto& fill_value_nullable = attributes[n]->fill_value_validity();

        // Do the filling.
        if (fd == (int32_t)frag_domains.size() - 1) {
          memset(dest_ptr, 0xFF, start * sizeof(OffType));
          memset(
              dest_ptr + end * sizeof(OffType),
              0xFF,
              (cell_slab.length_ - end) * sizeof(OffType));

          if (attributes[n]->nullable()) {
            std::memset(dest_validity_ptr, fill_value_nullable, start);
            std::memset(
                dest_validity_ptr + end,
                fill_value_nullable,
                cell_slab.length_ - end);
          }
        }
      }
    }

    // Check if we need to fill the whole slab or apply query condition.
    for (uint64_t n = 0; n < names.size(); n++) {
      // Calculate the destination pointers.
      auto dest_ptr = dst_bufs[n] + cell_offset * sizeof(OffType);
      auto dest_validity_ptr = dst_val_bufs[n] + cell_offset;
      const auto& fill_value_nullable = attributes[n]->fill_value_validity();

      // Need to fill the whole slab.
      if (frag_domains.size() == 0) {
        memset(dest_ptr, 0xFF, cell_slab.length_ * sizeof(OffType));

        if (attributes[n]->nullable()) {
          std::memset(
              dest_validity_ptr, fill_value_nullable, cell_slab.length_);
        }
      }

      if (!condition_.empty()) {
        // Apply query condition results to this slab.
        for (uint64_t c = 0; c < cell_slab.length_; c++) {
          if (!(qc_result[c + cell_offset] & 0x1)) {
            memset(dest_ptr + c * sizeof(OffType), 0xFF, sizeof(OffType));
          }

          if (attributes[n]->nullable()) {
            std::memset(dest_validity_ptr + c, fill_value_nullable, 1);
          }
        }
      }
    }

    // Adjust the cell offset for global order.
    if (layout_ == Layout::GLOBAL_ORDER) {
      cell_offset += cell_slab.length_;
    }

    ++iter;
  }

  return Status::Ok();
}

template <class DimType, class OffType>
Status DenseReader::copy_var_tiles(
    const std::vector<std::string>& names,
    const std::vector<uint8_t*>& dst_bufs,
    const std::vector<uint8_t*>& offsets_bufs,
    const std::vector<uint64_t>& data_type_sizes,
    const Subarray& subarray,
    const Subarray& tile_subarray,
    const uint64_t global_cell_offset,
    std::vector<std::vector<void*>>& var_data,
    const std::vector<RangeInfo>& range_info,
    bool last_tile,
    std::vector<uint64_t>& var_buffer_sizes) {
  // For easy reference
  auto dim_num = array_schema_.dim_num();

  // Initialise for global order, will be adjusted later for row/col major.
  uint64_t cell_offset = global_cell_offset;

  // Iterate over all coordinates, retrieved in cell slabs
  CellSlabIter<DimType> iter(&tile_subarray);
  RETURN_CANCEL_OR_ERROR(iter.begin());
  while (!iter.end()) {
    auto cell_slab = iter.cell_slab();
    ++iter;

    // Compute cell offset for row/col major orders.
    if (layout_ != Layout::GLOBAL_ORDER) {
      cell_offset = get_dest_cell_offset_row_col(
          dim_num,
          subarray,
          tile_subarray,
          cell_slab.coords_.data(),
          iter.range_coords(),
          range_info);
    }

    for (uint64_t n = 0; n < names.size(); n++) {
      // Setup variables for the copy.
      auto mult = elements_mode_ ? data_type_sizes[n] : 1;
      uint64_t size = 0;
      uint64_t offset = 0;
      uint64_t i = 0;

      // Copy the data cell by cell, last copy was taken out to take advantage
      // of vectorization.
      for (; i < cell_slab.length_ - 1; i++) {
        offset = ((OffType*)offsets_bufs[n])[cell_offset + i] * mult;
        size = ((OffType*)offsets_bufs[n])[cell_offset + i + 1] * mult - offset;
        std::memcpy(dst_bufs[n] + offset, var_data[n][cell_offset + i], size);
      }

      // Do the last copy.
      offset = ((OffType*)offsets_bufs[n])[cell_offset + i] * mult;
      if (last_tile && iter.end() && i == cell_slab.length_ - 1) {
        size = var_buffer_sizes[n] * mult - offset;
      } else {
        size = ((OffType*)offsets_bufs[n])[cell_offset + i + 1] * mult - offset;
      }
      std::memcpy(dst_bufs[n] + offset, var_data[n][cell_offset + i], size);
    }

    // Adjust cell offset for global order.
    if (layout_ == Layout::GLOBAL_ORDER)
      cell_offset += cell_slab.length_;
  }

  return Status::Ok();
}

Status DenseReader::add_extra_offset() {
  // Add extra offset element for all var size offset buffers.
  for (const auto& it : buffers_) {
    const auto& name = it.first;
    if (!array_schema_.var_size(name))
      continue;

    // Do not apply offset for empty results because we will
    // write backwards and corrupt memory we don't own.
    if (*it.second.buffer_size_ == 0)
      continue;

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

}  // namespace sm
}  // namespace tiledb
