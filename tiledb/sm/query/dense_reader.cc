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

#include "tiledb/sm/query/dense_reader.h"
#include "tiledb/sm/array/array.h"
#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/array_schema/dimension.h"
#include "tiledb/sm/fragment/fragment_metadata.h"
#include "tiledb/sm/misc/parallel_functions.h"
#include "tiledb/sm/misc/utils.h"
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
    StorageManager* storage_manager,
    Array* array,
    Config& config,
    std::unordered_map<std::string, QueryBuffer>& buffers,
    Subarray& subarray,
    Layout layout,
    QueryCondition& condition)
    : ReaderBase(
          stats,
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

Status DenseReader::init() {
  // Sanity checks.
  if (storage_manager_ == nullptr)
    return LOG_STATUS(Status::DenseReaderError(
        "Cannot initialize dense reader; Storage manager not set"));
  if (array_schema_ == nullptr)
    return LOG_STATUS(Status::DenseReaderError(
        "Cannot initialize dense reader; Array schema not set"));
  if (buffers_.empty())
    return LOG_STATUS(Status::DenseReaderError(
        "Cannot initialize dense reader; Buffers not set"));
  if (!subarray_.is_set())
    return LOG_STATUS(Status::ReaderError(
        "Cannot initialize reader; Dense reads must have a subarray set"));

  // Check subarray.
  RETURN_NOT_OK(check_subarray());

  // Initialize the read state.
  RETURN_NOT_OK(init_read_state());

  // Check the validity buffer sizes.
  RETURN_NOT_OK(check_validity_buffer_sizes());

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
      bool has_results = false;
      for (const auto& it : buffers_) {
        if (*(it.second.buffer_size_) != 0)
          has_results = true;
      }

      // Need to reset unsplittable if the results fit after all.
      if (has_results)
        read_state_.unsplittable_ = false;

      if (has_results || read_state_.done()) {
        return complete_read_loop();
      }

      RETURN_NOT_OK(read_state_.next());
    }
  } while (true);

  return Status::Ok();
}

void DenseReader::reset() {
}

template <class OffType>
Status DenseReader::dense_read() {
  auto type = array_schema_->domain()->dimension(0)->type();
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
      return LOG_STATUS(Status::ReaderError(
          "Cannot read dense array; Unsupported domain type"));
  }

  return Status::Ok();
}

template <class DimType, class OffType>
Status DenseReader::dense_read() {
  // Sanity checks.
  assert(std::is_integral<DimType>::value);

  // For easy reference.
  auto& subarray = read_state_.partitioner_.current();

  RETURN_NOT_OK(subarray.compute_tile_coords<DimType>());

  // Compute result space tiles. The result space tiles hold all the
  // relevant result tiles of the dense fragments.
  std::map<const DimType*, ResultSpaceTile<DimType>> result_space_tiles;
  compute_result_space_tiles<DimType>(subarray, &result_space_tiles);

  std::vector<ResultTile*> result_tiles;
  for (const auto& result_space_tile : result_space_tiles) {
    for (const auto& result_tile : result_space_tile.second.result_tiles()) {
      result_tiles.push_back(const_cast<ResultTile*>(&result_tile.second));
    }
  }

  // Compute subarrays for each tiles and tile offsets for global order.
  std::vector<Subarray> tile_subarrays;
  std::vector<uint64_t> tile_offsets;
  const auto& tile_coords = subarray.tile_coords();
  tile_subarrays.reserve(tile_coords.size());
  tile_offsets.reserve(tile_coords.size());

  const auto& layout =
      layout_ == Layout::GLOBAL_ORDER ? array_schema_->cell_order() : layout_;
  uint64_t tile_offset = 0;
  for (const auto& tc : tile_coords) {
    tile_subarrays.emplace_back(
        subarray.crop_to_tile((const DimType*)&tc[0], layout));

    tile_offsets.emplace_back(tile_offset);
    tile_offset += tile_subarrays.back().cell_num();
  }

  // Seperate attributes in condition and normal attributes.
  std::vector<std::string> names(1);
  std::vector<std::string> condition_names;
  condition_names.reserve(condition_.field_names().size());
  std::vector<std::string> attributes_names;
  attributes_names.reserve(buffers_.size() - condition_.field_names().size());

  for (const auto& it : buffers_) {
    const auto& name = it.first;
    if (condition_.field_names().find(name) != condition_.field_names().end()) {
      names[0] = name;
      condition_names.emplace_back(name);

      // Pre-load all attribute offsets into memory for attributes
      // in query condition to be read.
      load_tile_offsets(&subarray, &names);

      // Read and unfilter tiles.
      RETURN_CANCEL_OR_ERROR(read_attribute_tiles(&names, &result_tiles));
      RETURN_CANCEL_OR_ERROR(unfilter_tiles(name, &result_tiles));
    } else {
      attributes_names.emplace_back(name);
    }
  }

  // Compute the result of the query condition.
  std::vector<uint8_t> qc_result;
  auto status = apply_query_condition<DimType, OffType>(
      &condition_names,
      &subarray,
      &tile_subarrays,
      &tile_offsets,
      &result_space_tiles,
      &qc_result);
  RETURN_CANCEL_OR_ERROR(status);

  // Process each query condition attributes one at a time.
  for (const auto& name : condition_names) {
    // Copy attribute data to users buffers.
    status = read_attribute<DimType, OffType>(
        name,
        &subarray,
        &tile_subarrays,
        &tile_offsets,
        &result_space_tiles,
        &qc_result);
    RETURN_CANCEL_OR_ERROR(status);

    // Clear the tiles not needed anymore.
    clear_tiles(name, &result_tiles);

    if (read_state_.overflowed_)
      break;
  }

  if (read_state_.overflowed_)
    return Status::Ok();

  // Process each attributes one at a time.
  for (const auto& name : attributes_names) {
    names[0] = name;

    if (name == constants::coords || array_schema_->is_dim(name)) {
      continue;
    }

    // Pre-load all attribute offsets into memory for attributes
    // to be read.
    load_tile_offsets(&subarray, &names);

    // Read and unfilter tiles.
    RETURN_CANCEL_OR_ERROR(read_attribute_tiles(&names, &result_tiles));
    RETURN_CANCEL_OR_ERROR(unfilter_tiles(name, &result_tiles));

    // Copy attribute data to users buffers.
    auto status = read_attribute<DimType, OffType>(
        name,
        &subarray,
        &tile_subarrays,
        &tile_offsets,
        &result_space_tiles,
        &qc_result);
    RETURN_CANCEL_OR_ERROR(status);

    // Clear the tiles not needed anymore.
    clear_tiles(name, &result_tiles);

    if (read_state_.overflowed_)
      break;
  }

  // Fill coordinates if the user requested them.
  if (!read_state_.overflowed_ && has_coords()) {
    RETURN_CANCEL_OR_ERROR(fill_dense_coords<DimType>(subarray));
    read_state_.overflowed_ = copy_overflowed_;
  }

  return Status::Ok();
}

Status DenseReader::init_read_state() {
  auto timer_se = stats_->start_timer("init_state");

  // Check subarray.
  if (subarray_.layout() == Layout::GLOBAL_ORDER && subarray_.range_num() != 1)
    return LOG_STATUS(
        Status::ReaderError("Cannot initialize read "
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
        Status::ReaderError("Cannot initialize reader; Unsupported offsets "
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
        Status::ReaderError("Cannot initialize reader; Unsupported offsets "
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
      stats_);
  read_state_.overflowed_ = false;
  read_state_.unsplittable_ = false;

  // Set result size budget.
  for (const auto& a : buffers_) {
    auto attr_name = a.first;
    auto buffer_size = a.second.buffer_size_;
    auto buffer_var_size = a.second.buffer_var_size_;
    auto buffer_validity_size = a.second.validity_vector_.buffer_size();
    if (!array_schema_->var_size(attr_name)) {
      if (!array_schema_->is_nullable(attr_name)) {
        RETURN_NOT_OK(read_state_.partitioner_.set_result_budget(
            attr_name.c_str(), *buffer_size));
      } else {
        RETURN_NOT_OK(read_state_.partitioner_.set_result_budget_nullable(
            attr_name.c_str(), *buffer_size, *buffer_validity_size));
      }
    } else {
      if (!array_schema_->is_nullable(attr_name)) {
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
  copy_overflowed_ = false;
  read_state_.initialized_ = true;

  return Status::Ok();
}

/** Apply the query condition. */
template <class DimType, class OffType>
Status DenseReader::apply_query_condition(
    std::vector<std::string>* condition_names,
    Subarray* subarray,
    std::vector<Subarray>* tile_subarrays,
    std::vector<uint64_t>* tile_offsets,
    std::map<const DimType*, ResultSpaceTile<DimType>>* result_space_tiles,
    std::vector<uint8_t>* qc_result) {
  if (!condition_names->empty()) {
    // For easy reference.
    const auto& tile_coords = subarray->tile_coords();
    const auto cell_num = subarray->cell_num();
    const auto dim_num = array_schema_->dim_num();
    auto stride = array_schema_->domain()->stride<DimType>(layout_);
    const auto domain = array_schema_->domain();
    const auto cell_order = array_schema_->cell_order();

    if (stride == UINT64_MAX) {
      stride = 1;
    }

    // Initialize the result buffer.
    qc_result->resize(cell_num);
    memset(qc_result->data(), 0xFF, cell_num);

    // Process all tiles in parallel.
    auto status = parallel_for(
        storage_manager_->compute_tp(), 0, tile_coords.size(), [&](uint64_t t) {
          // Find out result space tile and tile subarray.
          const DimType* tc = (DimType*)&tile_coords[t][0];
          auto it = result_space_tiles->find(tc);
          assert(it != result_space_tiles->end());
          const auto tile_subarray = &tile_subarrays->at(t);

          const auto& frag_domains = it->second.frag_domains();
          uint64_t cell_offset = tile_offsets->at(t);
          auto dest_ptr = qc_result->data() + cell_offset;

          // Iterate over all coordinates, retrieved in cell slab.
          CellSlabIter<DimType> iter(tile_subarray);
          RETURN_NOT_OK(iter.begin());
          while (!iter.end()) {
            auto cell_slab = iter.cell_slab();

            // Compute destination pointer for row/col major orders.
            if (layout_ != Layout::GLOBAL_ORDER) {
              RETURN_NOT_OK(get_dest_cell_offset_row_col(
                  dim_num, subarray, cell_slab.coords_.data(), &cell_offset));
              dest_ptr = qc_result->data() + cell_offset;
            }

            // Get the source cell offset.
            uint64_t src_cell = 0;
            RETURN_NOT_OK(get_cell_pos_in_tile(
                cell_order,
                dim_num,
                domain,
                &it->second,
                cell_slab.coords_.data(),
                &src_cell));

            for (int32_t i = (int32_t)frag_domains.size() - 1; i >= 0; --i) {
              // If the cell slab overlaps this fragment domain range, apply
              // clause.
              uint64_t start = 0;
              uint64_t end = 0;
              if (cell_slab_overlaps_range(
                      dim_num,
                      frag_domains[i].second,
                      cell_slab.coords_.data(),
                      cell_slab.length_,
                      &start,
                      &end)) {
                RETURN_NOT_OK(condition_.apply_dense<DimType>(
                    array_schema_,
                    it->second.result_tile(frag_domains[i].first),
                    start,
                    end - start + 1,
                    src_cell,
                    stride,
                    dest_ptr));
              }
            }

            // Adjust the destination pointers for global order.
            if (layout_ == Layout::GLOBAL_ORDER) {
              dest_ptr += cell_slab.length_;
            }

            ++iter;
          }

          return Status::Ok();
        });
    RETURN_NOT_OK(status);
  }

  return Status::Ok();
}

template <class OffType>
void DenseReader::fix_offsets_buffer(
    const std::string& name,
    const bool nullable,
    const uint64_t cell_num,
    std::vector<void*>* var_data,
    uint64_t* offsets_buffer_size) {
  // For easy reference.
  const auto& fill_value = array_schema_->attribute(name)->fill_value();
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
      var_data->at(i) = (void*)fill_value.data();
    }
    offsets_buffer[i] = offset;
    offset += tmp;
  }

  // Set the output offset buffer sizes.
  *buffers_[name].buffer_size_ = cell_num * sizeof(OffType);

  if (nullable)
    *buffers_[name].validity_vector_.buffer_size() = cell_num;

  // Return the buffer size.
  *offsets_buffer_size = offset;
}

template <class DimType, class OffType>
Status DenseReader::read_attribute(
    const std::string& name,
    const Subarray* const subarray,
    const std::vector<Subarray>* const tile_subarrays,
    const std::vector<uint64_t>* const tile_offsets,
    std::map<const DimType*, ResultSpaceTile<DimType>>* result_space_tiles,
    const std::vector<uint8_t>* const qc_result) {
  // For easy reference
  const auto& tile_coords = subarray->tile_coords();
  const auto cell_num = subarray->cell_num();
  const bool nullable = array_schema_->is_nullable(name);

  if (array_schema_->var_size(name)) {
    // Make sure the user offset buffer is big enough.
    const auto required_size =
        (cell_num + offsets_extra_element_) * sizeof(OffType);
    if (required_size > *buffers_[name].buffer_size_) {
      read_state_.overflowed_ = true;
      return Status::Ok();
    }

    // Vector to hold pointers to the var data.
    std::vector<void*> var_data(cell_num);

    // Process offsets in parallel.
    auto status = parallel_for(
        storage_manager_->compute_tp(), 0, tile_coords.size(), [&](uint64_t t) {
          // Find out result space tile and tile subarray.
          const DimType* tc = (DimType*)&tile_coords[t][0];
          auto it = result_space_tiles->find(tc);
          assert(it != result_space_tiles->end());
          const auto tile_subarray = &tile_subarrays->at(t);

          // Copy the tile offsets.
          return copy_tile_offsets<DimType, OffType>(
              name,
              nullable,
              &it->second,
              subarray,
              tile_subarray,
              tile_offsets->at(t),
              &var_data,
              qc_result);
        });
    RETURN_NOT_OK(status);

    // We have the cell lengths in the users buffer, convert to offsets.
    uint64_t offsets_buffer_size = 0;
    fix_offsets_buffer<OffType>(
        name, nullable, cell_num, &var_data, &offsets_buffer_size);

    // Make sure the user var buffer is big enough.
    uint64_t required_var_size = offsets_buffer_size;
    if (elements_mode_)
      required_var_size *= datatype_size(array_schema_->type(name));

    // Exit early in case of overflow.
    if (read_state_.overflowed_ ||
        required_var_size > *buffers_[name].buffer_var_size_) {
      read_state_.overflowed_ = true;
      return Status::Ok();
    }

    // Process var data in parallel.
    status = parallel_for(
        storage_manager_->compute_tp(), 0, tile_coords.size(), [&](uint64_t t) {
          const auto tile_subarray = &tile_subarrays->at(t);

          return copy_tile_var<DimType, OffType>(
              name,
              subarray,
              tile_subarray,
              tile_offsets->at(t),
              &var_data,
              t == tile_coords.size() - 1,
              offsets_buffer_size);

          return Status::Ok();
        });
    RETURN_NOT_OK(status);

    // Set the output size for the var buffer.
    *buffers_[name].buffer_var_size_ = required_var_size;
  } else {
    // Make sure the user fixed buffer is big enough.
    const auto required_size = cell_num * array_schema_->cell_size(name);
    if (required_size > *buffers_[name].buffer_size_) {
      read_state_.overflowed_ = true;
      return Status::Ok();
    }

    // Process values in parallel.
    auto status = parallel_for(
        storage_manager_->compute_tp(), 0, tile_coords.size(), [&](uint64_t t) {
          // Find out result space tile and tile subarray.
          const DimType* tc = (DimType*)&tile_coords[t][0];
          auto it = result_space_tiles->find(tc);
          assert(it != result_space_tiles->end());
          const auto tile_subarray = &tile_subarrays->at(t);

          // Copy the tile fixed values.
          RETURN_NOT_OK(copy_tile_fixed(
              name,
              nullable,
              &it->second,
              subarray,
              tile_subarray,
              tile_offsets->at(t),
              qc_result));

          return Status::Ok();
        });
    RETURN_NOT_OK(status);

    // Set the output size for the fixed buffer.
    *buffers_[name].buffer_size_ = required_size;

    if (nullable)
      *buffers_[name].validity_vector_.buffer_size() = cell_num;
  }

  return Status::Ok();
}

template <class DimType>
Status DenseReader::get_cell_pos_in_tile(
    const Layout& cell_order,
    const int32_t dim_num,
    const Domain* const domain,
    const ResultSpaceTile<DimType>* const result_space_tile,
    const DimType* const coords,
    uint64_t* cell_pos) {
  uint64_t pos = 0;
  uint64_t mult = 1;

  const auto& start = result_space_tile->start_coords();
  if (cell_order == Layout::COL_MAJOR) {
    for (int32_t d = 0; d < dim_num; d++) {
      pos += mult * (coords[d] - start[d]);
      mult *= *(const DimType*)domain->tile_extent(d).data();
    }
  } else {
    for (auto d = dim_num - 1; d >= 0; d--) {
      pos += mult * (coords[d] - start[d]);
      mult *= *(const DimType*)domain->tile_extent(d).data();
    }
  }

  *cell_pos = pos;
  return Status::Ok();
}

template <class DimType>
bool DenseReader::cell_slab_overlaps_range(
    const unsigned dim_num,
    const NDRange& ndrange,
    const DimType* coords,
    const uint64_t length,
    uint64_t* start,
    uint64_t* end) {
  const unsigned slab_dim = (layout_ == Layout::COL_MAJOR) ? 0 : dim_num - 1;
  const DimType slab_start = coords[slab_dim];
  const DimType slab_end = slab_start + length - 1;

  // Check if there is any overlap.
  for (unsigned d = 0; d < dim_num; ++d) {
    auto dom = (const DimType*)ndrange[d].data();
    if (d == slab_dim) {
      if (slab_end < dom[0] || slab_start > dom[1]) {
        return false;
      }
    } else if (coords[d] < dom[0] || coords[d] > dom[1]) {
      return false;
    }
  }

  // Compute the normalized start and end coordinates for the slab.
  auto dom = (const DimType*)ndrange[slab_dim].data();
  *start = std::max(slab_start, dom[0]) - slab_start;
  *end = std::min(slab_end, dom[1]) - slab_start;
  return true;
}

template <class DimType>
Status DenseReader::get_dest_cell_offset_row_col(
    const int32_t dim_num,
    const Subarray* const subarray,
    const DimType* const coords,
    uint64_t* cell_offset) {
  uint64_t ret = 0;
  uint64_t mult = 1;

  if (subarray->layout() == Layout::COL_MAJOR) {
    for (int32_t d = 0; d < dim_num; d++) {
      auto subarray_dom =
          (DimType*)subarray->ranges_for_dim((uint32_t)d)[0].data();
      ret += mult * (coords[d] - subarray_dom[0]);
      mult *= subarray_dom[1] - subarray_dom[0] + 1;
    }
  } else {
    for (auto d = dim_num - 1; d >= 0; d--) {
      auto subarray_dom =
          (DimType*)subarray->ranges_for_dim((uint32_t)d)[0].data();
      ret += mult * (coords[d] - subarray_dom[0]);
      mult *= subarray_dom[1] - subarray_dom[0] + 1;
    }
  }

  *cell_offset = ret;
  return Status::Ok();
}

template <class DimType>
Status DenseReader::copy_tile_fixed(
    const std::string& name,
    const bool nullable,
    ResultSpaceTile<DimType>* result_space_tile,
    const Subarray* const subarray,
    const Subarray* const tile_subarray,
    const uint64_t global_cell_offset,
    const std::vector<uint8_t>* const qc_result) {
  // For easy reference
  const auto dim_num = array_schema_->dim_num();
  const auto domain = array_schema_->domain();
  const auto cell_size = array_schema_->cell_size(name);
  const auto cell_order = array_schema_->cell_order();
  const auto& fill_value = array_schema_->attribute(name)->fill_value();
  const auto fill_value_size = fill_value.size();
  const auto fill_value_nullable =
      array_schema_->attribute(name)->fill_value_validity();
  const auto stride = array_schema_->domain()->stride<DimType>(layout_);
  const auto& frag_domains = result_space_tile->frag_domains();

  // Initialise for global order, will be adjusted later for row/col major.
  uint64_t cell_offset = global_cell_offset;
  auto dest_ptr = (uint8_t*)buffers_[name].buffer_ + cell_offset * cell_size;
  auto dest_validity_ptr =
      buffers_[name].validity_vector_.buffer() + cell_offset;

  // Iterate over all coordinates, retrieved in cell slab.
  CellSlabIter<DimType> iter(tile_subarray);
  RETURN_CANCEL_OR_ERROR(iter.begin());
  while (!iter.end()) {
    auto cell_slab = iter.cell_slab();

    // Compute destination pointers for row/col major orders.
    if (layout_ != Layout::GLOBAL_ORDER) {
      RETURN_NOT_OK(get_dest_cell_offset_row_col(
          dim_num, subarray, cell_slab.coords_.data(), &cell_offset));
      dest_ptr = (uint8_t*)buffers_[name].buffer_ + cell_offset * cell_size;
      dest_validity_ptr =
          buffers_[name].validity_vector_.buffer() + cell_offset;
    }

    // Get the source cell offset.
    uint64_t src_cell = 0;
    RETURN_NOT_OK(get_cell_pos_in_tile(
        cell_order,
        dim_num,
        domain,
        result_space_tile,
        cell_slab.coords_.data(),
        &src_cell));

    // Iterate through all fragment domains and copy data.
    for (int32_t i = (int32_t)frag_domains.size() - 1; i >= 0; --i) {
      // If the cell slab overlaps this fragment domain range, copy data.
      uint64_t start = 0;
      uint64_t end = std::numeric_limits<uint64_t>::max();
      if (cell_slab_overlaps_range(
              dim_num,
              frag_domains[i].second,
              cell_slab.coords_.data(),
              cell_slab.length_,
              &start,
              &end)) {
        // Get the tile buffers.
        const auto tile_tuple =
            result_space_tile->result_tile(frag_domains[i].first)
                ->tile_tuple(name);
        assert(tile_tuple != nullptr);
        const Tile* const tile = &std::get<0>(*tile_tuple);
        const Tile* const tile_nullable = &std::get<2>(*tile_tuple);

        auto src_offset = (src_cell + start);

        // If the subarray and tile are in the same order, copy the whole slab.
        if (stride == UINT64_MAX) {
          RETURN_NOT_OK(tile->buffer()->read(
              dest_ptr + cell_size * start,
              cell_size * src_offset,
              cell_size * (end - start + 1)));

          if (nullable) {
            RETURN_NOT_OK(tile_nullable->buffer()->read(
                dest_validity_ptr + start, src_offset, (end - start + 1)));
          }
        } else {
          // Go cell by cell.
          auto dest = dest_ptr + cell_size * start;
          auto dest_validity = dest_validity_ptr + start;
          for (uint64_t i = 0; i < end - start + 1; ++i) {
            RETURN_NOT_OK(
                tile->buffer()->read(dest, cell_size * src_offset, cell_size));
            dest += cell_size;

            if (nullable) {
              RETURN_NOT_OK(
                  tile_nullable->buffer()->read(dest_validity, src_offset, 1));
              dest_validity++;
            }

            src_offset += stride;
          }
        }
      }

      // Fill the non written cells for the first fragment domain with the fill
      // value.
      if (i == (int32_t)frag_domains.size() - 1) {
        auto buff = dest_ptr;
        for (uint64_t i = 0; i < start; ++i) {
          std::memcpy(buff, fill_value.data(), fill_value_size);
          buff += fill_value_size;
        }

        end = end == std::numeric_limits<uint64_t>::max() ? 0 : end + 1;
        buff = dest_ptr + end * fill_value_size;
        for (uint64_t i = 0; i < cell_slab.length_ - end; ++i) {
          std::memcpy(buff, fill_value.data(), fill_value_size);
          buff += fill_value_size;
        }

        if (nullable) {
          std::memset(dest_validity_ptr, fill_value_nullable, start);
          std::memset(
              dest_validity_ptr + end,
              fill_value_nullable,
              cell_slab.length_ - end);
        }
      }
    }

    // Need to fill the whole slab.
    if (frag_domains.size() == 0) {
      auto buff = dest_ptr;
      for (uint64_t i = 0; i < cell_slab.length_; ++i) {
        std::memcpy(buff, fill_value.data(), fill_value_size);
        buff += fill_value_size;
      }

      if (nullable) {
        std::memset(dest_validity_ptr, fill_value_nullable, cell_slab.length_);
      }
    }

    // Apply query condition results to this slab.
    if (!condition_.empty()) {
      for (uint64_t c = 0; c < cell_slab.length_; c++) {
        if (!(qc_result->at(c + cell_offset) & 0x1)) {
          memcpy(dest_ptr + c * cell_size, fill_value.data(), fill_value_size);

          if (nullable) {
            std::memset(dest_validity_ptr + c, fill_value_nullable, 1);
          }
        }
      }
    }

    // Adjust the destination pointers for global order.
    if (layout_ == Layout::GLOBAL_ORDER) {
      dest_ptr += cell_slab.length_ * cell_size;
      dest_validity_ptr += cell_slab.length_;
    }

    ++iter;
  }

  return Status::Ok();
}

template <class DimType, class OffType>
Status DenseReader::copy_tile_offsets(
    const std::string& name,
    const bool nullable,
    ResultSpaceTile<DimType>* result_space_tile,
    const Subarray* const subarray,
    const Subarray* const tile_subarray,
    const uint64_t global_cell_offset,
    std::vector<void*>* var_data,
    const std::vector<uint8_t>* const qc_result) {
  // For easy reference
  const auto domain = array_schema_->domain();
  const auto dim_num = array_schema_->dim_num();
  const auto cell_order = array_schema_->cell_order();
  const auto data_type_size = datatype_size(array_schema_->type(name));
  const auto cell_num_per_tile = array_schema_->domain()->cell_num_per_tile();
  auto stride = array_schema_->domain()->stride<DimType>(layout_);
  const auto fill_value_nullable =
      array_schema_->attribute(name)->fill_value_validity();
  const auto& frag_domains = result_space_tile->frag_domains();

  if (stride == UINT64_MAX) {
    stride = 1;
  }

  // Initialise for global order, will be adjusted later for row/col major.
  uint64_t cell_offset = global_cell_offset;
  auto dest_ptr =
      (uint8_t*)buffers_[name].buffer_ + cell_offset * sizeof(OffType);
  auto var_data_buff = var_data->data() + cell_offset;
  auto dest_validity_ptr =
      buffers_[name].validity_vector_.buffer() + cell_offset;

  // Iterate over all coordinates, retrieved in cell slabs
  CellSlabIter<DimType> iter(tile_subarray);
  RETURN_CANCEL_OR_ERROR(iter.begin());
  while (!iter.end()) {
    auto cell_slab = iter.cell_slab();

    // Compute destination pointers for row/col major orders.
    if (layout_ != Layout::GLOBAL_ORDER) {
      RETURN_NOT_OK(get_dest_cell_offset_row_col(
          dim_num, subarray, cell_slab.coords_.data(), &cell_offset));
      dest_ptr =
          (uint8_t*)buffers_[name].buffer_ + cell_offset * sizeof(OffType);
      var_data_buff = var_data->data() + cell_offset;
      dest_validity_ptr =
          buffers_[name].validity_vector_.buffer() + cell_offset;
    }

    // Get the source cell offset.
    uint64_t src_cell = 0;
    RETURN_NOT_OK(get_cell_pos_in_tile(
        cell_order,
        dim_num,
        domain,
        result_space_tile,
        cell_slab.coords_.data(),
        &src_cell));

    // Iterate through all fragment domains and copy data.
    for (int32_t i = (int32_t)frag_domains.size() - 1; i >= 0; --i) {
      // If the cell slab overlaps this fragment domain range, copy data.
      uint64_t start = 0;
      uint64_t end = std::numeric_limits<uint64_t>::max();
      if (cell_slab_overlaps_range(
              dim_num,
              frag_domains[i].second,
              cell_slab.coords_.data(),
              cell_slab.length_,
              &start,
              &end)) {
        // Get the tile buffers.
        const auto tile_tuple =
            result_space_tile->result_tile(frag_domains[i].first)
                ->tile_tuple(name);
        assert(tile_tuple != nullptr);
        const Tile* const t_var = &std::get<1>(*tile_tuple);

        // Setup variables for the copy.
        auto src_buff = (uint64_t*)std::get<0>(*tile_tuple).buffer()->data() +
                        start + src_cell;
        auto src_buff_validity =
            nullable ? (uint8_t*)std::get<2>(*tile_tuple).buffer()->data() +
                           start + src_cell :
                       nullptr;
        auto div = elements_mode_ ? data_type_size : 1;
        auto dest = (OffType*)dest_ptr + start;

        // Copy the data cell by cell, last copy was taken out to take advantage
        // of vectorization.
        uint64_t i = 0;
        for (; i < end - start; ++i) {
          auto i_src = i * stride;
          dest[i] = (src_buff[i_src + 1] - src_buff[i_src]) / div;
          var_data_buff[i + start] =
              (char*)t_var->buffer()->data() + src_buff[i_src];
        }

        if (nullable) {
          i = 0;
          for (; i < end - start; ++i) {
            dest_validity_ptr[start + i] = src_buff_validity[i * stride];
          }
        }

        // Copy the last value.
        if (start + src_cell + (end - start) * stride >=
            cell_num_per_tile - 1) {
          dest[i] = (t_var->buffer()->size() - src_buff[i * stride]) / div;
        } else {
          auto i_src = i * stride;
          dest[i] = (src_buff[i_src + 1] - src_buff[i_src]) / div;
        }
        var_data_buff[i + start] =
            (char*)t_var->buffer()->data() + src_buff[i * stride];

        if (nullable)
          dest_validity_ptr[start + i] = src_buff_validity[i * stride];
      }

      // Fill the non written cells for the first fragment domain with max
      // value.
      if (i == (int32_t)frag_domains.size() - 1) {
        end = end == std::numeric_limits<uint64_t>::max() ? 0 : end + 1;
        memset(dest_ptr, 0xFF, start * sizeof(OffType));
        memset(
            dest_ptr + end * sizeof(OffType),
            0xFF,
            (cell_slab.length_ - end) * sizeof(OffType));

        if (nullable) {
          std::memset(dest_validity_ptr, fill_value_nullable, start);
          std::memset(
              dest_validity_ptr + end,
              fill_value_nullable,
              cell_slab.length_ - end);
        }
      }
    }

    // Need to fill the whole slab.
    if (frag_domains.size() == 0) {
      memset(dest_ptr, 0xFF, cell_slab.length_ * sizeof(OffType));

      if (nullable) {
        std::memset(dest_validity_ptr, fill_value_nullable, cell_slab.length_);
      }
    }

    if (!condition_.empty()) {
      // Apply query condition results to this slab.
      for (uint64_t c = 0; c < cell_slab.length_; c++) {
        if (!(qc_result->at(c + cell_offset) & 0x1)) {
          memset(dest_ptr + c * sizeof(OffType), 0xFF, sizeof(OffType));
        }

        if (nullable) {
          std::memset(dest_validity_ptr + c, fill_value_nullable, 1);
        }
      }
    }

    // Adjust the destination pointers for global order.
    if (layout_ == Layout::GLOBAL_ORDER) {
      dest_ptr += cell_slab.length_ * sizeof(OffType);
      var_data_buff += cell_slab.length_;
      dest_validity_ptr += cell_slab.length_;
    }

    ++iter;
  }

  return Status::Ok();
}

template <class DimType, class OffType>
Status DenseReader::copy_tile_var(
    const std::string& name,
    const Subarray* const subarray,
    const Subarray* const tile_subarray,
    const uint64_t global_cell_offset,
    std::vector<void*>* var_data,
    bool last_tile,
    uint64_t final_tile_size) {
  // For easy reference
  auto dim_num = array_schema_->dim_num();
  auto data_type_size = datatype_size(array_schema_->type(name));
  auto offsets = (OffType*)buffers_[name].buffer_;
  auto dest_buff = (uint8_t*)buffers_[name].buffer_var_;

  // Initialise for global order, will be adjusted later for row/col major.
  uint64_t cell_offset = global_cell_offset;

  // Iterate over all coordinates, retrieved in cell slabs
  CellSlabIter<DimType> iter(tile_subarray);
  RETURN_CANCEL_OR_ERROR(iter.begin());
  while (!iter.end()) {
    auto cell_slab = iter.cell_slab();
    ++iter;

    // Compute cell_offset for row/col major orders.
    if (layout_ != Layout::GLOBAL_ORDER) {
      RETURN_NOT_OK(get_dest_cell_offset_row_col(
          dim_num, subarray, cell_slab.coords_.data(), &cell_offset));
    }

    // Setup variables for the copy.
    auto mult = elements_mode_ ? data_type_size : 1;
    uint64_t size = 0;
    uint64_t offset = 0;
    uint64_t i = 0;

    // Copy the data cell by cell, last copy was taken out to take advantage
    // of vectorization.
    for (; i < cell_slab.length_ - 1; i++) {
      offset = offsets[cell_offset + i] * mult;
      size = offsets[cell_offset + i + 1] * mult - offset;
      std::memcpy(dest_buff + offset, var_data->at(cell_offset + i), size);
    }

    // Do the last copy.
    offset = offsets[cell_offset + i] * mult;
    if (last_tile && iter.end() && i == cell_slab.length_ - 1) {
      size = final_tile_size * mult - offset;
    } else {
      size = offsets[cell_offset + i + 1] * mult - offset;
    }
    std::memcpy(dest_buff + offset, var_data->at(cell_offset + i), size);

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
    if (!array_schema_->var_size(name))
      continue;

    auto buffer = static_cast<unsigned char*>(it.second.buffer_);
    if (offsets_format_mode_ == "bytes") {
      memcpy(
          buffer + *it.second.buffer_size_,
          it.second.buffer_var_size_,
          offsets_bytesize());
    } else if (offsets_format_mode_ == "elements") {
      auto elements = *it.second.buffer_var_size_ /
                      datatype_size(array_schema_->type(name));
      memcpy(buffer + *it.second.buffer_size_, &elements, offsets_bytesize());
    } else {
      return LOG_STATUS(Status::ReaderError(
          "Cannot add extra offset to buffer; Unsupported offsets format"));
    }

    *it.second.buffer_size_ += offsets_bytesize();
  }

  return Status::Ok();
}

}  // namespace sm
}  // namespace tiledb
