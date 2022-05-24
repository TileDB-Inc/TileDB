/**
 * @file   sparse_index_reader_base.cc
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
 * This file implements class SparseIndexReaderBase.
 */

#include "tiledb/sm/query/sparse_index_reader_base.h"
#include "tiledb/common/logger.h"
#include "tiledb/common/memory_tracker.h"
#include "tiledb/sm/array/array.h"
#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/filesystem/vfs.h"
#include "tiledb/sm/fragment/fragment_metadata.h"
#include "tiledb/sm/misc/parallel_functions.h"
#include "tiledb/sm/misc/resource_pool.h"
#include "tiledb/sm/query/iquery_strategy.h"
#include "tiledb/sm/query/query_buffer.h"
#include "tiledb/sm/query/query_macros.h"
#include "tiledb/sm/query/strategy_base.h"
#include "tiledb/sm/subarray/subarray.h"

#include <numeric>

namespace tiledb {
namespace sm {

/* ****************************** */
/*          CONSTRUCTORS          */
/* ****************************** */

SparseIndexReaderBase::SparseIndexReaderBase(
    stats::Stats* stats,
    shared_ptr<Logger> logger,
    StorageManager* storage_manager,
    Array* array,
    Config& config,
    std::unordered_map<std::string, QueryBuffer>& buffers,
    Subarray& subarray,
    Layout layout,
    QueryCondition& condition,
    bool consolidation_with_timestamps)
    : ReaderBase(
          stats,
          logger,
          storage_manager,
          array,
          config,
          buffers,
          subarray,
          layout,
          condition)
    , initial_data_loaded_(false)
    , memory_budget_(0)
    , array_memory_tracker_(array->memory_tracker())
    , memory_used_for_coords_total_(0)
    , memory_used_qc_tiles_total_(0)
    , memory_used_result_tile_ranges_(0)
    , memory_budget_ratio_coords_(0.5)
    , memory_budget_ratio_query_condition_(0.25)
    , memory_budget_ratio_tile_ranges_(0.1)
    , memory_budget_ratio_array_data_(0.1)
    , buffers_full_(false)
    , consolidation_with_timestamps_(consolidation_with_timestamps)
    , load_timestamps_(false) {
  read_state_.done_adding_result_tiles_ = false;
}

/* ****************************** */
/*        PROTECTED METHODS       */
/* ****************************** */

const typename SparseIndexReaderBase::ReadState*
SparseIndexReaderBase::read_state() const {
  return &read_state_;
}

typename SparseIndexReaderBase::ReadState* SparseIndexReaderBase::read_state() {
  return &read_state_;
}

Status SparseIndexReaderBase::init() {
  // Sanity checks
  if (storage_manager_ == nullptr)
    return logger_->status(Status_ReaderError(
        "Cannot initialize sparse global order reader; Storage manager not "
        "set"));
  if (buffers_.empty())
    return logger_->status(Status_ReaderError(
        "Cannot initialize sparse global order reader; Buffers not set"));

  // Check subarray
  RETURN_NOT_OK(check_subarray());

  // Load offset configuration options.
  bool found = false;
  offsets_format_mode_ = config_.get("sm.var_offsets.mode", &found);
  assert(found);
  if (offsets_format_mode_ != "bytes" && offsets_format_mode_ != "elements") {
    return logger_->status(
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
    return logger_->status(
        Status_ReaderError("Cannot initialize reader; "
                           "Unsupported offsets bitsize in configuration"));
  }

  // Check the validity buffer sizes.
  RETURN_NOT_OK(check_validity_buffer_sizes());

  return Status::Ok();
}

uint64_t SparseIndexReaderBase::cells_copied(
    const std::vector<std::string>& names) {
  auto& last_name = names.back();
  auto buffer_size = *buffers_[last_name].buffer_size_;
  if (array_schema_.var_size(last_name)) {
    if (buffer_size == 0)
      return 0;
    else
      return buffer_size / (offsets_bitsize_ / 8) - offsets_extra_element_;
  } else {
    return buffer_size / array_schema_.cell_size(last_name);
  }
}

template <class BitmapType>
tuple<Status, optional<std::pair<uint64_t, uint64_t>>>
SparseIndexReaderBase::get_coord_tiles_size(
    bool include_coords,
    unsigned dim_num,
    unsigned f,
    uint64_t t,
    bool discard_partial_timestamps) {
  uint64_t tiles_size = 0;

  // Add the coordinate tiles size.
  if (include_coords) {
    for (unsigned d = 0; d < dim_num; d++) {
      tiles_size += fragment_metadata_[f]->tile_size(dim_names_[d], t);

      if (is_dim_var_size_[d]) {
        auto&& [st, temp] =
            fragment_metadata_[f]->tile_var_size(dim_names_[d], t);
        RETURN_NOT_OK_TUPLE(st, nullopt);
        tiles_size += *temp;
      }
    }
  }

  // Add the result tile structure size.
  tiles_size += sizeof(ResultTileWithBitmap<BitmapType>);

  // Add the tile bitmap size if there is a subarray.
  if (subarray_.is_set())
    tiles_size += fragment_metadata_[f]->cell_num(t) * sizeof(BitmapType);

  if (consolidation_with_timestamps_ && load_timestamps_ &&
      fragment_metadata_[f]->has_timestamps() && !discard_partial_timestamps) {
    tiles_size +=
        fragment_metadata_[f]->cell_num(t) * constants::timestamp_size;
  }

  // Compute query condition tile sizes.
  uint64_t tiles_size_qc = 0;
  if (!qc_loaded_names_.empty()) {
    for (auto& name : qc_loaded_names_) {
      // Calculate memory consumption for this tile.
      auto&& [st, tile_size] = get_attribute_tile_size(name, f, t);
      RETURN_NOT_OK_TUPLE(st, nullopt);
      tiles_size_qc += *tile_size;
    }
  }

  return {Status::Ok(), std::make_pair(tiles_size, tiles_size_qc)};
}

Status SparseIndexReaderBase::load_initial_data(bool include_timestamps) {
  if (initial_data_loaded_)
    return Status::Ok();

  auto timer_se = stats_->start_timer("load_initial_data");
  read_state_.done_adding_result_tiles_ = false;

  // Make a list of dim/attr that will be loaded for query condition.
  if (!initial_data_loaded_) {
    if (!condition_.empty()) {
      for (auto& name : condition_.field_names()) {
        qc_loaded_names_.emplace_back(name);
      }
    }
  }

  // For easy reference.
  auto fragment_num = fragment_metadata_.size();

  // Make sure there is enough space for tiles data.
  read_state_.frag_idx_.resize(fragment_num);
  all_tiles_loaded_.resize(fragment_num);

  // Calculate ranges of tiles in the subarray, if set.
  if (subarray_.is_set()) {
    // At this point, full memory budget is available.
    array_memory_tracker_->set_budget(memory_budget_);

    // Make sure there is no memory taken by the subarray.
    subarray_.clear_tile_overlap();

    // Tile ranges computation will not stop if it exceeds memory budget.
    // This is ok as it is a soft limit and will be taken into consideration
    // later.
    RETURN_NOT_OK(subarray_.precompute_all_ranges_tile_overlap(
        storage_manager_->compute_tp(),
        read_state_.frag_idx_,
        &result_tile_ranges_));

    for (auto frag_result_tile_ranges : result_tile_ranges_) {
      memory_used_result_tile_ranges_ += frag_result_tile_ranges.size() *
                                         sizeof(std::pair<uint64_t, uint64_t>);
    }

    if (memory_used_result_tile_ranges_ >
        memory_budget_ratio_tile_ranges_ * memory_budget_)
      return logger_->status(
          Status_ReaderError("Exceeded memory budget for result tile ranges"));
  }

  // Set a limit to the array memory.
  array_memory_tracker_->set_budget(
      memory_budget_ * memory_budget_ratio_array_data_);

  // Preload zipped coordinate tile offsets. Note that this will
  // ignore fragments with a version >= 5.
  std::vector<std::string> zipped_coords_names = {constants::coords};
  RETURN_CANCEL_OR_ERROR(load_tile_offsets(subarray_, zipped_coords_names));

  // Preload unzipped coordinate tile offsets. Note that this will
  // ignore fragments with a version < 5.
  const auto dim_num = array_schema_.dim_num();
  dim_names_.reserve(dim_num);
  is_dim_var_size_.reserve(dim_num);
  std::vector<std::string> var_size_to_load;
  for (unsigned d = 0; d < dim_num; ++d) {
    dim_names_.emplace_back(array_schema_.dimension_ptr(d)->name());
    is_dim_var_size_.emplace_back(array_schema_.var_size(dim_names_[d]));
    if (is_dim_var_size_[d])
      var_size_to_load.emplace_back(dim_names_[d]);
  }
  RETURN_CANCEL_OR_ERROR(load_tile_offsets(subarray_, dim_names_));

  // Compute tile offsets to load and var size to load for attributes.
  std::vector<std::string> attr_tile_offsets_to_load;
  for (auto& it : buffers_) {
    const auto& name = it.first;
    if (array_schema_.is_dim(name))
      continue;

    attr_tile_offsets_to_load.emplace_back(name);

    if (array_schema_.var_size(name))
      var_size_to_load.emplace_back(name);
  }

  // Add timestamps if required.
  if (include_timestamps) {
    attr_tile_offsets_to_load.emplace_back(constants::timestamps);
  }

  // Load tile offsets and var sizes for attributes.
  RETURN_CANCEL_OR_ERROR(load_tile_var_sizes(subarray_, var_size_to_load));
  RETURN_CANCEL_OR_ERROR(
      load_tile_offsets(subarray_, attr_tile_offsets_to_load));

  logger_->debug("Initial data loaded");
  initial_data_loaded_ = true;
  return Status::Ok();
}

Status SparseIndexReaderBase::read_and_unfilter_coords(
    bool include_coords,
    bool include_timestamps,
    const std::vector<ResultTile*>& result_tiles) {
  auto timer_se = stats_->start_timer("read_and_unfilter_coords");

  // Not including coords or no query condition, exit.
  if (!include_coords && condition_.empty())
    return Status::Ok();

  if (subarray_.is_set() || include_coords) {
    // Read and unfilter zipped coordinate tiles. Note that
    // this will ignore fragments with a version >= 5.
    std::vector<std::string> zipped_coords_names = {constants::coords};
    RETURN_CANCEL_OR_ERROR(
        read_coordinate_tiles(zipped_coords_names, result_tiles, true));
    RETURN_CANCEL_OR_ERROR(
        unfilter_tiles(constants::coords, result_tiles, true));

    // Read and unfilter unzipped coordinate tiles. Note that
    // this will ignore fragments with a version < 5.
    RETURN_CANCEL_OR_ERROR(
        read_coordinate_tiles(dim_names_, result_tiles, true));
    for (const auto& dim_name : dim_names_) {
      RETURN_CANCEL_OR_ERROR(unfilter_tiles(dim_name, result_tiles, true));
    }
  }

  if (include_timestamps) {
    std::vector<std::string> timestamps_names = {constants::timestamps};
    RETURN_CANCEL_OR_ERROR(
        read_attribute_tiles(timestamps_names, result_tiles, true));
    RETURN_CANCEL_OR_ERROR(
        unfilter_tiles(constants::timestamps, result_tiles, true));
  }

  if (!condition_.empty()) {
    // Read and unfilter tiles for querty condition.
    RETURN_CANCEL_OR_ERROR(
        read_attribute_tiles(qc_loaded_names_, result_tiles, true));

    for (const auto& name : qc_loaded_names_) {
      RETURN_CANCEL_OR_ERROR(unfilter_tiles(name, result_tiles, true));
    }
  }

  logger_->debug("Done reading and unfiltering coords tiles");
  return Status::Ok();
}

template <class BitmapType>
Status SparseIndexReaderBase::allocate_tile_bitmap(
    ResultTileWithBitmap<BitmapType>* rt) {
  // Bitmap was already computed for this tile.
  if (rt->bitmap_result_num_ != std::numeric_limits<uint64_t>::max()) {
    return Status::Ok();
  }

  auto cell_num = fragment_metadata_[rt->frag_idx()]->cell_num(rt->tile_idx());
  rt->bitmap_.resize(cell_num, 1);

  return Status::Ok();
}

template <class BitmapType>
Status SparseIndexReaderBase::compute_tile_bitmaps(
    std::vector<ResultTile*>& result_tiles) {
  auto timer_se = stats_->start_timer("compute_tile_bitmaps");

  // For easy reference.
  const auto& domain{array_schema_.domain()};
  const auto dim_num = array_schema_.dim_num();
  const auto cell_order = array_schema_.cell_order();

  // No subarray set, return.
  if (!subarray_.is_set()) {
    return Status::Ok();
  }

  // Compute parallelization parameters.
  uint64_t num_range_threads = 1;
  const auto num_threads = storage_manager_->compute_tp()->concurrency_level();
  if (result_tiles.size() < num_threads) {
    // Ceil the division between thread_num and tile_num.
    num_range_threads = 1 + ((num_threads - 1) / result_tiles.size());
  }

  // Perforance runs have shown that running multiple parallel_for's has a
  // measurable performance impact. So only pre-allocate tile bitmaps if we
  // are going to run multiple range threads.
  if (num_range_threads != 1) {
    // Resize bitmaps to process for each tiles in parallel.
    auto status = parallel_for(
        storage_manager_->compute_tp(),
        0,
        result_tiles.size(),
        [&](uint64_t t) {
          return allocate_tile_bitmap(
              (ResultTileWithBitmap<BitmapType>*)result_tiles[t]);
        });
    RETURN_NOT_OK_ELSE(status, logger_->status(status));
  }

  // Process all tiles/cells in parallel.
  auto status = parallel_for_2d(
      storage_manager_->compute_tp(),
      0,
      result_tiles.size(),
      0,
      num_range_threads,
      [&](uint64_t t, uint64_t range_thread_idx) {
        // For easy reference.
        auto rt = (ResultTileWithBitmap<BitmapType>*)result_tiles[t];
        auto cell_num =
            fragment_metadata_[rt->frag_idx()]->cell_num(rt->tile_idx());

        // Bitmap was already computed for this tile.
        if (rt->bitmap_result_num_ != std::numeric_limits<uint64_t>::max())
          return Status::Ok();

        // Allocate the bitmap if not preallocated.
        if (num_range_threads == 1) {
          RETURN_NOT_OK(allocate_tile_bitmap(rt));
        }

        // Prevent processing past the end of the cells in case there are more
        // threads than cells.
        if (range_thread_idx > cell_num - 1) {
          return Status::Ok();
        }

        // Get the MBR for this tile.
        const auto& mbr =
            fragment_metadata_[rt->frag_idx()]->mbr(rt->tile_idx());

        // Compute bitmaps one dimension at a time.
        for (unsigned d = 0; d < dim_num; d++) {
          // For col-major cell ordering, iterate the dimensions
          // in reverse.
          const unsigned dim_idx =
              cell_order == Layout::COL_MAJOR ? dim_num - d - 1 : d;

          // No need to compute bitmaps for default dimensions.
          if (subarray_.is_default(dim_idx))
            continue;

          auto& ranges_for_dim = subarray_.ranges_for_dim(dim_idx);

          // Compute the list of range index to process.
          std::vector<uint64_t> relevant_ranges;
          relevant_ranges.reserve(ranges_for_dim.size());
          domain.dimension_ptr(dim_idx)->relevant_ranges(
              ranges_for_dim, mbr[dim_idx], relevant_ranges);

          // For non overlapping ranges, if we have full overlap on any range
          // there is no need to compute bitmaps.
          const bool non_overlapping = std::is_same<BitmapType, uint8_t>::value;
          if (non_overlapping) {
            std::vector<bool> covered_bitmap =
                domain.dimension_ptr(dim_idx)->covered_vec(
                    ranges_for_dim, mbr[dim_idx], relevant_ranges);

            // See if any range is covered.
            uint64_t count = std::accumulate(
                covered_bitmap.begin(), covered_bitmap.end(), 0);

            if (count != 0)
              continue;
          }

          // Compute the cells to process.
          auto part_num = std::min(cell_num, num_range_threads);
          auto min = (range_thread_idx * cell_num + part_num - 1) / part_num;
          auto max = std::min(
              ((range_thread_idx + 1) * cell_num + part_num - 1) / part_num,
              cell_num);

          // Compute the bitmap for the cells.
          {
            auto timer_compute_results_count_sparse =
                stats_->start_timer("compute_results_count_sparse");
            RETURN_NOT_OK(rt->compute_results_count_sparse(
                dim_idx,
                ranges_for_dim,
                relevant_ranges,
                rt->bitmap_,
                cell_order,
                min,
                max));
          }
        }

        // Only compute bitmap cells here if we are processing a single cell
        // range. If not, it will be done below.
        if (num_range_threads == 1) {
          RETURN_NOT_OK(count_tile_bitmap_cells(rt));
        }

        return Status::Ok();
      });
  RETURN_NOT_OK_ELSE(status, logger_->status(status));

  // For multiple range threads, bitmap cell count is done in a separate
  // parallel for.
  if (num_range_threads != 1) {
    // Compute number of cells in each bitmaps in parallel.
    status = parallel_for(
        storage_manager_->compute_tp(),
        0,
        result_tiles.size(),
        [&](uint64_t t) {
          return count_tile_bitmap_cells(
              (ResultTileWithBitmap<BitmapType>*)result_tiles[t]);
        });
    RETURN_NOT_OK_ELSE(status, logger_->status(status));
  }

  logger_->debug("Done computing tile bitmaps");
  return Status::Ok();
}

/** Count the number of cells in a bitmap. */
template <class BitmapType>
Status SparseIndexReaderBase::count_tile_bitmap_cells(
    ResultTileWithBitmap<BitmapType>* rt) {
  // Bitmap was already computed for this tile.
  if (rt->bitmap_result_num_ != std::numeric_limits<uint64_t>::max()) {
    return Status::Ok();
  }

  // Compute number of cells in this tile.
  auto cell_num = fragment_metadata_[rt->frag_idx()]->cell_num(rt->tile_idx());
  rt->bitmap_result_num_ = 0;
  for (uint64_t c = 0; c < cell_num; ++c) {
    rt->bitmap_result_num_ += rt->bitmap_[c];
  }

  const bool non_overlapping = std::is_same<BitmapType, uint8_t>::value;
  if (non_overlapping) {
    // Clear the bitmap, which will also signal the copy operation to copy the
    // whole tile.
    if (rt->bitmap_result_num_ == cell_num) {
      rt->bitmap_.resize(0);
    }
  }

  return Status::Ok();
}

template <class BitmapType>
Status SparseIndexReaderBase::apply_query_condition(
    std::vector<ResultTile*>& result_tiles) {
  auto timer_se = stats_->start_timer("apply_query_condition");

  if (!condition_.empty()) {
    // Process all tiles in parallel.
    auto status = parallel_for(
        storage_manager_->compute_tp(),
        0,
        result_tiles.size(),
        [&](uint64_t t) {
          // For easy reference.
          auto rt = (ResultTileWithBitmap<BitmapType>*)result_tiles[t];

          auto cell_num =
              fragment_metadata_[rt->frag_idx()]->cell_num(rt->tile_idx());

          // Set num_cells if no subarray as it's used to filter below.
          if (!subarray_.is_set()) {
            rt->bitmap_result_num_ = cell_num;
          }

          // Full overlap in bitmap calculation, make a bitmap.
          if (!rt->has_bmp()) {
            rt->bitmap_.resize(cell_num, 1);
            rt->bitmap_result_num_ = cell_num;
          }

          // Compute the result of the query condition for this tile.
          RETURN_NOT_OK(condition_.apply_sparse<BitmapType>(
              *(fragment_metadata_[rt->frag_idx()]->array_schema().get()),
              *rt,
              rt->bitmap_,
              &rt->bitmap_result_num_));

          return Status::Ok();
        });
    RETURN_NOT_OK_ELSE(status, logger_->status(status));
  }

  logger_->debug("Done applying query condition");
  return Status::Ok();
}

tuple<Status, optional<std::vector<uint64_t>>>
SparseIndexReaderBase::read_and_unfilter_attributes(
    const uint64_t memory_budget,
    const std::vector<std::string>& names,
    const std::vector<uint64_t>& mem_usage_per_attr,
    uint64_t* buffer_idx,
    std::vector<ResultTile*>& result_tiles) {
  auto timer_se = stats_->start_timer("read_and_unfilter_attributes");

  std::vector<std::string> names_to_read;
  std::vector<uint64_t> index_to_copy;
  uint64_t memory_used = 0;
  while (*buffer_idx < names.size()) {
    auto& name = names[*buffer_idx];

    auto attr_mem_usage = mem_usage_per_attr[*buffer_idx];
    if (memory_used + attr_mem_usage < memory_budget) {
      memory_used += attr_mem_usage;

      // We only read attributes, so dimensions have 0 cost.
      if (attr_mem_usage != 0)
        names_to_read.emplace_back(name);

      index_to_copy.emplace_back(*buffer_idx);
      (*buffer_idx)++;
    } else {
      break;
    }
  }

  // Read and unfilter tiles.
  RETURN_NOT_OK_TUPLE(
      read_attribute_tiles(names_to_read, result_tiles, true), nullopt);

  for (auto& name : names_to_read)
    RETURN_NOT_OK_TUPLE(unfilter_tiles(name, result_tiles, true), nullopt);

  return {Status::Ok(), std::move(index_to_copy)};
}

Status SparseIndexReaderBase::resize_output_buffers(uint64_t cells_copied) {
  // Resize buffers if the result cell slabs was truncated.
  for (auto& it : buffers_) {
    const auto& name = it.first;
    const auto size = *it.second.buffer_size_;
    uint64_t num_cells = 0;

    if (array_schema_.var_size(name)) {
      // Get the current number of cells from the offsets buffer.
      num_cells = size / constants::cell_var_offset_size;

      // Remove an element if the extra element flag is set.
      if (offsets_extra_element_ && num_cells > 0)
        num_cells--;

      // Buffer should be resized.
      if (num_cells > cells_copied) {
        // Offsets buffer is trivial.
        *(it.second.buffer_size_) =
            cells_copied * constants::cell_var_offset_size +
            offsets_extra_element_ * offsets_bytesize();

        // Since the buffer is shrunk, there is an offset for the next element
        // loaded, use it.
        if (offsets_bitsize_ == 64) {
          uint64_t offset_div =
              elements_mode_ ? datatype_size(array_schema_.type(name)) : 1;
          *it.second.buffer_var_size_ =
              ((uint64_t*)it.second.buffer_)[cells_copied] * offset_div;
        } else {
          uint32_t offset_div =
              elements_mode_ ? datatype_size(array_schema_.type(name)) : 1;
          *it.second.buffer_var_size_ =
              ((uint32_t*)it.second.buffer_)[cells_copied] * offset_div;
        }
      }
    } else {
      // Always adjust the size for fixed size attributes.
      auto cell_size = array_schema_.cell_size(name);
      *(it.second.buffer_size_) = cells_copied * cell_size;
    }

    // Always adjust validity vector size, if present.
    if (num_cells > cells_copied) {
      if (it.second.validity_vector_.buffer_size() != nullptr)
        *(it.second.validity_vector_.buffer_size()) =
            num_cells * constants::cell_validity_size;
    }
  }

  return Status::Ok();
}

Status SparseIndexReaderBase::add_extra_offset() {
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
          buffer + *it.second.buffer_size_ - offsets_bytesize(),
          it.second.buffer_var_size_,
          offsets_bytesize());
    } else if (offsets_format_mode_ == "elements") {
      auto elements =
          *it.second.buffer_var_size_ / datatype_size(array_schema_.type(name));
      memcpy(
          buffer + *it.second.buffer_size_ - offsets_bytesize(),
          &elements,
          offsets_bytesize());
    } else {
      return logger_->status(Status_ReaderError(
          "Cannot add extra offset to buffer; Unsupported offsets format"));
    }
  }

  return Status::Ok();
}

void SparseIndexReaderBase::remove_result_tile_range(uint64_t f) {
  result_tile_ranges_[f].pop_back();
  {
    std::unique_lock<std::mutex> lck(mem_budget_mtx_);
    memory_used_result_tile_ranges_ -= sizeof(std::pair<uint64_t, uint64_t>);
  }
}

// Explicit template instantiations
template tuple<Status, optional<std::pair<uint64_t, uint64_t>>>
SparseIndexReaderBase::get_coord_tiles_size<uint64_t>(
    bool, unsigned, unsigned, uint64_t, bool);
template tuple<Status, optional<std::pair<uint64_t, uint64_t>>>
SparseIndexReaderBase::get_coord_tiles_size<uint8_t>(
    bool, unsigned, unsigned, uint64_t, bool);
template Status SparseIndexReaderBase::apply_query_condition<uint64_t>(
    std::vector<ResultTile*>&);
template Status SparseIndexReaderBase::apply_query_condition<uint8_t>(
    std::vector<ResultTile*>&);
template Status SparseIndexReaderBase::compute_tile_bitmaps<uint64_t>(
    std::vector<ResultTile*>&);
template Status SparseIndexReaderBase::compute_tile_bitmaps<uint8_t>(
    std::vector<ResultTile*>&);

}  // namespace sm
}  // namespace tiledb
