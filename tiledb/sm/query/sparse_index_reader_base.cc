/**
 * @file   sparse_index_reader_base.cc
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
 * This file implements class SparseIndexReaderBase.
 */

#include "tiledb/sm/query/sparse_index_reader_base.h"
#include "tiledb/common/logger.h"
#include "tiledb/sm/array/array.h"
#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/filesystem/vfs.h"
#include "tiledb/sm/fragment/fragment_metadata.h"
#include "tiledb/sm/misc/parallel_functions.h"
#include "tiledb/sm/query/query_macros.h"
#include "tiledb/sm/query/strategy_base.h"
#include "tiledb/sm/storage_manager/open_array_memory_tracker.h"
#include "tiledb/sm/subarray/subarray.h"

namespace tiledb {
namespace sm {

/* ****************************** */
/*          CONSTRUCTORS          */
/* ****************************** */

SparseIndexReaderBase::SparseIndexReaderBase(
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
          condition)
    , range_num_(0)
    , done_adding_result_tiles_(false)
    , initial_data_loaded_(false)
    , memory_budget_(0)
    , array_memory_tracker_(nullptr)
    , memory_used_for_coords_total_(0)
    , memory_used_qc_tiles_(0)
    , memory_used_rcs_(0)
    , memory_used_result_tiles_(0)
    , memory_used_result_tile_ranges_(0)
    , memory_budget_ratio_coords_(0.5)
    , memory_budget_ratio_query_condition_(0.25)
    , memory_budget_ratio_tile_ranges_(0.1)
    , memory_budget_ratio_array_data_(0.1)
    , memory_budget_ratio_result_tiles_(0.05)
    , memory_budget_ratio_rcs_(0.05)
    , coords_loaded_(true) {
  read_state_.range_idx_ = 0;
}

/* ****************************** */
/*        PROTECTED METHODS       */
/* ****************************** */

const SparseIndexReaderBase::ReadState* SparseIndexReaderBase::read_state()
    const {
  return &read_state_;
}

SparseIndexReaderBase::ReadState* SparseIndexReaderBase::read_state() {
  return &read_state_;
}

Status SparseIndexReaderBase::get_coord_tiles_size(
    unsigned dim_num, unsigned f, uint64_t t, uint64_t* tiles_size) {
  *tiles_size = 0;
  for (unsigned d = 0; d < dim_num; d++) {
    *tiles_size += fragment_metadata_[f]->tile_size(dim_names_[d], t);

    if (is_dim_var_size_[d]) {
      uint64_t temp = 0;
      RETURN_NOT_OK(fragment_metadata_[f]->tile_var_size(
          *array_->encryption_key(), dim_names_[d], t, &temp));
      *tiles_size += temp;
    }
  }

  return Status::Ok();
}

Status SparseIndexReaderBase::load_initial_data() {
  if (initial_data_loaded_)
    return Status::Ok();

  auto timer_se = stats_->start_timer("load_initial_data");
  done_adding_result_tiles_ = false;

  // Validate that there is no lingering data from previous iterations.
  assert(memory_used_for_coords_total_ == 0);
  assert(memory_used_qc_tiles_ == 0);
  assert(memory_used_rcs_ == 0);
  assert(memory_used_result_tile_ranges_ == 0);
  assert(memory_used_result_tiles_ == 0);

  // For easy reference.
  auto fragment_num = fragment_metadata_.size();
  auto range_num = subarray_.range_num() - read_state_.range_idx_;

  // Make sure there is enough space for tiles data.
  read_state_.frag_tile_idx_.clear();
  all_tiles_loaded_.clear();
  read_state_.frag_tile_idx_.resize(fragment_num);
  all_tiles_loaded_.resize(fragment_num);

  // Calculate ranges of tiles in the subarray, if set.
  if (subarray_.is_set()) {
    // At this point, full memory budget is available.
    array_memory_tracker_->set_budget(memory_budget_);

    // Sometimes, memory size estimation bypasses memory constraints. In this
    // case, there might be a subarray that's already way past the memory
    // limits within this reader, for now clear the data and recompute.
    // TODO add stats here so it can be tracked as it will impact perf.
    // Also truncate the data instead of clearing it.
    auto ratio = range_num > 1 ? TILE_RANGES_TO_TILE_OVERLAP_RATIO_MULTI_RANGE :
                                 TILE_RANGES_TO_TILE_OVERLAP_RATIO_SINGLE_RANGE;
    if (subarray_.tile_overlap_byte_size() * ratio >
        memory_budget_ * memory_budget_ratio_tile_ranges_) {
      subarray_.clear_tile_overlap();
    }

    // Tile overlap computation will not stop if it exceeds memory budget.
    // This is ok as it is a soft limit and will be taken into consideration
    // later.
    uint64_t max_tile_overlap_size =
        memory_budget_ * memory_budget_ratio_tile_ranges_ / (ratio + 1);
    RETURN_NOT_OK(config_.set(
        "sm.max_tile_overlap_size", std::to_string(max_tile_overlap_size)));
    RETURN_NOT_OK(subarray_.precompute_tile_overlap(
        read_state_.range_idx_,
        subarray_.range_num() - 1,
        &config_,
        storage_manager_->compute_tp()));

    range_num_ = subarray_.subarray_tile_overlap()->range_idx_end() -
                 subarray_.subarray_tile_overlap()->range_idx_start() + 1;

    if (subarray_.subarray_tile_overlap()->range_idx_end() ==
        subarray_.range_num() - 1) {
      // Free the rtrees from memory.
      for (auto frag_md : fragment_metadata_) {
        frag_md->free_rtree();
      }
    }

    // Compute tile ranges.
    RETURN_CANCEL_OR_ERROR(compute_result_tiles_ranges(
        memory_budget_ * memory_budget_ratio_tile_ranges_));
  }

  // Set a limit to the array memory.
  array_memory_tracker_->set_budget(
      memory_budget_ * memory_budget_ratio_array_data_);

  // Only load this data once.
  if (read_state_.range_idx_ == 0) {
    // Preload zipped coordinate tile offsets. Note that this will
    // ignore fragments with a version >= 5.
    std::vector<std::string> zipped_coords_names = {constants::coords};
    RETURN_CANCEL_OR_ERROR(load_tile_offsets(&subarray_, &zipped_coords_names));

    // Preload unzipped coordinate tile offsets. Note that this will
    // ignore fragments with a version < 5.
    const auto dim_num = array_schema_->dim_num();
    dim_names_.reserve(dim_num);
    is_dim_var_size_.reserve(dim_num);
    for (unsigned d = 0; d < dim_num; ++d) {
      dim_names_.emplace_back(array_schema_->dimension(d)->name());
      is_dim_var_size_[d] = array_schema_->var_size(dim_names_[d]);
    }
    RETURN_CANCEL_OR_ERROR(load_tile_offsets(&subarray_, &dim_names_));
  }

  initial_data_loaded_ = true;
  return Status::Ok();
}

// Sort vector elements by second element of tuples.
bool reverse_tuple_sort_by_second(
    const tuple<uint64_t, uint64_t, uint64_t>& a,
    const tuple<uint64_t, uint64_t, uint64_t>& b) {
  return (std::get<1>(a) > std::get<1>(b));
}

Status SparseIndexReaderBase::compute_result_tiles_ranges(
    uint64_t memory_budget) {
  auto timer_se = stats_->start_timer("compute_result_tiles_ranges");

  // For easy reference.
  auto fragment_num = fragment_metadata_.size();

  // For this operation, all the memory budget can be used. This is mostly a
  // sanity check as the tile overlap size is already capped at less than
  // 25% of the memory budget but ensures the memory budget will be respected.
  // See load_initial_data for the provenance of the number 4.
  if (subarray_.tile_overlap_byte_size() > memory_budget_ / 4) {
    return LOG_STATUS(
        Status::ReaderError("Exceeded memory budget for tile overlap to "
                            "compute result tile ranges"));
  }

  // Build vectors of sorted tile ranges, per fragments.
  result_tile_ranges_.clear();
  result_tile_ranges_.resize(fragment_num);

  auto status = parallel_for(
      storage_manager_->compute_tp(), 0, fragment_num, [&](uint64_t f) {
        // Filter out ranges that are smaller than the tile index.
        auto tile_idx = read_state_.frag_tile_idx_[f].first;

        for (uint64_t r = 0; r < range_num_; ++r) {
          // Insert range of tiles.
          const auto& tile_ranges = subarray_.tile_overlap(f, r)->tile_ranges_;
          for (const auto& tr : tile_ranges) {
            // Make sure all ranges start at a minumum at the tile index.
            if (tile_idx <= tr.second)
              result_tile_ranges_[f].emplace_back(
                  std::max(tile_idx, tr.first), tr.second);
          }

          // Insert single tiles.
          const auto& o_tiles = subarray_.tile_overlap(f, r)->tiles_;
          for (const auto& o_tile : o_tiles) {
            if (tile_idx <= o_tile.first) {
              auto range_end = o_tile.first;
              if (range_num_ <= 1) {
                if (o_tile.second == 0.0) {
                  range_end = NO_OVERLAP;
                } else if (o_tile.second != 1.0) {
                  range_end = COMPUTE_OVERLAP;
                }
              }
              result_tile_ranges_[f].emplace_back(o_tile.first, range_end);
            }
          }
        }

        std::sort(
            result_tile_ranges_[f].rbegin(), result_tile_ranges_[f].rend());

        // Add the data to the memory budget for single range, multiple will
        // be added in the step below.
        if (range_num_ == 1) {
          std::unique_lock<std::mutex> lck(mem_budget_mtx_);
          memory_used_result_tile_ranges_ +=
              result_tile_ranges_[f].size() *
              sizeof(std::pair<uint64_t, uint64_t>);
        }

        return Status::Ok();
      });
  RETURN_NOT_OK_ELSE(status, LOG_STATUS(status));

  if (range_num_ > 1) {
    // Go though the sorted vectors of ranges, and coalesce them, in place.
    status = parallel_for(
        storage_manager_->compute_tp(), 0, fragment_num, [&](uint64_t f) {
          // For easy reference.
          auto& vec = result_tile_ranges_[f];

          uint64_t final_idx = 0, i = 0;
          while (i < vec.size()) {
            // Save the first value, initialize a value to track max second.
            vec[final_idx].first = vec[i].first;
            auto max_second = vec[i].second;

            // Go as long as tile index is same or contiguous.
            while (i < vec.size() - 1 && vec[i].first - vec[i + 1].first <= 1) {
              ++i;
              max_second = std::max(max_second, vec[i].second);
            }

            vec[final_idx].first = vec[i].first;
            vec[final_idx].second = max_second;
            ++final_idx;
            ++i;
          }

          // Adjust the vector size.
          vec.resize(final_idx);

          // Add the data to the memory budget.
          {
            std::unique_lock<std::mutex> lck(mem_budget_mtx_);
            memory_used_result_tile_ranges_ +=
                vec.size() * sizeof(std::pair<uint64_t, uint64_t>);
          }

          return Status::Ok();
        });
    RETURN_NOT_OK_ELSE(status, LOG_STATUS(status));
  }

  // Build vectors of sorted tile ranges, per range.
  range_result_tiles_ranges_.clear();
  if (range_num_ > 1) {
    range_result_tiles_ranges_.resize(range_num_);

    status = parallel_for(
        storage_manager_->compute_tp(), 0, range_num_, [&](uint64_t r) {
          // Process fragments in reverse as the result is in descending order.
          for (int64_t f = fragment_num - 1; f >= 0; f--) {
            // Filter out ranges that are smaller than the tile index.
            auto tile_idx = read_state_.frag_tile_idx_[f].first;
            auto saved_size = range_result_tiles_ranges_[r].size();

            // Inset range of tiles.
            const auto& tile_ranges =
                subarray_.tile_overlap(f, r)->tile_ranges_;
            for (const auto& tr : tile_ranges) {
              range_result_tiles_ranges_[r].emplace_back(
                  f, tr.first, tr.second);
            }

            // Insert single tiles.
            const auto& o_tiles = subarray_.tile_overlap(f, r)->tiles_;
            for (const auto& o_tile : o_tiles) {
              if (tile_idx <= o_tile.first) {
                auto range_end = o_tile.first;
                if (o_tile.second == 0.0) {
                  range_end = NO_OVERLAP;
                } else if (o_tile.second != 1.0) {
                  range_end = COMPUTE_OVERLAP;
                }
                range_result_tiles_ranges_[r].emplace_back(
                    f, o_tile.first, range_end);
              }
            }

            // Sort only the ranges for this fragment.
            std::sort(
                range_result_tiles_ranges_[r].begin() + saved_size,
                range_result_tiles_ranges_[r].end(),
                reverse_tuple_sort_by_second);
          }

          // Add the data to the memory budget.
          {
            std::unique_lock<std::mutex> lck(mem_budget_mtx_);
            memory_used_result_tile_ranges_ +=
                range_result_tiles_ranges_[r].size() *
                sizeof(std::tuple<uint64_t, uint64_t, uint64_t>);
          }

          return Status::Ok();
        });
    RETURN_NOT_OK_ELSE(status, LOG_STATUS(status));
  }

  // Free memory for tile overlap data.
  subarray_.clear_tile_overlap();

  // If memory budget is busted, exit.
  if (memory_used_result_tile_ranges_ >= memory_budget)
    return LOG_STATUS(
        Status::ReaderError("Exceeded memory budget for result tile ranges"));

  return Status::Ok();
}

Status SparseIndexReaderBase::compute_coord_tiles_result_bitmap(
    ResultTile* tile,
    uint64_t range_idx,
    std::vector<uint8_t>* coord_tiles_result_bitmap) {
  auto timer_se = stats_->start_timer("compute_coord_tiles_result_bitmap");

  // For easy reference.
  auto dim_num = array_schema_->dim_num();
  auto cell_order = array_schema_->cell_order();
  auto range_coords = subarray_.get_range_coords(range_idx);

  // Compute result and overwritten bitmap per dimension
  for (unsigned d = 0; d < dim_num; ++d) {
    // For col-major cell ordering, iterate the dimensions
    // in reverse.
    const unsigned dim_idx =
        cell_order == Layout::COL_MAJOR ? dim_num - d - 1 : d;
    if (!subarray_.is_default(dim_idx)) {
      const auto& ranges = subarray_.ranges_for_dim(dim_idx);
      RETURN_NOT_OK(tile->compute_results_sparse(
          dim_idx,
          ranges[range_coords[dim_idx]],
          coord_tiles_result_bitmap,
          cell_order));
    }
  }

  return Status::Ok();
}

Status SparseIndexReaderBase::resize_output_buffers() {
  // Count number of elements actually copied.
  uint64_t cells_copied = 0;
  for (uint64_t i = 0; i < copy_end_.first - 1; i++) {
    cells_copied += read_state_.result_cell_slabs_[i].length_;
  }

  cells_copied += copy_end_.second;

  // Resize buffers if the result cell slabs was truncated.
  for (auto& it : buffers_) {
    const auto& name = it.first;
    const auto size = *it.second.buffer_size_;
    uint64_t num_cells = 0;

    if (array_schema_->var_size(name)) {
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
            offsets_extra_element_;

        // Since the buffer is shrunk, there is an offset for the next element
        // loaded, use it.
        *(it.second.buffer_var_size_) =
            ((uint64_t*)it.second.buffer_)[cells_copied];
      }
    } else {
      // Always adjust the size for fixed size attributes.
      auto cell_size = array_schema_->cell_size(name);
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
    if (!array_schema_->var_size(name))
      continue;

    auto buffer = static_cast<unsigned char*>(it.second.buffer_);
    if (offsets_format_mode_ == "bytes") {
      memcpy(
          buffer + *it.second.buffer_size_ - offsets_bytesize(),
          it.second.buffer_var_size_,
          offsets_bytesize());
    } else if (offsets_format_mode_ == "elements") {
      auto elements = *it.second.buffer_var_size_ /
                      datatype_size(array_schema_->type(name));
      memcpy(
          buffer + *it.second.buffer_size_ - offsets_bytesize(),
          &elements,
          offsets_bytesize());
    } else {
      return LOG_STATUS(Status::ReaderError(
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

void SparseIndexReaderBase::remove_range_result_tile_range(uint64_t r) {
  range_result_tiles_ranges_[r].pop_back();
  {
    std::unique_lock<std::mutex> lck(mem_budget_mtx_);
    memory_used_result_tile_ranges_ -=
        sizeof(std::tuple<uint64_t, uint64_t, uint64_t>);
  }
}

}  // namespace sm
}  // namespace tiledb
