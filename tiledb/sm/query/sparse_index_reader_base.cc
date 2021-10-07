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

  // For easy reference.
  auto fragment_num = fragment_metadata_.size();

  // Make sure we have enough space for tiles data.
  read_state_.frag_tile_idx_.resize(fragment_num);
  all_tiles_loaded_.resize(fragment_num);

  // Calculate ranges of tiles in the subarray, if set.
  if (subarray_.is_set()) {
    // We have the full memory budget at this point, use it.
    array_memory_tracker_->set_budget(memory_budget_);

    // TODO Tile overlap computation will not stop if it exceeds memory budget.
    RETURN_NOT_OK(subarray_.precompute_tile_overlap(
        0, 0, &config_, storage_manager_->compute_tp()));

    // Free the rtrees from memory.
    for (auto frag_md : fragment_metadata_) {
      frag_md->free_rtree();
    }

    // Compute tile ranges.
    RETURN_CANCEL_OR_ERROR(compute_result_tiles_ranges(
        memory_budget_ * memory_budget_ratio_tile_ranges_));
  }

  // Set a limit to the array memory.
  array_memory_tracker_->set_budget(
      memory_budget_ * memory_budget_ratio_array_data_);

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

  initial_data_loaded_ = true;
  return Status::Ok();
}

Status SparseIndexReaderBase::compute_result_tiles_ranges(
    uint64_t memory_budget) {
  auto timer_se = stats_->start_timer("compute_result_tiles_ranges");

  // For easy reference.
  auto range_num = subarray_.range_num();
  auto fragment_num = fragment_metadata_.size();

  // To sort the ranges, we need double the memory of the tile overlap data.
  if (subarray_.tile_overlap_byte_size() > memory_budget_ / 2) {
    return LOG_STATUS(
        Status::ReaderError("Exceeded memory budget for tile overlap"));
  }

  // Build vectors of sorted ranges, per fragments.
  std::vector<std::vector<std::pair<uint64_t, uint64_t>>> sorted_ranges;
  sorted_ranges.resize(fragment_num);

  auto status = parallel_for(
      storage_manager_->compute_tp(), 0, fragment_num, [&](uint64_t f) {
        // Filter out ranges that are smaller than the tile index.
        auto tile_idx = read_state_.frag_tile_idx_[f].first;
        for (uint64_t r = 0; r < range_num; ++r) {
          // Inset range of tiles.
          const auto& tile_ranges = subarray_.tile_overlap(f, r)->tile_ranges_;
          for (const auto& tr : tile_ranges) {
            // Make sure all ranges start at a minumum at the tile index.
            if (tile_idx <= tr.second)
              sorted_ranges[f].emplace_back(
                  std::max(tile_idx, tr.first), tr.second);
          }

          // Insert single tiles.
          const auto& o_tiles = subarray_.tile_overlap(f, r)->tiles_;
          for (const auto& o_tile : o_tiles) {
            if (tile_idx <= o_tile.first)
              sorted_ranges[f].emplace_back(o_tile.first, o_tile.first);
          }
        }

        std::sort(sorted_ranges[f].begin(), sorted_ranges[f].end());
        return Status::Ok();
      });
  RETURN_NOT_OK_ELSE(status, LOG_STATUS(status));

  // Free memory for tile overlap data.
  subarray_.clear_tile_overlap();

  // Go though the sorted list of ranges, and coalesce them.
  result_tile_ranges_.resize(fragment_num);
  status = parallel_for(
      storage_manager_->compute_tp(), 0, fragment_num, [&](uint64_t f) {
        auto it = sorted_ranges[f].begin();
        while (it != sorted_ranges[f].end()) {
          auto it2 = it + 1;
          while (it2 != sorted_ranges[f].end()) {
            // Same start, we can ignore *it* since *it2* end will be greater.
            if (it->first == it2->first) {
              it++;
            } else if (it2->first <= it->second) {
              // The start of the second range is included in the first.
              it->second = std::max(it->second, it2->second);
            } else {
              // We start a new range.
              break;
            }
            it2++;
          }
          result_tile_ranges_[f].emplace_back(it->first, it->second);
          memory_used_result_tile_ranges_ += 2 * sizeof(uint64_t);

          // If we busted our memory budget, exit.
          if (memory_used_result_tile_ranges_ >= memory_budget)
            return LOG_STATUS(Status::ReaderError(
                "Exceeded memory budget for result tile ranges"));

          it = it2;
        }
        return Status::Ok();
      });
  RETURN_NOT_OK_ELSE(status, LOG_STATUS(status));

  return Status::Ok();
}

Status SparseIndexReaderBase::compute_coord_tiles_result_bitmap(
    bool subarray_set,
    ResultTile* tile,
    std::vector<uint8_t>* coord_tiles_result_bitmap) {
  auto timer_se = stats_->start_timer("compute_coord_tiles_result_bitmap");

  // No subarray means we process all cells.
  if (!subarray_set)
    return Status::Ok();

  // For easy reference.
  auto coords_num = tile->cell_num();
  auto dim_num = array_schema_->dim_num();
  auto cell_order = array_schema_->cell_order();
  auto range_coords = subarray_.get_range_coords(0);

  std::vector<uint8_t> result_bitmap(coords_num, 1);

  // Compute result and overwritten bitmap per dimension
  for (unsigned d = 0; d < dim_num; ++d) {
    // For col-major cell ordering, iterate the dimensions
    // in reverse.
    const unsigned dim_idx =
        cell_order == Layout::COL_MAJOR ? dim_num - d - 1 : d;
    if (!subarray_.is_default(dim_idx)) {
      const auto& ranges = subarray_.ranges_for_dim(dim_idx);
      RETURN_NOT_OK(tile->compute_results_sparse(
          dim_idx, ranges[range_coords[dim_idx]], &result_bitmap, cell_order));
    }
  }

  *coord_tiles_result_bitmap = std::move(result_bitmap);

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

        // Since we shrink the buffer, there is an offset for the next element
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

}  // namespace sm
}  // namespace tiledb
