/**
 * @file   sparse_global_order_reader.cc
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
 * This file implements class SparseGlobalOrderReader.
 */

#include "tiledb/sm/query/sparse_global_order_reader.h"
#include "tiledb/sm/array/array.h"
#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/array_schema/dimension.h"
#include "tiledb/sm/fragment/fragment_metadata.h"
#include "tiledb/sm/misc/comparators.h"
#include "tiledb/sm/misc/hilbert.h"
#include "tiledb/sm/misc/parallel_functions.h"
#include "tiledb/sm/misc/utils.h"
#include "tiledb/sm/query/query_macros.h"
#include "tiledb/sm/query/result_tile.h"
#include "tiledb/sm/stats/global_stats.h"
#include "tiledb/sm/storage_manager/open_array_memory_tracker.h"
#include "tiledb/sm/storage_manager/storage_manager.h"
#include "tiledb/sm/subarray/subarray.h"

using namespace tiledb;
using namespace tiledb::common;
using namespace tiledb::sm::stats;

namespace tiledb {
namespace sm {

/* ****************************** */
/*          CONSTRUCTORS          */
/* ****************************** */

SparseGlobalOrderReader::SparseGlobalOrderReader(
    stats::Stats* stats,
    StorageManager* storage_manager,
    Array* array,
    Config& config,
    std::unordered_map<std::string, QueryBuffer>& buffers,
    Subarray& subarray,
    Layout layout,
    QueryCondition& condition)
    : SparseIndexReaderBase(
          stats,
          storage_manager,
          array,
          config,
          buffers,
          subarray,
          layout,
          condition) {
  // Defines specific bahavior in the tile copy code for this reader.
  fix_var_sized_overflows_ = true;
  clear_coords_tiles_on_copy_ = false;
  array_memory_tracker_ =
      storage_manager_->array_memory_tracker(array->array_uri());
}

/* ****************************** */
/*               API              */
/* ****************************** */

bool SparseGlobalOrderReader::incomplete() const {
  return copy_overflowed_ || !read_state_.result_cell_slabs_.empty() ||
         !done_adding_result_tiles_;
}

Status SparseGlobalOrderReader::init() {
  // Sanity checks
  if (storage_manager_ == nullptr)
    return LOG_STATUS(Status::SparseGlobalOrderReaderError(
        "Cannot initialize sparse global order reader; Storage manager not "
        "set"));
  if (array_schema_ == nullptr)
    return LOG_STATUS(Status::SparseGlobalOrderReaderError(
        "Cannot initialize sparse global order reader; Array schema not set"));
  if (buffers_.empty())
    return LOG_STATUS(Status::SparseGlobalOrderReaderError(
        "Cannot initialize sparse global order reader; Buffers not set"));

  // Check subarray
  RETURN_NOT_OK(check_subarray());

  // Load offset configuration options.
  bool found = false;
  RETURN_NOT_OK(config_.get<bool>(
      "sm.var_offsets.extra_element", &offsets_extra_element_, &found));
  assert(found);
  RETURN_NOT_OK(config_.get<uint32_t>(
      "sm.var_offsets.bitsize", &offsets_bitsize_, &found));
  if (offsets_bitsize_ != 32 && offsets_bitsize_ != 64) {
    return LOG_STATUS(Status::SparseGlobalOrderReaderError(
        "Cannot initialize reader; "
        "Unsupported offsets bitsize in configuration"));
  }
  assert(found);
  RETURN_NOT_OK(
      config_.get<uint64_t>("sm.mem.total_budget", &memory_budget_, &found));
  assert(found);
  RETURN_NOT_OK(config_.get<double>(
      "sm.mem.reader.sparse_global_order.ratio_array_data",
      &memory_budget_ratio_array_data_,
      &found));
  assert(found);
  RETURN_NOT_OK(config_.get<double>(
      "sm.mem.reader.sparse_global_order.ratio_coords",
      &memory_budget_ratio_coords_,
      &found));
  assert(found);
  RETURN_NOT_OK(config_.get<double>(
      "sm.mem.reader.sparse_global_order.ratio_query_condition",
      &memory_budget_ratio_query_condition_,
      &found));
  assert(found);
  RETURN_NOT_OK(config_.get<double>(
      "sm.mem.reader.sparse_global_order.ratio_tile_ranges",
      &memory_budget_ratio_tile_ranges_,
      &found));
  assert(found);
  RETURN_NOT_OK(config_.get<double>(
      "sm.mem.reader.sparse_global_order.ratio_result_tiles",
      &memory_budget_ratio_result_tiles_,
      &found));
  assert(found);
  RETURN_NOT_OK(config_.get<double>(
      "sm.mem.reader.sparse_global_order.ratio_rcs",
      &memory_budget_ratio_rcs_,
      &found));
  assert(found);

  // Check the validity buffer sizes.
  RETURN_NOT_OK(check_validity_buffer_sizes());

  return Status::Ok();
}

Status SparseGlobalOrderReader::dowork() {
  auto timer_se = stats_->start_timer("dowork");

  // For easy reference.
  auto fragment_num = fragment_metadata_.size();

  // Check that the query condition is valid.
  RETURN_NOT_OK(condition_.check(array_schema_));

  get_dim_attr_stats();

  // Reset the copy overflow flag.
  copy_overflowed_ = false;

  // Handle empty array.
  if (fragment_metadata_.empty()) {
    done_adding_result_tiles_ = true;
    zero_out_buffer_sizes();
    return Status::Ok();
  }

  reset_buffer_sizes();

  // Make sure we have enough space for tiles data.
  memory_used_for_coords_.resize(fragment_num);
  result_tiles_.resize(fragment_num);

  // Load initial data, if not loaded already.
  RETURN_NOT_OK(load_initial_data());

  // If the result cell slab is empty, populate it.
  if (read_state_.result_cell_slabs_.empty())
    RETURN_NOT_OK(compute_result_cell_slab());

  // No more tiles to process, done.
  if (read_state_.result_cell_slabs_.empty()) {
    done_adding_result_tiles_ = true;
    zero_out_buffer_sizes();
    return Status::Ok();
  }

  // First try to limit the maximum number of cells we copy using the size
  // of the output buffers for fixed sized attributes. Later we will validate
  // the memory budget. This is the first line of defence used to try to prevent
  // overflows when copying data.
  uint64_t num_cells = std::numeric_limits<uint64_t>::max();
  for (const auto& it : buffers_) {
    const auto& name = it.first;
    const auto size = *it.second.buffer_size_;
    if (array_schema_->var_size(name)) {
      auto temp_num_cells = size / constants::cell_var_offset_size;

      if (offsets_extra_element_ && temp_num_cells > 0)
        temp_num_cells--;

      num_cells = std::min(num_cells, temp_num_cells);
    } else {
      auto temp_num_cells = size / array_schema_->cell_size(name);
      num_cells = std::min(num_cells, temp_num_cells);
    }
  }

  // User gave us some empty buffers, exit.
  if (num_cells == 0) {
    zero_out_buffer_sizes();
    return Status::Ok();
  }

  // Compute an initial boundary for the copy. Also generate a set of the
  // ResultTile pointers to use later. Tiles in tmp_result_tiles should be
  // unique and come in the same order as in the result cell slabs to work
  // with copy_attribute_values.
  auto it = read_state_.result_cell_slabs_.begin();
  std::unordered_set<ResultTile*> result_tiles_set;
  std::vector<ResultTile*> tmp_result_tiles;
  copy_end_.first = 0;
  while (it != read_state_.result_cell_slabs_.end()) {
    // Add the ResultTile* to our list if it's not in there already.
    if (result_tiles_set.find(it->tile_) == result_tiles_set.end()) {
      result_tiles_set.emplace(it->tile_);
      tmp_result_tiles.push_back(it->tile_);
    }

    if (it->length_ > num_cells) {
      copy_end_.first++;
      copy_end_.second = num_cells;
      break;
    } else {
      copy_end_.first++;
      num_cells -= it->length_;
      it++;
    }
  }

  if (it == read_state_.result_cell_slabs_.end()) {
    auto& last_rcs = read_state_.result_cell_slabs_.back();
    copy_end_.second = last_rcs.length_;
  }

  // No longer needed.
  result_tiles_set.clear();

  // TODO Whenever a buffer overflows in copy, move it to the front of the
  //      list, this way we will prevent reading tiles we don't need on
  //      future reads.

  if (coords_loaded_) {
    // Copy the coordinates data.
    RETURN_NOT_OK(
        copy_coordinates(&tmp_result_tiles, &read_state_.result_cell_slabs_));

    // copy_coordinates will only have an unrecoverable overflow if a single
    // cell is too big for the user's buffers.
    if (copy_overflowed_) {
      zero_out_buffer_sizes();
      return Status::Ok();
    }
  }

  // Calculate memory budget. For array data, copy_attribute_values might load
  // more tile offsets so use the max budget.
  uint64_t memory_budget_copy =
      memory_budget_ - memory_used_for_coords_total_ - memory_used_qc_tiles_ -
      memory_used_rcs_ - memory_used_result_tiles_ -
      memory_used_result_tile_ranges_ -
      memory_budget_ratio_array_data_ * memory_budget_;

  // Copy the attributes data.
  RETURN_NOT_OK(copy_attribute_values(
      UINT64_MAX,
      &tmp_result_tiles,
      &read_state_.result_cell_slabs_,
      subarray_,
      memory_budget_copy,
      !coords_loaded_));

  // copy_coordinates will only have an unrecoverable overflow if a single cell
  // is too big for the user's buffers.
  if (copy_overflowed_) {
    zero_out_buffer_sizes();
    return Status::Ok();
  }

  // Fix the output buffer sizes.
  RETURN_NOT_OK(resize_output_buffers());

  // End the iteration.
  RETURN_NOT_OK(end_iteration());

  return Status::Ok();
}

void SparseGlobalOrderReader::reset() {
}

Status SparseGlobalOrderReader::clear_result_tiles() {
  if (!result_tiles_.empty()) {
    for (unsigned f = 0; f < fragment_metadata_.size(); f++) {
      while (!result_tiles_[f].empty()) {
        RETURN_NOT_OK(remove_result_tile(f, result_tiles_[f].begin()));
      }
    }
  }

  coords_loaded_ = false;

  return Status::Ok();
}

ResultTile* SparseGlobalOrderReader::add_result_tile_unsafe(
    unsigned dim_num, unsigned f, uint64_t t, const Domain* domain) {
  bool unused;
  add_result_tile(
      dim_num,
      std::numeric_limits<uint64_t>::max(),
      std::numeric_limits<uint64_t>::max(),
      f,
      t,
      domain,
      &unused);
  return &result_tiles_[f].back();
}

Status SparseGlobalOrderReader::add_result_tile(
    unsigned dim_num,
    uint64_t memory_budget_result_tiles,
    uint64_t memory_budget_coords_tiles,
    unsigned f,
    uint64_t t,
    const Domain* domain,
    bool* budget_exceeded) {
  // Calculate memory consumption for this tile.
  uint64_t tiles_size = 0;
  RETURN_NOT_OK(get_coord_tiles_size(dim_num, f, t, &tiles_size));

  // Don't load more tiles than the memory budget.
  if (memory_used_for_coords_[f] + tiles_size > memory_budget_coords_tiles) {
    *budget_exceeded = true;
    return Status::Ok();
  }

  memory_used_for_coords_[f] += tiles_size;
  result_tiles_[f].emplace_back(f, t, domain);

  std::unique_lock<std::mutex> lck(mem_budget_mtx_);
  memory_used_for_coords_total_ += tiles_size;
  memory_used_result_tiles_ += sizeof(ResultTile);
  if (memory_used_result_tiles_ > memory_budget_result_tiles) {
    *budget_exceeded = true;
  }

  return Status::Ok();
}

Status SparseGlobalOrderReader::create_result_tiles(bool* tiles_found) {
  auto timer_se = stats_->start_timer("create_result_tiles");

  // For easy reference.
  auto fragment_num = fragment_metadata_.size();
  auto domain = array_schema_->domain();
  auto dim_num = array_schema_->dim_num();

  // Get the number of fragments to process.
  unsigned num_fragments_to_process = 0;
  for (auto all_loaded : all_tiles_loaded_)
    num_fragments_to_process += !all_loaded;

  per_fragment_memory_ =
      memory_budget_ * memory_budget_ratio_coords_ / num_fragments_to_process;

  uint64_t memory_budget_result_tiles =
      memory_budget_ * memory_budget_ratio_result_tiles_;

  // Create result tiles.
  if (subarray_.is_set()) {
    // Load as many tiles as the memory budget allows.
    auto status = parallel_for(
        storage_manager_->compute_tp(), 0, fragment_num, [&](uint64_t f) {
          auto range_it = result_tile_ranges_[f].begin();
          uint64_t t = 0;
          bool budget_exceeded = false;
          while (range_it != result_tile_ranges_[f].end()) {
            // Figure out the start index.
            auto start = range_it->first;
            if (!result_tiles_[f].empty()) {
              start = std::max(start, result_tiles_[f].back().tile_idx() + 1);
            }

            for (t = start; t <= range_it->second; t++) {
              RETURN_NOT_OK(add_result_tile(
                  dim_num,
                  memory_budget_result_tiles,
                  per_fragment_memory_,
                  f,
                  t,
                  domain,
                  &budget_exceeded));
              *tiles_found = true;

              if (budget_exceeded)
                break;
            }

            if (budget_exceeded) {
              if (result_tiles_[f].empty())
                return LOG_STATUS(Status::SparseGlobalOrderReaderError(
                    "Cannot load a single tile for fragment, increase memory "
                    "budget"));
              break;
            }
            range_it++;
          }

          all_tiles_loaded_[f] = !budget_exceeded;
          return Status::Ok();
        });
  } else {
    // Load as many tiles as the memory budget allows.
    auto status = parallel_for(
        storage_manager_->compute_tp(), 0, fragment_num, [&](uint64_t f) {
          uint64_t t = 0;
          auto tile_num = fragment_metadata_[f]->tile_num();
          bool budget_exceeded = false;

          // Figure out the start index.
          auto start = read_state_.frag_tile_idx_[f].first;
          if (!result_tiles_[f].empty()) {
            start = std::max(start, result_tiles_[f].back().tile_idx() + 1);
          }

          for (t = start; t < tile_num; t++) {
            RETURN_NOT_OK(add_result_tile(
                dim_num,
                memory_budget_result_tiles,
                per_fragment_memory_,
                f,
                t,
                domain,
                &budget_exceeded));
            *tiles_found = true;

            if (budget_exceeded) {
              if (result_tiles_[f].empty())
                return LOG_STATUS(Status::SparseGlobalOrderReaderError(
                    "Cannot load a single tile for fragment, increase memory "
                    "budget"));
              break;
            }
          }

          all_tiles_loaded_[f] = !budget_exceeded;
          return Status::Ok();
        });
  }

  bool done_adding_result_tiles = true;
  for (unsigned int f = 0; f < fragment_num; f++) {
    done_adding_result_tiles &= all_tiles_loaded_[f];
  }

  done_adding_result_tiles_ = done_adding_result_tiles;
  return Status::Ok();
}

Status SparseGlobalOrderReader::compute_result_cell_slab() {
  auto timer_se = stats_->start_timer("compute_result_cell_slab");

  // Create the result tiles we are going to process.
  bool tiles_found = false;
  RETURN_NOT_OK(create_result_tiles(&tiles_found));

  // No tiles found, return.
  if (!tiles_found) {
    return Status::Ok();
  }

  coords_loaded_ = true;

  // Maintain a temporary vector with pointers to result tiles, so that
  // `read_tiles`, `unfilter_tiles` can work without changes.
  std::vector<ResultTile*> tmp_result_tiles;
  for (auto& frag_result_tile : result_tiles_)
    for (auto& result_tile : frag_result_tile)
      tmp_result_tiles.push_back(&result_tile);

  // Read and unfilter zipped coordinate tiles. Note that
  // this will ignore fragments with a version >= 5.
  std::vector<std::string> zipped_coords_names = {constants::coords};
  RETURN_CANCEL_OR_ERROR(
      read_coordinate_tiles(&zipped_coords_names, &tmp_result_tiles));
  RETURN_CANCEL_OR_ERROR(unfilter_tiles(constants::coords, &tmp_result_tiles));

  // Read and unfilter unzipped coordinate tiles. Note that
  // this will ignore fragments with a version < 5.
  RETURN_CANCEL_OR_ERROR(read_coordinate_tiles(&dim_names_, &tmp_result_tiles));
  for (const auto& dim_name : dim_names_) {
    RETURN_CANCEL_OR_ERROR(unfilter_tiles(dim_name, &tmp_result_tiles));
  }

  // Compute the result cell slabs with the loaded coordinate tiles.
  uint64_t memory_budget_rcs = memory_budget_ratio_rcs_ * memory_budget_;
  if (array_schema_->cell_order() == Layout::HILBERT) {
    // Account for this in memory budget.
    std::vector<std::vector<uint64_t>> values(fragment_metadata_.size());
    RETURN_CANCEL_OR_ERROR(merge_result_cell_slabs(
        memory_budget_rcs,
        HilbertCmpReverse(array_schema_->domain(), &values)));
  } else {
    RETURN_CANCEL_OR_ERROR(merge_result_cell_slabs(
        memory_budget_rcs, GlobalCmpReverse(array_schema_->domain())));
  }

  // TODO This can be moved before calculating result cell slabs.
  // Finally apply the query condition.
  uint64_t memory_budget_tiles =
      memory_budget_ * memory_budget_ratio_query_condition_;

  memory_budget_rcs -= memory_used_rcs_;
  uint64_t memory_used_tiles = 0;
  RETURN_CANCEL_OR_ERROR(apply_query_condition(
      &read_state_.result_cell_slabs_,
      &tmp_result_tiles,
      &subarray_,
      std::numeric_limits<uint64_t>::max(),
      memory_budget_rcs,
      memory_budget_tiles,
      &memory_used_tiles));
  memory_used_rcs_ =
      read_state_.result_cell_slabs_.size() * sizeof(ResultCellSlab);
  memory_used_qc_tiles_ += memory_used_tiles;

  return Status::Ok();
}

template <class T>
Status SparseGlobalOrderReader::add_next_tile_to_queue(
    bool subarray_set,
    unsigned int frag_idx,
    uint64_t cell_idx,
    std::vector<std::list<ResultTile>::iterator>& result_tiles_it,
    std::vector<bool>& result_tile_used,
    std::vector<std::vector<uint8_t>>& coord_tiles_result_bitmap,
    std::priority_queue<ResultCoords, std::vector<ResultCoords>, T>& tile_queue,
    std::mutex& tile_queue_mutex,
    T& cmp,
    bool* need_more_tiles) {
  bool found = false;

  if (!result_tile_used[frag_idx]) {
    auto to_delete = result_tiles_it[frag_idx];
    to_delete--;
    remove_result_tile(frag_idx, to_delete);
  }

  while (!found && result_tiles_it[frag_idx] != result_tiles_[frag_idx].end()) {
    found = !subarray_set;
    auto tile = &*result_tiles_it[frag_idx];

    // Calculate the bitmap for the cells.
    RETURN_NOT_OK(compute_coord_tiles_result_bitmap(
        subarray_set, tile, &coord_tiles_result_bitmap[frag_idx]));

    // Calculate hilbert values, this is templated out for non hilbert code.
    RETURN_NOT_OK(calculate_hilbert_values(
        subarray_set, tile, coord_tiles_result_bitmap, cmp));

    // Find a cell that's in the subarray.
    if (subarray_set) {
      while (cell_idx < tile->cell_num()) {
        if (coord_tiles_result_bitmap[frag_idx][cell_idx]) {
          found = true;
          break;
        }

        cell_idx++;
      }
    }

    // There was more tiles in this fragment, insert it in the queue.
    if (found) {
      read_state_.frag_tile_idx_[tile->frag_idx()] =
          std::pair<uint64_t, uint64_t>(tile->tile_idx(), cell_idx);

      if (subarray_set) {
        // Adjust result tile ranges.
        while (!result_tile_ranges_[frag_idx].empty() &&
               tile->tile_idx() >
                   result_tile_ranges_[frag_idx].front().second) {
          result_tile_ranges_[frag_idx].pop_front();
          memory_used_result_tile_ranges_ -= 2 * sizeof(uint64_t);
        }
        result_tile_ranges_[frag_idx].front().first = tile->tile_idx();
      }

      std::unique_lock<std::mutex> ul(tile_queue_mutex);
      tile_queue.emplace(tile, cell_idx);
      result_tiles_it[frag_idx]++;
    } else {
      auto to_delete = result_tiles_it[frag_idx];
      result_tiles_it[frag_idx]++;
      remove_result_tile(frag_idx, to_delete);
    }

    result_tile_used[frag_idx] = false;

    // Once we move to the next tile, the saved cell index doesn't matter.
    cell_idx = 0;
  }

  if (!found) {
    // This fragment has more tiles potentially.
    if (!all_tiles_loaded_[frag_idx]) {
      // Try to find a next tile.
      read_state_.frag_tile_idx_[frag_idx].second = 0;
      if (subarray_.is_set()) {
        // Look in the result tile ranges to find a tile.
        if (!result_tile_ranges_[frag_idx].empty()) {
          auto& first_range = result_tile_ranges_[frag_idx].front();
          if (first_range.first == first_range.second) {
            result_tile_ranges_[frag_idx].pop_front();
            memory_used_result_tile_ranges_ -= 2 * sizeof(uint64_t);
          } else {
            first_range.first++;
          }

          read_state_.frag_tile_idx_[frag_idx].first =
              result_tile_ranges_[frag_idx].front().first;

          *need_more_tiles = true;
        }
      } else {
        // Set the next tile and return we need more tiles.
        read_state_.frag_tile_idx_[frag_idx].first++;
        *need_more_tiles = true;
      }

      // If there are no more tiles in this fragment, set the bool.
      all_tiles_loaded_[frag_idx] = !*need_more_tiles;
    } else {
      if (subarray_.is_set()) {
        result_tile_ranges_[frag_idx].clear();
      }
      read_state_.frag_tile_idx_[frag_idx] = std::pair<uint64_t, uint64_t>(
          fragment_metadata_[frag_idx]->tile_num(), 0);
    }
  }

  return Status::Ok();
}

// No Hilbert values for global order.
template <>
Status SparseGlobalOrderReader::calculate_hilbert_values<GlobalCmpReverse>(
    bool, ResultTile*, std::vector<std::vector<uint8_t>>&, GlobalCmpReverse&) {
  return Status::Ok();
}

template <>
Status SparseGlobalOrderReader::calculate_hilbert_values<HilbertCmpReverse>(
    bool subarray_set,
    ResultTile* tile,
    std::vector<std::vector<uint8_t>>& coord_tiles_result_bitmap,
    HilbertCmpReverse& cmp) {
  auto timer_se = stats_->start_timer("calculate_hilbert_values");

  // For easy reference.
  auto dim_num = array_schema_->dim_num();
  auto cell_num = tile->cell_num();
  auto frag_idx = tile->frag_idx();

  Hilbert h(dim_num);
  auto bits = h.bits();
  auto max_bucket_val = ((uint64_t)1 << bits) - 1;

  std::vector<uint64_t> hilbert_values(cell_num);

  // Calculate Hilbert values in parallel
  auto status = parallel_for(
      storage_manager_->compute_tp(), 0, cell_num, [&](uint64_t c) {
        // Process only values in subarray, if set.
        if (!subarray_set || coord_tiles_result_bitmap[frag_idx][c]) {
          // Compute Hilbert number for all dimensions first.
          std::vector<uint64_t> coords(dim_num);
          for (uint32_t d = 0; d < dim_num; ++d) {
            auto dim = array_schema_->dimension(d);
            auto rc = ResultCoords(tile, c);
            coords[d] = dim->map_to_uint64(rc, d, bits, max_bucket_val);
          }

          // Now we are ready to get the final number.
          hilbert_values[c] = h.coords_to_hilbert(&coords[0]);
        }
        return Status::Ok();
      });
  RETURN_NOT_OK_ELSE(status, LOG_STATUS(status));

  // Set the values in the comparator.
  cmp.set_values_for_fragment(frag_idx, hilbert_values);

  return Status::Ok();
}

template <class T>
Status SparseGlobalOrderReader::merge_result_cell_slabs(
    uint64_t memory_budget, T cmp) {
  auto timer_se = stats_->start_timer("merge_result_cell_slabs");

  // For easy reference.
  bool subarray_set = subarray_.is_set();
  auto allows_dups = array_schema_->allows_dups();

  // A tile min heap, contains one Result coords per fragment.
  std::vector<ResultCoords> container;
  container.reserve(result_tiles_.size());
  std::priority_queue<ResultCoords, std::vector<ResultCoords>, T> tile_queue(
      cmp, std::move(container));

  std::mutex tile_queue_mutex;

  // If any fragments needs to load more tiles.
  bool need_more_tiles = false;

  // Tile iterators, per fragments.
  std::vector<std::list<ResultTile>::iterator> result_tiles_it(
      result_tiles_.size());

  // Boolean per fragment that keeps track of when a tile is used.
  std::vector<bool> result_tile_used(result_tiles_.size(), true);

  // Bitmaps for the current tile, per fragment for the cells to process.
  std::vector<std::vector<uint8_t>> coord_tiles_result_bitmap(
      result_tiles_.size());

  // For all fragments, get the first tile.
  auto status = parallel_for(
      storage_manager_->compute_tp(), 0, result_tiles_.size(), [&](uint64_t f) {
        if (result_tiles_[f].size() > 0) {
          // Initialize the iterator for this fragment.
          result_tiles_it[f] = result_tiles_[f].begin();

          // Get the cell index we were processing.
          auto cell_idx = read_state_.frag_tile_idx_[f].second;

          // Add the tile to the queue.
          RETURN_NOT_OK(add_next_tile_to_queue(
              subarray_set,
              f,
              cell_idx,
              result_tiles_it,
              result_tile_used,
              coord_tiles_result_bitmap,
              tile_queue,
              tile_queue_mutex,
              cmp,
              &need_more_tiles));
        }

        return Status::Ok();
      });
  RETURN_NOT_OK_ELSE(status, LOG_STATUS(status));

  // Process all elements.
  while (!tile_queue.empty() && !need_more_tiles) {
    auto to_process = tile_queue.top();
    tile_queue.pop();

    // Process all cells with the same coordinates at once.
    while (!tile_queue.empty() && to_process.same_coords(tile_queue.top())) {
      // Potentially the next cell.
      auto next_tile = tile_queue.top();
      tile_queue.pop();

      // Take the cell with the highest fagment index.
      if (to_process.tile_->frag_idx() < next_tile.tile_->frag_idx()) {
        std::swap(to_process, next_tile);
      }

      // If we allow duplicates, create one slab for all the dups.
      if (allows_dups) {
        result_tile_used[next_tile.tile_->frag_idx()] = true;
        read_state_.result_cell_slabs_.emplace_back(
            next_tile.tile_, next_tile.pos_, 1);
        memory_used_rcs_ += sizeof(ResultCellSlab);
      }

      // Put the next cell in the queue.
      if (!next_tile.next()) {
        // Done with this tile, fetch another.
        RETURN_NOT_OK(add_next_tile_to_queue(
            subarray_set,
            next_tile.tile_->frag_idx(),
            0,
            result_tiles_it,
            result_tile_used,
            coord_tiles_result_bitmap,
            tile_queue,
            tile_queue_mutex,
            cmp,
            &need_more_tiles));
      } else {
        // Put the next cell on the queue again to be resorted.
        read_state_.frag_tile_idx_[next_tile.tile_->frag_idx()] =
            std::pair<uint64_t, uint64_t>(
                next_tile.tile_->tile_idx(), next_tile.pos_);
        tile_queue.emplace(std::move(next_tile));
      }
    }

    result_tile_used[to_process.tile_->frag_idx()] = true;

    // Find how many cells to process using the top of the queue.
    auto& next_tile = tile_queue.top();

    // Temp result coord used to find the last position.
    ResultCoords temp_rc = to_process;

    // Check the top of the queue against last cell of the current tile.
    temp_rc.pos_ = to_process.tile_->cell_num() - 1;

    // If there is more than one fragment and we can't add the whole tile,
    // find the last possible cell in this tile smaller than the top of the
    // queue. Otherwise we are adding the whole tile.
    if (!tile_queue.empty() && cmp(temp_rc, next_tile)) {
      // TODO Parallelize.

      // Run a bisection seach on to find the last cell.
      uint64_t left = to_process.pos_;
      uint64_t right = temp_rc.pos_;
      while (left != right - 1) {
        // Check against mid.
        temp_rc.pos_ = left + (right - left) / 2;

        if (!cmp(temp_rc, next_tile))
          left = temp_rc.pos_;
        else
          right = temp_rc.pos_;
      }

      // Left is the last position smaller than the top of the queue.
      temp_rc.pos_ = left;
    }

    // Generate the result cell slabs.
    auto start = to_process.pos_;
    const auto& tile = to_process.tile_;

    // If no subarray is set, add all cells.
    if (!subarray_set) {
      auto length = temp_rc.pos_ - to_process.pos_ + 1;
      read_state_.result_cell_slabs_.emplace_back(tile, start, length);
      memory_used_rcs_ += sizeof(ResultCellSlab);
    } else {
      // Process all cells, when there is a "hole" in the cell contiguity,
      // push a new cell slab.
      uint64_t length = 0;
      for (auto c = to_process.pos_; c <= temp_rc.pos_; c++) {
        if (!coord_tiles_result_bitmap[tile->frag_idx()][c]) {
          if (length != 0) {
            read_state_.result_cell_slabs_.emplace_back(tile, start, length);
            memory_used_rcs_ += sizeof(ResultCellSlab);
            start = c + 1;
            length = 0;
          }
        } else {
          length++;
        }
      }

      // Add the last cell slab.
      if (length != 0) {
        read_state_.result_cell_slabs_.emplace_back(tile, start, length);
        memory_used_rcs_ += sizeof(ResultCellSlab);
      }
    }

    // Update the position in the tile.
    to_process.pos_ = temp_rc.pos_;

    // Put the next cell in the queue.
    if (!to_process.next()) {
      // Done with this tile, fetch another.
      RETURN_NOT_OK(add_next_tile_to_queue(
          subarray_set,
          tile->frag_idx(),
          0,
          result_tiles_it,
          result_tile_used,
          coord_tiles_result_bitmap,
          tile_queue,
          tile_queue_mutex,
          cmp,
          &need_more_tiles));
    } else {
      // Put the next cell on the queue to be resorted.
      read_state_.frag_tile_idx_[tile->frag_idx()] =
          std::pair<uint64_t, uint64_t>(tile->tile_idx(), to_process.pos_);
      tile_queue.emplace(std::move(to_process));
    }

    // If we busted our memory budget, exit.
    if (memory_used_rcs_ >= memory_budget)
      break;
  }

  return Status::Ok();
};

Status SparseGlobalOrderReader::remove_result_tile(
    unsigned frag_idx, std::list<ResultTile>::iterator rt) {
  // Remove coord tile size from memory budget.
  auto tile_idx = rt->tile_idx();
  uint64_t tiles_size = 0;
  RETURN_NOT_OK(get_coord_tiles_size(
      array_schema_->dim_num(), frag_idx, tile_idx, &tiles_size));
  memory_used_for_coords_[frag_idx] -= tiles_size;

  for (const auto& name : condition_.field_names()) {
    uint64_t tile_size = 0;
    RETURN_NOT_OK(get_attribute_tile_size(name, &*rt, &tile_size));
    memory_used_qc_tiles_ -= tile_size;
  }

  // Delete the tile.
  result_tiles_[frag_idx].erase(rt);

  std::unique_lock<std::mutex> lck(mem_budget_mtx_);
  memory_used_result_tiles_ -= sizeof(ResultTile);
  memory_used_for_coords_total_ -= tiles_size;

  return Status::Ok();
}

Status SparseGlobalOrderReader::end_iteration() {
  // For easy reference.
  auto fragment_num = fragment_metadata_.size();

  // Remove the processed cell slabs.
  auto& new_front = read_state_.result_cell_slabs_[copy_end_.first - 1];

  // If the last cell slab processed wasn't processed fully, split it.
  if (new_front.length_ != copy_end_.second) {
    new_front.start_ += copy_end_.second;
    new_front.length_ -= copy_end_.second;
    copy_end_.first--;
  }

  // Clear result tiles that are not necessary anymore.
  auto cs_to_del_end = read_state_.result_cell_slabs_.begin() + copy_end_.first;

  // Last tile processed, initialized to the first tile in each fragments.
  std::vector<uint64_t> last_tile_processed(fragment_num);
  for (unsigned frag_idx = 0; frag_idx < fragment_num; frag_idx++) {
    if (!result_tiles_[frag_idx].empty())
      last_tile_processed[frag_idx] =
          result_tiles_[frag_idx].front().tile_idx();
  }

  // Record the last tile seen for each fragments in the slabs.
  for (auto it = read_state_.result_cell_slabs_.begin(); it < cs_to_del_end;
       it++) {
    last_tile_processed[it->tile_->frag_idx()] = it->tile_->tile_idx();
  }

  // Clear the tiles in each fragments until the front is the last seen.
  auto status = parallel_for(
      storage_manager_->compute_tp(), 0, fragment_num, [&](uint64_t f) {
        if (!result_tiles_[f].empty()) {
          while (result_tiles_[f].front().tile_idx() !=
                 last_tile_processed[f]) {
            RETURN_NOT_OK(remove_result_tile(f, result_tiles_[f].begin()));
          }
        }

        return Status::Ok();
      });

  // Erase from the vector.
  read_state_.result_cell_slabs_.erase(
      read_state_.result_cell_slabs_.begin(),
      read_state_.result_cell_slabs_.begin() + copy_end_.first);

  auto uint64_t_max = std::numeric_limits<uint64_t>::max();
  copy_end_ = std::pair<uint64_t, uint64_t>(uint64_t_max, uint64_t_max);

  // Make sure to clear all processed result tiles if we are done with the
  // result cell slabs.
  if (read_state_.result_cell_slabs_.empty()) {
    for (unsigned frag_idx = 0; frag_idx < fragment_num; frag_idx++) {
      if (!result_tiles_[frag_idx].empty()) {
        if (result_tiles_[frag_idx].front().tile_idx() <
            read_state_.frag_tile_idx_[frag_idx].first) {
          RETURN_NOT_OK(
              remove_result_tile(frag_idx, result_tiles_[frag_idx].begin()));
        }
      }
    }
  }

  array_memory_tracker_->set_budget(std::numeric_limits<uint64_t>::max());
  return Status::Ok();
}

}  // namespace sm
}  // namespace tiledb
