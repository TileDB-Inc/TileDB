/**
 * @file   sparse_unordered_with_dups_reader.cc
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
 * This file implements class SparseUnorderedWithDupsReader.
 */

#include "tiledb/sm/query/sparse_unordered_with_dups_reader.h"
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

SparseUnorderedWithDupsReader::SparseUnorderedWithDupsReader(
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

bool SparseUnorderedWithDupsReader::incomplete() const {
  bool last_range =
      !subarray_.is_set() || read_state_.range_idx_ == subarray_.range_num();
  return copy_overflowed_ || !read_state_.result_cell_slabs_.empty() ||
         !done_adding_result_tiles_ || !last_range;
}

Status SparseUnorderedWithDupsReader::init() {
  // Sanity checks
  if (storage_manager_ == nullptr)
    return LOG_STATUS(Status::SparseUnorderedWithDupsReaderError(
        "Cannot initialize sparse global order reader; Storage manager not "
        "set"));
  if (array_schema_ == nullptr)
    return LOG_STATUS(Status::SparseUnorderedWithDupsReaderError(
        "Cannot initialize sparse global order reader; Array schema not set"));
  if (buffers_.empty())
    return LOG_STATUS(Status::SparseUnorderedWithDupsReaderError(
        "Cannot initialize sparse global order reader; Buffers not set"));

  // Check subarray
  RETURN_NOT_OK(check_subarray());

  // Load offset configuration options.
  bool found = false;
  offsets_format_mode_ = config_.get("sm.var_offsets.mode", &found);
  assert(found);
  if (offsets_format_mode_ != "bytes" && offsets_format_mode_ != "elements") {
    return LOG_STATUS(
        Status::ReaderError("Cannot initialize reader; Unsupported offsets "
                            "format in configuration"));
  }
  RETURN_NOT_OK(config_.get<bool>(
      "sm.var_offsets.extra_element", &offsets_extra_element_, &found));
  assert(found);
  RETURN_NOT_OK(config_.get<uint32_t>(
      "sm.var_offsets.bitsize", &offsets_bitsize_, &found));
  if (offsets_bitsize_ != 32 && offsets_bitsize_ != 64) {
    return LOG_STATUS(Status::SparseUnorderedWithDupsReaderError(
        "Cannot initialize reader; "
        "Unsupported offsets bitsize in configuration"));
  }
  assert(found);
  RETURN_NOT_OK(
      config_.get<uint64_t>("sm.mem.total_budget", &memory_budget_, &found));
  assert(found);
  RETURN_NOT_OK(config_.get<double>(
      "sm.mem.reader.sparse_unordered_with_dups.ratio_array_data",
      &memory_budget_ratio_array_data_,
      &found));
  assert(found);
  RETURN_NOT_OK(config_.get<double>(
      "sm.mem.reader.sparse_unordered_with_dups.ratio_coords",
      &memory_budget_ratio_coords_,
      &found));
  assert(found);
  RETURN_NOT_OK(config_.get<double>(
      "sm.mem.reader.sparse_unordered_with_dups.ratio_query_condition",
      &memory_budget_ratio_query_condition_,
      &found));
  assert(found);
  RETURN_NOT_OK(config_.get<double>(
      "sm.mem.reader.sparse_unordered_with_dups.ratio_tile_ranges",
      &memory_budget_ratio_tile_ranges_,
      &found));
  assert(found);
  RETURN_NOT_OK(config_.get<double>(
      "sm.mem.reader.sparse_unordered_with_dups.ratio_result_tiles",
      &memory_budget_ratio_result_tiles_,
      &found));
  assert(found);
  RETURN_NOT_OK(config_.get<double>(
      "sm.mem.reader.sparse_unordered_with_dups.ratio_rcs",
      &memory_budget_ratio_rcs_,
      &found));
  assert(found);

  // Check the validity buffer sizes.
  RETURN_NOT_OK(check_validity_buffer_sizes());

  return Status::Ok();
}

Status SparseUnorderedWithDupsReader::dowork() {
  auto timer_se = stats_->start_timer("dowork");

  // For easy reference.
  bool subarray_set = subarray_.is_set();

  // Check that the query condition is valid.
  RETURN_NOT_OK(condition_.check(array_schema_));

  get_dim_attr_stats();

  // Reset the copy overflow flag.
  copy_overflowed_ = false;

  // Handle empty array.
  if (fragment_metadata_.empty()) {
    done_adding_result_tiles_ = true;
    read_state_.range_idx_ = subarray_set ? subarray_.range_num() : 1;
    zero_out_buffer_sizes();
    return Status::Ok();
  }

  reset_buffer_sizes();

  // Load initial data, if not loaded already.
  RETURN_NOT_OK(load_initial_data());

  // If the result cell slab is empty, populate it.
  if (read_state_.result_cell_slabs_.empty())
    RETURN_NOT_OK(compute_result_cell_slab());

  // No more tiles to process, done.
  if (read_state_.result_cell_slabs_.empty()) {
    done_adding_result_tiles_ = true;
    read_state_.range_idx_ += range_num_;
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
      memory_budget_ - memory_used_qc_tiles_ - memory_used_rcs_ -
      memory_used_result_tiles_ - memory_used_result_tile_ranges_ -
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

  // Possibly move to the next range.
  if (subarray_set && read_state_.result_cell_slabs_.empty() &&
      done_adding_result_tiles_) {
    read_state_.range_idx_ += range_num_;
    initial_data_loaded_ = false;
  }

  return Status::Ok();
}

void SparseUnorderedWithDupsReader::reset() {
}

Status SparseUnorderedWithDupsReader::clear_result_tiles() {
  while (!result_tiles_.empty()) {
    auto f = result_tiles_.front().frag_idx();
    RETURN_NOT_OK(remove_result_tile(f, result_tiles_.begin()));
  }

  coords_loaded_ = false;

  return Status::Ok();
}

ResultTile* SparseUnorderedWithDupsReader::add_result_tile_unsafe(
    unsigned dim_num, unsigned f, uint64_t t, const Domain* domain) {
  bool unused;
  add_result_tile(
      dim_num,
      std::numeric_limits<uint64_t>::max(),
      std::numeric_limits<uint64_t>::max(),
      std::numeric_limits<uint64_t>::max(),
      f,
      t,
      domain,
      &unused);
  return &result_tiles_.back();
}

Status SparseUnorderedWithDupsReader::add_result_tile(
    unsigned dim_num,
    uint64_t memory_budget_result_tiles,
    uint64_t memory_budget_qc_tiles,
    uint64_t memory_budget_coords_tiles,
    unsigned f,
    uint64_t t,
    const Domain* domain,
    bool* budget_exceeded) {
  // Calculate memory consumption for this tile.
  uint64_t tiles_size = 0;
  RETURN_NOT_OK(get_coord_tiles_size(dim_num, f, t, &tiles_size));

  // Don't load more tiles than the memory budget.
  if (memory_used_for_coords_total_ + tiles_size > memory_budget_coords_tiles) {
    *budget_exceeded = true;
    return Status::Ok();
  }

  memory_used_for_coords_total_ += tiles_size;

  result_tiles_.emplace_back(f, t, domain);

  if (!condition_.empty()) {
    uint64_t tiles_size = 0;
    for (auto& name : condition_.field_names()) {
      // Calculate memory consumption for this tile.
      uint64_t tile_size = 0;
      RETURN_NOT_OK(
          get_attribute_tile_size(name, &result_tiles_.back(), &tile_size));
      tiles_size += tile_size;
    }

    memory_used_qc_tiles_ += tiles_size;
    if (memory_used_qc_tiles_ > memory_budget_qc_tiles) {
      *budget_exceeded = true;
    }
  }

  memory_used_result_tiles_ += sizeof(ResultTile);
  if (memory_used_result_tiles_ > memory_budget_result_tiles) {
    *budget_exceeded = true;
  }

  return Status::Ok();
}

Status SparseUnorderedWithDupsReader::create_result_tiles(bool* tiles_found) {
  auto timer_se = stats_->start_timer("create_result_tiles");

  // For easy reference.
  auto fragment_num = fragment_metadata_.size();
  auto domain = array_schema_->domain();
  auto dim_num = array_schema_->dim_num();

  if (!condition_.empty()) {
    // To respect the memory budget, we only load as many tiles as we can
    // process for the query condition. Load the tiles offsets first.
    std::vector<std::string> names(condition_.field_names().size());
    for (auto& name : condition_.field_names()) {
      names.emplace_back(name);
    }

    load_tile_offsets(&subarray_, &names);
  }

  uint64_t memory_budget_result_tiles =
      memory_budget_ * memory_budget_ratio_result_tiles_;
  uint64_t memory_budget_qc_tiles =
      memory_budget_ * memory_budget_ratio_query_condition_;
  uint64_t memory_budget_coords = memory_budget_ * memory_budget_ratio_coords_;

  // Create result tiles.
  if (subarray_.is_set()) {
    // Load as many tiles as the memory budget allows.
    bool budget_exceeded = false;
    unsigned int f = 0;
    while (f < fragment_num && !budget_exceeded) {
      auto range_it = result_tile_ranges_[f].rbegin();
      uint64_t t = 0;
      while (range_it != result_tile_ranges_[f].rend()) {
        auto end =
            range_it->second >= NO_OVERLAP ? range_it->first : range_it->second;
        for (t = range_it->first; t <= end; t++) {
          // Figure out the start index.
          auto start = range_it->first;
          if (!result_tiles_.empty() && result_tiles_.back().frag_idx() == f) {
            start = std::max(start, result_tiles_.back().tile_idx() + 1);
          }

          RETURN_NOT_OK(add_result_tile(
              dim_num,
              memory_budget_result_tiles,
              memory_budget_qc_tiles,
              memory_budget_coords,
              f,
              t,
              domain,
              &budget_exceeded));
          *tiles_found = true;

          if (budget_exceeded)
            break;
        }

        if (budget_exceeded)
          break;
        range_it++;
      }

      all_tiles_loaded_[f] = !budget_exceeded;
      f++;
    }
  } else {
    // Load as many tiles as the memory budget allows.
    bool budget_exceeded = false;
    unsigned int f = 0;
    while (f < fragment_num && !budget_exceeded) {
      uint64_t t = 0;
      auto tile_num = fragment_metadata_[f]->tile_num();

      // Figure out the start index.
      auto start = read_state_.frag_tile_idx_[f].first;
      if (!result_tiles_.empty() && result_tiles_.back().frag_idx() == f) {
        start = std::max(start, result_tiles_.back().tile_idx() + 1);
      }

      for (t = start; t < tile_num; t++) {
        RETURN_NOT_OK(add_result_tile(
            dim_num,
            memory_budget_result_tiles,
            memory_budget_qc_tiles,
            memory_budget_coords,
            f,
            t,
            domain,
            &budget_exceeded));
        *tiles_found = true;

        if (budget_exceeded)
          break;
      }

      all_tiles_loaded_[f] = !budget_exceeded;
      f++;
    }
  }

  bool done_adding_result_tiles = true;
  for (unsigned int f = 0; f < fragment_num; f++) {
    done_adding_result_tiles &= all_tiles_loaded_[f];
  }

  done_adding_result_tiles_ = done_adding_result_tiles;
  return Status::Ok();
}

Status SparseUnorderedWithDupsReader::compute_result_cell_slab() {
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
  for (auto& result_tile : result_tiles_)
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
  RETURN_CANCEL_OR_ERROR(create_result_cell_slabs(memory_budget_rcs));

  // TODO This can be moved before calculating result cell slabs.
  // Finally apply the query condition.
  uint64_t memory_budget_tiles =
      memory_budget_ * memory_budget_ratio_query_condition_;
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

Status SparseUnorderedWithDupsReader::create_result_cell_slabs(
    uint64_t memory_budget) {
  auto timer_se = stats_->start_timer("create_result_cell_slabs");

  // For easy reference.
  auto subarray_set = subarray_.is_set();

  for (auto& rt : result_tiles_) {
    // If no subarray is set, add all cells.
    if (!subarray_set) {
      read_state_.result_cell_slabs_.emplace_back(&rt, 0, rt.cell_num());
      memory_used_rcs_ += sizeof(ResultCellSlab);
    } else {
      auto status = parallel_for(
          storage_manager_->compute_tp(), 0, range_num_, [&](uint64_t r) {
            // Figure out what to do with the tile.
            bool compute_bitmap = false;
            bool add_full_tile = false;
            if (range_num_ <= 1) {
              auto& current_range = result_tile_ranges_[rt.frag_idx()].back();
              if (current_range.second == COMPUTE_OVERLAP) {
                compute_bitmap = true;
              } else if (current_range.second != NO_OVERLAP) {
                add_full_tile = true;
              }
            } else {
              if (!range_result_tiles_ranges_[r].empty()) {
                auto& current_range = range_result_tiles_ranges_[r].back();
                if (std::get<0>(current_range) == rt.frag_idx() &&
                    std::get<1>(current_range) == rt.tile_idx()) {
                  if (std::get<2>(current_range) == COMPUTE_OVERLAP) {
                    // The single tile range can be removed.
                    remove_range_result_tile_range(r);

                    compute_bitmap = true;
                  } else if (std::get<2>(current_range) == NO_OVERLAP) {
                    // The single tile range can be removed.
                    remove_range_result_tile_range(r);
                  } else {
                    // The single tile range can be removed.
                    if (std::get<1>(current_range) ==
                        std::get<2>(current_range)) {
                      remove_range_result_tile_range(r);
                    } else {
                      // Move the range to the next tile.
                      std::get<1>(current_range)++;
                    }
                    add_full_tile = true;
                  }
                }
              }
            }

            if (compute_bitmap) {
              // Calculate the bitmap for the cells.
              std::vector<uint8_t> coord_tiles_result_bitmap;
              coord_tiles_result_bitmap.resize(rt.cell_num(), 1);
              RETURN_NOT_OK(compute_coord_tiles_result_bitmap(
                  &rt, r + read_state_.range_idx_, &coord_tiles_result_bitmap));

              // Process all cells, when there is a "hole" in the cell
              // contiguity, push a new cell slab.
              uint64_t start = 0;
              uint64_t length = 0;
              for (uint64_t c = 0; c < rt.cell_num(); c++) {
                if (!coord_tiles_result_bitmap[c]) {
                  if (length != 0) {
                    {
                      std::unique_lock<std::mutex> lck(mem_budget_mtx_);
                      read_state_.result_cell_slabs_.emplace_back(
                          &rt, start, length);
                      memory_used_rcs_ += sizeof(ResultCellSlab);
                    }
                    length = 0;
                  }

                  start = c + 1;
                } else {
                  length++;
                }
              }

              // Add the last cell slab.
              if (length != 0) {
                std::unique_lock<std::mutex> lck(mem_budget_mtx_);
                read_state_.result_cell_slabs_.emplace_back(&rt, start, length);
                memory_used_rcs_ += sizeof(ResultCellSlab);
              }
            }

            if (add_full_tile) {
              // Add all tile.
              read_state_.result_cell_slabs_.emplace_back(
                  &rt, 0, rt.cell_num());
              memory_used_rcs_ += sizeof(ResultCellSlab);
            }

            return Status::Ok();
          });
      RETURN_NOT_OK_ELSE(status, LOG_STATUS(status));

      // Adjust result tile ranges.
      auto& first_range = result_tile_ranges_[rt.frag_idx()].back();
      if (first_range.second == rt.tile_idx()) {
        remove_result_tile_range(rt.frag_idx());
      } else {
        first_range.first = rt.tile_idx() + 1;
      }
    }

    read_state_.frag_tile_idx_[rt.frag_idx()] =
        std::pair<uint64_t, uint64_t>(rt.tile_idx() + 1, 0);

    // If we busted our memory budget, exit.
    if (memory_used_rcs_ >= memory_budget)
      break;
  }

  return Status::Ok();
};

Status SparseUnorderedWithDupsReader::remove_result_tile(
    unsigned frag_idx, std::list<ResultTile>::iterator rt) {
  // Remove coord tile size from memory budget.
  auto tile_idx = rt->tile_idx();
  uint64_t tiles_size = 0;
  RETURN_NOT_OK(get_coord_tiles_size(
      array_schema_->dim_num(), frag_idx, tile_idx, &tiles_size));
  memory_used_for_coords_total_ -= tiles_size;

  for (const auto& name : condition_.field_names()) {
    uint64_t tile_size = 0;
    RETURN_NOT_OK(get_attribute_tile_size(name, &*rt, &tile_size));
    memory_used_qc_tiles_ -= tile_size;
  }

  // Delete the tile.
  result_tiles_.erase(rt);

  {
    std::unique_lock<std::mutex> lck(mem_budget_mtx_);
    memory_used_result_tiles_ -= sizeof(ResultTile);
  }

  return Status::Ok();
}

Status SparseUnorderedWithDupsReader::end_iteration() {
  // Remove the processed cell slabs.
  auto& new_front = read_state_.result_cell_slabs_[copy_end_.first - 1];

  // If the last cell slab processed wasn't processed fully, split it.
  if (new_front.length_ != copy_end_.second) {
    new_front.start_ += copy_end_.second;
    new_front.length_ -= copy_end_.second;
    copy_end_.first--;
  }

  // Clear result tiles that are not necessary anymore.
  while (result_tiles_.front().frag_idx() != new_front.tile_->frag_idx() ||
         result_tiles_.front().tile_idx() != new_front.tile_->tile_idx()) {
    auto f = result_tiles_.front().frag_idx();
    RETURN_NOT_OK(remove_result_tile(f, result_tiles_.begin()));
  }

  // Erase from the vector.
  read_state_.result_cell_slabs_.erase(
      read_state_.result_cell_slabs_.begin(),
      read_state_.result_cell_slabs_.begin() + copy_end_.first);
  memory_used_rcs_ -= copy_end_.first * sizeof(ResultCellSlab);

  // If the result cell slabs are empty, check if we need to remove the last
  // tile.
  auto last_f = result_tiles_.front().frag_idx();
  if (read_state_.result_cell_slabs_.empty() &&
      result_tiles_.front().tile_idx() <
          read_state_.frag_tile_idx_[last_f].first) {
    RETURN_NOT_OK(remove_result_tile(last_f, result_tiles_.begin()));
  }

  auto uint64_t_max = std::numeric_limits<uint64_t>::max();
  copy_end_ = std::pair<uint64_t, uint64_t>(uint64_t_max, uint64_t_max);

  if (offsets_extra_element_) {
    RETURN_NOT_OK(add_extra_offset());
  }

  array_memory_tracker_->set_budget(uint64_t_max);
  return Status::Ok();
}

}  // namespace sm
}  // namespace tiledb