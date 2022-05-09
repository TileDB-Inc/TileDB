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
#include "tiledb/common/logger.h"
#include "tiledb/common/memory_tracker.h"
#include "tiledb/sm/array/array.h"
#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/array_schema/dimension.h"
#include "tiledb/sm/fragment/fragment_metadata.h"
#include "tiledb/sm/misc/comparators.h"
#include "tiledb/sm/misc/hash.h"
#include "tiledb/sm/misc/hilbert.h"
#include "tiledb/sm/misc/parallel_functions.h"
#include "tiledb/sm/misc/utils.h"
#include "tiledb/sm/query/hilbert_order.h"
#include "tiledb/sm/query/query_macros.h"
#include "tiledb/sm/query/result_tile.h"
#include "tiledb/sm/stats/global_stats.h"
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
    shared_ptr<Logger> logger,
    StorageManager* storage_manager,
    Array* array,
    Config& config,
    std::unordered_map<std::string, QueryBuffer>& buffers,
    Subarray& subarray,
    Layout layout,
    QueryCondition& condition)
    : SparseIndexReaderBase(
          stats,
          logger->clone("SparseGlobalOrderReader", ++logger_id_),
          storage_manager,
          array,
          config,
          buffers,
          subarray,
          layout,
          condition)
    , result_tiles_(array->fragment_metadata().size())
    , memory_used_for_coords_(array->fragment_metadata().size())
    , memory_used_for_qc_tiles_(array->fragment_metadata().size()) {
}

/* ****************************** */
/*               API              */
/* ****************************** */

bool SparseGlobalOrderReader::incomplete() const {
  return !read_state_.done_adding_result_tiles_ ||
         memory_used_for_coords_total_ != 0;
}

QueryStatusDetailsReason SparseGlobalOrderReader::status_incomplete_reason()
    const {
  return incomplete() ? QueryStatusDetailsReason::REASON_USER_BUFFER_SIZE :
                        QueryStatusDetailsReason::REASON_NONE;
}

Status SparseGlobalOrderReader::init() {
  RETURN_NOT_OK(SparseIndexReaderBase::init());

  // Initialize memory budget variables.
  RETURN_NOT_OK(initialize_memory_budget());

  return Status::Ok();
}

Status SparseGlobalOrderReader::initialize_memory_budget() {
  bool found = false;
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

  return Status::Ok();
}

Status SparseGlobalOrderReader::dowork() {
  auto timer_se = stats_->start_timer("dowork");

  // For easy reference.
  auto fragment_num = fragment_metadata_.size();

  // Check that the query condition is valid.
  RETURN_NOT_OK(condition_.check(array_schema_));

  get_dim_attr_stats();

  // Start with out buffer sizes as zero.
  zero_out_buffer_sizes();

  // Handle empty array.
  if (fragment_metadata_.empty()) {
    read_state_.done_adding_result_tiles_ = true;
    return Status::Ok();
  }

  // Load initial data, if not loaded already.
  RETURN_NOT_OK(load_initial_data());

  // Attributes names to process.
  std::vector<std::string> names;
  names.reserve(buffers_.size());

  std::vector<tuple<>> buffers;
  for (auto& buffer : buffers_) {
    names.emplace_back(buffer.first);
  }

  buffers_full_ = false;
  do {
    stats_->add_counter("loop_num", 1);

    // Create the result tiles we are going to process.
    auto&& [st, tiles_found] = create_result_tiles();
    RETURN_NOT_OK(st);

    if (*tiles_found) {
      // Maintain a temporary vector with pointers to result tiles for calling
      // read_and_unfilter_coords.
      std::vector<ResultTile*> tmp_result_tiles;
      for (auto& rt_list : result_tiles_) {
        for (auto& result_tile : rt_list) {
          if (!result_tile.coords_loaded_) {
            result_tile.coords_loaded_ = true;
            tmp_result_tiles.emplace_back(&result_tile);
          }
        }
      }

      // Read and unfilter coords.
      RETURN_NOT_OK(read_and_unfilter_coords(true, tmp_result_tiles));

      // Compute the tile bitmaps.
      RETURN_NOT_OK(compute_tile_bitmaps<uint8_t>(tmp_result_tiles));

      // Apply query condition.
      RETURN_NOT_OK(apply_query_condition<uint8_t>(tmp_result_tiles));

      // Clear result tiles that are not necessary anymore.
      auto status = parallel_for(
          storage_manager_->compute_tp(), 0, fragment_num, [&](uint64_t f) {
            auto it = result_tiles_[f].begin();
            while (it != result_tiles_[f].end()) {
              if (it->bitmap_result_num_ == 0) {
                RETURN_NOT_OK(remove_result_tile(f, it++));
              } else {
                it++;
              }
            }

            return Status::Ok();
          });
      RETURN_NOT_OK_ELSE(status, logger_->status(status));

      // Compute hilbert values.
      if (array_schema_.cell_order() == Layout::HILBERT) {
        RETURN_NOT_OK(compute_hilbert_values(tmp_result_tiles));
      }
    }

    // Compute RCS.
    auto&& [st_rcs, result_cell_slabs] = compute_result_cell_slab();
    RETURN_NOT_OK(st_rcs);

    // No more tiles to process, done.
    if (!result_cell_slabs.has_value() || result_cell_slabs->empty()) {
      break;
    }

    // Copy cell slabs.
    if (offsets_bitsize_ == 64) {
      RETURN_NOT_OK(process_slabs<uint64_t>(names, *result_cell_slabs));
    } else {
      RETURN_NOT_OK(process_slabs<uint32_t>(names, *result_cell_slabs));
    }

    // End the iteration.
    RETURN_NOT_OK(end_iteration());
  } while (!buffers_full_ && incomplete());

  // Fix the output buffer sizes.
  RETURN_NOT_OK(resize_output_buffers(cells_copied(names)));

  if (offsets_extra_element_) {
    RETURN_NOT_OK(add_extra_offset());
  }

  return Status::Ok();
}

void SparseGlobalOrderReader::reset() {
}

tuple<Status, optional<bool>> SparseGlobalOrderReader::add_result_tile(
    const unsigned dim_num,
    const uint64_t memory_budget_coords_tiles,
    const uint64_t memory_budget_qc_tiles,
    const unsigned f,
    const uint64_t t,
    const ArraySchema& array_schema) {
  // Calculate memory consumption for this tile.
  auto&& [st, tiles_sizes] = get_coord_tiles_size<uint8_t>(true, dim_num, f, t);
  RETURN_NOT_OK_TUPLE(st, nullopt);
  auto tiles_size = tiles_sizes->first;
  auto tiles_size_qc = tiles_sizes->second;

  // Account for hilbert data.
  if (array_schema_.cell_order() == Layout::HILBERT) {
    tiles_size += fragment_metadata_[f]->cell_num(t) * sizeof(uint64_t);
  }

  // Don't load more tiles than the memory budget.
  if (memory_used_for_coords_[f] + tiles_size > memory_budget_coords_tiles ||
      memory_used_for_qc_tiles_[f] + tiles_size_qc > memory_budget_qc_tiles) {
    return {Status::Ok(), true};
  }

  // Adjust total memory used.
  {
    std::unique_lock<std::mutex> lck(mem_budget_mtx_);
    memory_used_for_coords_total_ += tiles_size + sizeof(ResultTile);
    memory_used_qc_tiles_total_ += tiles_size_qc;
  }

  // Adjust per fragment memory used.
  memory_used_for_coords_[f] += tiles_size + sizeof(ResultTile);
  memory_used_for_qc_tiles_[f] += tiles_size_qc;

  // Add the tile.
  result_tiles_[f].emplace_back(f, t, array_schema);

  return {Status::Ok(), false};
}

tuple<Status, optional<bool>> SparseGlobalOrderReader::create_result_tiles() {
  auto timer_se = stats_->start_timer("create_result_tiles");

  // For easy reference.
  auto fragment_num = fragment_metadata_.size();
  auto dim_num = array_schema_.dim_num();

  // Get the number of fragments to process.
  unsigned num_fragments_to_process = 0;
  for (auto all_loaded : all_tiles_loaded_)
    num_fragments_to_process += !all_loaded;

  per_fragment_memory_ =
      memory_budget_ * memory_budget_ratio_coords_ / num_fragments_to_process;
  per_fragment_qc_memory_ = memory_budget_ *
                            memory_budget_ratio_query_condition_ /
                            num_fragments_to_process;

  // Create result tiles.
  bool tiles_found = false;
  if (subarray_.is_set()) {
    // Load as many tiles as the memory budget allows.
    auto status = parallel_for(
        storage_manager_->compute_tp(), 0, fragment_num, [&](uint64_t f) {
          uint64_t t = 0;
          while (!result_tile_ranges_[f].empty()) {
            auto& range = result_tile_ranges_[f].back();
            for (t = range.first; t <= range.second; t++) {
              auto&& [st, budget_exceeded] = add_result_tile(
                  dim_num,
                  per_fragment_memory_,
                  per_fragment_qc_memory_,
                  f,
                  t,
                  *(fragment_metadata_[f]->array_schema()).get());
              RETURN_NOT_OK(st);
              tiles_found = true;

              if (*budget_exceeded) {
                logger_->debug(
                    "Budget exceeded adding result tiles, fragment {0}, tile "
                    "{1}",
                    f,
                    t);

                if (result_tiles_[f].empty())
                  return logger_->status(Status_SparseGlobalOrderReaderError(
                      "Cannot load a single tile for fragment, increase memory "
                      "budget"));
                return Status::Ok();
              }

              range.first++;
            }

            remove_result_tile_range(f);
          }

          all_tiles_loaded_[f] = true;
          return Status::Ok();
        });
    RETURN_NOT_OK_ELSE_TUPLE(status, logger_->status(status), nullopt);
  } else {
    // Load as many tiles as the memory budget allows.
    auto status = parallel_for(
        storage_manager_->compute_tp(), 0, fragment_num, [&](uint64_t f) {
          uint64_t t = 0;
          auto tile_num = fragment_metadata_[f]->tile_num();

          // Figure out the start index.
          auto start = read_state_.frag_tile_idx_[f].first;
          if (!result_tiles_[f].empty()) {
            start = std::max(start, result_tiles_[f].back().tile_idx() + 1);
          }

          for (t = start; t < tile_num; t++) {
            auto&& [st, budget_exceeded] = add_result_tile(
                dim_num,
                per_fragment_memory_,
                per_fragment_qc_memory_,
                f,
                t,
                *(fragment_metadata_[f]->array_schema()).get());
            RETURN_NOT_OK(st);
            tiles_found = true;

            if (*budget_exceeded) {
              logger_->debug(
                  "Budget exceeded adding result tiles, fragment {0}, tile {1}",
                  f,
                  t);

              if (result_tiles_[f].empty())
                return logger_->status(Status_SparseGlobalOrderReaderError(
                    "Cannot load a single tile for fragment, increase memory "
                    "budget"));
              return Status::Ok();
            }
          }

          all_tiles_loaded_[f] = true;
          return Status::Ok();
        });
    RETURN_NOT_OK_ELSE_TUPLE(status, logger_->status(status), nullopt);
  }

  bool done_adding_result_tiles = true;
  uint64_t num_rt = 0;
  for (unsigned int f = 0; f < fragment_num; f++) {
    num_rt += result_tiles_[f].size();
    done_adding_result_tiles &= all_tiles_loaded_[f] != 0;
  }

  logger_->debug("Done adding result tiles, num result tiles {0}", num_rt);

  if (done_adding_result_tiles) {
    logger_->debug("All result tiles loaded");
  }

  read_state_.done_adding_result_tiles_ = done_adding_result_tiles;
  return {Status::Ok(), tiles_found};
}

tuple<Status, optional<std::vector<ResultCellSlab>>>
SparseGlobalOrderReader::compute_result_cell_slab() {
  auto timer_se = stats_->start_timer("compute_result_cell_slab");

  // First try to limit the maximum number of cells we copy using the size
  // of the output buffers for fixed sized attributes. Later we will validate
  // the memory budget. This is the first line of defence used to try to prevent
  // overflows when copying data.
  uint64_t num_cells = std::numeric_limits<uint64_t>::max();
  for (const auto& it : buffers_) {
    const auto& name = it.first;
    const auto size = it.second.original_buffer_size_ - *it.second.buffer_size_;
    if (array_schema_.var_size(name)) {
      auto temp_num_cells = size / constants::cell_var_offset_size;

      if (offsets_extra_element_ && temp_num_cells > 0)
        temp_num_cells--;

      num_cells = std::min(num_cells, temp_num_cells);
    } else {
      auto temp_num_cells = size / array_schema_.cell_size(name);
      num_cells = std::min(num_cells, temp_num_cells);
    }
  }

  // User gave us some empty buffers, exit.
  if (num_cells == 0) {
    buffers_full_ = true;
    return {Status::Ok(), nullopt};
  }

  if (array_schema_.cell_order() == Layout::HILBERT) {
    return merge_result_cell_slabs(
        num_cells, HilbertCmpReverse(array_schema_.domain()));
  } else {
    return merge_result_cell_slabs(
        num_cells, GlobalCmpReverse(array_schema_.domain()));
  }
}

template <class T>
tuple<Status, optional<bool>> SparseGlobalOrderReader::add_next_tile_to_queue(
    unsigned int frag_idx,
    uint64_t cell_idx,
    std::vector<std::list<ResultTileWithBitmap<uint8_t>>::iterator>&
        result_tiles_it,
    std::vector<uint8_t>& result_tile_used,
    std::priority_queue<ResultCoords, std::vector<ResultCoords>, T>& tile_queue,
    std::mutex& tile_queue_mutex) {
  bool found = false;

  // Remove the tile from result tiles if it wasn't used at all.
  if (!result_tile_used[frag_idx]) {
    auto to_delete = result_tiles_it[frag_idx];
    to_delete--;
    remove_result_tile(frag_idx, to_delete);
  }

  // Try to find a tile.
  while (!found && result_tiles_it[frag_idx] != result_tiles_[frag_idx].end()) {
    bool has_bmp = result_tiles_it[frag_idx]->bitmap_.size() > 0;
    found = !has_bmp;
    auto tile = &*result_tiles_it[frag_idx];

    // Find a cell that's in the subarray.
    if (has_bmp) {
      while (cell_idx < tile->cell_num()) {
        if (tile->bitmap_.size() == 0 || tile->bitmap_[cell_idx]) {
          found = true;
          break;
        }

        cell_idx++;
      }
    }

    // There was more tiles in this fragment, insert it in the queue.
    if (found) {
      std::unique_lock<std::mutex> ul(tile_queue_mutex);
      tile_queue.emplace(tile, cell_idx);
      result_tiles_it[frag_idx]++;
    } else {
      // Remove the tile.
      auto to_delete = result_tiles_it[frag_idx];
      result_tiles_it[frag_idx]++;
      remove_result_tile(frag_idx, to_delete);
    }

    result_tile_used[frag_idx] = false;

    // Once we move to the next tile, the saved cell index doesn't matter.
    cell_idx = 0;
  }

  if (!found) {
    // Increment the tile index, which should clear all tiles in end_iteration.
    if (!result_tiles_[frag_idx].empty()) {
      read_state_.frag_tile_idx_[frag_idx].first++;
      read_state_.frag_tile_idx_[frag_idx].second = 0;
    }

    // This fragment has more tiles potentially.
    if (!all_tiles_loaded_[frag_idx]) {
      // Return we need more tiles.
      return {Status::Ok(), true};
    }
  }

  return {Status::Ok(), false};
}

Status SparseGlobalOrderReader::compute_hilbert_values(
    std::vector<ResultTile*>& result_tiles) {
  auto timer_se = stats_->start_timer("compute_hilbert_values");

  // For easy reference.
  auto dim_num = array_schema_.dim_num();

  // Create a Hilbet class.
  Hilbert h(dim_num);
  auto bits = h.bits();
  auto max_bucket_val = ((uint64_t)1 << bits) - 1;

  // Parallelize on tiles.
  auto status = parallel_for(
      storage_manager_->compute_tp(), 0, result_tiles.size(), [&](uint64_t t) {
        auto tile = (ResultTileWithBitmap<uint8_t>*)result_tiles[t];
        auto cell_num = tile->cell_num();
        auto rc = ResultCoords(tile, 0);
        std::vector<uint64_t> coords(dim_num);

        tile->hilbert_values_.resize(cell_num);
        for (rc.pos_ = 0; rc.pos_ < cell_num; rc.pos_++) {
          // Process only values in bitmap.
          if (tile->bitmap_.size() == 0 || tile->bitmap_[rc.pos_]) {
            // Compute Hilbert number for all dimensions first.
            for (uint32_t d = 0; d < dim_num; ++d) {
              auto dim{array_schema_.dimension_ptr(d)};
              coords[d] = hilbert_order::map_to_uint64(
                  *dim, rc, d, bits, max_bucket_val);
            }

            // Now we are ready to get the final number.
            tile->hilbert_values_[rc.pos_] = h.coords_to_hilbert(&coords[0]);
          }
        }

        return Status::Ok();
      });
  RETURN_NOT_OK_ELSE(status, logger_->status(status));

  return Status::Ok();
}

template <class T>
tuple<Status, optional<std::vector<ResultCellSlab>>>
SparseGlobalOrderReader::merge_result_cell_slabs(uint64_t num_cells, T cmp) {
  auto timer_se = stats_->start_timer("merge_result_cell_slabs");
  std::vector<ResultCellSlab> result_cell_slabs;

  // TODO Parallelize.

  // For easy reference.
  auto allows_dups = array_schema_.allows_dups();

  // A tile min heap, contains one ResultCoords per fragment.
  std::vector<ResultCoords> container;
  container.reserve(result_tiles_.size());
  std::priority_queue<ResultCoords, std::vector<ResultCoords>, T> tile_queue(
      cmp, std::move(container));

  std::mutex tile_queue_mutex;

  // If any fragments needs to load more tiles.
  bool need_more_tiles = false;

  // Tile iterators, per fragments.
  std::vector<std::list<ResultTileWithBitmap<uint8_t>>::iterator>
      result_tiles_it(result_tiles_.size());

  // Boolean per fragment that keeps track of when a tile is used.
  std::vector<uint8_t> result_tile_used(result_tiles_.size(), true);

  // For all fragments, get the first tile.
  auto status = parallel_for(
      storage_manager_->compute_tp(), 0, result_tiles_.size(), [&](uint64_t f) {
        if (result_tiles_[f].size() > 0) {
          // Initialize the iterator for this fragment.
          result_tiles_it[f] = result_tiles_[f].begin();

          // Get the cell index we were processing.
          auto cell_idx = read_state_.frag_tile_idx_[f].second;

          // Add the tile to the queue.
          auto&& [st, more_tiles] = add_next_tile_to_queue(
              f,
              cell_idx,
              result_tiles_it,
              result_tile_used,
              tile_queue,
              tile_queue_mutex);
          RETURN_NOT_OK(st);
          need_more_tiles = *more_tiles;
        }

        return Status::Ok();
      });
  RETURN_NOT_OK_ELSE_TUPLE(status, logger_->status(status), nullopt);

  // Process all elements.
  while (!tile_queue.empty() && !need_more_tiles && num_cells > 0) {
    auto to_process = tile_queue.top();
    tile_queue.pop();

    // Process all cells with the same coordinates at once.
    while (!tile_queue.empty() && to_process.same_coords(tile_queue.top()) &&
           num_cells > 0) {
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
        result_cell_slabs.emplace_back(next_tile.tile_, next_tile.pos_, 1);
        num_cells--;
        read_state_.frag_tile_idx_[next_tile.tile_->frag_idx()] =
            std::pair<uint64_t, uint64_t>(
                next_tile.tile_->tile_idx(), next_tile.pos_);
      }

      // Put the next cell in the queue.
      if (!next_tile.next()) {
        // Done with this tile, fetch another.
        auto&& [st, more_tiles] = add_next_tile_to_queue(
            next_tile.tile_->frag_idx(),
            0,
            result_tiles_it,
            result_tile_used,
            tile_queue,
            tile_queue_mutex);
        RETURN_NOT_OK_TUPLE(st, nullopt);
        need_more_tiles = *more_tiles;
      } else {
        tile_queue.emplace(std::move(next_tile));
      }
    }

    if (num_cells == 0) {
      break;
    }

    // Get the tile and flag it as used.
    auto tile = (ResultTileWithBitmap<uint8_t>*)to_process.tile_;
    auto has_bmp = tile->bitmap_.size() > 0;
    result_tile_used[tile->frag_idx()] = true;

    // Find how many cells to process using the top of the queue.
    // Temp result coord used to find the last position.
    ResultCoords temp_rc = to_process;

    // Check the top of the queue against last possible cell in the current
    // tile.
    if (!has_bmp) {
      temp_rc.pos_ =
          std::min(tile->cell_num() - 1, to_process.pos_ + num_cells - 1);
    } else {
      temp_rc.pos_ =
          tile->pos_with_given_result_sum(to_process.pos_, num_cells);
    }

    // If there is more than one fragment and we can't add the whole tile,
    // find the last possible cell in this tile smaller than the top of the
    // queue. Otherwise we are adding everything.
    if (!tile_queue.empty()) {
      auto& next_tile = tile_queue.top();
      if (cmp(temp_rc, next_tile)) {
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
    }

    // Generate the result cell slabs.
    auto start = to_process.pos_;
    const auto tile_idx = tile->tile_idx();
    auto& frag_idx = read_state_.frag_tile_idx_[tile->frag_idx()];

    // If no bitmap is set, add all cells.
    if (!has_bmp) {
      auto length = std::min(temp_rc.pos_ - to_process.pos_ + 1, num_cells);
      result_cell_slabs.emplace_back(tile, start, length);
      frag_idx = std::pair<uint64_t, uint64_t>(tile_idx, start + length);
      num_cells -= length;
    } else {
      // Process all cells, when there is a "hole" in the cell contiguity,
      // push a new cell slab.
      uint64_t length = 0;
      for (auto c = to_process.pos_; c <= temp_rc.pos_; c++) {
        if (!tile->bitmap_[c]) {
          if (length != 0) {
            result_cell_slabs.emplace_back(tile, start, length);
            frag_idx = std::pair<uint64_t, uint64_t>(tile_idx, start + length);
            num_cells -= length;
            length = 0;
          }

          start = c + 1;
        } else {
          length++;

          if (length == num_cells)
            break;
        }
      }

      // Add the last cell slab.
      if (length != 0) {
        result_cell_slabs.emplace_back(tile, start, length);
        frag_idx = std::pair<uint64_t, uint64_t>(tile_idx, start + length - 1);
        num_cells -= length;
      }
    }

    // Update the position in the tile.
    to_process.pos_ = temp_rc.pos_;

    // Put the next cell in the queue.
    if (!to_process.next()) {
      // Done with this tile, fetch another.
      auto&& [st, more_tiles] = add_next_tile_to_queue(
          tile->frag_idx(),
          0,
          result_tiles_it,
          result_tile_used,
          tile_queue,
          tile_queue_mutex);
      RETURN_NOT_OK_TUPLE(st, nullopt);
      need_more_tiles = *more_tiles;
    } else {
      // Put the next cell on the queue to be resorted.
      read_state_.frag_tile_idx_[tile->frag_idx()] =
          std::pair<uint64_t, uint64_t>(tile->tile_idx(), to_process.pos_);
      tile_queue.emplace(std::move(to_process));
    }
  }

  buffers_full_ = num_cells == 0;

  logger_->debug(
      "Done merging result cell slabs, num slabs {0}, buffers full {1}",
      result_cell_slabs.size(),
      buffers_full_);

  return {Status::Ok(), std::move(result_cell_slabs)};
};

tuple<uint64_t, uint64_t, uint64_t, bool>
SparseGlobalOrderReader::compute_parallelization_parameters(
    const uint64_t range_thread_idx,
    const uint64_t num_range_threads,
    const uint64_t start,
    const uint64_t length,
    const uint64_t cell_offset) {
  // Prevent processing past the end of the cells in case there are more
  // threads than cells.
  if (length == 0 || range_thread_idx > length - 1) {
    return {0, 0, 0, true};
  }

  // Compute the cells to process.
  auto part_num = std::min(length, num_range_threads);
  uint64_t min_pos =
      start + (range_thread_idx * length + part_num - 1) / part_num;
  uint64_t max_pos = std::min(
      start + ((range_thread_idx + 1) * length + part_num - 1) / part_num,
      start + length);

  return {min_pos, max_pos, cell_offset + min_pos - start, false};
}

template <class OffType>
Status SparseGlobalOrderReader::copy_offsets_tiles(
    const std::string& name,
    const uint64_t num_range_threads,
    const bool nullable,
    const OffType offset_div,
    const std::vector<ResultCellSlab>& result_cell_slabs,
    const std::vector<uint64_t>& cell_offsets,
    QueryBuffer& query_buffer,
    std::vector<void*>& var_data) {
  auto timer_se = stats_->start_timer("copy_offsets_tiles");

  // Process all tiles/cells in parallel.
  auto status = parallel_for_2d(
      storage_manager_->compute_tp(),
      0,
      result_cell_slabs.size(),
      0,
      num_range_threads,
      [&](uint64_t i, uint64_t range_thread_idx) {
        // For easy reference.
        auto& rcs = result_cell_slabs[i];
        auto rt = (ResultTileWithBitmap<uint8_t>*)result_cell_slabs[i].tile_;

        // Get source buffers.
        const auto tile_tuple = rt->tile_tuple(name);
        const auto t = &std::get<0>(*tile_tuple);
        const auto t_var = &std::get<1>(*tile_tuple);
        const auto src_buff = t->data_as<uint64_t>();
        const auto src_var_buff = t_var->data_as<char>();
        const auto t_val = &std::get<2>(*tile_tuple);
        const auto cell_num =
            fragment_metadata_[rt->frag_idx()]->cell_num(rt->tile_idx());

        // Compute parallelization parameters.
        auto&& [min_pos, max_pos, dest_cell_offset, skip_copy] =
            compute_parallelization_parameters(
                range_thread_idx,
                num_range_threads,
                rcs.start_,
                rcs.length_,
                cell_offsets[i]);
        if (skip_copy) {
          return Status::Ok();
        }

        // Get dest buffers.
        auto buffer = (OffType*)query_buffer.buffer_ + dest_cell_offset;
        auto val_buffer =
            query_buffer.validity_vector_.buffer() + dest_cell_offset;
        auto var_data_buffer = &var_data[dest_cell_offset - cell_offsets[0]];

        // Copy full tile. Last cell might be taken out for vectorization.
        uint64_t end = (max_pos == cell_num) ? max_pos - 1 : max_pos;
        for (uint64_t c = min_pos; c < end; c++) {
          *buffer = (OffType)(src_buff[c + 1] - src_buff[c]) / offset_div;
          buffer++;
          *var_data_buffer = src_var_buff + src_buff[c];
          var_data_buffer++;
        }

        // Copy last cell.
        if (max_pos == cell_num) {
          *buffer =
              (OffType)(t_var->size() - src_buff[max_pos - 1]) / offset_div;
          *var_data_buffer = src_var_buff + src_buff[max_pos - 1];
        }

        // Copy nullable values.
        if (nullable) {
          const auto src_val_buff = t_val->data_as<uint8_t>();
          for (uint64_t c = min_pos; c < max_pos; c++) {
            *val_buffer = src_val_buff[c];
            val_buffer++;
          }
        }

        return Status::Ok();
      });
  RETURN_NOT_OK_ELSE(status, logger_->status(status));

  return Status::Ok();
}

template <class OffType>
Status SparseGlobalOrderReader::copy_var_data_tiles(
    const uint64_t num_range_threads,
    const OffType offset_div,
    const uint64_t var_buffer_size,
    const std::vector<ResultCellSlab>& result_cell_slabs,
    const std::vector<uint64_t>& cell_offsets,
    QueryBuffer& query_buffer,
    const std::vector<void*>& var_data) {
  auto timer_se = stats_->start_timer("copy_var_tiles");

  // For easy reference.
  auto var_data_buffer = (uint8_t*)query_buffer.buffer_var_;

  // Process all tiles/cells in parallel.
  auto status = parallel_for_2d(
      storage_manager_->compute_tp(),
      0,
      result_cell_slabs.size(),
      0,
      num_range_threads,
      [&](uint64_t i, uint64_t range_thread_idx) {
        // For easy reference.
        auto& rcs = result_cell_slabs[i];
        bool last_slab = i == result_cell_slabs.size() - 1;

        // Compute parallelization parameters.
        auto&& [min_pos, max_pos, dest_cell_offset, skip_copy] =
            compute_parallelization_parameters(
                range_thread_idx,
                num_range_threads,
                0,
                rcs.length_,
                cell_offsets[i]);
        (void)dest_cell_offset;
        if (skip_copy) {
          return Status::Ok();
        }

        if (max_pos != min_pos) {
          // Get offsets buffer.
          auto offsets_buffer =
              (OffType*)query_buffer.buffer_ + cell_offsets[i];

          // Copy the data cells by cells. Last copy taken out for
          // vectorization.
          auto last_partition = last_slab && max_pos == rcs.length_;
          auto end = last_partition ? max_pos - 1 : max_pos;
          for (uint64_t c = min_pos; c < end; c++) {
            auto size =
                (offsets_buffer[c + 1] - offsets_buffer[c]) * offset_div;
            memcpy(
                var_data_buffer + offsets_buffer[c] * offset_div,
                var_data[c + cell_offsets[i] - cell_offsets[0]],
                size);
          }

          // Last copy for last tile.
          if (last_partition) {
            auto size =
                (var_buffer_size - offsets_buffer[max_pos - 1]) * offset_div;
            memcpy(
                var_data_buffer + offsets_buffer[max_pos - 1] * offset_div,
                var_data[max_pos - 1 + cell_offsets[i] - cell_offsets[0]],
                size);
          }
        }

        return Status::Ok();
      });
  RETURN_NOT_OK_ELSE(status, logger_->status(status));

  return Status::Ok();
}

Status SparseGlobalOrderReader::copy_fixed_data_tiles(
    const std::string& name,
    const uint64_t num_range_threads,
    const bool is_dim,
    const bool nullable,
    const unsigned dim_idx,
    const uint64_t cell_size,
    const std::vector<ResultCellSlab>& result_cell_slabs,
    const std::vector<uint64_t>& cell_offsets,
    QueryBuffer& query_buffer) {
  auto timer_se = stats_->start_timer("copy_fixed_data_tiles");

  // Process all tiles/cells in parallel.
  auto status = parallel_for_2d(
      storage_manager_->compute_tp(),
      0,
      result_cell_slabs.size(),
      0,
      num_range_threads,
      [&](uint64_t i, uint64_t range_thread_idx) {
        // For easy reference.
        auto& rcs = result_cell_slabs[i];
        auto rt = (ResultTileWithBitmap<uint8_t>*)result_cell_slabs[i].tile_;

        // Get source buffers.
        const auto stores_zipped_coords = is_dim && rt->stores_zipped_coords();
        const auto tile_tuple = stores_zipped_coords ?
                                    rt->tile_tuple(constants::coords) :
                                    rt->tile_tuple(name);
        const auto t = &std::get<0>(*tile_tuple);
        const auto src_buff = t->data_as<uint8_t>();
        const auto t_val = &std::get<2>(*tile_tuple);

        // Compute parallelization parameters.
        auto&& [min_pos, max_pos, dest_cell_offset, skip_copy] =
            compute_parallelization_parameters(
                range_thread_idx,
                num_range_threads,
                rcs.start_,
                rcs.length_,
                cell_offsets[i]);
        if (skip_copy) {
          return Status::Ok();
        }

        // Get dest buffers.
        auto buffer =
            (uint8_t*)query_buffer.buffer_ + dest_cell_offset * cell_size;
        auto val_buffer =
            query_buffer.validity_vector_.buffer() + dest_cell_offset;

        if (!stores_zipped_coords) {
          // Copy tile.
          memcpy(
              buffer,
              src_buff + min_pos * cell_size,
              (max_pos - min_pos) * cell_size);
        } else {  // Copy for zipped coords.
          const auto dim_num = rt->domain()->dim_num();
          for (uint64_t c = min_pos; c < max_pos; c++) {
            auto pos = c * dim_num + dim_idx;
            memcpy(buffer, src_buff + pos * cell_size, cell_size);
            buffer += cell_size;
          }
        }

        if (nullable) {
          const auto src_val_buff = t_val->data_as<uint8_t>();
          memcpy(val_buffer, src_val_buff + min_pos, max_pos - min_pos);
        }

        return Status::Ok();
      });
  RETURN_NOT_OK_ELSE(status, logger_->status(status));

  return Status::Ok();
}

tuple<Status, optional<std::vector<uint64_t>>>
SparseGlobalOrderReader::respect_copy_memory_budget(
    const std::vector<std::string>& names,
    const uint64_t memory_budget,
    std::vector<ResultCellSlab>& result_cell_slabs) {
  // Process all attributes in parallel.
  uint64_t max_cs_idx = result_cell_slabs.size();
  std::mutex max_cs_idx_mtx;
  std::vector<uint64_t> total_mem_usage_per_attr(names.size());
  auto status = parallel_for(
      storage_manager_->compute_tp(), 0, names.size(), [&](uint64_t i) {
        // For easy reference.
        const auto& name = names[i];
        const auto var_sized = array_schema_.var_size(name);
        uint64_t* mem_usage = &total_mem_usage_per_attr[i];

        // Keep track of tiles already accounted for.
        std::
            unordered_set<std::pair<uint64_t, uint64_t>, utils::hash::pair_hash>
                accounted_tiles;

        // For dimensions or query condition fields, tiles are already all
        // loaded in memory.
        if (array_schema_.is_dim(name) ||
            condition_.field_names().count(name) != 0)
          return Status::Ok();

        // Get the size for this tile.
        uint64_t idx = 0;
        for (; idx < max_cs_idx; idx++) {
          auto rt =
              (ResultTileWithBitmap<uint8_t>*)result_cell_slabs[idx].tile_;
          auto id =
              std::pair<uint64_t, uint64_t>(rt->frag_idx(), rt->tile_idx());
          if (accounted_tiles.count(id) == 0) {
            accounted_tiles.emplace(id);

            // Size of the tile in memory.
            auto&& [st, tile_size] =
                get_attribute_tile_size(name, rt->frag_idx(), rt->tile_idx());
            RETURN_NOT_OK(st);

            // Account for the pointers to the var data that is created in
            // copy_tiles for var sized attributes.
            if (var_sized) {
              auto cell_num = rt->bitmap_result_num_ !=
                                      std::numeric_limits<uint64_t>::max() ?
                                  rt->bitmap_result_num_ :
                                  fragment_metadata_[rt->frag_idx()]->cell_num(
                                      rt->tile_idx());
              *tile_size += sizeof(void*) * cell_num;
            }

            // Stop when we reach the budget.
            if (*mem_usage + *tile_size > memory_budget) {
              break;
            }

            // Adjust memory usage.
            *mem_usage += *tile_size;
          }
        }

        // Save the minimum result tile index that we saw for all attributes.
        {
          std::unique_lock<std::mutex> ul(max_cs_idx_mtx);
          max_cs_idx = std::min(idx, max_cs_idx);
        }

        return Status::Ok();
      });
  RETURN_NOT_OK_ELSE_TUPLE(status, logger_->status(status), nullopt);

  if (max_cs_idx == 0)
    return {Status_SparseUnorderedWithDupsReaderError(
                "Unable to copy one slab with current budget/buffers"),
            nullopt};

  // Resize the result tiles vector.
  buffers_full_ &= max_cs_idx == result_cell_slabs.size();
  result_cell_slabs.resize(max_cs_idx);

  return {Status::Ok(), std::move(total_mem_usage_per_attr)};
}

template <class OffType>
uint64_t SparseGlobalOrderReader::compute_var_size_offsets(
    stats::Stats* stats,
    std::vector<ResultCellSlab>& result_cell_slabs,
    std::vector<uint64_t>& cell_offsets,
    QueryBuffer& query_buffer) {
  auto timer_se = stats->start_timer("switch_sizes_to_offsets");

  auto new_var_buffer_size = *query_buffer.buffer_var_size_;

  // Switch offsets buffer from cell size to offsets.
  auto offsets_buff = (OffType*)query_buffer.buffer_;
  for (uint64_t c = cell_offsets[0]; c < cell_offsets[result_cell_slabs.size()];
       c++) {
    auto tmp = offsets_buff[c];
    offsets_buff[c] = new_var_buffer_size;
    new_var_buffer_size += tmp;
  }

  // Make sure var size buffer can fit the data.
  if (query_buffer.original_buffer_var_size_ < new_var_buffer_size) {
    // Buffers are full.
    buffers_full_ = true;

    // Make sure that the start of the last RCS can fit the buffers. If not,
    // pop the last slab until it does.
    auto total_cells = cell_offsets[result_cell_slabs.size() - 1];
    new_var_buffer_size = ((OffType*)query_buffer.buffer_)[total_cells];
    while (query_buffer.original_buffer_var_size_ < new_var_buffer_size) {
      // Revert progress for this slab in read state, and pop it.
      auto& last_rcs = result_cell_slabs.back();
      read_state_.frag_tile_idx_[last_rcs.tile_->frag_idx()] =
          std::pair<uint64_t, uint64_t>(
              last_rcs.tile_->tile_idx(), last_rcs.start_);
      result_cell_slabs.pop_back();

      // Update the new var buffer size.
      auto total_cells = cell_offsets[result_cell_slabs.size() - 1];
      new_var_buffer_size = ((OffType*)query_buffer.buffer_)[total_cells];
    }

    // Add as many cells from the last slab as possible, it could be 0.
    auto& last_rcs = result_cell_slabs.back();

    // Find the totals cells we can fit.
    total_cells = cell_offsets[result_cell_slabs.size() - 1];
    auto max = cell_offsets[result_cell_slabs.size()] - 1;
    for (; total_cells < max; total_cells++) {
      if (((OffType*)query_buffer.buffer_)[total_cells + 1] >
          query_buffer.original_buffer_var_size_)
        break;
    }

    // Adjust cell offsets and rcs length.
    cell_offsets[result_cell_slabs.size()] = total_cells;
    last_rcs.length_ = total_cells - cell_offsets[result_cell_slabs.size() - 1];

    // Remove empty cell slab.
    if (last_rcs.length_ == 0) {
      result_cell_slabs.pop_back();
    }

    // Update the buffer size.
    new_var_buffer_size = ((OffType*)query_buffer.buffer_)[total_cells];

    // Update the cell progress.
    read_state_.frag_tile_idx_[last_rcs.tile_->frag_idx()] =
        std::pair<uint64_t, uint64_t>(
            last_rcs.tile_->tile_idx(), last_rcs.start_ + last_rcs.length_);
  }

  return new_var_buffer_size;
}

template <class OffType>
Status SparseGlobalOrderReader::process_slabs(
    std::vector<std::string>& names,
    std::vector<ResultCellSlab>& result_cell_slabs) {
  auto timer_se = stats_->start_timer("process_slabs");

  // Compute parallelization parameters.
  uint64_t num_range_threads = 1;
  const auto num_threads = storage_manager_->compute_tp()->concurrency_level();
  if (result_cell_slabs.size() < num_threads) {
    // Ceil the division between thread_num and tile_num.
    num_range_threads = 1 + ((num_threads - 1) / result_cell_slabs.size());
  }

  // Vector for storing the cell offsets of each tiles into the user buffers.
  // This also stores the last offset to facilitate calculations later on.
  std::vector<uint64_t> cell_offsets(result_cell_slabs.size() + 1);

  // Compute tile offsets.
  uint64_t offset = cells_copied(names);
  for (uint64_t i = 0; i < result_cell_slabs.size(); i++) {
    cell_offsets[i] = offset;
    offset += result_cell_slabs[i].length_;
  }
  cell_offsets[result_cell_slabs.size()] = offset;

  // Calculating the initial copy bound and making sure we respect the memory
  // budget for the copy operation.
  uint64_t memory_budget = memory_budget_ - memory_used_qc_tiles_total_ -
                           memory_used_for_coords_total_ -
                           memory_used_result_tile_ranges_ -
                           array_memory_tracker_->get_memory_usage();
  auto&& [st, mem_usage_per_attr] =
      respect_copy_memory_budget(names, memory_budget, result_cell_slabs);
  RETURN_NOT_OK(st);

  // There is no space for any tiles in the user buffer, exit.
  if (result_cell_slabs.empty()) {
    return Status::Ok();
  }

  // Make a list of unique result tiles.
  std::vector<ResultTile*> result_tiles;
  {
    std::unordered_set<ResultTile*> found_tiles;
    for (auto& rcs : result_cell_slabs) {
      if (found_tiles.count(rcs.tile_) == 0) {
        found_tiles.emplace(rcs.tile_);
        result_tiles.emplace_back(rcs.tile_);
      }
    }
  }

  // Read a few attributes a a time.
  uint64_t buffer_idx = 0;
  while (buffer_idx < names.size()) {
    // Read and unfilter as many attributes as can fit in the budget.
    auto&& [st, index_to_copy] = read_and_unfilter_attributes(
        memory_budget, names, *mem_usage_per_attr, &buffer_idx, result_tiles);
    RETURN_NOT_OK(st);

    for (const auto& idx : *index_to_copy) {
      // For easy reference.
      const auto& name = names[idx];
      const auto is_dim = array_schema_.is_dim(name);
      const auto var_sized = array_schema_.var_size(name);
      const auto nullable = array_schema_.is_nullable(name);
      const auto cell_size = array_schema_.cell_size(name);
      auto& query_buffer = buffers_[name];

      // Pointers to var size data, generated when offsets are processed.
      std::vector<void*> var_data;
      if (var_sized) {
        var_data.resize(
            cell_offsets[result_cell_slabs.size()] - cell_offsets[0]);
      }

      // Get dim idx for zipped coords copy.
      auto dim_idx = 0;
      if (is_dim) {
        const auto& dim_names = array_schema_.dim_names();
        while (name != dim_names[dim_idx])
          dim_idx++;
      }

      // Process all fixed tiles in parallel.
      OffType offset_div =
          elements_mode_ ? datatype_size(array_schema_.type(name)) : 1;
      if (var_sized) {
        RETURN_NOT_OK(copy_offsets_tiles<OffType>(
            name,
            num_range_threads,
            nullable,
            offset_div,
            result_cell_slabs,
            cell_offsets,
            query_buffer,
            var_data));
      } else {
        RETURN_NOT_OK(copy_fixed_data_tiles(
            name,
            num_range_threads,
            is_dim,
            nullable,
            dim_idx,
            cell_size,
            result_cell_slabs,
            cell_offsets,
            query_buffer));
      }

      uint64_t var_buffer_size = 0;
      if (var_sized) {
        // Adjust the offsets buffer and make sure all data fits.
        var_buffer_size = compute_var_size_offsets<OffType>(
            stats_, result_cell_slabs, cell_offsets, query_buffer);

        // Now copy the var size data.
        RETURN_NOT_OK(copy_var_data_tiles(
            num_range_threads,
            offset_div,
            var_buffer_size,
            result_cell_slabs,
            cell_offsets,
            query_buffer,
            var_data));
      }

      // Adjust buffer sizes.
      auto total_cells = cell_offsets[result_cell_slabs.size()];
      if (var_sized) {
        *query_buffer.buffer_size_ = total_cells * sizeof(OffType);

        if (offsets_extra_element_)
          (*query_buffer.buffer_size_) += sizeof(OffType);

        *query_buffer.buffer_var_size_ = var_buffer_size * offset_div;
      } else {
        *query_buffer.buffer_size_ = total_cells * cell_size;
      }

      if (nullable)
        *buffers_[name].validity_vector_.buffer_size() = total_cells;

      // Clear tiles from memory.
      if (!is_dim && condition_.field_names().count(name) == 0) {
        clear_tiles(name, result_tiles);
      }
    }
  }

  logger_->debug("Done copying tiles, buffers full {0}", buffers_full_);
  return Status::Ok();
}

Status SparseGlobalOrderReader::remove_result_tile(
    const unsigned frag_idx,
    std::list<ResultTileWithBitmap<uint8_t>>::iterator rt) {
  // Remove coord tile size from memory budget.
  auto tile_idx = rt->tile_idx();
  auto&& [st, tiles_sizes] = get_coord_tiles_size<uint8_t>(
      true, array_schema_.dim_num(), frag_idx, tile_idx);
  RETURN_NOT_OK(st);
  auto tiles_size = tiles_sizes->first;
  auto tiles_size_qc = tiles_sizes->second;

  // Account for hilbert data.
  if (array_schema_.cell_order() == Layout::HILBERT) {
    tiles_size += fragment_metadata_[frag_idx]->cell_num(rt->tile_idx()) *
                  sizeof(uint64_t);
  }

  // Adjust per fragment memory usage.
  memory_used_for_coords_[frag_idx] -= tiles_size + sizeof(ResultTile);
  memory_used_for_qc_tiles_[frag_idx] -= tiles_size_qc;

  // Adjust total memory usage.
  {
    std::unique_lock<std::mutex> lck(mem_budget_mtx_);
    memory_used_for_coords_total_ -= tiles_size + sizeof(ResultTile);
    memory_used_qc_tiles_total_ -= tiles_size_qc;
  }

  // Delete the tile.
  result_tiles_[frag_idx].erase(rt);

  return Status::Ok();
}

Status SparseGlobalOrderReader::end_iteration() {
  // For easy reference.
  auto fragment_num = fragment_metadata_.size();

  // Clear fully processed tiles in each fragments.
  auto status = parallel_for(
      storage_manager_->compute_tp(), 0, fragment_num, [&](uint64_t f) {
        while (!result_tiles_[f].empty() &&
               result_tiles_[f].front().tile_idx() !=
                   read_state_.frag_tile_idx_[f].first) {
          RETURN_NOT_OK(remove_result_tile(f, result_tiles_[f].begin()));
        }

        return Status::Ok();
      });

  if (!incomplete()) {
    assert(memory_used_for_coords_total_ == 0);
    assert(memory_used_qc_tiles_total_ == 0);
    assert(memory_used_result_tile_ranges_ == 0);
  }

  uint64_t num_rt = 0;
  for (unsigned int f = 0; f < fragment_num; f++) {
    num_rt += result_tiles_[f].size();
  }

  logger_->debug("Done with iteration, num result tiles {0}", num_rt);

  array_memory_tracker_->set_budget(std::numeric_limits<uint64_t>::max());
  return Status::Ok();
}

}  // namespace sm
}  // namespace tiledb
