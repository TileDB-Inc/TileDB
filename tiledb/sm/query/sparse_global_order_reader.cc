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
    , memory_used_rcs_(0)
    , memory_used_result_tiles_(0)
    , memory_budget_ratio_coords_(0.5) {
  // Defines specific bahavior in the tile copy code for this reader.
  fix_var_sized_overflows_ = true;
  clear_coords_tiles_on_copy_ = false;
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

  // Check the validity buffer sizes.
  RETURN_NOT_OK(check_validity_buffer_sizes());

  return Status::Ok();
}

Status SparseGlobalOrderReader::dowork() {
  // Check that the query condition is valid.
  RETURN_NOT_OK(condition_.check(array_schema_));

  auto timer_se = stats_->start_timer("dowork");

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

  // TODO Get rid of the copy here by keeping an end index in resultbase.
  // Try to make a result cell slab that will fit in the user's buffers, at
  // least for fixed sized attributes. Since the copy code might modify the
  // result cell slabs, this is also good to have a copy.
  auto it = read_state_.result_cell_slabs_.begin();
  std::vector<ResultCellSlab> to_copy;
  while (num_cells && it != read_state_.result_cell_slabs_.end()) {
    auto tile = it->tile_;
    if (it->length_ > num_cells) {
      to_copy.emplace_back(tile, it->start_, num_cells);
    } else {
      to_copy.push_back(*it);
      it++;
    }

    num_cells -= to_copy.back().length_;
  }

  // TODO Calculate/maintain memory budget for copy operations.

  // TODO Whenever a buffer overflows in copy, move it to the front of the
  //      list, this way we will prevent reading tiles we don't need on
  //      future reads.

  // Copy the coordinates data. The input vector will be adjusted to reflect
  // only the data that was copied of there was an overflow.
  RETURN_NOT_OK(copy_coordinates(tmp_result_tiles_, to_copy));

  // copy_coordinates will only have an unrecoverable overflow if a single cell
  // is too big for the user's buffers. Otherwise, the result cell slab vector
  // will be adjusted to reflect only what was copied.
  if (copy_overflowed_) {
    zero_out_buffer_sizes();
    return Status::Ok();
  }

  // Copy the attributes data. The input vector will be adjusted to reflect
  // only the data that was copied of there was an overflow.
  RETURN_NOT_OK(
      copy_attribute_values(UINT64_MAX, tmp_result_tiles_, to_copy, subarray_));

  // copy_coordinates will only have an unrecoverable overflow if a single cell
  // is too big for the user's buffers. Otherwise, the result cell slab vector
  // will be adjusted to reflect only what was copied.
  if (copy_overflowed_) {
    zero_out_buffer_sizes();
    return Status::Ok();
  }

  // Fix the output buffer sizes.
  RETURN_NOT_OK(resize_output_buffers(to_copy));

  // End the iteration.
  RETURN_NOT_OK(end_iteration(to_copy));

  return Status::Ok();
}

void SparseGlobalOrderReader::reset() {
}

Status SparseGlobalOrderReader::load_initial_data() {
  if (initial_data_loaded_)
    return Status::Ok();

  auto timer_se = stats_->start_timer("load_initial_data");

  // For easy reference.
  auto fragment_num = fragment_metadata_.size();

  // Calculate a bit vector of the tiles in the subarray, if set.
  if (subarray_.is_set()) {
    // TODO Compute the memory usage here and release after computation.
    RETURN_NOT_OK(subarray_.precompute_tile_overlap(
        0, 0, &config_, storage_manager_->compute_tp()));

    // TODO Warning against memory budget if this gets too big and add a
    // configuration option to turn off.
    result_tile_set_.resize(fragment_num);
    RETURN_CANCEL_OR_ERROR(
        compute_result_tiles_set(subarray_, result_tile_set_));
  }

  // TODO Account for tile offsets in memory budget.
  // Preload zipped coordinate tile offsets. Note that this will
  // ignore fragments with a version >= 5.
  RETURN_CANCEL_OR_ERROR(load_tile_offsets(subarray_, {constants::coords}));

  // Preload unzipped coordinate tile offsets. Note that this will
  // ignore fragments with a version < 5.
  const auto dim_num = array_schema_->dim_num();
  dim_names_.reserve(dim_num);
  is_dim_var_size_.reserve(dim_num);
  for (unsigned d = 0; d < dim_num; ++d) {
    dim_names_.emplace_back(array_schema_->dimension(d)->name());
    is_dim_var_size_[d] = array_schema_->var_size(dim_names_[d]);
  }
  RETURN_CANCEL_OR_ERROR(load_tile_offsets(subarray_, dim_names_));

  // Make sure we have enough space for tiles data.
  result_tiles_.resize(fragment_num);
  read_state_.frag_tile_idx_.resize(fragment_num);
  all_tiles_loaded_.resize(fragment_num);
  memory_used_for_coords_.resize(fragment_num);

  initial_data_loaded_ = true;
  return Status::Ok();
}

Status SparseGlobalOrderReader::get_coord_tile_size(
    unsigned dim_num, unsigned f, uint64_t t, uint64_t* tile_size) {
  for (unsigned d = 0; d < dim_num; d++) {
    *tile_size += fragment_metadata_[f]->tile_size(dim_names_[d], t);

    if (is_dim_var_size_[d]) {
      uint64_t temp = 0;
      RETURN_NOT_OK(fragment_metadata_[f]->tile_var_size(
          *array_->encryption_key(), dim_names_[d], t, &temp));
      *tile_size += temp;
    }
  }

  return Status::Ok();
}

Status SparseGlobalOrderReader::add_result_tile(
    unsigned dim_num,
    uint64_t per_fragment_memory,
    unsigned f,
    uint64_t t,
    const Domain* domain,
    bool* budget_exceeded) {
  // Calculate memory consumption for this tile.
  uint64_t tile_size = 0;
  RETURN_NOT_OK(get_coord_tile_size(dim_num, f, t, &tile_size));

  // Don't load more tiles than the memory budget.
  if (memory_used_for_coords_[f] + tile_size > per_fragment_memory) {
    *budget_exceeded = true;
    return Status::Ok();
  }

  memory_used_for_coords_[f] += tile_size;

  result_tiles_[f].emplace_back(f, t, domain);
  memory_used_result_tiles_ += sizeof(ResultTile);

  if (memory_used_result_tiles_ > per_fragment_memory) {
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

  // Add 2 "fragments" to account for result cell slabs and result tiles.
  num_fragments_to_process += 2;

  per_fragment_memory_ =
      memory_budget_ * memory_budget_ratio_coords_ / num_fragments_to_process;

  // TODO Spew warning if memory budget is too low for coordinate tiles.
  // TODO Parallelize.

  // Create result tiles.
  bool done_adding_result_tiles = true;
  if (subarray_.is_set()) {
    // Load as many tiles as the memory budget allows.
    for (unsigned int f = 0; f < fragment_num; f++) {
      uint64_t t = 0;
      auto tile_num = fragment_metadata_[f]->tile_num();
      for (t = read_state_.frag_tile_idx_[f].first; t < tile_num; t++) {
        // Use the bit vector to only load tiles in the subarray.
        auto idx = t / 8;
        auto bit = 1 << (t % 8);
        if (result_tile_set_[f][idx] & bit) {
          bool budget_exceeded = false;
          RETURN_NOT_OK(add_result_tile(
              dim_num, per_fragment_memory_, f, t, domain, &budget_exceeded));
          *tiles_found = true;

          if (budget_exceeded)
            break;
        }
      }

      all_tiles_loaded_[f] = t == tile_num;
      done_adding_result_tiles &= all_tiles_loaded_[f];
    }
  } else {
    // Load as many tiles as the memory budget allows.
    for (unsigned int f = 0; f < fragment_num; f++) {
      uint64_t t = 0;
      auto tile_num = fragment_metadata_[f]->tile_num();
      for (t = read_state_.frag_tile_idx_[f].first; t < tile_num; t++) {
        bool budget_exceeded = false;
        RETURN_NOT_OK(add_result_tile(
            dim_num, per_fragment_memory_, f, t, domain, &budget_exceeded));
        *tiles_found = true;

        if (budget_exceeded)
          break;
      }

      all_tiles_loaded_[f] = t == tile_num;
      done_adding_result_tiles &= all_tiles_loaded_[f];
    }
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

  // Populate the temp vector for the common functions to work without change.
  tmp_result_tiles_.clear();
  for (auto& frag_result_tile : result_tiles_)
    for (auto& result_tile : frag_result_tile)
      tmp_result_tiles_.push_back(&result_tile);

  // Read and unfilter zipped coordinate tiles. Note that
  // this will ignore fragments with a version >= 5. Unfilter
  // with a nullptr `rcs_index` argument to bypass selective
  // unfiltering.
  RETURN_CANCEL_OR_ERROR(
      read_coordinate_tiles({constants::coords}, tmp_result_tiles_));
  RETURN_CANCEL_OR_ERROR(
      unfilter_tiles(constants::coords, tmp_result_tiles_, nullptr));

  // Read and unfilter unzipped coordinate tiles. Note that
  // this will ignore fragments with a version < 5. Unfilter
  // with a nullptr `rcs_index` argument to bypass selective
  // unfiltering.
  RETURN_CANCEL_OR_ERROR(read_coordinate_tiles(dim_names_, tmp_result_tiles_));
  for (const auto& dim_name : dim_names_) {
    RETURN_CANCEL_OR_ERROR(
        unfilter_tiles(dim_name, tmp_result_tiles_, nullptr));
  }

  // Compute the result cell slabs with the loaded coordinate tiles.
  if (array_schema_->cell_order() == Layout::HILBERT) {
    // Account for this in memory budget.
    std::vector<std::vector<uint64_t>> values(fragment_metadata_.size());
    RETURN_CANCEL_OR_ERROR(merge_result_cell_slabs(
        per_fragment_memory_,
        HilbertCmpReverse(array_schema_->domain(), &values)));
  } else {
    RETURN_CANCEL_OR_ERROR(merge_result_cell_slabs(
        per_fragment_memory_, GlobalCmpReverse(array_schema_->domain())));
  }

  // TODO Take into consideration memory budget. Maybe this needs to move to
  //      the copy part to facilitate.
  // TODO This can be moved before calculating result cell slabs.
  // Finally apply the query condition.
  RETURN_CANCEL_OR_ERROR(apply_query_condition(
      &read_state_.result_cell_slabs_, tmp_result_tiles_, subarray_));

  return Status::Ok();
}

Status SparseGlobalOrderReader::compute_result_tiles_set(
    Subarray& subarray, std::vector<std::vector<char>>& result_tile_set) {
  auto timer_se = stats_->start_timer("compute_result_tiles_set");

  // For easy reference.
  auto range_num = subarray.range_num();
  auto fragment_num = fragment_metadata_.size();

  for (unsigned f = 0; f < fragment_num; ++f) {
    result_tile_set[f].resize(fragment_metadata_[f]->tile_num() / 8 + 1);
    for (uint64_t r = 0; r < range_num; ++r) {
      // Handle range of tiles (full overlap)
      const auto& tile_ranges = subarray.tile_overlap(f, r)->tile_ranges_;
      for (const auto& tr : tile_ranges) {
        for (uint64_t t = tr.first; t <= tr.second; ++t) {
          auto idx = t / 8;
          auto bit = 1 << (t % 8);
          result_tile_set[f][idx] |= bit;
        }
      }

      // Handle single tiles
      const auto& o_tiles = subarray.tile_overlap(f, r)->tiles_;
      for (const auto& o_tile : o_tiles) {
        auto t = o_tile.first;
        auto idx = t / 8;
        auto bit = 1 << (t % 8);
        result_tile_set[f][idx] |= bit;
      }
    }
  }

  return Status::Ok();
}

template <class T>
Status SparseGlobalOrderReader::add_next_tile_to_queue(
    bool subarray_set,
    unsigned int frag_idx,
    std::vector<std::list<ResultTile>::iterator>& result_tiles_it,
    std::vector<std::vector<uint8_t>>& coord_tiles_result_bitmap,
    std::priority_queue<ResultCoords, std::vector<ResultCoords>, T>& tile_queue,
    T& cmp,
    bool* need_more_tiles) {
  result_tiles_it[frag_idx]++;

  // There was more tiles in this fragment, insert it in the queue.
  if (result_tiles_it[frag_idx] != result_tiles_[frag_idx].end()) {
    auto tile = &*result_tiles_it[frag_idx];

    // Calculate the bitmap for the cells.
    RETURN_NOT_OK(compute_coord_tiles_result_bitmap(
        subarray_set, tile, coord_tiles_result_bitmap));

    // Calculate hilbert values, this is templated out for non hilbert code.
    RETURN_NOT_OK(calculate_hilbert_values(
        subarray_set, tile, coord_tiles_result_bitmap, cmp));

    read_state_.frag_tile_idx_[tile->frag_idx()] =
        std::pair<uint64_t, uint64_t>(tile->tile_idx(), 0);
    tile_queue.emplace(tile, 0);
  } else {
    // This fragment has more tiles potentially.
    if (!all_tiles_loaded_[frag_idx]) {
      // Try to find a next tile.
      read_state_.frag_tile_idx_[frag_idx].second = 0;
      if (subarray_.is_set()) {
        // Look in the result tile set to find a tile.
        while (++read_state_.frag_tile_idx_[frag_idx].first <
               fragment_metadata_[frag_idx]->tile_num()) {
          // Look in the bit set to see if the tile is present.
          auto idx = read_state_.frag_tile_idx_[frag_idx].first / 8;
          auto bit = 1 << (read_state_.frag_tile_idx_[frag_idx].first % 8);
          if (result_tile_set_[frag_idx][idx] & bit) {
            // Found it, break the loop;
            *need_more_tiles = true;
            break;
          }
        }
      } else {
        // Set the next tile and return we need more tiles.
        read_state_.frag_tile_idx_[frag_idx].first++;
        *need_more_tiles = true;
      }

      // If there are no more tiles in this fragment, set the bool.
      all_tiles_loaded_[frag_idx] = !*need_more_tiles;
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

Status SparseGlobalOrderReader::compute_coord_tiles_result_bitmap(
    bool subarray_set,
    ResultTile* tile,
    std::vector<std::vector<uint8_t>>& coord_tiles_result_bitmap) {
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

  coord_tiles_result_bitmap[tile->frag_idx()] = std::move(result_bitmap);

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
  std::priority_queue<ResultCoords, std::vector<ResultCoords>, T> tile_queue(
      cmp);

  // Tile iterators, per fragments.
  std::vector<std::list<ResultTile>::iterator> result_tiles_it(
      result_tiles_.size());

  // Bitmaps for the current tile, per fragment for the cells to process.
  std::vector<std::vector<uint8_t>> coord_tiles_result_bitmap(
      result_tiles_.size());

  // For all fragments, get the first tile.
  for (unsigned int frag_idx = 0; frag_idx < result_tiles_.size(); frag_idx++) {
    if (result_tiles_[frag_idx].size() > 0) {
      // Initialize the iterator for this fragment.
      result_tiles_it[frag_idx] = result_tiles_[frag_idx].begin();
      auto tile = &*result_tiles_it[frag_idx];

      // Get the cell index we were processing.
      auto cell_idx = read_state_.frag_tile_idx_[frag_idx].second;

      // Calculate the bitmap for the cells.
      RETURN_NOT_OK(compute_coord_tiles_result_bitmap(
          subarray_set, tile, coord_tiles_result_bitmap));

      // Calculate hilbert values, this is templated out for non hilbert code.
      RETURN_NOT_OK(calculate_hilbert_values(
          subarray_set, tile, coord_tiles_result_bitmap, cmp));

      // Add the tile to the queue.
      tile_queue.emplace(tile, cell_idx);
    }
  }

  // The result cell slab in progress.
  ResultTile* tile = tile_queue.top().tile_;
  uint64_t start = tile_queue.top().pos_;
  uint64_t length = 0;

  // Used to store cells that have the same coordinate as the one in process.
  std::stack<ResultCoords> same_coords_stack;

  // Process all elements.
  // TODO Implement parallelization here.
  bool need_more_tiles = false;
  while (!tile_queue.empty() && !need_more_tiles) {
    auto next_tile = tile_queue.top();
    tile_queue.pop();

    // See if the cell is in the subarray, if set.
    bool cell_in_subarray = true;
    if (subarray_set) {
      cell_in_subarray = coord_tiles_result_bitmap[next_tile.tile_->frag_idx()]
                                                  [next_tile.pos_];
    }

    // Process all cells with the same coordinates at once.
    while (!tile_queue.empty() && next_tile.same_coords(tile_queue.top())) {
      // Potentially the next cell.
      auto next_tile_tmp = tile_queue.top();
      tile_queue.pop();

      // If we allow duplicates, create one slab for all the dups.
      if (allows_dups && cell_in_subarray) {
        if (length != 0) {
          read_state_.result_cell_slabs_.emplace_back(tile, start, length);
          memory_used_rcs_ += sizeof(ResultCellSlab);
        }

        tile = next_tile_tmp.tile_;
        start = next_tile_tmp.pos_;
        length = 1;
      }

      // Take the cell with the highest fagment index.
      if (next_tile.tile_->frag_idx() < next_tile_tmp.tile_->frag_idx()) {
        same_coords_stack.push(std::move(next_tile));
        next_tile = std::move(next_tile_tmp);
      } else {
        same_coords_stack.push(std::move(next_tile_tmp));
      }
    }

    // New tile or cell not in subarray, time for a new result cell slab.
    if (tile != next_tile.tile_ || !cell_in_subarray) {
      if (length != 0) {
        read_state_.result_cell_slabs_.emplace_back(tile, start, length);
        memory_used_rcs_ += sizeof(ResultCellSlab);
      }

      // If the coord is not in range the possible rcs starts at the next index.
      tile = next_tile.tile_;
      start = next_tile.pos_ + !cell_in_subarray;
      length = 0;
    }

    // Increment the result cell slab length if the cell is in the subarray.
    if (cell_in_subarray) {
      length++;
    }

    // Done with this tile, fetch another.
    if (!next_tile.next()) {
      RETURN_NOT_OK(add_next_tile_to_queue(
          subarray_set,
          next_tile.tile_->frag_idx(),
          result_tiles_it,
          coord_tiles_result_bitmap,
          tile_queue,
          cmp,
          &need_more_tiles));
    } else {
      // Put the next cell on the queue to be resorted.
      tile_queue.emplace(std::move(next_tile));
      read_state_.frag_tile_idx_[next_tile.tile_->frag_idx()] =
          std::pair<uint64_t, uint64_t>(
              next_tile.tile_->tile_idx(), next_tile.pos_);
    }

    // Purge the cells with same coords, adding their next cell to the heap.
    while (!same_coords_stack.empty()) {
      auto rc = same_coords_stack.top();
      same_coords_stack.pop();

      // Done with this tile, fetch another.
      if (!rc.next()) {
        RETURN_NOT_OK(add_next_tile_to_queue(
            subarray_set,
            rc.tile_->frag_idx(),
            result_tiles_it,
            coord_tiles_result_bitmap,
            tile_queue,
            cmp,
            &need_more_tiles));
      } else {
        // put the next cell on the queue again to be resorted.
        tile_queue.emplace(std::move(rc));
        read_state_.frag_tile_idx_[next_tile.tile_->frag_idx()] =
            std::pair<uint64_t, uint64_t>(
                next_tile.tile_->tile_idx(), next_tile.pos_);
      }
    }

    // If we busted our memory budget, exit.
    if (memory_used_rcs_ >= memory_budget)
      break;
  }

  // Add the final result cell slab, and done.
  if (length != 0) {
    read_state_.result_cell_slabs_.emplace_back(tile, start, length);
    memory_used_rcs_ += sizeof(ResultCellSlab);
  }

  return Status::Ok();
};

Status SparseGlobalOrderReader::resize_output_buffers(
    std::vector<ResultCellSlab>& copied) {
  // Copy might truncate the result cell slab.
  // Count number of elements actually copied.
  uint64_t cells_copied = 0;
  for (auto rcs : copied) {
    cells_copied += rcs.length_;
  }

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

Status SparseGlobalOrderReader::end_iteration(
    std::vector<ResultCellSlab>& copied) {
  // For easy reference.
  auto dim_num = array_schema_->dim_num();
  auto fragment_num = fragment_metadata_.size();

  // Remove the processed cell slabs.
  uint64_t num_cs_to_del = copied.size();
  auto& new_front = read_state_.result_cell_slabs_[num_cs_to_del - 1];

  // If the last cell slab processed wasn't processed fully, split it.
  if (new_front.tile_ == copied.back().tile_ &&
      new_front.start_ == copied.back().start_ &&
      new_front.length_ != copied.back().length_) {
    new_front.start_ += copied.back().length_;
    new_front.length_ -= copied.back().length_;
    num_cs_to_del--;
  }

  // Clear result tiles that are not necessary anymore.
  auto cs_to_del_end = read_state_.result_cell_slabs_.begin() + num_cs_to_del;

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
  for (unsigned frag_idx = 0; frag_idx < fragment_num; frag_idx++) {
    if (!result_tiles_[frag_idx].empty())
      while (result_tiles_[frag_idx].front().tile_idx() !=
             last_tile_processed[frag_idx]) {
        // Remove coord tile size from memory budget.
        auto tile_idx = result_tiles_[frag_idx].front().tile_idx();
        uint64_t tile_size = 0;
        RETURN_NOT_OK(
            get_coord_tile_size(dim_num, frag_idx, tile_idx, &tile_size));
        memory_used_for_coords_[frag_idx] -= tile_size;

        // Delete the tile.
        result_tiles_[frag_idx].pop_front();
      }
  }

  // Erase from the vector.
  read_state_.result_cell_slabs_.erase(
      read_state_.result_cell_slabs_.begin(),
      read_state_.result_cell_slabs_.begin() + num_cs_to_del);

  return Status::Ok();
}

}  // namespace sm
}  // namespace tiledb