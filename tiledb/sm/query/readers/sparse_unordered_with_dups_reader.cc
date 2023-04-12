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
#include "tiledb/sm/query/readers/sparse_unordered_with_dups_reader.h"
#include "tiledb/common/logger.h"
#include "tiledb/common/memory_tracker.h"
#include "tiledb/sm/array/array.h"
#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/array_schema/dimension.h"
#include "tiledb/sm/fragment/fragment_metadata.h"
#include "tiledb/sm/misc/comparators.h"
#include "tiledb/sm/misc/hilbert.h"
#include "tiledb/sm/misc/parallel_functions.h"
#include "tiledb/sm/misc/utils.h"
#include "tiledb/sm/query/query_macros.h"
#include "tiledb/sm/query/readers/result_tile.h"
#include "tiledb/sm/stats/global_stats.h"
#include "tiledb/sm/storage_manager/storage_manager.h"
#include "tiledb/sm/subarray/subarray.h"

using namespace tiledb;
using namespace tiledb::common;
using namespace tiledb::sm::stats;

namespace tiledb {
namespace sm {

class SparseUnorderedWithDupsReaderStatusException : public StatusException {
 public:
  explicit SparseUnorderedWithDupsReaderStatusException(
      const std::string& message)
      : StatusException("SparseUnorderedWithDupsReader", message) {
  }
};

/* ****************************** */
/*          CONSTRUCTORS          */
/* ****************************** */

template <class BitmapType>
SparseUnorderedWithDupsReader<BitmapType>::SparseUnorderedWithDupsReader(
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
    : SparseIndexReaderBase(
          stats,
          logger->clone("SparseUnorderedWithDupsReader", ++logger_id_),
          storage_manager,
          array,
          config,
          buffers,
          subarray,
          layout,
          condition)
    , tile_offsets_min_frag_idx_(std::numeric_limits<unsigned>::max())
    , tile_offsets_max_frag_idx_(0) {
  include_coords_ = false;
  SparseIndexReaderBase::init(skip_checks_serialization);

  // Initialize memory budget variables.
  initialize_memory_budget();

  // Get the setting that allows to partially load tile offsets. This is done
  // for this reader only for now.
  bool found = false;
  if (!config_
           .get<bool>(
               "sm.partial_tile_offsets_loading",
               &partial_tile_offsets_loading_,
               &found)
           .ok()) {
    throw SparseUnorderedWithDupsReaderStatusException("Cannot get setting");
  }
  assert(found);
}

/* ****************************** */
/*               API              */
/* ****************************** */

template <class BitmapType>
bool SparseUnorderedWithDupsReader<BitmapType>::incomplete() const {
  return !read_state_.done_adding_result_tiles_ || !result_tiles_.empty();
}

template <class BitmapType>
QueryStatusDetailsReason
SparseUnorderedWithDupsReader<BitmapType>::status_incomplete_reason() const {
  if (array_->is_remote()) {
    return QueryStatusDetailsReason::REASON_USER_BUFFER_SIZE;
  }

  if (!incomplete()) {
    return QueryStatusDetailsReason::REASON_NONE;
  }

  return result_tiles_.empty() ?
             QueryStatusDetailsReason::REASON_MEMORY_BUDGET :
             QueryStatusDetailsReason::REASON_USER_BUFFER_SIZE;
}

template <class BitmapType>
void SparseUnorderedWithDupsReader<BitmapType>::initialize_memory_budget() {
  bool found = false;
  throw_if_not_ok(
      config_.get<uint64_t>("sm.mem.total_budget", &memory_budget_, &found));
  assert(found);

  throw_if_not_ok(config_.get<double>(
      "sm.mem.reader.sparse_unordered_with_dups.ratio_array_data",
      &memory_budget_ratio_array_data_,
      &found));
  assert(found);

  throw_if_not_ok(config_.get<double>(
      "sm.mem.reader.sparse_unordered_with_dups.ratio_coords",
      &memory_budget_ratio_coords_,
      &found));
  assert(found);

  throw_if_not_ok(config_.get<double>(
      "sm.mem.reader.sparse_unordered_with_dups.ratio_query_condition",
      &memory_budget_ratio_query_condition_,
      &found));
  assert(found);

  throw_if_not_ok(config_.get<double>(
      "sm.mem.reader.sparse_unordered_with_dups.ratio_tile_ranges",
      &memory_budget_ratio_tile_ranges_,
      &found));
  assert(found);
}

template <class BitmapType>
Status SparseUnorderedWithDupsReader<BitmapType>::dowork() {
  // Subarray is not known to be explicitly set until buffers are deserialized
  include_coords_ = subarray_.is_set();

  auto timer_se = stats_->start_timer("dowork");

  // Make sure user didn't request delete timestamps.
  if (buffers_.count(constants::delete_timestamps) != 0) {
    return logger_->status(Status_SparseUnorderedWithDupsReaderError(
        "Reader cannot process delete timestamps"));
  }

  // Check that the query condition is valid.
  RETURN_NOT_OK(condition_.check(array_schema_));

  get_dim_attr_stats();

  // This reader assumes ranges are sorted.
  assert(subarray_.ranges_sorted());

  // Start with out buffer sizes as zero.
  zero_out_buffer_sizes();

  // Handle empty array.
  if (fragment_metadata_.empty()) {
    read_state_.done_adding_result_tiles_ = true;
    return Status::Ok();
  }

  // Load initial data, if not loaded already. Coords are only included if the
  // subarray is set.
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

    // Load as much tile offsets data in memory as possible.
    load_tile_offsets_data();

    // Create the result tiles we are going to process.
    create_result_tiles();

    // No more tiles to process, done.
    if (result_tiles_.empty()) {
      assert(read_state_.done_adding_result_tiles_);
      break;
    }

    // Generate the list of created/loaded result tiles.
    std::vector<ResultTile*> result_tiles_created;
    std::vector<ResultTile*> result_tiles_loaded;
    for (auto& result_tile : result_tiles_) {
      if (!result_tile.coords_loaded()) {
        result_tile.set_coords_loaded();
        result_tiles_created.emplace_back(&result_tile);
      } else {
        result_tiles_loaded.emplace_back(&result_tile);
      }
    }

    if (!result_tiles_created.empty()) {
      // Read and unfilter coords.
      RETURN_NOT_OK(read_and_unfilter_coords(result_tiles_created));

      // Compute the tile bitmaps.
      RETURN_NOT_OK(compute_tile_bitmaps<BitmapType>(result_tiles_created));

      // Apply query condition.
      auto st = apply_query_condition<
          UnorderedWithDupsResultTile<BitmapType>,
          BitmapType>(result_tiles_created);
      RETURN_NOT_OK(st);

      // Clear result tiles that are not necessary anymore.
      uint64_t current = 0;
      for (uint64_t i = 0; i < result_tiles_created.size(); i++) {
        if (((ResultTileWithBitmap<uint64_t>*)result_tiles_created[i])
                ->result_num() != 0) {
          result_tiles_created[current++] = result_tiles_created[i];
        }
      }
      result_tiles_created.resize(current);

      // Clear result tiles that are not necessary anymore, part 2.
      auto it = result_tiles_.begin();
      while (it != result_tiles_.end()) {
        auto f = it->frag_idx();
        if (it->result_num() == 0) {
          RETURN_NOT_OK(remove_result_tile(f, it++));
        } else {
          it++;
        }
      }

      result_tiles_loaded.insert(
          result_tiles_loaded.end(),
          result_tiles_created.begin(),
          result_tiles_created.end());
    }

    // No more tiles to process, continue.
    if (result_tiles_.empty()) {
      continue;
    }

    // Copy tiles.
    if (offsets_bitsize_ == 64) {
      RETURN_NOT_OK(process_tiles<uint64_t>(names, result_tiles_loaded));
    } else {
      RETURN_NOT_OK(process_tiles<uint32_t>(names, result_tiles_loaded));
    }

    // End the iteration.
    RETURN_NOT_OK(end_iteration());
  } while (!buffers_full_ && incomplete());

  // Fix the output buffer sizes.
  const auto cells = cells_copied(names);
  stats_->add_counter("result_num", cells);
  RETURN_NOT_OK(resize_output_buffers(cells));

  if (offsets_extra_element_) {
    RETURN_NOT_OK(add_extra_offset());
  }

  stats_->add_counter("ignored_tiles", ignored_tiles_.size());

  return Status::Ok();
}

template <class BitmapType>
void SparseUnorderedWithDupsReader<BitmapType>::reset() {
}

template <class BitmapType>
void SparseUnorderedWithDupsReader<BitmapType>::load_tile_offsets_data() {
  // For easy reference.
  bool initial_load =
      tile_offsets_min_frag_idx_ == std::numeric_limits<unsigned>::max() &&
      tile_offsets_max_frag_idx_ == 0;
  uint64_t available_memory = array_memory_tracker_->get_memory_available() -
                              array_memory_tracker_->get_memory_usage(
                                  MemoryTracker::MemoryType::TILE_OFFSETS);
  auto& relevant_fragments = subarray_.relevant_fragments();

  if (!partial_tile_offsets_loading_) {
    // When partial loading is not allowed, we load everything in memory on the
    // first pass.
    if (initial_load) {
      // Load all tile offsets in memory. Make sure we have enough space for
      // tile offsets data.
      uint64_t total_tile_offset_usage = tile_offsets_size(relevant_fragments);
      if (total_tile_offset_usage > available_memory) {
        throw SparseUnorderedWithDupsReaderStatusException(
            "Cannot load tile offsets, computed size (" +
            std::to_string(total_tile_offset_usage) +
            ") is larger than available memory (" +
            std::to_string(available_memory) +
            "). Total budget for array data (" +
            std::to_string(array_memory_tracker_->get_memory_budget()) + ").");
      }

      // Load the tile offsets.
      load_tile_offsets_for_fragments(relevant_fragments);
      tile_offsets_min_frag_idx_ = 0;
      tile_offsets_max_frag_idx_ =
          static_cast<unsigned>(fragment_metadata_.size());
    }
  } else {
    if (initial_load ||
        (all_tiles_loaded_[tile_offsets_max_frag_idx_ - 1] &&
         tile_offsets_max_frag_idx_ != fragment_metadata_.size())) {
      // For the initial load the min index is 0. Otherwise, it is max + 1.
      if (initial_load) {
        tile_offsets_min_frag_idx_ = 0;
      } else {
        // Clear tile offsets data from loaded fragments.
        for (unsigned f = tile_offsets_min_frag_idx_;
             f < tile_offsets_max_frag_idx_;
             f++) {
          fragment_metadata_[f]->free_tile_offsets();
        }

        tile_offsets_min_frag_idx_ = tile_offsets_max_frag_idx_;
      }

      // Load as much data in memory as possible.
      for (tile_offsets_max_frag_idx_ = tile_offsets_min_frag_idx_;
           tile_offsets_max_frag_idx_ < fragment_metadata_.size();
           tile_offsets_max_frag_idx_++) {
        // If we don't have enough memory for the current fragment, stop.
        if (per_frag_tile_offsets_usage_[tile_offsets_max_frag_idx_] >
            available_memory) {
          break;
        }

        // Adjust available memory.
        available_memory -=
            per_frag_tile_offsets_usage_[tile_offsets_max_frag_idx_];
      }

      // Make sure plan to load tile offsets for at least one fragment.
      if (tile_offsets_min_frag_idx_ == tile_offsets_max_frag_idx_) {
        throw SparseUnorderedWithDupsReaderStatusException(
            "Cannot load tile offsets for only one fragment. Offsets size for "
            "the fragment (" +
            std::to_string(
                per_frag_tile_offsets_usage_[tile_offsets_max_frag_idx_]) +
            ") is larger than available memory (" +
            std::to_string(available_memory) +
            "). Total budget for array data (" +
            std::to_string(array_memory_tracker_->get_memory_budget()) + ").");
      }

      // Load the tile offsets.
      RelevantFragments to_load(
          relevant_fragments,
          tile_offsets_min_frag_idx_,
          tile_offsets_max_frag_idx_);
      load_tile_offsets_for_fragments(std::move(to_load));
    }
  }
}

template <class BitmapType>
std::pair<uint64_t, uint64_t>
SparseUnorderedWithDupsReader<BitmapType>::get_coord_tiles_size(
    unsigned dim_num, unsigned f, uint64_t t) {
  auto tiles_sizes =
      SparseIndexReaderBase::get_coord_tiles_size<BitmapType>(dim_num, f, t);

  auto frag_meta = fragment_metadata_[f];

  // Add the result tile structure size.
  tiles_sizes.first += sizeof(UnorderedWithDupsResultTile<BitmapType>);

  // Add the tile bitmap size if there is a subarray or any condition to
  // process.
  if (subarray_.is_set() || has_post_deduplication_conditions(*frag_meta) ||
      process_partial_timestamps(*frag_meta)) {
    tiles_sizes.first += frag_meta->cell_num(t) * sizeof(BitmapType);
  }

  return tiles_sizes;
}

template <class BitmapType>
bool SparseUnorderedWithDupsReader<BitmapType>::add_result_tile(
    const unsigned dim_num,
    const uint64_t memory_budget_qc_tiles,
    const uint64_t memory_budget_coords_tiles,
    const unsigned f,
    const uint64_t t,
    const uint64_t last_t,
    const FragmentMetadata& frag_md) {
  // Calculate memory consumption for this tile.
  auto tiles_sizes = get_coord_tiles_size(dim_num, f, t);
  auto tiles_size = tiles_sizes.first;
  auto tiles_size_qc = tiles_sizes.second;

  // Don't load more tiles than the memory budget.
  if (memory_used_for_coords_total_ + tiles_size > memory_budget_coords_tiles ||
      memory_used_qc_tiles_total_ + tiles_size_qc > memory_budget_qc_tiles) {
    return true;
  }

  // Adjust memory usage.
  memory_used_for_coords_total_ += tiles_size;
  memory_used_qc_tiles_total_ += tiles_size_qc;

  // Add the result tile.
  result_tiles_.emplace_back(f, t, frag_md);

  // Are all tiles loaded for this fragment.
  if (t == last_t) {
    all_tiles_loaded_[f] = true;
  }

  return false;
}

template <class BitmapType>
void SparseUnorderedWithDupsReader<BitmapType>::create_result_tiles() {
  auto timer_se = stats_->start_timer("create_result_tiles");

  // For easy reference.
  const auto fragment_num = fragment_metadata_.size();
  const auto dim_num = array_schema_.dim_num();

  const uint64_t memory_budget_qc_tiles =
      memory_budget_ * memory_budget_ratio_query_condition_;
  const uint64_t memory_budget_coords =
      memory_budget_ * memory_budget_ratio_coords_;

  // Create result tiles.
  if (subarray_.is_set()) {
    // Load as many tiles as the memory budget allows.
    bool budget_exceeded = false;
    unsigned int f = 0;
    while (f < tile_offsets_max_frag_idx_ && !budget_exceeded) {
      if (!all_tiles_loaded_[f]) {
        all_tiles_loaded_[f] = result_tile_ranges_[f].empty();
        while (!result_tile_ranges_[f].empty()) {
          auto& range = result_tile_ranges_[f].back();
          const auto last_t = result_tile_ranges_[f].front().second;

          // Add all tiles for this range.
          for (uint64_t t = range.first; t <= range.second; t++) {
            budget_exceeded = add_result_tile(
                dim_num,
                memory_budget_qc_tiles,
                memory_budget_coords,
                f,
                t,
                last_t,
                *fragment_metadata_[f]);

            // Make sure we can add at least one tile.
            if (budget_exceeded) {
              logger_->debug(
                  "Budget exceeded adding result tiles, fragment {0}, tile "
                  "{1}",
                  f,
                  t);
              if (result_tiles_.empty())
                throw SparseUnorderedWithDupsReaderStatusException(
                    "Cannot load a single tile, increase memory budget");
              break;
            }

            range.first++;
          }

          if (budget_exceeded)
            break;
          remove_result_tile_range(f);
        }
      }

      f++;
    }
  } else {
    // Load as many tiles as the memory budget allows.
    bool budget_exceeded = false;
    unsigned int f = 0;
    while (f < tile_offsets_max_frag_idx_ && !budget_exceeded) {
      if (!all_tiles_loaded_[f]) {
        auto tile_num = fragment_metadata_[f]->tile_num();

        // Figure out the start index.
        auto start = read_state_.frag_idx_[f].tile_idx_;
        if (!result_tiles_.empty() && result_tiles_.back().frag_idx() == f) {
          start = std::max(start, result_tiles_.back().tile_idx() + 1);
        }

        // Add all tiles for this fragment.
        all_tiles_loaded_[f] = start == tile_num;
        for (uint64_t t = start; t < tile_num; t++) {
          budget_exceeded = add_result_tile(
              dim_num,
              memory_budget_qc_tiles,
              memory_budget_coords,
              f,
              t,
              tile_num - 1,
              *fragment_metadata_[f]);
          // Make sure we can add at least one tile.
          if (budget_exceeded) {
            logger_->debug(
                "Budget exceeded adding result tiles, fragment {0}, tile {1}",
                f,
                t);
            if (result_tiles_.empty())
              throw SparseUnorderedWithDupsReaderStatusException(
                  "Cannot load a single tile, increase memory budget");
            break;
          }
        }
      }
      f++;
    }
  }

  // Check if we are done adding result tiles.
  bool done_adding_result_tiles = true;
  for (unsigned int f = 0; f < fragment_num; f++) {
    done_adding_result_tiles &= all_tiles_loaded_[f] != 0;
  }

  logger_->debug(
      "Done adding result tiles, num result tiles {0}", result_tiles_.size());

  if (done_adding_result_tiles) {
    logger_->debug("All result tiles loaded");
  }

  read_state_.done_adding_result_tiles_ = done_adding_result_tiles;
}

template <class BitmapType>
tuple<bool, uint64_t, uint64_t, uint64_t>
SparseUnorderedWithDupsReader<BitmapType>::compute_parallelization_parameters(
    const uint64_t range_thread_idx,
    const uint64_t num_range_threads,
    const uint64_t min_pos_tile,
    const uint64_t max_pos_tile,
    const uint64_t cell_offset,
    const UnorderedWithDupsResultTile<BitmapType>* rt) {
  // Prevent processing past the end of the cells in case there are more
  // threads than cells.
  auto cell_num = max_pos_tile - min_pos_tile;
  if (cell_num == 0 || range_thread_idx > cell_num - 1) {
    return {true, 0, 0, 0};
  }

  // Compute the cells to process.
  auto part_num = std::min(cell_num, num_range_threads);
  auto src_min_pos =
      min_pos_tile + (range_thread_idx * cell_num + part_num - 1) / part_num;
  auto src_max_pos = std::min(
      min_pos_tile +
          ((range_thread_idx + 1) * cell_num + part_num - 1) / part_num,
      min_pos_tile + cell_num);

  // Adjust the cell offset so that we copy to the right location in the
  // user output buffers.
  auto dest_cell_offset = cell_offset;
  if (rt != nullptr) {
    dest_cell_offset += rt->result_num_between_pos(min_pos_tile, src_min_pos);
  }

  return {false, src_min_pos, src_max_pos, dest_cell_offset};
}

/** Copy offsets with a result count bitmap. */
template <>
template <class OffType>
Status SparseUnorderedWithDupsReader<uint64_t>::copy_offsets_tile(
    const std::string& name,
    const bool nullable,
    const OffType offset_div,
    UnorderedWithDupsResultTile<uint64_t>* rt,
    uint64_t src_min_pos,
    uint64_t src_max_pos,
    OffType* buffer,
    uint8_t* val_buffer,
    const void** var_data) {
  // Get source buffers.
  const auto cell_num =
      fragment_metadata_[rt->frag_idx()]->cell_num(rt->tile_idx());
  const auto tile_tuple = rt->tile_tuple(name);

  // If the tile_tuple is null, this is a field added in schema
  // evolution. Use the fill value.
  const uint64_t* src_buff = nullptr;
  const uint8_t* src_var_buff = nullptr;
  bool use_fill_value = false;
  OffType fill_value_size = 0;
  uint64_t t_var_size = 0;
  if (tile_tuple == nullptr) {
    use_fill_value = true;
    fill_value_size = static_cast<OffType>(
        array_schema_.attribute(name)->fill_value().size());
    src_var_buff = array_schema_.attribute(name)->fill_value().data();
  } else {
    const auto& t = tile_tuple->fixed_tile();
    const auto& t_var = tile_tuple->var_tile();
    t_var_size = t_var.size();
    src_buff = t.template data_as<uint64_t>();
    src_var_buff = t_var.template data_as<uint8_t>();
  }

  // Process all cells. Last cell might be taken out for vectorization.
  uint64_t end = (src_max_pos == cell_num && !use_fill_value) ?
                     src_max_pos - 1 :
                     src_max_pos;
  if (!use_fill_value) {
    for (uint64_t c = src_min_pos; c < end; c++) {
      for (uint64_t i = 0; i < rt->bitmap()[c]; i++) {
        *buffer = (OffType)(src_buff[c + 1] - src_buff[c]) / offset_div;
        buffer++;
        *var_data = src_var_buff + src_buff[c];
        var_data++;
      }
    }

    // Do last cell.
    if (src_max_pos == cell_num) {
      for (uint64_t i = 0; i < rt->bitmap()[src_max_pos - 1]; i++) {
        *buffer =
            (OffType)(t_var_size - src_buff[src_max_pos - 1]) / offset_div;
        buffer++;
        *var_data = src_var_buff + src_buff[src_max_pos - 1];
        var_data++;
      }
    }
  } else {
    for (uint64_t c = src_min_pos; c < end; c++) {
      for (uint64_t i = 0; i < rt->bitmap()[c]; i++) {
        *buffer = fill_value_size / offset_div;
        buffer++;
        *var_data = src_var_buff;
        var_data++;
      }
    }
  }

  // Copy nullable values.
  if (nullable) {
    if (!use_fill_value) {
      const auto& t_val = tile_tuple->validity_tile();
      const auto src_val_buff = t_val.data_as<uint8_t>();
      for (uint64_t c = src_min_pos; c < src_max_pos; c++) {
        for (uint64_t i = 0; i < rt->bitmap()[c]; i++) {
          *val_buffer = src_val_buff[c];
          val_buffer++;
        }
      }
    } else {
      uint8_t v = array_schema_.attribute(name)->fill_value_validity();
      for (uint64_t c = src_min_pos; c < src_max_pos; c++) {
        for (uint64_t i = 0; i < rt->bitmap()[c]; i++) {
          *val_buffer = v;
          val_buffer++;
        }
      }
    }
  }

  return Status::Ok();
}

/** Copy offsets with a result bitmap. */
template <>
template <class OffType>
Status SparseUnorderedWithDupsReader<uint8_t>::copy_offsets_tile(
    const std::string& name,
    const bool nullable,
    const OffType offset_div,
    UnorderedWithDupsResultTile<uint8_t>* rt,
    const uint64_t src_min_pos,
    const uint64_t src_max_pos,
    OffType* buffer,
    uint8_t* val_buffer,
    const void** var_data) {
  // Get source buffers.
  const auto cell_num =
      fragment_metadata_[rt->frag_idx()]->cell_num(rt->tile_idx());
  const auto tile_tuple = rt->tile_tuple(name);

  // If the tile_tuple is null, this is a field added in schema
  // evolution. Use the fill value.
  const uint64_t* src_buff = nullptr;
  const uint8_t* src_var_buff = nullptr;
  bool use_fill_value = false;
  OffType fill_value_size = 0;
  uint64_t t_var_size = 0;
  if (tile_tuple == nullptr) {
    use_fill_value = true;
    fill_value_size = static_cast<OffType>(
        array_schema_.attribute(name)->fill_value().size());
    src_var_buff = array_schema_.attribute(name)->fill_value().data();
  } else {
    const auto& t = tile_tuple->fixed_tile();
    const auto& t_var = tile_tuple->var_tile();
    t_var_size = t_var.size();
    src_buff = t.template data_as<uint64_t>();
    src_var_buff = t_var.template data_as<uint8_t>();
  }

  if (!rt->copy_full_tile() || use_fill_value) {
    // Process all cells. Last cell might be taken out for vectorization.
    uint64_t end = (src_max_pos == cell_num && !use_fill_value) ?
                       src_max_pos - 1 :
                       src_max_pos;
    if (!use_fill_value) {
      for (uint64_t c = src_min_pos; c < end; c++) {
        if (rt->bitmap()[c]) {
          *buffer = (OffType)(src_buff[c + 1] - src_buff[c]) / offset_div;
          buffer++;
          *var_data = src_var_buff + src_buff[c];
          var_data++;
        }
      }

      // Do last cell.
      if (src_max_pos == cell_num && rt->bitmap()[src_max_pos - 1]) {
        *buffer =
            (OffType)(t_var_size - src_buff[src_max_pos - 1]) / offset_div;
        *var_data = src_var_buff + src_buff[src_max_pos - 1];
      }
    } else {
      for (uint64_t c = src_min_pos; c < end; c++) {
        if (!rt->has_bmp() || rt->bitmap()[c]) {
          *buffer = fill_value_size / offset_div;
          buffer++;
          *var_data = src_var_buff;
          var_data++;
        }
      }
    }

    // Copy nullable values.
    if (nullable) {
      if (!use_fill_value) {
        const auto& t_val = tile_tuple->validity_tile();
        const auto src_val_buff = t_val.data_as<uint8_t>();
        for (uint64_t c = src_min_pos; c < src_max_pos; c++) {
          if (rt->bitmap()[c]) {
            *val_buffer = src_val_buff[c];
            val_buffer++;
          }
        }
      } else {
        uint8_t v = array_schema_.attribute(name)->fill_value_validity();
        for (uint64_t c = src_min_pos; c < src_max_pos; c++) {
          if (rt->bitmap()[c]) {
            *val_buffer = v;
            val_buffer++;
          }
        }
      }
    }
  } else {
    // Copy full tile. Last cell might be taken out for vectorization.
    uint64_t end = (src_max_pos == cell_num) ? src_max_pos - 1 : src_max_pos;
    for (uint64_t c = src_min_pos; c < end; c++) {
      *buffer = (OffType)(src_buff[c + 1] - src_buff[c]) / offset_div;
      buffer++;
      *var_data = src_var_buff + src_buff[c];
      var_data++;
    }

    // Copy last cell.
    if (src_max_pos == cell_num) {
      *buffer = (OffType)(t_var_size - src_buff[src_max_pos - 1]) / offset_div;
      *var_data = src_var_buff + src_buff[src_max_pos - 1];
    }

    // Copy nullable values.
    if (nullable) {
      const auto& t_val = tile_tuple->validity_tile();
      const auto src_val_buff = t_val.data_as<uint8_t>();
      for (uint64_t c = src_min_pos; c < src_max_pos; c++) {
        *val_buffer = src_val_buff[c];
        val_buffer++;
      }
    }
  }

  return Status::Ok();
}

template <class BitmapType>
template <class OffType>
Status SparseUnorderedWithDupsReader<BitmapType>::copy_offsets_tiles(
    const std::string& name,
    const uint64_t num_range_threads,
    const bool nullable,
    const OffType offset_div,
    const std::vector<ResultTile*>& result_tiles,
    const std::vector<uint64_t>& cell_offsets,
    QueryBuffer& query_buffer,
    std::vector<const void*>& var_data) {
  auto timer_se = stats_->start_timer("copy_offsets_tiles");

  // For easy reference.
  auto buffer = (OffType*)query_buffer.buffer_;
  auto val_buffer = query_buffer.validity_vector_.buffer();

  // Process all tiles/cells in parallel.
  auto status = parallel_for_2d(
      storage_manager_->compute_tp(),
      0,
      result_tiles.size(),
      0,
      num_range_threads,
      [&](uint64_t i, uint64_t range_thread_idx) {
        // For easy reference.
        auto rt = (UnorderedWithDupsResultTile<BitmapType>*)result_tiles[i];

        // We might have a partially processed result tile from last run.
        auto min_pos_tile = 0;
        if (i == 0) {
          min_pos_tile = read_state_.frag_idx_[rt->frag_idx()].cell_idx_;
        }

        auto max_pos_tile =
            fragment_metadata_[rt->frag_idx()]->cell_num(rt->tile_idx());

        // Adjust max cell if this is the last tile.
        if (i == result_tiles.size() - 1) {
          auto to_copy = cell_offsets[i + 1] - cell_offsets[i];
          max_pos_tile =
              rt->pos_with_given_result_sum(min_pos_tile, to_copy) + 1;
        }

        auto&& [skip_copy, src_min_pos, src_max_pos, dest_cell_offset] =
            compute_parallelization_parameters(
                range_thread_idx,
                num_range_threads,
                min_pos_tile,
                max_pos_tile,
                cell_offsets[i],
                rt);
        if (skip_copy) {
          return Status::Ok();
        }

        // Copy tile.
        RETURN_NOT_OK(copy_offsets_tile<OffType>(
            name,
            nullable,
            offset_div,
            &*rt,
            src_min_pos,
            src_max_pos,
            buffer + dest_cell_offset,
            val_buffer + dest_cell_offset,
            &var_data[dest_cell_offset - cell_offsets[0]]));

        return Status::Ok();
      });
  RETURN_NOT_OK_ELSE(status, logger_->status_no_return_value(status));

  return Status::Ok();
}

/** Copy Var data. */
template <class BitmapType>
template <class OffType>
Status SparseUnorderedWithDupsReader<BitmapType>::copy_var_data_tile(
    const bool last_partition,
    const uint64_t var_data_offset,
    const uint64_t offset_div,
    const uint64_t var_buffer_size,
    const uint64_t src_min_pos,
    const uint64_t src_max_pos,
    const void** var_data,
    const OffType* offsets_buffer,
    uint8_t* var_data_buffer) {
  if (src_max_pos != src_min_pos) {
    // Copy the data cells by cells. Last copy taken out for
    // vectorization.
    auto end = last_partition ? src_max_pos - 1 : src_max_pos;
    for (uint64_t c = src_min_pos; c < end; c++) {
      auto size = (offsets_buffer[c + 1] - offsets_buffer[c]) * offset_div;
      memcpy(
          var_data_buffer + offsets_buffer[c] * offset_div,
          var_data[c + var_data_offset],
          size);
    }

    // Last copy for last tile.
    if (last_partition) {
      memcpy(
          var_data_buffer + offsets_buffer[src_max_pos - 1] * offset_div,
          var_data[src_max_pos - 1 + var_data_offset],
          (var_buffer_size - offsets_buffer[src_max_pos - 1]) * offset_div);
    }
  }

  return Status::Ok();
}

template <class BitmapType>
template <class OffType>
Status SparseUnorderedWithDupsReader<BitmapType>::copy_var_data_tiles(
    const uint64_t num_range_threads,
    const OffType offset_div,
    const uint64_t var_buffer_size,
    const std::vector<ResultTile*>& result_tiles,
    const std::vector<uint64_t>& cell_offsets,
    QueryBuffer& query_buffer,
    std::vector<const void*>& var_data) {
  auto timer_se = stats_->start_timer("copy_var_tiles");

  // For easy reference.
  auto offsets_buffer = (OffType*)query_buffer.buffer_;
  auto var_data_buffer = (uint8_t*)query_buffer.buffer_var_;

  // Process all tiles/cells in parallel.
  auto status = parallel_for_2d(
      storage_manager_->compute_tp(),
      0,
      result_tiles.size(),
      0,
      num_range_threads,
      [&](uint64_t i, uint64_t range_thread_idx) {
        // For easy reference.
        auto max_pos_tile = cell_offsets[i + 1] - cell_offsets[i];
        bool last_tile = i == result_tiles.size() - 1;

        auto&& [skip_copy, src_min_pos, src_max_pos, dest_cell_offset] =
            compute_parallelization_parameters(
                range_thread_idx,
                num_range_threads,
                0,
                max_pos_tile,
                cell_offsets[i],
                nullptr);
        if (skip_copy) {
          return Status::Ok();
        }

        RETURN_NOT_OK(copy_var_data_tile(
            last_tile && src_max_pos == max_pos_tile,
            dest_cell_offset - cell_offsets[0],
            offset_div,
            var_buffer_size,
            src_min_pos,
            src_max_pos,
            (const void**)var_data.data(),
            offsets_buffer + dest_cell_offset,
            var_data_buffer));

        return Status::Ok();
      });
  RETURN_NOT_OK_ELSE(status, logger_->status_no_return_value(status));

  return Status::Ok();
}

/** Copy fixed data with a result count bitmap. */
template <>
Status SparseUnorderedWithDupsReader<uint64_t>::copy_fixed_data_tile(
    const std::string& name,
    const bool is_dim,
    const bool nullable,
    const unsigned dim_idx,
    const uint64_t cell_size,
    UnorderedWithDupsResultTile<uint64_t>* rt,
    const uint64_t src_min_pos,
    const uint64_t src_max_pos,
    uint8_t* buffer,
    uint8_t* val_buffer) {
  // Get source buffers.
  const auto stores_zipped_coords = is_dim && rt->stores_zipped_coords();
  const auto tile_tuple = stores_zipped_coords ?
                              rt->tile_tuple(constants::coords) :
                              rt->tile_tuple(name);

  // If the tile_tuple is null, this is a field added in schema
  // evolution. Use the fill value.
  const uint8_t* src_buff = nullptr;
  bool use_fill_value = false;
  if (tile_tuple == nullptr) {
    use_fill_value = true;
    src_buff = array_schema_.attribute(name)->fill_value().data();
  } else {
    const auto& t = tile_tuple->fixed_tile();
    src_buff = t.template data_as<uint8_t>();
  }

  // Copy values.
  if (!stores_zipped_coords) {
    if (!use_fill_value) {
      for (uint64_t c = src_min_pos; c < src_max_pos; c++) {
        for (uint64_t i = 0; i < rt->bitmap()[c]; i++) {
          memcpy(buffer, src_buff + c * cell_size, cell_size);
          buffer += cell_size;
        }
      }
    } else {
      for (uint64_t c = src_min_pos; c < src_max_pos; c++) {
        for (uint64_t i = 0; i < rt->bitmap()[c]; i++) {
          memcpy(buffer, src_buff, cell_size);
          buffer += cell_size;
        }
      }
    }
  } else {  // Copy for zipped coords.
    const auto dim_num = rt->domain()->dim_num();
    for (uint64_t c = src_min_pos; c < src_max_pos; c++) {
      for (uint64_t i = 0; i < rt->bitmap()[c]; i++) {
        auto pos = c * dim_num + dim_idx;
        memcpy(buffer, src_buff + pos * cell_size, cell_size);
        buffer += cell_size;
      }
    }
  }

  // Copy nullable values.
  if (nullable) {
    if (!use_fill_value) {
      const auto& t_val = tile_tuple->validity_tile();
      const auto src_val_buff = t_val.data_as<uint8_t>();
      for (uint64_t c = src_min_pos; c < src_max_pos; c++) {
        for (uint64_t i = 0; i < rt->bitmap()[c]; i++) {
          *val_buffer = src_val_buff[c];
          val_buffer++;
        }
      }
    } else {
      uint8_t v = array_schema_.attribute(name)->fill_value_validity();
      for (uint64_t c = src_min_pos; c < src_max_pos; c++) {
        for (uint64_t i = 0; i < rt->bitmap()[c]; i++) {
          *val_buffer = v;
          val_buffer++;
        }
      }
    }
  }

  return Status::Ok();
}

/** Copy fixed data with a result bitmap. */
template <>
Status SparseUnorderedWithDupsReader<uint8_t>::copy_fixed_data_tile(
    const std::string& name,
    const bool is_dim,
    const bool nullable,
    const unsigned dim_idx,
    const uint64_t cell_size,
    UnorderedWithDupsResultTile<uint8_t>* rt,
    const uint64_t src_min_pos,
    const uint64_t src_max_pos,
    uint8_t* buffer,
    uint8_t* val_buffer) {
  // Get source buffers.
  const auto stores_zipped_coords = is_dim && rt->stores_zipped_coords();
  const auto tile_tuple = stores_zipped_coords ?
                              rt->tile_tuple(constants::coords) :
                              rt->tile_tuple(name);

  // If the tile_tuple is null, this is a field added in schema
  // evolution. Use the fill value.
  const uint8_t* src_buff = nullptr;
  bool use_fill_value = false;
  if (tile_tuple == nullptr) {
    use_fill_value = true;
    src_buff = array_schema_.attribute(name)->fill_value().data();
  } else {
    const auto& t = tile_tuple->fixed_tile();
    src_buff = t.template data_as<uint8_t>();
  }

  if (!rt->copy_full_tile() || use_fill_value) {
    // Copy values.
    if (!stores_zipped_coords) {
      if (!use_fill_value) {
        // Go through bitmap, when there is a hole in cell contiguity, do a
        // memcpy.
        uint64_t length = 0;
        uint64_t start = src_min_pos;
        for (uint64_t c = src_min_pos; c < src_max_pos; c++) {
          if (rt->bitmap()[c]) {
            length++;
          } else {
            if (length != 0) {
              memcpy(buffer, src_buff + start * cell_size, length * cell_size);
              buffer += length * cell_size;
              length = 0;
            }

            start = c + 1;
          }
        }

        // Do last memcpy.
        if (length != 0) {
          memcpy(buffer, src_buff + start * cell_size, length * cell_size);
          buffer += length * cell_size;
        }
      } else {
        for (uint64_t c = src_min_pos; c < src_max_pos; c++) {
          if (!rt->has_bmp() || rt->bitmap()[c]) {
            memcpy(buffer, src_buff, cell_size);
            buffer += cell_size;
          }
        }
      }
    } else {  // Copy for zipped coords.
      const auto dim_num = rt->domain()->dim_num();
      for (uint64_t c = src_min_pos; c < src_max_pos; c++) {
        if (rt->bitmap()[c]) {
          auto pos = c * dim_num + dim_idx;
          memcpy(buffer, src_buff + pos * cell_size, cell_size);
          buffer += cell_size;
        }
      }
    }

    // Copy nullable values.
    if (nullable) {
      if (!use_fill_value) {
        // Go through bitmap, when there is a hole in cell contiguity, do a
        // memcpy.
        const auto& t_val = tile_tuple->validity_tile();
        uint64_t length = 0;
        uint64_t start = src_min_pos;
        const auto src_val_buff = t_val.data_as<uint8_t>();
        for (uint64_t c = src_min_pos; c < src_max_pos; c++) {
          if (rt->bitmap()[c]) {
            length++;
          } else {
            if (length != 0) {
              memcpy(val_buffer, src_val_buff + start, length);
              val_buffer += length;
              length = 0;
            }

            start = c + 1;
          }
        }

        // Do last memcpy.
        if (length != 0) {
          memcpy(val_buffer, src_val_buff + start, length);
          val_buffer += length;
        }
      } else {
        uint8_t v = array_schema_.attribute(name)->fill_value_validity();
        for (uint64_t c = src_min_pos; c < src_max_pos; c++) {
          for (uint64_t i = 0; i < rt->bitmap()[c]; i++) {
            *val_buffer = v;
            val_buffer++;
          }
        }
      }
    }
  } else {  // Copy full tile.
    memcpy(
        buffer,
        src_buff + src_min_pos * cell_size,
        (src_max_pos - src_min_pos) * cell_size);

    if (nullable) {
      const auto& t_val = tile_tuple->validity_tile();
      const auto src_val_buff = t_val.data_as<uint8_t>();
      memcpy(val_buffer, src_val_buff + src_min_pos, src_max_pos - src_min_pos);
    }
  }

  return Status::Ok();
}

template <>
Status SparseUnorderedWithDupsReader<uint64_t>::copy_timestamp_data_tile(
    UnorderedWithDupsResultTile<uint64_t>* rt,
    const uint64_t src_min_pos,
    const uint64_t src_max_pos,
    uint8_t* buffer) {
  // Get source buffers.
  const auto tile_tuple = rt->tile_tuple(constants::timestamps);
  Tile* t = nullptr;
  uint8_t* src_buff = nullptr;
  if (tile_tuple != nullptr) {
    t = &tile_tuple->fixed_tile();
    src_buff = t->data_as<uint8_t>();
  }

  const uint64_t cell_size = constants::timestamp_size;

  if (fragment_metadata_[rt->frag_idx()]->has_timestamps()) {
    // Copy values.
    for (uint64_t c = src_min_pos; c < src_max_pos; c++) {
      for (uint64_t i = 0; i < rt->bitmap()[c]; i++) {
        memcpy(buffer, src_buff + c * cell_size, cell_size);
        buffer += cell_size;
      }
    }
  } else {
    // Copy fragment timestamp.
    auto timestamp = fragment_timestamp(rt);
    for (uint64_t c = src_min_pos; c < src_max_pos; c++) {
      for (uint64_t i = 0; i < rt->bitmap()[c]; i++) {
        memcpy(buffer, &timestamp, cell_size);
        buffer += cell_size;
      }
    }
  }
  return Status::Ok();
}

template <>
Status SparseUnorderedWithDupsReader<uint8_t>::copy_timestamp_data_tile(
    UnorderedWithDupsResultTile<uint8_t>* rt,
    const uint64_t src_min_pos,
    const uint64_t src_max_pos,
    uint8_t* buffer) {
  auto timer_se = stats_->start_timer("copy_timestamps_tiles");
  // Get source buffers.
  const auto tile_tuple = rt->tile_tuple(constants::timestamps);
  Tile* t = nullptr;
  uint8_t* src_buff = nullptr;
  if (tile_tuple != nullptr) {
    t = &tile_tuple->fixed_tile();
    src_buff = t->data_as<uint8_t>();
  }

  const uint64_t cell_size = constants::timestamp_size;
  auto frag_timestamp = fragment_timestamp(rt);

  if (!rt->copy_full_tile()) {
    // Copy values.
    // Go through bitmap, when there is a hole in cell contiguity, do a
    // memcpy.
    uint64_t length = 0;
    uint64_t start = src_min_pos;
    for (uint64_t c = src_min_pos; c < src_max_pos; c++) {
      if (rt->bitmap()[c]) {
        length++;
      } else {
        if (length != 0) {
          if (fragment_metadata_[rt->frag_idx()]->has_timestamps()) {
            memcpy(buffer, src_buff + start * cell_size, length * cell_size);
          } else {
            std::vector<uint64_t> timestamps(length, frag_timestamp);
            memcpy(buffer, timestamps.data(), length * cell_size);
          }
          buffer += length * cell_size;
          length = 0;
        }
        start = c + 1;
      }
    }

    // Do last memcpy.
    if (length != 0) {
      if (fragment_metadata_[rt->frag_idx()]->has_timestamps()) {
        memcpy(buffer, src_buff + start * cell_size, length * cell_size);
      } else {
        std::vector<uint64_t> timestamps(length, frag_timestamp);
        memcpy(buffer, timestamps.data(), length * cell_size);
      }
      buffer += length * cell_size;
    }
  } else {  // Copy full tile.
    if (fragment_metadata_[rt->frag_idx()]->has_timestamps()) {
      memcpy(
          buffer,
          src_buff + src_min_pos * cell_size,
          (src_max_pos - src_min_pos) * cell_size);
    } else {
      std::vector<uint64_t> timestamps(
          src_max_pos - src_min_pos, frag_timestamp);
      memcpy(
          buffer, timestamps.data(), (src_max_pos - src_min_pos) * cell_size);
    }
  }

  return Status::Ok();
}

template <class BitmapType>
Status SparseUnorderedWithDupsReader<BitmapType>::copy_fixed_data_tiles(
    const std::string& name,
    const uint64_t num_range_threads,
    const bool is_dim,
    const bool nullable,
    const uint64_t dim_idx,
    const uint64_t cell_size,
    const std::vector<ResultTile*>& result_tiles,
    const std::vector<uint64_t>& cell_offsets,
    QueryBuffer& query_buffer) {
  auto timer_se = stats_->start_timer("copy_fixed_data_tiles");

  // For easy reference.
  auto buffer = (uint8_t*)query_buffer.buffer_;
  auto val_buffer = query_buffer.validity_vector_.buffer();

  // Process all tiles/cells in parallel.
  auto status = parallel_for_2d(
      storage_manager_->compute_tp(),
      0,
      result_tiles.size(),
      0,
      num_range_threads,
      [&](uint64_t i, uint64_t range_thread_idx) {
        // For easy reference.
        auto rt = (UnorderedWithDupsResultTile<BitmapType>*)result_tiles[i];

        // We might have a partially processed result tile from last run.
        auto min_pos_tile = 0;
        if (i == 0) {
          min_pos_tile = read_state_.frag_idx_[rt->frag_idx()].cell_idx_;
        }

        auto max_pos_tile =
            fragment_metadata_[rt->frag_idx()]->cell_num(rt->tile_idx());

        // Adjust max cell if this is the last tile.
        if (i == result_tiles.size() - 1) {
          auto to_copy = cell_offsets[i + 1] - cell_offsets[i];
          max_pos_tile =
              rt->pos_with_given_result_sum(min_pos_tile, to_copy) + 1;
        }

        auto&& [skip_copy, src_min_pos, src_max_pos, dest_cell_offset] =
            compute_parallelization_parameters(
                range_thread_idx,
                num_range_threads,
                min_pos_tile,
                max_pos_tile,
                cell_offsets[i],
                rt);
        if (skip_copy) {
          return Status::Ok();
        }

        if (name == constants::timestamps) {
          RETURN_NOT_OK(copy_timestamp_data_tile(
              &*rt,
              src_min_pos,
              src_max_pos,
              buffer + dest_cell_offset * cell_size));
        } else {
          // Copy tile.
          RETURN_NOT_OK(copy_fixed_data_tile(
              name,
              is_dim,
              nullable,
              dim_idx,
              cell_size,
              &*rt,
              src_min_pos,
              src_max_pos,
              buffer + dest_cell_offset * cell_size,
              val_buffer + dest_cell_offset));
        }

        return Status::Ok();
      });
  RETURN_NOT_OK_ELSE(status, logger_->status_no_return_value(status));

  return Status::Ok();
}

template <class BitmapType>
tuple<bool, std::vector<uint64_t>>
SparseUnorderedWithDupsReader<BitmapType>::resize_fixed_result_tiles_to_copy(
    uint64_t max_num_cells,
    uint64_t initial_cell_offset,
    uint64_t first_tile_min_pos,
    std::vector<ResultTile*>& result_tiles) {
  bool buffers_full;
  std::vector<uint64_t> cell_offsets;
  cell_offsets.reserve(result_tiles.size() + 1);

  // Compute initial bound for result tiles by looking at what can fit into
  // the user's buffer. We use either the number of cells in the bitmap when
  // a subarray is set (or we have a query condition) or the number of cells
  // in the fragment metadata to do so.
  uint64_t cell_offset = initial_cell_offset;
  for (uint64_t i = 0; i < result_tiles.size(); i++) {
    auto rt = (ResultTileWithBitmap<BitmapType>*)result_tiles[i];
    auto cell_num = rt->result_num();

    // First tile might have been partially copied. Adjust cell_num to account
    // for it.
    if (i == 0) {
      cell_num -= rt->result_num_between_pos(0, first_tile_min_pos);
    }

    if (cell_offset + cell_num > max_num_cells) {
      break;
    }

    cell_offsets.emplace_back(cell_offset);
    cell_offset += cell_num;
  }

  // If we filled the buffer, add an extra offset to ease calculations
  // later on. If not, add a partial tile at the end.
  if (cell_offset == max_num_cells ||
      cell_offsets.size() == result_tiles.size()) {
    buffers_full = cell_offset == max_num_cells;
    cell_offsets.emplace_back(cell_offset);
  } else {
    buffers_full = true;
    cell_offsets.emplace_back(cell_offset);

    // For overlapping ranges, a cell might be included multiple times and we
    // can only process it if we can include all of the values as the progress
    // we save in the read state doesn't allow to track partial progress for a
    // cell.
    uint64_t rt_idx = cell_offsets.size() - 1;
    auto rt = (ResultTileWithBitmap<BitmapType>*)result_tiles[rt_idx];
    uint64_t min_pos = rt_idx == 0 ? first_tile_min_pos : 0;
    uint64_t cells_to_copy = max_num_cells - cell_offset;

    // Get the position of the cell that gets us to the desired number of
    // cells.
    uint64_t pos = rt->pos_with_given_result_sum(min_pos, cells_to_copy);

    // Count the actual number of results.
    uint64_t actual_cells_to_copy =
        rt->result_num_between_pos(min_pos, pos + 1);

    // If the last cell has a count > 1, it is possible to overflow the number
    // of cells to copy. Don't include the last cell if that is the case.
    if (cell_offset + actual_cells_to_copy > max_num_cells) {
      actual_cells_to_copy = rt->result_num_between_pos(min_pos, pos);
    }

    // It is possible that the first cell of the partial tile doesn't fit. In
    // that case, we don't include an extra cell offset.
    if (actual_cells_to_copy != 0) {
      cell_offsets.emplace_back(cell_offset + actual_cells_to_copy);
    }
  }

  // Resize the result tiles vector.
  result_tiles.resize(cell_offsets.size() - 1);

  return {buffers_full, cell_offsets};
}

template <class BitmapType>
std::vector<uint64_t>
SparseUnorderedWithDupsReader<BitmapType>::resize_fixed_results_to_copy(
    const std::vector<std::string>& names,
    std::vector<ResultTile*>& result_tiles) {
  auto timer_se = stats_->start_timer("resize_fixed_results_to_copy");

  // First try to limit the maximum number of cells we copy using the size
  // of the output buffers for fixed sized attributes. Later we will validate
  // the memory budget. This is the first line of defence used to try to
  // prevent overflows when copying data.
  auto max_num_cells = std::numeric_limits<uint64_t>::max();
  for (const auto& it : buffers_) {
    const auto& name = it.first;
    const auto size = it.second.original_buffer_size_;
    if (array_schema_.var_size(name)) {
      // we only check the var-size buffer here because we enforce
      // size(offsets_buf) == size(validity_buf) and/or
      // size(validity_buf) == size(data_buf) in the Query:set calls
      auto temp_num_cells = size / constants::cell_var_offset_size;

      if (offsets_extra_element_ && temp_num_cells > 0) {
        temp_num_cells--;
      }

      max_num_cells = std::min(max_num_cells, temp_num_cells);
    } else {
      auto temp_num_cells = size / array_schema_.cell_size(name);
      max_num_cells = std::min(max_num_cells, temp_num_cells);
    }
  }

  // User gave us some empty buffers, exit.
  if (max_num_cells == 0) {
    result_tiles.clear();
    return std::vector<uint64_t>();
  }

  uint64_t initial_cell_offset = cells_copied(names);
  uint64_t first_tile_min_pos =
      read_state_.frag_idx_[result_tiles[0]->frag_idx()].cell_idx_;

  auto&& [buffers_full, cell_offsets] = resize_fixed_result_tiles_to_copy(
      max_num_cells, initial_cell_offset, first_tile_min_pos, result_tiles);
  buffers_full_ |= buffers_full;
  return cell_offsets;
}

template <class BitmapType>
tuple<Status, optional<std::vector<uint64_t>>>
SparseUnorderedWithDupsReader<BitmapType>::respect_copy_memory_budget(
    const std::vector<std::string>& names,
    const uint64_t memory_budget,
    std::vector<ResultTile*>& result_tiles) {
  // Process all attributes in parallel.
  uint64_t max_rt_idx = result_tiles.size();
  std::mutex max_rt_idx_mtx;
  std::vector<uint64_t> total_mem_usage_per_attr(names.size());
  auto status = parallel_for(
      storage_manager_->compute_tp(), 0, names.size(), [&](uint64_t i) {
        // For easy reference.
        const auto& name = names[i];
        const auto var_sized = array_schema_.var_size(name);
        auto mem_usage = &total_mem_usage_per_attr[i];
        const bool is_timestamps = name == constants::timestamps ||
                                   name == constants::delete_timestamps;

        // For dimensions, when we have a subarray, tiles are already all
        // loaded in memory.
        if ((include_coords_ && array_schema_.is_dim(name)) ||
            qc_loaded_attr_names_set_.count(name) != 0 || is_timestamps) {
          return Status::Ok();
        }

        // Get the size for all tiles.
        uint64_t idx = 0;
        for (; idx < max_rt_idx; idx++) {
          // Size of the tile in memory.
          auto rt = (UnorderedWithDupsResultTile<BitmapType>*)result_tiles[idx];

          // Skip for fields added in schema evolution.
          if (!fragment_metadata_[rt->frag_idx()]->array_schema()->is_field(
                  name)) {
            continue;
          }

          auto tile_size =
              get_attribute_tile_size(name, rt->frag_idx(), rt->tile_idx());

          // Account for the pointers to the var data that is created in
          // copy_tiles for var sized attributes.
          if (var_sized) {
            tile_size += sizeof(void*) * rt->result_num();
          }

          // Stop when we reach the budget.
          if (*mem_usage + tile_size > memory_budget) {
            break;
          }

          // Adjust memory usage.
          *mem_usage += tile_size;
        }

        // Save the minimum result tile index that we saw for all attributes.
        {
          std::unique_lock<std::mutex> ul(max_rt_idx_mtx);
          max_rt_idx = std::min(idx, max_rt_idx);
        }

        return Status::Ok();
      });
  RETURN_NOT_OK_ELSE_TUPLE(
      status, logger_->status_no_return_value(status), nullopt);

  if (max_rt_idx == 0)
    return {
        Status_SparseUnorderedWithDupsReaderError(
            "Unable to copy one tile with current budget/buffers"),
        nullopt};

  // Resize the result tiles vector.
  buffers_full_ &= max_rt_idx == result_tiles.size();
  result_tiles.resize(max_rt_idx);

  return {Status::Ok(), std::move(total_mem_usage_per_attr)};
}

template <class BitmapType>
template <class OffType>
tuple<bool, uint64_t, uint64_t>
SparseUnorderedWithDupsReader<BitmapType>::compute_var_size_offsets(
    stats::Stats* stats,
    const std::vector<ResultTile*>& result_tiles,
    const uint64_t first_tile_min_pos,
    std::vector<uint64_t>& cell_offsets,
    QueryBuffer& query_buffer) {
  auto timer_se = stats->start_timer("switch_sizes_to_offsets");

  auto new_var_buffer_size = *query_buffer.buffer_var_size_;
  auto new_result_tiles_size = result_tiles.size();
  bool buffers_full = false;

  // Switch offsets buffer from cell size to offsets.
  auto offsets_buff = (OffType*)query_buffer.buffer_;
  for (uint64_t c = cell_offsets[0]; c < cell_offsets[new_result_tiles_size];
       c++) {
    auto tmp = offsets_buff[c];
    offsets_buff[c] = new_var_buffer_size;
    new_var_buffer_size += tmp;
  }

  // Make sure var size buffer can fit the data.
  if (query_buffer.original_buffer_var_size_ < new_var_buffer_size) {
    // Buffers are full.
    buffers_full = true;

    // First find the last full result tile that we can fit.
    while (query_buffer.original_buffer_var_size_ < new_var_buffer_size) {
      auto total_cells = cell_offsets[--new_result_tiles_size];
      new_var_buffer_size = ((OffType*)query_buffer.buffer_)[total_cells];
    }

    // Add in a partial tile if the buffer is not full.
    if (query_buffer.original_buffer_var_size_ != new_var_buffer_size) {
      auto last_tile = (UnorderedWithDupsResultTile<BitmapType>*)
          result_tiles[new_result_tiles_size];

      new_result_tiles_size++;
      auto last_tile_num_cells = cell_offsets[new_result_tiles_size] -
                                 cell_offsets[new_result_tiles_size - 1];
      cell_offsets[new_result_tiles_size] =
          cell_offsets[new_result_tiles_size - 1];

      const auto min_pos = new_result_tiles_size == 1 ? first_tile_min_pos : 0;
      const auto max_pos =
          last_tile->pos_with_given_result_sum(min_pos, last_tile_num_cells);
      for (uint64_t c = min_pos; c <= max_pos - 1; c++) {
        auto cell_count = last_tile->has_bmp() ? last_tile->bitmap()[c] : 1;

        auto new_size =
            ((OffType*)query_buffer
                 .buffer_)[cell_offsets[new_result_tiles_size] + cell_count];
        if (new_size > query_buffer.original_buffer_var_size_) {
          break;
        }

        cell_offsets[new_result_tiles_size] += cell_count;
      }

      if (cell_offsets[new_result_tiles_size] ==
          cell_offsets[new_result_tiles_size - 1]) {
        // No new cell was added. Remove the tile.
        new_result_tiles_size--;
      } else {
        // Update the buffer size.
        auto total_cells = cell_offsets[new_result_tiles_size];
        new_var_buffer_size = ((OffType*)query_buffer.buffer_)[total_cells];
      }
    }
  }

  return {buffers_full, new_var_buffer_size, new_result_tiles_size};
}

template <class BitmapType>
template <class OffType>
Status SparseUnorderedWithDupsReader<BitmapType>::process_tiles(
    std::vector<std::string>& names, std::vector<ResultTile*>& result_tiles) {
  auto timer_se = stats_->start_timer("process_tiles");

  // Vector for storing the cell offsets of each tiles into the user buffers.
  // This also stores the last offset to facilitate calculations later on.
  std::vector<uint64_t> cell_offsets =
      resize_fixed_results_to_copy(names, result_tiles);

  // Making sure we respect the memory budget for the copy operation.
  uint64_t memory_budget = memory_budget_ - memory_used_qc_tiles_total_ -
                           memory_used_for_coords_total_ -
                           memory_used_result_tile_ranges_ -
                           array_memory_tracker_->get_memory_usage();
  auto&& [st, mem_usage_per_attr] =
      respect_copy_memory_budget(names, memory_budget, result_tiles);
  RETURN_NOT_OK(st);

  // There is no space for any tiles in the user buffer, exit.
  if (result_tiles.empty()) {
    return Status::Ok();
  }

  // Compute parallelization parameters.
  uint64_t num_range_threads = 1;
  const auto num_threads = storage_manager_->compute_tp()->concurrency_level();
  if (result_tiles.size() < num_threads) {
    // Ceil the division between thread_num and tile_num.
    num_range_threads = 1 + ((num_threads - 1) / result_tiles.size());
  }

  // Read a few attributes at a time.
  uint64_t buffer_idx = 0;
  while (buffer_idx < names.size()) {
    // Read and unfilter as many attributes as can fit in the budget.
    auto&& [st, index_to_copy] = read_and_unfilter_attributes(
        memory_budget, names, *mem_usage_per_attr, &buffer_idx, result_tiles);
    RETURN_NOT_OK(st);

    // Copy one attribute at a time for buffers in memory.
    for (const auto& idx : *index_to_copy) {
      // For easy reference.
      const auto& name = names[idx];
      const auto is_dim = array_schema_.is_dim(name);
      const auto var_sized = array_schema_.var_size(name);
      const auto nullable = array_schema_.is_nullable(name);
      const auto cell_size = array_schema_.cell_size(name);
      auto& query_buffer = buffers_[name];

      // Get dim idx for zipped coords copy.
      auto dim_idx = 0;
      if (is_dim) {
        const auto& dim_names = array_schema_.dim_names();
        while (name != dim_names[dim_idx])
          dim_idx++;
      }

      // Pointers to var size data, generated when offsets are processed.
      std::vector<const void*> var_data;
      if (var_sized) {
        var_data.resize(cell_offsets[result_tiles.size()] - cell_offsets[0]);
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
            result_tiles,
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
            result_tiles,
            cell_offsets,
            query_buffer));
      }

      uint64_t var_buffer_size = 0;

      if (var_sized) {
        uint64_t first_tile_min_pos =
            read_state_.frag_idx_[result_tiles[0]->frag_idx()].cell_idx_;

        // Adjust the offsets buffer and make sure all data fits.
        auto&& [buffers_full, new_var_buffer_size, new_result_tiles_size] =
            compute_var_size_offsets<OffType>(
                stats_,
                result_tiles,
                first_tile_min_pos,
                cell_offsets,
                query_buffer);
        buffers_full_ |= buffers_full;

        // Clear tiles from memory and adjust result_tiles.
        for (const auto& copy_idx : *index_to_copy) {
          const auto& name_to_clear = names[copy_idx];
          const auto is_dim_to_clear = array_schema_.is_dim(name_to_clear);
          if (qc_loaded_attr_names_set_.count(name_to_clear) == 0 &&
              (!include_coords_ || !is_dim_to_clear)) {
            clear_tiles(name_to_clear, result_tiles, new_result_tiles_size);
          }
        }
        result_tiles.resize(new_result_tiles_size);

        // Now copy the var size data.
        RETURN_NOT_OK(copy_var_data_tiles(
            num_range_threads,
            offset_div,
            new_var_buffer_size,
            result_tiles,
            cell_offsets,
            query_buffer,
            var_data));

        var_buffer_size = new_var_buffer_size;
      }

      // Adjust buffer sizes.
      auto total_cells = cell_offsets[result_tiles.size()];
      if (var_sized) {
        *query_buffer.buffer_size_ = total_cells * sizeof(OffType);

        if (offsets_extra_element_)
          (*query_buffer.buffer_size_) += sizeof(OffType);

        *query_buffer.buffer_var_size_ = var_buffer_size * offset_div;
      } else {
        *query_buffer.buffer_size_ = total_cells * cell_size;
      }

      if (nullable)
        *query_buffer.validity_vector_.buffer_size() = total_cells;

      // Clear tiles from memory.
      if (qc_loaded_attr_names_set_.count(name) == 0 &&
          (!include_coords_ || !is_dim) && name != constants::timestamps &&
          name != constants::delete_timestamps) {
        clear_tiles(name, result_tiles);
      }
    }
  }

  // Compute the number of cells copied for the last tile before updating tile
  // index.
  uint64_t last_tile_cells_copied = 0;
  if (result_tiles.size() > 0) {
    auto last_tile =
        (UnorderedWithDupsResultTile<BitmapType>*)result_tiles.back();
    auto& frag_tile_idx = read_state_.frag_idx_[last_tile->frag_idx()];
    last_tile_cells_copied = cell_offsets[result_tiles.size()] -
                             cell_offsets[result_tiles.size() - 1];
    if (frag_tile_idx.tile_idx_ == last_tile->tile_idx()) {
      last_tile_cells_copied +=
          last_tile->result_num_between_pos(0, frag_tile_idx.cell_idx_);
    }
  }

  // Adjust tile index.
  for (auto rt : result_tiles) {
    read_state_.frag_idx_[rt->frag_idx()] = FragIdx(rt->tile_idx() + 1, 0);
  }

  // If the last tile is not fully copied, save the cell index.
  if (result_tiles.size() > 0) {
    auto last_tile =
        (UnorderedWithDupsResultTile<BitmapType>*)result_tiles.back();
    auto& frag_tile_idx = read_state_.frag_idx_[last_tile->frag_idx()];
    if (last_tile->result_num() != last_tile_cells_copied) {
      frag_tile_idx.tile_idx_ = last_tile->tile_idx();
      frag_tile_idx.cell_idx_ =
          last_tile->pos_with_given_result_sum(0, last_tile_cells_copied) + 1;
    }
  }

  logger_->debug("Done copying tiles");
  return Status::Ok();
}

template <class BitmapType>
Status SparseUnorderedWithDupsReader<BitmapType>::remove_result_tile(
    const unsigned frag_idx,
    typename std::list<UnorderedWithDupsResultTile<BitmapType>>::iterator rt) {
  // Remove coord tile size from memory budget.
  const auto tile_idx = rt->tile_idx();
  auto tiles_sizes =
      get_coord_tiles_size(array_schema_.dim_num(), frag_idx, tile_idx);
  auto tiles_size = tiles_sizes.first;
  auto tiles_size_qc = tiles_sizes.second;

  {
    std::unique_lock<std::mutex> lck(mem_budget_mtx_);
    memory_used_for_coords_total_ -= tiles_size;
    memory_used_qc_tiles_total_ -= tiles_size_qc;
  }

  // Delete the tile.
  result_tiles_.erase(rt);

  return Status::Ok();
}

template <class BitmapType>
Status SparseUnorderedWithDupsReader<BitmapType>::end_iteration() {
  // Clear result tiles that are not necessary anymore.
  while (
      !result_tiles_.empty() &&
      result_tiles_.front().tile_idx() <
          read_state_.frag_idx_[result_tiles_.front().frag_idx()].tile_idx_) {
    const auto f = result_tiles_.front().frag_idx();

    RETURN_NOT_OK(remove_result_tile(f, result_tiles_.begin()));
  }

  // Validate memory usage.
  if (!incomplete()) {
    assert(memory_used_for_coords_total_ == 0);
    assert(memory_used_qc_tiles_total_ == 0);
    assert(memory_used_result_tile_ranges_ == 0);
  }

  logger_->debug(
      "Done with iteration, num result tiles {0}", result_tiles_.size());

  const auto uint64_t_max = std::numeric_limits<uint64_t>::max();
  array_memory_tracker_->set_budget(uint64_t_max);
  return Status::Ok();
}

// Explicit template instantiations
template SparseUnorderedWithDupsReader<uint8_t>::SparseUnorderedWithDupsReader(
    stats::Stats*,
    shared_ptr<Logger>,
    StorageManager*,
    Array*,
    Config&,
    std::unordered_map<std::string, QueryBuffer>&,
    Subarray&,
    Layout,
    QueryCondition&,
    bool);
template SparseUnorderedWithDupsReader<uint64_t>::SparseUnorderedWithDupsReader(
    stats::Stats*,
    shared_ptr<Logger>,
    StorageManager*,
    Array*,
    Config&,
    std::unordered_map<std::string, QueryBuffer>&,
    Subarray&,
    Layout,
    QueryCondition&,
    bool);

}  // namespace sm
}  // namespace tiledb