/**
 * @file   sparse_unordered_with_dups_reader.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2024 TileDB, Inc.
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
#include "tiledb/common/assert.h"
#include "tiledb/common/logger.h"
#include "tiledb/common/memory_tracker.h"
#include "tiledb/sm/array/array.h"
#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/array_schema/dimension.h"
#include "tiledb/sm/fragment/fragment_metadata.h"
#include "tiledb/sm/misc/comparators.h"
#include "tiledb/sm/misc/hilbert.h"
#include "tiledb/sm/misc/parallel_functions.h"
#include "tiledb/sm/query/query_macros.h"
#include "tiledb/sm/query/readers/result_tile.h"
#include "tiledb/sm/stats/global_stats.h"
#include "tiledb/sm/subarray/subarray.h"

using namespace tiledb;
using namespace tiledb::common;
using namespace tiledb::sm::stats;

namespace tiledb::sm {

class SparseUnorderedWithDupsReaderException : public StatusException {
 public:
  explicit SparseUnorderedWithDupsReaderException(const std::string& message)
      : StatusException("SparseUnorderedWithDupsReader", message) {
  }
};

/* ****************************** */
/*          CONSTRUCTORS          */
/* ****************************** */

template <class BitmapType>
SparseUnorderedWithDupsReader<BitmapType>::SparseUnorderedWithDupsReader(
    stats::Stats* stats, shared_ptr<Logger> logger, StrategyParams& params)
    : SparseIndexReaderBase(
          "sparse_unordered_with_dups", stats, logger, params, false)
    , tile_offsets_min_frag_idx_(std::numeric_limits<unsigned>::max())
    , tile_offsets_max_frag_idx_(0) {
  // Initialize memory budget variables.
  refresh_config();

  // Get the setting that allows to partially load tile offsets. This is
  // done for this reader only for now.
  partial_tile_offsets_loading_ =
      config_.get<bool>("sm.partial_tile_offsets_loading", Config::must_find);
}

/* ****************************** */
/*               API              */
/* ****************************** */

template <class BitmapType>
bool SparseUnorderedWithDupsReader<BitmapType>::incomplete() const {
  return !read_state_.done_adding_result_tiles() ||
         !result_tiles_leftover_.empty();
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

  return result_tiles_leftover_.empty() ?
             QueryStatusDetailsReason::REASON_MEMORY_BUDGET :
             QueryStatusDetailsReason::REASON_USER_BUFFER_SIZE;
}

template <class BitmapType>
void SparseUnorderedWithDupsReader<BitmapType>::refresh_config() {
  memory_budget_.refresh_config(config_, "sparse_unordered_with_dups");
}

template <class BitmapType>
Status SparseUnorderedWithDupsReader<BitmapType>::dowork() {
  auto timer_se = stats_->start_timer("dowork");
  stats_->add_counter("loop_num", 1);

  // Make sure user didn't request delete timestamps.
  if (buffers_.count(constants::delete_timestamps) != 0) {
    return logger_->status(Status_SparseUnorderedWithDupsReaderError(
        "Reader cannot process delete timestamps"));
  }

  // Check that the query condition is valid.
  if (condition_.has_value()) {
    throw_if_not_ok(condition_->check(array_schema_));
  }

  get_dim_attr_stats();

  // This reader assumes ranges are sorted.
  iassert(subarray_.ranges_sorted());

  // Start with out buffer sizes as zero.
  zero_out_buffer_sizes();

  // Handle empty array.
  if (fragment_metadata_.empty()) {
    read_state_.set_done_adding_result_tiles(true);
    return Status::Ok();
  }

  // Subarray is not known to be explicitly set until buffers are deserialized
  subarray_.reset_default_ranges();
  include_coords_ = subarray_.is_set();

  // Load initial data, if not loaded already. Coords are only included if the
  // subarray is set.
  throw_if_not_ok(load_initial_data());

  // Field names to process.
  std::vector<std::string> names = field_names_to_process();

  bool user_buffers_full = false;
  do {
    stats_->add_counter("internal_loop_num", 1);

    // Load as much tile offsets data in memory as possible.
    load_tile_offsets_data();

    ResultTilesList result_tiles;
    std::vector<ResultTile*> result_tiles_ptr;
    if (result_tiles_leftover_.size() == 0) {
      // Create the result tiles we are going to process or use the ones from a
      // previous iteration.
      result_tiles = create_result_tiles();

      // No more tiles to process, done.
      if (result_tiles.empty()) {
        iassert(read_state_.done_adding_result_tiles());
        break;
      }

      for (auto& result_tile : result_tiles) {
        result_tiles_ptr.emplace_back(&result_tile);
      }

      // Read and unfilter coords.
      throw_if_not_ok(read_and_unfilter_coords(result_tiles_ptr));

      // Compute the tile bitmaps.
      compute_tile_bitmaps<BitmapType>(result_tiles_ptr);

      // Apply query condition.
      apply_query_condition<
          UnorderedWithDupsResultTile<BitmapType>,
          BitmapType>(result_tiles_ptr);

      clean_tile_list(result_tiles, result_tiles_ptr);

      // No more tiles to process, continue.
      if (result_tiles.empty()) {
        continue;
      }
    } else {
      result_tiles = std::move(result_tiles_leftover_);
      result_tiles_leftover_.clear();

      for (auto& result_tile : result_tiles) {
        result_tiles_ptr.emplace_back(&result_tile);
      }
    }

    // Copy tiles.
    if (offsets_bitsize_ == 64) {
      user_buffers_full = process_tiles<uint64_t>(names, result_tiles_ptr);
    } else {
      user_buffers_full = process_tiles<uint32_t>(names, result_tiles_ptr);
    }

    // End the iteration.
    end_iteration(result_tiles);
  } while (!user_buffers_full && incomplete());

  // Fix the output buffer sizes.
  const auto cells = cells_copied(names);
  stats_->add_counter("result_num", cells);
  resize_output_buffers(cells);

  if (offsets_extra_element_) {
    add_extra_offset();
  }

  stats_->add_counter("ignored_tiles", tmp_read_state_.num_ignored_tiles());

  return Status::Ok();
}

template <class BitmapType>
void SparseUnorderedWithDupsReader<BitmapType>::reset() {
}

template <class BitmapType>
std::string SparseUnorderedWithDupsReader<BitmapType>::name() {
  return "SparseUnorderedWithDupsReader<" +
         std::string(typeid(BitmapType).name()) + ">";
}

template <class BitmapType>
void SparseUnorderedWithDupsReader<BitmapType>::load_tile_offsets_data() {
  // For easy reference.
  bool initial_load =
      tile_offsets_min_frag_idx_ == std::numeric_limits<unsigned>::max() &&
      tile_offsets_max_frag_idx_ == 0;
  uint64_t available_memory = array_memory_tracker_->get_memory_available();
  auto& relevant_fragments = subarray_.relevant_fragments();

  if (!partial_tile_offsets_loading_) {
    // When partial loading is not allowed, we load everything in memory on the
    // first pass.
    if (initial_load) {
      // Load all tile offsets in memory. Make sure we have enough space for
      // tile offsets data.
      uint64_t total_tile_offset_usage = tile_offsets_size(relevant_fragments);
      if (total_tile_offset_usage > available_memory) {
        throw SparseUnorderedWithDupsReaderException(
            "Cannot load tile offsets, computed size (" +
            std::to_string(total_tile_offset_usage) +
            ") is larger than available memory (" +
            std::to_string(available_memory) +
            "), increase memory budget. Total budget for array data (" +
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
        (tmp_read_state_.all_tiles_loaded(tile_offsets_max_frag_idx_ - 1) &&
         tile_offsets_max_frag_idx_ != fragment_metadata_.size())) {
      // For the initial load the min index is 0. Otherwise, it is max + 1.
      if (initial_load) {
        tile_offsets_min_frag_idx_ = 0;
      } else {
        // Clear tile offsets data from loaded fragments.
        for (unsigned f = tile_offsets_min_frag_idx_;
             f < tile_offsets_max_frag_idx_;
             f++) {
          fragment_metadata_[f]->loaded_metadata()->free_tile_offsets();
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
        throw SparseUnorderedWithDupsReaderException(
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
uint64_t SparseUnorderedWithDupsReader<BitmapType>::get_coord_tiles_size(
    unsigned dim_num, unsigned f, uint64_t t) {
  auto tiles_size =
      SparseIndexReaderBase::get_coord_tiles_size<BitmapType>(dim_num, f, t);

  auto frag_meta = fragment_metadata_[f];

  // Add the result tile structure size.
  tiles_size += sizeof(UnorderedWithDupsResultTile<BitmapType>);

  // Add the tile bitmap size if there is a subarray or any condition to
  // process.
  if (subarray_.is_set() || has_post_deduplication_conditions(*frag_meta) ||
      process_partial_timestamps(*frag_meta)) {
    tiles_size += frag_meta->cell_num(t) * sizeof(BitmapType);
  }

  return tiles_size;
}

template <class BitmapType>
bool SparseUnorderedWithDupsReader<BitmapType>::add_result_tile(
    const unsigned dim_num,
    const unsigned f,
    const uint64_t t,
    const uint64_t last_t,
    const FragmentMetadata& frag_md,
    ResultTilesList& result_tiles) {
  if (tmp_read_state_.is_ignored_tile(f, t)) {
    return false;
  }

  // Use either the coordinate portion of the total budget or the tile upper
  // memory limit as the upper memory limit, whichever is smaller.
  const uint64_t upper_memory_limit = std::min<uint64_t>(
      memory_budget_.tile_upper_memory_limit(),
      memory_budget_.coordinates_budget());

  // Calculate memory consumption for this tile.
  auto tiles_size = get_coord_tiles_size(dim_num, f, t);

  // Don't load more tiles than the memory budget.
  if (memory_used_for_coords_total_ + tiles_size > upper_memory_limit) {
    // If we don't have any tiles loaded and the new tile still can fit in the
    // available memory, allow loading it so we can make progress.
    if (!result_tiles.empty() || tiles_size > available_memory()) {
      logger_->debug(
          "Total memory budget of {0} exceeded adding result tiles with upper "
          "memory limit of {1}, fragment "
          "{2}, tile {3}/{4} with size {5}",
          memory_budget_.total_budget(),
          upper_memory_limit,
          f,
          t,
          last_t,
          tiles_size);

      // Make sure we can add at least one tile.
      if (result_tiles.empty()) {
        throw SparseUnorderedWithDupsReaderException(
            "Cannot load a single tile requiring " +
            std::to_string(tiles_size) + " bytes, increase memory budget (" +
            std::to_string(memory_budget_.total_budget()) + ")");
      }

      return true;
    }
  }

  // Adjust memory usage.
  memory_used_for_coords_total_ += tiles_size;

  // Add the result tile.
  result_tiles.emplace_back(f, t, frag_md, query_memory_tracker_);

  // Are all tiles loaded for this fragment.
  if (t == last_t) {
    tmp_read_state_.set_all_tiles_loaded(f);
  }

  return false;
}

template <class BitmapType>
std::list<UnorderedWithDupsResultTile<BitmapType>>
SparseUnorderedWithDupsReader<BitmapType>::create_result_tiles() {
  auto timer_se = stats_->start_timer("create_result_tiles");

  ResultTilesList result_tiles;

  // For easy reference.
  const auto dim_num = array_schema_.dim_num();

  // Create result tiles.
  if (subarray_.is_set()) {
    // Load as many tiles as the memory budget allows.
    bool budget_exceeded = false;
    unsigned int f = 0;
    while (f < tile_offsets_max_frag_idx_ && !budget_exceeded) {
      auto& tile_ranges = tmp_read_state_.tile_ranges(f);
      if (!tmp_read_state_.all_tiles_loaded(f)) {
        if (tile_ranges.empty()) {
          tmp_read_state_.set_all_tiles_loaded(f);
        }

        while (!tile_ranges.empty()) {
          auto& range = tile_ranges.back();
          const auto last_t = tile_ranges.front().second;

          // Add all tiles for this range.
          for (uint64_t t = range.first; t <= range.second; t++) {
            budget_exceeded = add_result_tile(
                dim_num, f, t, last_t, *fragment_metadata_[f], result_tiles);
            if (budget_exceeded) {
              break;
            }

            range.first++;
          }

          if (budget_exceeded) {
            break;
          }

          tmp_read_state_.remove_tile_range(f);
        }
      }

      f++;
    }
  } else {
    // Load as many tiles as the memory budget allows.
    bool budget_exceeded = false;
    unsigned int f = 0;
    while (f < tile_offsets_max_frag_idx_ && !budget_exceeded) {
      if (!tmp_read_state_.all_tiles_loaded(f)) {
        auto tile_num = fragment_metadata_[f]->tile_num();

        // Figure out the start index.
        auto start = read_state_.frag_idx()[f].tile_idx_;

        // Add all tiles for this fragment.
        if (start == tile_num) {
          tmp_read_state_.set_all_tiles_loaded(f);
        }

        for (uint64_t t = start; t < tile_num; t++) {
          budget_exceeded = add_result_tile(
              dim_num,
              f,
              t,
              tile_num - 1,
              *fragment_metadata_[f],
              result_tiles);
          if (budget_exceeded) {
            break;
          }
        }
      }
      f++;
    }
  }

  // Check if we are done adding result tiles.
  bool done_adding_result_tiles = tmp_read_state_.done_adding_result_tiles();

  logger_->debug(
      "Done adding result tiles, num result tiles {0}", result_tiles.size());

  if (done_adding_result_tiles) {
    logger_->debug("All result tiles loaded");
  }

  read_state_.set_done_adding_result_tiles(done_adding_result_tiles);

  return result_tiles;
}

template <class BitmapType>
void SparseUnorderedWithDupsReader<BitmapType>::clean_tile_list(
    ResultTilesList& result_tiles, std::vector<ResultTile*>& result_tiles_ptr) {
  // Clear result tiles that are not necessary anymore.
  uint64_t current = 0;
  for (uint64_t i = 0; i < result_tiles_ptr.size(); i++) {
    if (((ResultTileWithBitmap<BitmapType>*)result_tiles_ptr[i])
            ->result_num() != 0) {
      result_tiles_ptr[current++] = result_tiles_ptr[i];
    }
  }
  result_tiles_ptr.resize(current);

  // Clear result tiles that are not necessary anymore, part 2.
  auto it = result_tiles.begin();
  while (it != result_tiles.end()) {
    auto f = it->frag_idx();
    if (it->result_num() == 0) {
      tmp_read_state_.add_ignored_tile(*it);
      remove_result_tile(f, result_tiles, it++);
    } else {
      it++;
    }
  }
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
void SparseUnorderedWithDupsReader<uint64_t>::copy_offsets_tile(
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
  const auto tile_tuple = rt->tile_tuple(name);

  // If the tile_tuple is null, this is a field added in schema
  // evolution. Use the fill value.
  const offsets_t* src_buff = nullptr;
  const uint8_t* src_var_buff = nullptr;
  bool use_fill_value = false;
  OffType fill_value_size = 0;
  if (tile_tuple == nullptr) {
    use_fill_value = true;
    fill_value_size = static_cast<OffType>(
        array_schema_.attribute(name)->fill_value().size());
    src_var_buff = array_schema_.attribute(name)->fill_value().data();
  } else {
    const auto& t = tile_tuple->fixed_tile();
    const auto& t_var = tile_tuple->var_tile();
    src_buff = t.template data_as<offsets_t>();
    src_var_buff = t_var.template data_as<uint8_t>();
  }

  // Process all cells.
  if (!use_fill_value) {
    for (uint64_t c = src_min_pos; c < src_max_pos; c++) {
      for (uint64_t i = 0; i < rt->bitmap()[c]; i++) {
        *buffer = (OffType)(src_buff[c + 1] - src_buff[c]) / offset_div;
        buffer++;
        *var_data = src_var_buff + src_buff[c];
        var_data++;
      }
    }
  } else {
    for (uint64_t c = src_min_pos; c < src_max_pos; c++) {
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
}

/** Copy offsets with a result bitmap. */
template <>
template <class OffType>
void SparseUnorderedWithDupsReader<uint8_t>::copy_offsets_tile(
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
  const auto tile_tuple = rt->tile_tuple(name);

  // If the tile_tuple is null, this is a field added in schema
  // evolution. Use the fill value.
  const offsets_t* src_buff = nullptr;
  const uint8_t* src_var_buff = nullptr;
  bool use_fill_value = false;
  OffType fill_value_size = 0;

  if (tile_tuple == nullptr) {
    use_fill_value = true;
    fill_value_size = static_cast<OffType>(
        array_schema_.attribute(name)->fill_value().size());
    src_var_buff = array_schema_.attribute(name)->fill_value().data();
  } else {
    const auto& t = tile_tuple->fixed_tile();
    const auto& t_var = tile_tuple->var_tile();
    src_buff = t.template data_as<offsets_t>();
    src_var_buff = t_var.template data_as<uint8_t>();
  }

  if (!rt->copy_full_tile() || use_fill_value) {
    // Process all cells.
    if (!use_fill_value) {
      for (uint64_t c = src_min_pos; c < src_max_pos; c++) {
        if (rt->bitmap()[c]) {
          *buffer = (OffType)(src_buff[c + 1] - src_buff[c]) / offset_div;
          buffer++;
          *var_data = src_var_buff + src_buff[c];
          var_data++;
        }
      }
    } else {
      for (uint64_t c = src_min_pos; c < src_max_pos; c++) {
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
    // Copy full tile.
    for (uint64_t c = src_min_pos; c < src_max_pos; c++) {
      *buffer = (OffType)(src_buff[c + 1] - src_buff[c]) / offset_div;
      buffer++;
      *var_data = src_var_buff + src_buff[c];
      var_data++;
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
}

template <class BitmapType>
template <class OffType>
void SparseUnorderedWithDupsReader<BitmapType>::copy_offsets_tiles(
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
  throw_if_not_ok(parallel_for_2d(
      &resources_.compute_tp(),
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
          min_pos_tile = read_state_.frag_idx()[rt->frag_idx()].cell_idx_;
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
        copy_offsets_tile<OffType>(
            name,
            nullable,
            offset_div,
            &*rt,
            src_min_pos,
            src_max_pos,
            buffer + dest_cell_offset,
            val_buffer + dest_cell_offset,
            &var_data[dest_cell_offset - cell_offsets[0]]);

        return Status::Ok();
      }));
}

/** Copy Var data. */
template <class BitmapType>
template <class OffType>
void SparseUnorderedWithDupsReader<BitmapType>::copy_var_data_tile(
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
}

template <class BitmapType>
template <class OffType>
void SparseUnorderedWithDupsReader<BitmapType>::copy_var_data_tiles(
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
  throw_if_not_ok(parallel_for_2d(
      &resources_.compute_tp(),
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

        copy_var_data_tile(
            last_tile && src_max_pos == max_pos_tile,
            dest_cell_offset - cell_offsets[0],
            offset_div,
            var_buffer_size,
            src_min_pos,
            src_max_pos,
            (const void**)var_data.data(),
            offsets_buffer + dest_cell_offset,
            var_data_buffer);

        return Status::Ok();
      }));
}

/** Copy fixed data with a result count bitmap. */
template <>
void SparseUnorderedWithDupsReader<uint64_t>::copy_fixed_data_tile(
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
}

/** Copy fixed data with a result bitmap. */
template <>
void SparseUnorderedWithDupsReader<uint8_t>::copy_fixed_data_tile(
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
}

template <>
void SparseUnorderedWithDupsReader<uint64_t>::copy_timestamp_data_tile(
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
}

template <>
void SparseUnorderedWithDupsReader<uint8_t>::copy_timestamp_data_tile(
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
}

template <class BitmapType>
void SparseUnorderedWithDupsReader<BitmapType>::copy_fixed_data_tiles(
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
  throw_if_not_ok(parallel_for_2d(
      &resources_.compute_tp(),
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
          min_pos_tile = read_state_.frag_idx()[rt->frag_idx()].cell_idx_;
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
        if (name == constants::timestamps) {
          copy_timestamp_data_tile(
              &*rt,
              src_min_pos,
              src_max_pos,
              buffer + dest_cell_offset * cell_size);
        } else {
          copy_fixed_data_tile(
              name,
              is_dim,
              nullable,
              dim_idx,
              cell_size,
              &*rt,
              src_min_pos,
              src_max_pos,
              buffer + dest_cell_offset * cell_size,
              val_buffer + dest_cell_offset);
        }

        return Status::Ok();
      }));
}

template <class BitmapType>
tuple<bool, std::vector<uint64_t>>
SparseUnorderedWithDupsReader<BitmapType>::resize_fixed_result_tiles_to_copy(
    uint64_t max_num_cells,
    uint64_t initial_cell_offset,
    uint64_t first_tile_min_pos,
    std::vector<ResultTile*>& result_tiles) {
  bool user_buffers_full;
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
    user_buffers_full = cell_offset == max_num_cells;
    cell_offsets.emplace_back(cell_offset);
  } else {
    user_buffers_full = true;
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

  return {user_buffers_full, cell_offsets};
}

template <class BitmapType>
tuple<bool, std::vector<uint64_t>>
SparseUnorderedWithDupsReader<BitmapType>::resize_fixed_results_to_copy(
    const std::vector<std::string>& names,
    std::vector<ResultTile*>& result_tiles) {
  [auto timer_se =
      stats_->start_timer("resize_fixed_results_to_copy");

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
    return {false, std::vector<uint64_t>()};
  }

  uint64_t initial_cell_offset = cells_copied(names);
  uint64_t first_tile_min_pos =
      read_state_.frag_idx()[result_tiles[0]->frag_idx()].cell_idx_;

  return resize_fixed_result_tiles_to_copy(
      max_num_cells, initial_cell_offset, first_tile_min_pos, result_tiles);
}

template <class BitmapType>
std::vector<uint64_t>
SparseUnorderedWithDupsReader<BitmapType>::respect_copy_memory_budget(
    const std::vector<std::string>& names,
    std::vector<ResultTile*>& result_tiles,
    bool& user_buffers_full) {
  // Use either the tile upper memory limit, or the available memory as the
  // upper memory limit, whichever is smaller.
  uint64_t upper_memory_limit =
      std::min(memory_budget_.tile_upper_memory_limit(), available_memory());

  // Process all attributes in parallel.
  uint64_t max_rt_idx = result_tiles.size();
  std::mutex max_rt_idx_mtx;
  std::vector<uint64_t> total_mem_usage_per_attr(names.size());
  throw_if_not_ok(
      parallel_for(&resources_.compute_tp(), 0, names.size(), [&](uint64_t i) {
        // For easy reference.
        const auto& name = names[i];
        const bool agg_only = aggregate_only(name);
        const auto var_sized = array_schema_.var_size(name);
        auto mem_usage = &total_mem_usage_per_attr[i];
        const bool is_timestamps = name == constants::timestamps ||
                                   name == constants::delete_timestamps;

        // For dimensions, when we have a subarray, tiles are already all
        // loaded in memory.
        if ((include_coords_ && array_schema_.is_dim(name)) ||
            qc_loaded_attr_names_set_.count(name) != 0 || is_timestamps ||
            name == constants::count_of_rows) {
          return Status::Ok();
        }

        // Get the size for all tiles.
        uint64_t idx = 0;
        for (; idx < max_rt_idx; idx++) {
          // Size of the tile in memory.
          auto rt = static_cast<UnorderedWithDupsResultTile<BitmapType>*>(
              result_tiles[idx]);

          // Skip this tile if it's aggregate only and we can aggregate it with
          // the fragment metadata only.
          if (agg_only && can_aggregate_tile_with_frag_md(rt)) {
            continue;
          }

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

          // Stop when we reach the upper limit.
          if (*mem_usage + tile_size > upper_memory_limit) {
            // We can allow the first tile to go above the upper limit if it
            // fits the available memory.
            if (idx != 0 || tile_size > available_memory()) {
              break;
            }
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
      }));

  if (max_rt_idx == 0) {
    throw SparseUnorderedWithDupsReaderException(
        "Unable to copy one tile with current budget/buffers");
  }

  // Resize the result tiles vector.
  user_buffers_full &= max_rt_idx == result_tiles.size();
  result_tiles.resize(max_rt_idx);

  return total_mem_usage_per_attr;
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
  [auto timer_se =
      stats->start_timer("switch_sizes_to_offsets");

  auto new_var_buffer_size = *query_buffer.buffer_var_size_;
  auto new_result_tiles_size = result_tiles.size();
  bool caused_overflow = false;

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
    caused_overflow = true;

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

  return {caused_overflow, new_var_buffer_size, new_result_tiles_size};
}

template <class BitmapType>
template <class OffType>
bool SparseUnorderedWithDupsReader<BitmapType>::process_tiles(
    std::vector<std::string>& names, std::vector<ResultTile*>& result_tiles) {
  auto timer_se = stats_->start_timer("process_tiles");

  // Vector for storing the cell offsets of each tiles into the user buffers.
  // This also stores the last offset to facilitate calculations later on.
  auto&& [user_buffers_full, cell_offsets] =
      resize_fixed_results_to_copy(names, result_tiles);

  // Making sure we respect the memory budget for the copy operation.
  auto mem_usage_per_attr =
      respect_copy_memory_budget(names, result_tiles, user_buffers_full);

  // There is no space for any tiles in the user buffer, exit.
  if (result_tiles.empty()) {
    return user_buffers_full;
  }

  // Compute parallelization parameters.
  uint64_t num_range_threads = 1;
  const auto num_threads = resources_.compute_tp().concurrency_level();
  if (result_tiles.size() < num_threads) {
    // Ceil the division between thread_num and tile_num.
    num_range_threads = 1 + ((num_threads - 1) / result_tiles.size());
  }

  // Read a few attributes at a time.
  std::optional<std::string> last_field_to_overflow{std::nullopt};
  uint64_t buffer_idx{0};
  optional<std::vector<ResultTile*>> result_tiles_agg_only;
  while (buffer_idx < names.size()) {
    // Generate a list of filtered result tiles for aggregates only fields.
    bool agg_only = aggregate_only(names[buffer_idx]);
    if (agg_only && result_tiles_agg_only == nullopt) {
      result_tiles_agg_only = std::vector<ResultTile*>();
      for (auto& rt : result_tiles) {
        if (!can_aggregate_tile_with_frag_md(
                static_cast<UnorderedWithDupsResultTile<BitmapType>*>(rt))) {
          result_tiles_agg_only->emplace_back(rt);
        }
      }
    }

    // Read and unfilter as many attributes as can fit in the budget.
    auto names_to_copy = read_and_unfilter_attributes(
        names,
        mem_usage_per_attr,
        &buffer_idx,
        agg_only ? *result_tiles_agg_only : result_tiles,
        agg_only);

    // Process one field at a time for buffers in memory.
    for (const auto& name : names_to_copy) {
      // For easy reference.
      const auto is_dim = array_schema_.is_dim(name);

      // Copy the data only if the name is in the buffer list.
      if (buffers_.count(name) != 0) {
        user_buffers_full |= copy_tiles<OffType>(
            num_range_threads,
            name,
            names_to_copy,
            is_dim,
            cell_offsets,
            result_tiles,
            last_field_to_overflow);
      }

      // Process aggregates.
      if (aggregates_.count(name) != 0) {
        process_aggregates(num_range_threads, name, cell_offsets, result_tiles);
      }

      // Clear tiles from memory.
      if (qc_loaded_attr_names_set_.count(name) == 0 &&
          (!include_coords_ || !is_dim) && name != constants::timestamps &&
          name != constants::delete_timestamps) {
        clear_tiles(name, result_tiles);
      }
    }
  }

  // If any overflow happened after a field with aggregates, we would need to
  // recompute the aggregate. For now just throw an exception as this will only
  // happen in very rare cases.
  if (last_field_to_overflow.has_value()) {
    for (auto& name : names) {
      if (name == last_field_to_overflow.value()) {
        break;
      }

      if (aggregates_.count(name) != 0) {
        for (auto& aggregates : aggregates_[name]) {
          if (aggregates->need_recompute_on_overflow()) {
            throw SparseUnorderedWithDupsReaderException(
                "Overflow happened after aggregate was computed, aggregate "
                "recompute pass is not yet implemented");
          }
        }
      }
    }
  }

  // Compute the number of cells copied for the last tile before updating tile
  // index.
  uint64_t last_tile_cells_copied = 0;
  if (result_tiles.size() > 0) {
    auto last_tile =
        (UnorderedWithDupsResultTile<BitmapType>*)result_tiles.back();
    auto& frag_tile_idx = read_state_.frag_idx()[last_tile->frag_idx()];
    last_tile_cells_copied = cell_offsets[result_tiles.size()] -
                             cell_offsets[result_tiles.size() - 1];
    if (frag_tile_idx.tile_idx_ == last_tile->tile_idx()) {
      last_tile_cells_copied +=
          last_tile->result_num_between_pos(0, frag_tile_idx.cell_idx_);
    }
  }

  // Adjust tile index.
  for (auto rt : result_tiles) {
    read_state_.set_frag_idx(rt->frag_idx(), FragIdx(rt->tile_idx() + 1, 0));
  }

  // If the last tile is not fully copied, save the cell index.
  if (result_tiles.size() > 0) {
    auto last_tile =
        (UnorderedWithDupsResultTile<BitmapType>*)result_tiles.back();
    if (last_tile->result_num() != last_tile_cells_copied) {
      read_state_.set_frag_idx(
          last_tile->frag_idx(),
          FragIdx(
              last_tile->tile_idx(),
              last_tile->pos_with_given_result_sum(0, last_tile_cells_copied) +
                  1));
    }
  }

  logger_->debug("Done processing tiles");
  return user_buffers_full;
}

template <class BitmapType>
template <class OffType>
bool SparseUnorderedWithDupsReader<BitmapType>::copy_tiles(
    const uint64_t num_range_threads,
    const std::string name,
    const std::vector<std::string>& names_to_copy,
    const bool is_dim,
    std::vector<uint64_t>& cell_offsets,
    std::vector<ResultTile*>& result_tiles,
    std::optional<std::string>& last_field_to_overflow) {
  const auto var_sized = array_schema_.var_size(name);
  const auto nullable = array_schema_.is_nullable(name);
  const auto cell_size = array_schema_.cell_size(name);
  auto& query_buffer = buffers_[name];

  bool user_buffers_full = false;

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
    copy_offsets_tiles<OffType>(
        name,
        num_range_threads,
        nullable,
        offset_div,
        result_tiles,
        cell_offsets,
        query_buffer,
        var_data);
  } else {
    copy_fixed_data_tiles(
        name,
        num_range_threads,
        is_dim,
        nullable,
        dim_idx,
        cell_size,
        result_tiles,
        cell_offsets,
        query_buffer);
  }

  uint64_t var_buffer_size = 0;
  if (var_sized) {
    uint64_t first_tile_min_pos =
        read_state_.frag_idx()[result_tiles[0]->frag_idx()].cell_idx_;

    // Adjust the offsets buffer and make sure all data fits.
    auto&& [caused_overflow, new_var_buffer_size, new_result_tiles_size] =
        compute_var_size_offsets<OffType>(
            stats_,
            result_tiles,
            first_tile_min_pos,
            cell_offsets,
            query_buffer);
    user_buffers_full |= caused_overflow;

    // Save the last field to overflow.
    if (caused_overflow) {
      last_field_to_overflow = name;
    }

    // Clear tiles from memory and adjust result_tiles.
    for (const auto& name_to_clear : names_to_copy) {
      const auto is_dim_to_clear = array_schema_.is_dim(name_to_clear);
      if (qc_loaded_attr_names_set_.count(name_to_clear) == 0 &&
          (!include_coords_ || !is_dim_to_clear)) {
        clear_tiles(name_to_clear, result_tiles, new_result_tiles_size);
      }
    }

    // If we shrink the size of the result tiles, we might have aggregates to
    // recompute.
    if (result_tiles.size() != new_result_tiles_size) {
    }

    result_tiles.resize(new_result_tiles_size);

    // Now copy the var size data.
    copy_var_data_tiles(
        num_range_threads,
        offset_div,
        new_var_buffer_size,
        result_tiles,
        cell_offsets,
        query_buffer,
        var_data);

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

  if (nullable) {
    *query_buffer.validity_vector_.buffer_size() = total_cells;
  }

  return user_buffers_full;
}

template <class BitmapType>
AggregateBuffer
SparseUnorderedWithDupsReader<BitmapType>::make_aggregate_buffer(
    const std::string name,
    const bool var_sized,
    const bool nullable,
    const uint64_t cell_size,
    const bool count_bitmap,
    const uint64_t min_cell,
    const uint64_t max_cell,
    UnorderedWithDupsResultTile<BitmapType>& rt) {
  return AggregateBuffer(
      min_cell,
      max_cell,
      name == constants::count_of_rows ?
          nullptr :
          rt.tile_tuple(name)->fixed_tile().data(),
      var_sized ?
          std::make_optional(
              rt.tile_tuple(name)->var_tile().template data_as<char>()) :
          nullopt,
      nullable ? std::make_optional(rt.tile_tuple(name)
                                        ->validity_tile()
                                        .template data_as<uint8_t>()) :
                 nullopt,
      count_bitmap,
      rt.bitmap().data() != nullptr ? std::make_optional(rt.bitmap().data()) :
                                      nullopt,
      cell_size);
}

template <class BitmapType>
void SparseUnorderedWithDupsReader<BitmapType>::process_aggregates(
    const uint64_t num_range_threads,
    const std::string name,
    std::vector<uint64_t>& cell_offsets,
    std::vector<ResultTile*>& result_tiles) {
  auto& aggregates = aggregates_[name];
  const bool validity_only = null_count_aggregate_only(name);

  bool var_sized = false;
  bool nullable = false;
  unsigned cell_val_num = 0;

  if (name != constants::count_of_rows) {
    var_sized = array_schema_.var_size(name);
    nullable = array_schema_.is_nullable(name);
    cell_val_num = array_schema_.cell_val_num(name);
  }

  const bool count_bitmap = std::is_same<BitmapType, uint64_t>::value;

  // Process all tiles/cells in parallel.
  throw_if_not_ok(parallel_for_2d(
      &resources_.compute_tp(),
      0,
      result_tiles.size(),
      0,
      num_range_threads,
      [&](uint64_t i, uint64_t range_thread_idx) {
        // For easy reference.
        auto rt = (UnorderedWithDupsResultTile<BitmapType>*)result_tiles[i];

        // The first tile might have already been processed by the last
        // computation. We only process a tile the first time.
        if (i == 0 && read_state_.frag_idx()[rt->frag_idx()].cell_idx_ != 0) {
          return Status::Ok();
        }

        if (can_aggregate_tile_with_frag_md(rt)) {
          if (range_thread_idx == 0) {
            auto t = rt->tile_idx();
            auto md =
                fragment_metadata_[rt->frag_idx()]->get_tile_metadata(name, t);
            for (auto& aggregate : aggregates) {
              aggregate->aggregate_tile_with_frag_md(md);
            }
          }
        } else {
          uint64_t cell_num =
              fragment_metadata_[rt->frag_idx()]->cell_num(rt->tile_idx());
          auto&& [skip_aggregate, src_min_pos, src_max_pos, dest_cell_offset] =
              compute_parallelization_parameters(
                  range_thread_idx,
                  num_range_threads,
                  0,
                  cell_num,
                  cell_offsets[i],
                  nullptr);
          if (skip_aggregate) {
            return Status::Ok();
          }

          // Compute aggregate.
          AggregateBuffer aggregate_buffer{make_aggregate_buffer(
              name,
              var_sized && !validity_only,
              nullable,
              cell_val_num,
              count_bitmap,
              src_min_pos,
              src_max_pos,
              *rt)};
          for (auto& aggregate : aggregates) {
            aggregate->aggregate_data(aggregate_buffer);
          }
        }

        return Status::Ok();
      }));
}

template <class BitmapType>
void SparseUnorderedWithDupsReader<BitmapType>::remove_result_tile(
    const unsigned frag_idx,
    ResultTilesList& result_tiles,
    typename ResultTilesList::iterator rt) {
  // Remove coord tile size from memory budget.
  const auto tile_idx = rt->tile_idx();
  auto tiles_size =
      get_coord_tiles_size(array_schema_.dim_num(), frag_idx, tile_idx);

  memory_used_for_coords_total_ -= tiles_size;

  // Delete the tile.
  result_tiles.erase(rt);
}

template <class BitmapType>
void SparseUnorderedWithDupsReader<BitmapType>::end_iteration(
    ResultTilesList& result_tiles) {
  // Clear result tiles that are not necessary anymore.
  while (
      !result_tiles.empty() &&
      result_tiles.front().tile_idx() <
          read_state_.frag_idx()[result_tiles.front().frag_idx()].tile_idx_) {
    const auto f = result_tiles.front().frag_idx();

    remove_result_tile(f, result_tiles, result_tiles.begin());
  }

  result_tiles_leftover_ = std::move(result_tiles);

  // Validate memory usage.
  if (!incomplete()) {
    iassert(memory_used_for_coords_total_ == 0);
    iassert(tmp_read_state_.memory_used_tile_ranges() == 0);
  }

  logger_->debug(
      "Done with iteration, num result tiles {0}", result_tiles.size());

  const auto uint64_t_max = std::numeric_limits<uint64_t>::max();
  array_memory_tracker_->set_budget(uint64_t_max);
}

// Explicit template instantiations
template SparseUnorderedWithDupsReader<uint8_t>::SparseUnorderedWithDupsReader(
    stats::Stats*, shared_ptr<Logger>, StrategyParams&);
template SparseUnorderedWithDupsReader<uint64_t>::SparseUnorderedWithDupsReader(
    stats::Stats*, shared_ptr<Logger>, StrategyParams&);

}  // namespace tiledb::sm
