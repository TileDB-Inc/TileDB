/**
 * @file   sparse_global_order_reader.cc
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
 * This file implements class SparseGlobalOrderReader.
 */

#include "tiledb/sm/query/readers/sparse_global_order_reader.h"
#include "tiledb/common/algorithm/parallel_merge.h"
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
#include "tiledb/sm/query/hilbert_order.h"
#include "tiledb/sm/query/query_macros.h"
#include "tiledb/sm/query/readers/result_tile.h"
#include "tiledb/sm/stats/global_stats.h"
#include "tiledb/sm/subarray/subarray.h"
#include "tiledb/sm/tile/comparators.h"

using namespace tiledb;
using namespace tiledb::common;
using namespace tiledb::sm::stats;

namespace tiledb::sm {

class SparseGlobalOrderReaderException : public StatusException {
 public:
  explicit SparseGlobalOrderReaderException(const std::string& message)
      : StatusException("SparseGlobalOrderReader", message) {
  }
};

/* ****************************** */
/*          CONSTRUCTORS          */
/* ****************************** */

template <class BitmapType>
SparseGlobalOrderReader<BitmapType>::SparseGlobalOrderReader(
    stats::Stats* stats,
    shared_ptr<Logger> logger,
    StrategyParams& params,
    bool consolidation_with_timestamps)
    : SparseIndexReaderBase(
          "sparse_global_order",
          stats,
          logger->clone("SparseUnorderedWithDupsReader", ++logger_id_),
          params,
          true)
    , result_tiles_leftover_(array_->fragment_metadata().size())
    , consolidation_with_timestamps_(consolidation_with_timestamps)
    , last_cells_(array_->fragment_metadata().size())
    , tile_offsets_loaded_(false) {
  // Initialize memory budget variables.
  refresh_config();
}

/* ****************************** */
/*               API              */
/* ****************************** */

template <class BitmapType>
bool SparseGlobalOrderReader<BitmapType>::incomplete() const {
  return !read_state_.done_adding_result_tiles() ||
         memory_used_for_coords_total_ != 0;
}

template <class BitmapType>
QueryStatusDetailsReason
SparseGlobalOrderReader<BitmapType>::status_incomplete_reason() const {
  if (array_->is_remote()) {
    return QueryStatusDetailsReason::REASON_USER_BUFFER_SIZE;
  }

  return incomplete() ? QueryStatusDetailsReason::REASON_USER_BUFFER_SIZE :
                        QueryStatusDetailsReason::REASON_NONE;
}

template <class BitmapType>
void SparseGlobalOrderReader<BitmapType>::refresh_config() {
  memory_budget_.refresh_config(config_, "sparse_global_order");
}

template <class BitmapType>
Status SparseGlobalOrderReader<BitmapType>::dowork() {
  auto timer_se = stats_->start_timer("dowork");
  stats_->add_counter("loop_num", 1);

  // Check that the query condition is valid.
  if (condition_.has_value()) {
    throw_if_not_ok(condition_->check(array_schema_));
  }

  get_dim_attr_stats();

  // Start with out buffer sizes as zero.
  zero_out_buffer_sizes();

  // Handle empty array.
  if (fragment_metadata_.empty()) {
    read_state_.set_done_adding_result_tiles(true);
    return Status::Ok();
  }

  subarray_.reset_default_ranges();

  // Load initial data, if not loaded already.
  throw_if_not_ok(load_initial_data());

  // Determine result tile order
  // (this happens after load_initial_data which identifies which tiles pass
  // subarray)
  if (result_tile_ids_.empty()) {
    compute_result_tile_order();
  }

  purge_deletes_consolidation_ = !deletes_consolidation_no_purge_ &&
                                 consolidation_with_timestamps_ &&
                                 !delete_and_update_conditions_.empty();
  purge_deletes_no_dups_mode_ =
      !array_schema_.allows_dups() && purge_deletes_consolidation_;

  // Load tile offsets, if required.
  // FIXME: only needs to load offsets in result_tile_ids_
  load_all_tile_offsets();

  // Field names to process.
  std::vector<std::string> names = field_names_to_process();

  bool user_buffers_full = false;
  std::vector<ResultTilesList> result_tiles = std::move(result_tiles_leftover_);
  do {
    stats_->add_counter("internal_loop_num", 1);

    // Create the result tiles we are going to process.
    auto created_tiles = create_result_tiles(result_tiles);

    if (created_tiles.size() > 0) {
      // Read and unfilter coords.
      throw_if_not_ok(read_and_unfilter_coords(created_tiles));

      // Compute the tile bitmaps.
      compute_tile_bitmaps<BitmapType>(created_tiles);

      // Apply query condition.
      apply_query_condition<GlobalOrderResultTile<BitmapType>, BitmapType>(
          created_tiles);

      // Run deduplication for tiles with timestamps, if required.
      dedup_tiles_with_timestamps(created_tiles);

      // Compute hilbert values.
      if (array_schema_.cell_order() == Layout::HILBERT) {
        compute_hilbert_values(created_tiles);
      }

      // Clear result tiles that are not necessary anymore.
      clean_tile_list(result_tiles);
    }

    // For fragments with timestamps, check first and last cell of every tiles
    // and if they have the same coordinates, only keep the cell with the
    // greater timestamp.
    dedup_fragments_with_timestamps(result_tiles);

    // Compute RCS.
    std::vector<ResultCellSlab> result_cell_slabs;
    if (array_schema_.cell_order() == Layout::HILBERT) {
      auto&& [user_buffs_full, rcs] =
          merge_result_cell_slabs<HilbertCmpReverse>(
              max_num_cells_to_copy(), result_tiles);
      user_buffers_full = user_buffs_full;
      result_cell_slabs = std::move(rcs);
    } else {
      auto&& [user_buffs_full, rcs] = merge_result_cell_slabs<GlobalCmpReverse>(
          max_num_cells_to_copy(), result_tiles);
      user_buffers_full = user_buffs_full;
      result_cell_slabs = std::move(rcs);
    }

    // No more tiles to process, done.
    if (!result_cell_slabs.empty()) {
      // Copy cell slabs.
      if (offsets_bitsize_ == 64) {
        process_slabs<uint64_t>(names, result_cell_slabs, user_buffers_full);
      } else {
        process_slabs<uint32_t>(names, result_cell_slabs, user_buffers_full);
      }
    }

    // End the iteration.
    end_iteration(result_tiles);
  } while (!user_buffers_full && incomplete());

  result_tiles_leftover_ = std::move(result_tiles);

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
void SparseGlobalOrderReader<BitmapType>::reset() {
}

template <class BitmapType>
std::string SparseGlobalOrderReader<BitmapType>::name() {
  return "SparseGlobalOrderReader<" + std::string(typeid(BitmapType).name()) +
         ">";
}

template <class BitmapType>
void SparseGlobalOrderReader<BitmapType>::load_all_tile_offsets() {
  if (!tile_offsets_loaded_) {
    // Make sure we have enough space for tile offsets data.
    uint64_t total_tile_offset_usage =
        tile_offsets_size(subarray_.relevant_fragments());
    uint64_t available_memory = array_memory_tracker_->get_memory_available();
    if (total_tile_offset_usage > available_memory) {
      throw SparseGlobalOrderReaderException(
          "Cannot load tile offsets, computed size (" +
          std::to_string(total_tile_offset_usage) +
          ") is larger than available memory (" +
          std::to_string(available_memory) +
          "), increase memory budget. Total budget for array data (" +
          std::to_string(array_memory_tracker_->get_memory_budget()) + ").");
    }

    tile_offsets_loaded_ = true;
    load_tile_offsets_for_fragments(subarray_.relevant_fragments());
  }
}

template <class BitmapType>
uint64_t SparseGlobalOrderReader<BitmapType>::get_coord_tiles_size(
    unsigned dim_num, unsigned f, uint64_t t) {
  auto tiles_size =
      SparseIndexReaderBase::get_coord_tiles_size<BitmapType>(dim_num, f, t);
  auto frag_meta = fragment_metadata_[f];

  // Add the result tile structure size.
  tiles_size += sizeof(GlobalOrderResultTile<BitmapType>);

  // Account for hilbert data.
  if (array_schema_.cell_order() == Layout::HILBERT) {
    tiles_size += fragment_metadata_[f]->cell_num(t) * sizeof(uint64_t);
  }

  // Add the tile bitmap size if there is a subarray or pre query condition to
  // be processed.
  const bool dups = array_schema_.allows_dups();
  if (subarray_.is_set() || process_partial_timestamps(*frag_meta) ||
      (dups && has_post_deduplication_conditions(*frag_meta))) {
    tiles_size += frag_meta->cell_num(t) * sizeof(BitmapType);
  }

  // Add the extra bitmap size if there is a condition to process and no dups.
  // We will also create the bitmap as a temporay bitmap to compute delete
  // condition results.
  if ((!dups && has_post_deduplication_conditions(*frag_meta)) ||
      deletes_consolidation_no_purge_) {
    tiles_size += frag_meta->cell_num(t) * sizeof(BitmapType);
  }

  return tiles_size;
}

template <class BitmapType>
bool SparseGlobalOrderReader<BitmapType>::add_result_tile(
    const unsigned dim_num,
    const uint64_t memory_budget_coords_tiles,
    const unsigned f,
    const uint64_t t,
    const FragmentMetadata& frag_md,
    std::vector<ResultTilesList>& result_tiles) {
  if (tmp_read_state_.is_ignored_tile(f, t)) {
    return false;
  }

  // Calculate memory consumption for this tile.
  auto tiles_size = get_coord_tiles_size(dim_num, f, t);

  // Don't load more tiles than the memory budget.
  if (memory_used_for_coords_total_ + tiles_size > memory_budget_coords_tiles) {
    return true;
  }

  // Adjust total memory used.
  memory_used_for_coords_total_ += tiles_size;

  // Add the tile.
  result_tiles[f].emplace_back(
      f,
      t,
      array_schema_.allows_dups(),
      deletes_consolidation_no_purge_,
      frag_md,
      query_memory_tracker_);

  return false;
}

/**
 * @precondition the `TempReadState` is up to date with which tiles pass the
 * subarray (e.g. by calling `load_initial_data`)
 */
template <class BitmapType>
void SparseGlobalOrderReader<BitmapType>::compute_result_tile_order() {
  const auto& relevant_fragments = subarray_.relevant_fragments();
  const uint64_t num_relevant_fragments = relevant_fragments.size();

  // first apply subarray (in parallel)
  std::vector<std::vector<ResultTileId>> fragment_result_tiles(
      fragment_metadata_.size());

  // TODO: ideally this could be async or on demand for each tile
  // so that we could be closer to a proper LIMIT
  subarray_.load_relevant_fragment_rtrees(&resources_.compute_tp());

  if (!subarray_.is_set()) {
    for (const auto& f : relevant_fragments) {
      fragment_result_tiles[f].reserve(fragment_metadata_[f]->tile_num());
    }
    auto populate_fragment_relevant_tiles = parallel_for(
        &resources_.compute_tp(), 0, num_relevant_fragments, [&](unsigned rf) {
          const unsigned f = relevant_fragments[rf];
          fragment_result_tiles[f].reserve(fragment_metadata_[f]->tile_num());
          for (uint64_t t = 0; t < fragment_metadata_[f]->tile_num(); t++) {
            fragment_result_tiles[f].push_back(
                ResultTileId{.fragment_idx_ = f, .tile_idx_ = t});
          }
          return Status::Ok();
        });
    throw_if_not_ok(populate_fragment_relevant_tiles);
  } else {
    /**
     * Determines which tiles from a given fragment qualify for the subarray.
     */
    auto populate_relevant_tiles = [&](unsigned rf) {
      const unsigned f = relevant_fragments[rf];

      std::vector<bool> tile_is_relevant(
          fragment_metadata_[f]->tile_num(), false);

      for (uint64_t t = 0; t < fragment_metadata_[f]->tile_num(); t++) {
        if (tmp_read_state_.contains_tile(f, t)) {
          tile_is_relevant[t] = true;
        }
      }

      const uint64_t num_relevant_tiles =
          std::count(tile_is_relevant.begin(), tile_is_relevant.end(), true);

      fragment_result_tiles[f].reserve(num_relevant_tiles);
      for (uint64_t t = 0; t < tile_is_relevant.size(); t++) {
        if (tile_is_relevant[t]) {
          fragment_result_tiles[f].push_back(
              ResultTileId{.fragment_idx_ = f, .tile_idx_ = t});
        }
      }

      return Status::Ok();
    };

    auto populate_fragment_relevant_tiles = parallel_for(
        &resources_.compute_tp(),
        0,
        num_relevant_fragments,
        populate_relevant_tiles);
    throw_if_not_ok(populate_fragment_relevant_tiles);

    tmp_read_state_.clear_tile_ranges();
  }

  size_t num_result_tiles = 0;
  for (const auto& fragment : fragment_result_tiles) {
    num_result_tiles += fragment.size();
  }

  /* then do parallel merge */
  std::vector<std::span<ResultTileId>> fragment_result_tile_spans;
  fragment_result_tile_spans.reserve(fragment_result_tiles.size());
  for (auto& f : fragment_result_tiles) {
    fragment_result_tile_spans.push_back(std::span(f));
  }

  std::vector<ResultTileId> merged_result_tiles(
      num_result_tiles, ResultTileId{.fragment_idx_ = 0, .tile_idx_ = 0});

  algorithm::ParallelMergeOptions merge_options = {
      .parallel_factor = resources_.compute_tp().concurrency_level(),
      .min_merge_items =
          128  // TODO: do some experiments to figure something out
  };

  algorithm::ParallelMergeMemoryResources merge_resources(
      *query_memory_tracker_.get());

  auto do_global_order_merge = [&]<Layout TILE_ORDER, Layout CELL_ORDER>() {
    GlobalOrderMBRCmp<TILE_ORDER, CELL_ORDER> cmp(
        array_schema_.domain(), fragment_metadata_);
    auto merge_stream = algorithm::parallel_merge<ResultTileId, decltype(cmp)>(
        resources_.compute_tp(),
        merge_resources,
        merge_options,
        fragment_result_tile_spans,
        cmp,
        &merged_result_tiles[0]);

    // TODO: we can begin the query as soon as the first merge unit is
    // done. this will require extending the lifetime of `cmp`.
    merge_stream->block();
  };

  switch (array_schema_.cell_order()) {
    case Layout::ROW_MAJOR:
      switch (array_schema_.tile_order()) {
        case Layout::ROW_MAJOR: {
          do_global_order_merge
              .template operator()<Layout::ROW_MAJOR, Layout::ROW_MAJOR>();
          break;
        }
        case Layout::COL_MAJOR: {
          do_global_order_merge
              .template operator()<Layout::ROW_MAJOR, Layout::COL_MAJOR>();
          break;
        }
        default:
          abort();  // TODO: rpobably an error?
      }
      break;
    case Layout::COL_MAJOR:
      switch (array_schema_.tile_order()) {
        case Layout::ROW_MAJOR: {
          do_global_order_merge
              .template operator()<Layout::COL_MAJOR, Layout::ROW_MAJOR>();
          break;
        }
        case Layout::COL_MAJOR: {
          do_global_order_merge
              .template operator()<Layout::COL_MAJOR, Layout::COL_MAJOR>();
          break;
        }
        default:
          abort();  // TODO: rpobably an error?
      }
      break;
    case Layout::HILBERT:
    default:
      abort();  // TODO
  }

  result_tile_ids_ = merged_result_tiles;
  result_tile_cursor_ = 0;
}

template <class BitmapType>
std::vector<ResultTile*>
SparseGlobalOrderReader<BitmapType>::create_result_tiles(
    std::vector<ResultTilesList>& result_tiles) {
  auto timer_se = stats_->start_timer("create_result_tiles");

  // For easy reference.
  const auto num_fragments = fragment_metadata_.size();
  const auto num_dims = array_schema_.dim_num();

  // Distinguish between leftover result tiles from the previous `submit`
  // and result tiles which are being added now
  std::vector<uint64_t> rt_list_num_tiles(result_tiles.size());
  for (uint64_t i = 0; i < result_tiles.size(); i++) {
    rt_list_num_tiles[i] = result_tiles[i].size();
  }

  if (result_tile_cursor_ < result_tile_ids_.size()) {
    size_t rt;
    for (rt = result_tile_cursor_; rt < result_tile_ids_.size(); rt++) {
      const auto f = result_tile_ids_[rt].fragment_idx_;
      const auto t = result_tile_ids_[rt].tile_idx_;

      auto budget_exceeded = add_result_tile(
          num_dims,
          memory_budget_.coordinates_budget(),
          f,
          t,
          *fragment_metadata_[f],
          result_tiles);

      if (budget_exceeded) {
        logger_->debug(
            "Budget exceeded adding result tiles, fragment {0}, tile "
            "{1}",
            f,
            t);

        if (rt == result_tile_cursor_) {
          // this means we cannot safely produce any results
          const auto tiles_size = get_coord_tiles_size(num_dims, f, t);
          throw SparseGlobalOrderReaderException(
              "Cannot load a single tile, increase memory budget: "
              "next tile size = " +
              std::to_string(tiles_size) + ", total_budget = " +
              std::to_string(memory_budget_.total_budget()));
        } else {
          // this tile has the lowest MBR lower bound of the remaining tiles,
          // we cannot safely emit cells exceeding its lower bound later
          break;
        }
      }
    }

    // update position for next iteration
    result_tile_cursor_ = rt;

    if (result_tile_cursor_ == result_tile_ids_.size()) {
      // TODO: original version sets a flag in tmp_read_state_ on a per-fragment
      // basis, does that have any effect other than computing this?
      read_state_.set_done_adding_result_tiles(true);

      for (unsigned f = 0; f < num_fragments; f++) {
        tmp_read_state_.set_all_tiles_loaded(f);
      }
    }
  }

  bool done_adding_result_tiles = tmp_read_state_.done_adding_result_tiles();
  uint64_t num_rt = 0;
  for (unsigned int f = 0; f < num_fragments; f++) {
    num_rt += result_tiles[f].size();
  }

  logger_->debug("Done adding result tiles, num result tiles {0}", num_rt);

  if (done_adding_result_tiles) {
    logger_->debug("All result tiles loaded");
  }

  read_state_.set_done_adding_result_tiles(done_adding_result_tiles);

  // Return the list of tiles added.
  std::vector<ResultTile*> created_tiles;
  for (uint64_t i = 0; i < result_tiles.size(); i++) {
    TileListIt it = result_tiles[i].begin();
    std::advance(it, rt_list_num_tiles[i]);
    for (; it != result_tiles[i].end(); ++it) {
      created_tiles.emplace_back(&*it);
    }
  }

  return created_tiles;
}

template <class BitmapType>
void SparseGlobalOrderReader<BitmapType>::clean_tile_list(
    std::vector<ResultTilesList>& result_tiles) {
  // Clear result tiles that are not necessary anymore.
  auto fragment_num = fragment_metadata_.size();
  throw_if_not_ok(
      parallel_for(&resources_.compute_tp(), 0, fragment_num, [&](uint64_t f) {
        auto it = result_tiles[f].begin();
        while (it != result_tiles[f].end()) {
          if (it->result_num() == 0) {
            tmp_read_state_.add_ignored_tile(*it);
            remove_result_tile(f, it++, result_tiles);
          } else {
            it++;
          }
        }

        return Status::Ok();
      }));
}

template <class BitmapType>
void SparseGlobalOrderReader<BitmapType>::dedup_tiles_with_timestamps(
    std::vector<ResultTile*>& result_tiles) {
  // For consolidation with timestamps or arrays with duplicates, no need to
  // do deduplication.
  if (consolidation_with_timestamps_ || array_schema_.allows_dups()) {
    return;
  }

  auto timer_se = stats_->start_timer("dedup_tiles_with_timestamps");

  // Process all tiles in parallel.
  throw_if_not_ok(parallel_for(
      &resources_.compute_tp(), 0, result_tiles.size(), [&](uint64_t t) {
        const auto f = result_tiles[t]->frag_idx();
        if (fragment_metadata_[f]->has_timestamps()) {
          // For easy reference.
          auto rt =
              static_cast<GlobalOrderResultTile<BitmapType>*>(result_tiles[t]);
          auto cell_num =
              fragment_metadata_[rt->frag_idx()]->cell_num(rt->tile_idx());

          // Make a bitmap if necessary.
          if (!rt->has_bmp()) {
            rt->alloc_bitmap();
          }

          // Process all cells.
          uint64_t c = 0;
          while (c < cell_num - 1) {
            // If the cell is in the bitmap.
            if (rt->bitmap()[c]) {
              // Save the current cell timestamp as max and move to the next.
              uint64_t max_timestamp = rt->timestamp(c);
              uint64_t max = c;
              c++;

              // Process all cells with the same coordinates and keep only the
              // one with the biggest timestamp in the bitmap.
              while (c < cell_num && rt->same_coords(max, c)) {
                // If the cell is in the bitmap.
                if (rt->bitmap()[c]) {
                  uint64_t current_timestamp = rt->timestamp(c);

                  // If the current cell has a bigger timestamp, clear the old
                  // max in the bitmap and save the new max.
                  if (current_timestamp > max_timestamp) {
                    rt->clear_cell(max);
                    max_timestamp = current_timestamp;
                    max = c;
                  } else {
                    // Clear this cell from the bitmap.
                    rt->clear_cell(c);
                  }
                }

                // Next cell.
                c++;
              }
            } else {
              // Cell not in bitmap, move to next.
              c++;
            }
          }

          // Count new number of cells in the bitmap.
          rt->count_cells();
        }

        return Status::Ok();
      }));

  logger_->debug("Done processing fragments with timestamps");
}

template <class BitmapType>
void SparseGlobalOrderReader<BitmapType>::dedup_fragments_with_timestamps(
    std::vector<ResultTilesList>& result_tiles) {
  // For consolidation with timestamps or arrays with duplicates, no need to
  // do deduplication.
  if (consolidation_with_timestamps_ || array_schema_.allows_dups()) {
    return;
  }

  auto timer_se = stats_->start_timer("dedup_fragments_with_timestamps");

  // Run all fragments in parallel.
  auto fragment_num = fragment_metadata_.size();
  throw_if_not_ok(
      parallel_for(&resources_.compute_tp(), 0, fragment_num, [&](uint64_t f) {
        // Run only for fragments with timestamps.
        if (fragment_metadata_[f]->has_timestamps()) {
          // Process all tiles.
          auto it = result_tiles[f].begin();
          while (it != result_tiles[f].end()) {
            // Compare the current tile to the next.
            auto next_tile = it;
            next_tile++;
            if (next_tile == result_tiles[f].end()) {
              // No more tiles, save the last cell for this fragment for later
              // processing.
              last_cells_[f] =
                  FragIdx(it->tile_idx(), it->last_cell_in_bitmap());
              it++;
            } else {
              // Compare the last tile from current to the first from next.
              auto last = it->last_cell_in_bitmap();
              auto first = next_tile->first_cell_in_bitmap();
              if (!it->same_coords(*next_tile, last, first)) {
                // Not the same coords, move to the next tile.
                it++;
              } else {
                // Same coords, compare timestamps.
                if (it->timestamp(last) > next_tile->timestamp(first)) {
                  // Remove the cell in the next tile.
                  if (next_tile->result_num() == 1) {
                    // Only one cell in the bitmap, delete next tile.
                    // Stay on this tile as we will compare to the new next.
                    tmp_read_state_.add_ignored_tile(*next_tile);
                    remove_result_tile(f, next_tile, result_tiles);
                  } else {
                    // Remove the cell in the bitmap and move to the next tile.
                    next_tile->clear_cell(first);
                    it++;
                  }
                } else {
                  // Remove the cell in the current tile.
                  if (next_tile->result_num() == 1) {
                    // Only one cell in the bitmap, delete current tile.
                    auto to_delete = it;
                    it++;
                    tmp_read_state_.add_ignored_tile(*to_delete);
                    remove_result_tile(f, to_delete, result_tiles);
                  } else {
                    // Remove the cell in the bitmap and move to the next tile.
                    it->clear_cell(last);
                    it++;
                  }
                }
              }
            }
          }
        }

        return Status::Ok();
      }));
}

template <class BitmapType>
uint64_t SparseGlobalOrderReader<BitmapType>::max_num_cells_to_copy() {
  auto timer_se = stats_->start_timer("max_num_cells_to_copy");

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

  return num_cells;
}

template <class BitmapType>
template <class CompType>
bool SparseGlobalOrderReader<BitmapType>::add_all_dups_to_queue(
    GlobalOrderResultCoords<BitmapType>& rc,
    std::vector<TileListIt>& result_tiles_it,
    const std::vector<ResultTilesList>& result_tiles,
    TileMinHeap<CompType>& tile_queue,
    std::vector<TileListIt>& to_delete) {
  auto frag_idx = rc.tile_->frag_idx();
  auto dups = array_schema_.allows_dups();
  uint64_t last_cell_pos;
  if (rc.tile_->has_bmp()) {
    last_cell_pos = rc.tile_->last_cell_in_bitmap();
  } else {
    last_cell_pos =
        fragment_metadata_[frag_idx]->cell_num(rc.tile_->tile_idx()) - 1;
  }

  while (rc.next_cell_same_coords()) {
    // Construct a new result coords that specifies it has no next cell.
    // A cell will be added after this one so we don't want to process it
    // twice.
    tile_queue.emplace(rc.tile_, rc.pos_, false);
    rc.advance_to_next_cell();

    // For arrays with no duplicates, we cannot use the last cell of a
    // fragment with timestamps if not all tiles are loaded.
    if (!dups && last_in_memory_cell_of_consolidated_fragment(
                     frag_idx, rc, result_tiles)) {
      return true;
    }

    // If we are at the last cell of this tile, check the next tile.
    if (rc.pos_ == last_cell_pos) {
      auto next_tile = result_tiles_it[frag_idx];
      next_tile++;
      if (next_tile != result_tiles[frag_idx].end()) {
        GlobalOrderResultCoords rc2(&*next_tile, 0);

        // All tiles should at least have one cell available.
        if (!rc2.advance_to_next_cell()) {
          throw std::logic_error("All tiles should have at least one cell.");
        }

        // Next tile starts with the same coords, switch to it.
        if (rc.same_coords(rc2)) {
          tile_queue.emplace(rc.tile_, rc.pos_, false);

          // Remove the current tile if not used.
          if (!rc.tile_->used()) {
            tmp_read_state_.add_ignored_tile(*result_tiles_it[frag_idx]);
            to_delete.emplace_back(result_tiles_it[frag_idx]);
          }

          result_tiles_it[frag_idx] = next_tile;
          rc = rc2;
        }
      }
    }
  }

  return false;
}

template <class BitmapType>
template <class CompType>
bool SparseGlobalOrderReader<BitmapType>::add_next_cell_to_queue(
    GlobalOrderResultCoords<BitmapType>& rc,
    std::vector<TileListIt>& result_tiles_it,
    const std::vector<ResultTilesList>& result_tiles,
    TileMinHeap<CompType>& tile_queue,
    std::vector<TileListIt>& to_delete) {
  auto frag_idx = rc.tile_->frag_idx();
  auto dups = array_schema_.allows_dups();

  // Exit early if the result coords specifies it has no next cell to process.
  // This would be because a cell after this one in the fragment was added to
  // the queue as it had the same coordinates as this one.
  if (!rc.has_next_) {
    return false;
  }

  // Try the next cell in the same tile.
  if (!rc.advance_to_next_cell()) {
    // Save the potential tile to delete and increment the tile iterator.
    auto to_delete_it = result_tiles_it[frag_idx];
    result_tiles_it[frag_idx]++;

    // Remove the tile from result tiles if it wasn't used at all.
    if (!rc.tile_->used()) {
      tmp_read_state_.add_ignored_tile(*to_delete_it);
      to_delete.push_back(to_delete_it);
    }

    // Try to find a new tile.
    if (result_tiles_it[frag_idx] != result_tiles[frag_idx].end()) {
      // Find a cell in the current result tile.
      rc = GlobalOrderResultCoords(&*result_tiles_it[frag_idx], 0);

      // All tiles should at least have one cell available.
      if (!rc.advance_to_next_cell()) {
        throw std::logic_error("All tiles should have at least one cell.");
      }
    } else {
      // Increment the tile index, which should clear all tiles in
      // end_iteration.
      if (!result_tiles[frag_idx].empty()) {
        uint64_t new_tile_idx = read_state_.frag_idx()[frag_idx].tile_idx_ + 1;
        read_state_.set_frag_idx(frag_idx, FragIdx(new_tile_idx, 0));
      }

      // This fragment has more tiles potentially.
      if (!tmp_read_state_.all_tiles_loaded(frag_idx)) {
        // Return we need more tiles.
        return true;
      }

      // All tiles processed, done.
      return false;
    }
  }

  // We have a cell, add it to the list.
  {
    // For arrays with no duplicates, we cannot use the last cell of a fragment
    //  with timestamps if not all tiles are loaded.
    if (!dups && last_in_memory_cell_of_consolidated_fragment(
                     frag_idx, rc, result_tiles)) {
      return true;
    }

    // If the cell value exceeds the lower bound of the un-populated result
    // tiles then it is not correct to emit it; hopefully we cleared out
    // a tile somewhere and trying again will make progress
    if (result_tile_cursor_ < result_tile_ids_.size()) {
      const auto& next_global_order_tile =
          result_tile_ids_[result_tile_cursor_];
      const auto& emit_bound =
          fragment_metadata_[next_global_order_tile.fragment_idx_]->mbr(
              next_global_order_tile.tile_idx_);

      // Skip comparison if the next one is the same fragment,
      // in that case we know the cells are ordered correctly
      if (frag_idx != next_global_order_tile.fragment_idx_) {
        struct FutureResultTileLowerBound {
          const ResultTileId& tile;
          const NDRange& mbr;

          const void* coord(unsigned dim) const {
            return mbr[dim].data();
          }

          UntypedDatumView dimension_datum(
              const Dimension&, unsigned dim) const {
            return mbr[dim].start_datum();
          }

          unsigned fragment_idx() const {
            return tile.fragment_idx_;
          }

          uint64_t tile_idx() const {
            return tile.tile_idx_;
          }
        };

        FutureResultTileLowerBound target = {
            .tile = next_global_order_tile, .mbr = emit_bound};

        GlobalCellCmp cmp(array_schema_.domain());

        if (cmp(target, rc)) {
          // more tiles needed, out-of-order tiles is a possibility if we
          // continue

          // TODO: make sure we have test coverage here.
          // 1) scenario where this does not guarantee progress, i.e.
          //    too many overlapping tiles, we can't emit anything else
          //    and increasing memory budget really is the only way out
          // 2) scenario where this does make progress because we
          //    finish a tile and thus gain budget to load the next one

          return true;
        }
      }
    }

    std::unique_lock<std::mutex> ul(tile_queue_mutex_);

    // Add all the cells in this tile with the same coordinates as this cell
    // for purge deletes with no dups mode.
    if (purge_deletes_no_dups_mode_ &&
        fragment_metadata_[frag_idx]->has_timestamps()) {
      if (add_all_dups_to_queue(
              rc, result_tiles_it, result_tiles, tile_queue, to_delete)) {
        return true;
      }
    }
    tile_queue.emplace(std::move(rc));
  }

  // We don't need more tiles as a tile was found.
  return false;
}

template <class BitmapType>
void SparseGlobalOrderReader<BitmapType>::compute_hilbert_values(
    std::vector<ResultTile*>& result_tiles) {
  auto timer_se = stats_->start_timer("compute_hilbert_values");

  // For easy reference.
  auto dim_num = array_schema_.dim_num();

  // Create a Hilbet class.
  Hilbert h(dim_num);
  auto bits = h.bits();
  auto max_bucket_val = ((uint64_t)1 << bits) - 1;

  // Parallelize on tiles.
  throw_if_not_ok(parallel_for(
      &resources_.compute_tp(), 0, result_tiles.size(), [&](uint64_t t) {
        auto tile =
            static_cast<GlobalOrderResultTile<BitmapType>*>(result_tiles[t]);
        auto cell_num =
            fragment_metadata_[tile->frag_idx()]->cell_num(tile->tile_idx());
        auto rc = GlobalOrderResultCoords(tile, 0);
        std::vector<uint64_t> coords(dim_num);

        tile->allocate_hilbert_vector();
        for (rc.pos_ = 0; rc.pos_ < cell_num; rc.pos_++) {
          // Process only values in bitmap.
          if (!tile->has_bmp() || tile->bitmap()[rc.pos_]) {
            // Compute Hilbert number for all dimensions first.
            for (uint32_t d = 0; d < dim_num; ++d) {
              auto dim{array_schema_.dimension_ptr(d)};
              coords[d] = hilbert_order::map_to_uint64(
                  *dim, rc, d, bits, max_bucket_val);
            }

            // Now we are ready to get the final number.
            tile->set_hilbert_value(rc.pos_, h.coords_to_hilbert(&coords[0]));
          }
        }

        return Status::Ok();
      }));
}

template <class BitmapType>
void SparseGlobalOrderReader<BitmapType>::update_frag_idx(
    GlobalOrderResultTile<BitmapType>* tile, uint64_t c) {
  auto& frag_idx = read_state_.frag_idx()[tile->frag_idx()];
  auto t = tile->tile_idx();
  if ((t == frag_idx.tile_idx_ && c > frag_idx.cell_idx_) ||
      t > frag_idx.tile_idx_) {
    read_state_.set_frag_idx(tile->frag_idx(), FragIdx(t, c));
  }
}

template <class BitmapType>
template <class CompType>
tuple<bool, std::vector<ResultCellSlab>>
SparseGlobalOrderReader<BitmapType>::merge_result_cell_slabs(
    uint64_t num_cells, std::vector<ResultTilesList>& result_tiles) {
  auto timer_se = stats_->start_timer("merge_result_cell_slabs");

  // User gave us some empty buffers, exit.
  if (num_cells == 0) {
    return {true, std::vector<ResultCellSlab>()};
  }

  std::vector<ResultCellSlab> result_cell_slabs;
  CompType cmp_max_slab_length(
      array_schema_.domain(), false, false, &fragment_metadata_);

  // TODO Parallelize.

  // For easy reference.
  const bool return_all_dups =
      array_schema_.allows_dups() || consolidation_with_timestamps_;

  // A tile min heap, contains one GlobalOrderResultCoords per fragment.
  std::vector<GlobalOrderResultCoords<BitmapType>> container;
  container.reserve(result_tiles.size());
  CompType cmp(
      array_schema_.domain(),
      !array_schema_.allows_dups(),
      true,
      &fragment_metadata_);
  TileMinHeap<CompType> tile_queue(cmp, std::move(container));

  // If any fragments needs to load more tiles.
  bool need_more_tiles = false;

  // Tile iterators, per fragments.
  std::vector<TileListIt> rt_it(result_tiles.size());

  // For all fragments, get the first tile in the sorting queue.
  std::vector<TileListIt> to_delete;
  throw_if_not_ok(parallel_for(
      &resources_.compute_tp(), 0, result_tiles.size(), [&](uint64_t f) {
        if (result_tiles[f].size() > 0) {
          // Initialize the iterator for this fragment.
          rt_it[f] = result_tiles[f].begin();

          // Add the tile to the queue.
          uint64_t cell_idx =
              read_state_.frag_idx()[f].tile_idx_ == rt_it[f]->tile_idx() ?
                  read_state_.frag_idx()[f].cell_idx_ :
                  0;
          GlobalOrderResultCoords rc(&*(rt_it[f]), cell_idx);
          bool res = add_next_cell_to_queue(
              rc, rt_it, result_tiles, tile_queue, to_delete);
          {
            std::unique_lock<std::mutex> ul(tile_queue_mutex_);
            need_more_tiles |= res;
          }
        }

        return Status::Ok();
      }));

  const bool non_overlapping_ranges = std::is_same<BitmapType, uint8_t>::value;

  // Process all elements.
  bool user_buffers_full = false;
  while (!tile_queue.empty() && !need_more_tiles && num_cells > 0) {
    auto to_process = tile_queue.top();
    auto tile = to_process.tile_;
    tile_queue.pop();

    // Used only for purge delete condolidation.
    bool stop_creating_slabs = false;

    // Process all cells with the same coordinates at once.
    bool deleted_dups = false;
    while (!tile_queue.empty() && to_process.same_coords(tile_queue.top()) &&
           num_cells > 0) {
      // For consolidation with deletes, check if the cell was deleted and
      // stop copying if it is. All cells after this in the queue have a
      // smaller timestamp so they should be deleted.
      if (purge_deletes_no_dups_mode_) {
        stop_creating_slabs |= tile->post_dedup_bitmap()[to_process.pos_] == 0;
      }

      if (return_all_dups && !stop_creating_slabs) {
        // If we return duplicates, create one slab for all the dups.
        if (non_overlapping_ranges) {
          if (!purge_deletes_no_dups_mode_ ||
              tile->post_dedup_bitmap()[to_process.pos_] != 0) {
            tile->set_used();
            result_cell_slabs.emplace_back(tile, to_process.pos_, 1);
            num_cells--;
          }
        } else {
          // For overlapping ranges, create as many slabs as there are counts.
          auto num = tile->post_dedup_bitmap()[to_process.pos_];
          if (num_cells < num) {
            num_cells = 0;
            break;
          }

          if (num > 0) {
            tile->set_used();
          }

          for (uint64_t i = 0; i < num; i++) {
            result_cell_slabs.emplace_back(tile, to_process.pos_, 1);
            num_cells--;
          }
        }
      }

      // For no dups, we just remove the cells from the queue as the one with
      // the higher timestamp is already in `to_process` and will be processed
      // below. For dups, `to_process` was already added above, replace it with
      // the top of the queue.
      if (!return_all_dups) {
        auto to_remove = tile_queue.top();
        deleted_dups =
            !to_remove.tile_->has_post_dedup_bmp() ||
            to_remove.tile_->post_dedup_bitmap()[to_remove.pos_] != 0;
        update_frag_idx(to_remove.tile_, to_remove.pos_ + 1);
        tile_queue.pop();

        // Put the next cell from the processed tile in the queue.
        need_more_tiles = add_next_cell_to_queue(
            to_remove, rt_it, result_tiles, tile_queue, to_delete);
      } else {
        update_frag_idx(tile, to_process.pos_ + 1);

        // Put the next cell from the processed tile in the queue.
        need_more_tiles = add_next_cell_to_queue(
            to_process, rt_it, result_tiles, tile_queue, to_delete);

        to_process = tile_queue.top();
        tile_queue.pop();
        tile = to_process.tile_;
      }

      if (num_cells == 0) {
        break;
      }
    }

    if (num_cells == 0) {
      break;
    }

    if (!stop_creating_slabs) {
      // Get data from the result coord.
      auto start = to_process.pos_;
      const auto frag_idx = tile->frag_idx();

      // For purge delete no dups mode, we cannot merge more than one cell at
      // time as we don't know if any cells in the generated slab are
      // duplicates.
      bool single_cell_only = purge_deletes_no_dups_mode_ &&
                              fragment_metadata_[frag_idx]->has_timestamps();

      // Compute the length of the cell slab.
      uint64_t length = 1;
      if (to_process.has_next_ || single_cell_only) {
        if (tile_queue.empty()) {
          length = to_process.max_slab_length();
        } else {
          length =
              to_process.max_slab_length(tile_queue.top(), cmp_max_slab_length);
        }
      }

      if (length != 0) {
        // Flag the tile as used.
        to_process.tile_->set_used();

        // Make sure we don't merge more cells than the buffers.
        length = std::min(length, num_cells);

        // Update the position in the result coord.
        to_process.pos_ += length - 1;

        // Make sure we don't process the last in memory cell of a consolidated
        // with timestamps fragment if there are more tiles for that fragment.
        if (last_in_memory_cell_of_consolidated_fragment(
                frag_idx, to_process, result_tiles)) {
          length--;
          to_process.pos_--;
        }

        // Generate the result cell slabs.
        if (non_overlapping_ranges) {
          result_cell_slabs.emplace_back(tile, start, length);
          update_frag_idx(tile, start + length);
          num_cells -= length;
        } else {
          auto num = to_process.tile_->bitmap()[to_process.pos_];
          if (num > num_cells) {
            num_cells = 0;
            break;
          }

          for (uint64_t i = 0; i < num; i++) {
            result_cell_slabs.emplace_back(tile, start, length);
            num_cells -= length;
          }

          update_frag_idx(tile, start + length);
        }
      } else if (deleted_dups) {
        // If a duplicate was deleted by this well, we need to insert an empty
        // cell slab. This will ensure that if we need to revert progress before
        // this cell, the deleted cell will be merged again in all cases to
        // delete the duplicates again.
        result_cell_slabs.emplace_back(tile, start, 0);
      }
    }

    // Put the next cell in the queue.
    need_more_tiles = add_next_cell_to_queue(
        to_process, rt_it, result_tiles, tile_queue, to_delete);
  }

  user_buffers_full = num_cells == 0;

  // Remove empty cell slab at the end of the structure to prevent copy issues.
  while (result_cell_slabs.size() > 0 &&
         result_cell_slabs.back().length_ == 0) {
    result_cell_slabs.pop_back();
  }

  logger_->debug(
      "Done merging result cell slabs, num slabs {0}, buffers full {1}",
      result_cell_slabs.size(),
      user_buffers_full);

  // Delete tiles that were marked for deletion. Make one last check on the used
  // variable as one duplicate cell might have been merged and changed the
  // status.
  for (auto& it : to_delete) {
    if (!it->used()) {
      remove_result_tile(it->frag_idx(), it, result_tiles);
    }
  }

  return {user_buffers_full, std::move(result_cell_slabs)};
};

template <class BitmapType>
tuple<uint64_t, uint64_t, uint64_t, bool>
SparseGlobalOrderReader<BitmapType>::compute_parallelization_parameters(
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

template <class BitmapType>
template <class OffType>
void SparseGlobalOrderReader<BitmapType>::copy_offsets_tiles(
    const std::string& name,
    const uint64_t num_range_threads,
    const bool nullable,
    const OffType offset_div,
    const std::vector<ResultCellSlab>& result_cell_slabs,
    const std::vector<uint64_t>& cell_offsets,
    QueryBuffer& query_buffer,
    std::vector<const void*>& var_data) {
  auto timer_se = stats_->start_timer("copy_offsets_tiles");

  // Process all tiles/cells in parallel.
  throw_if_not_ok(parallel_for_2d(
      &resources_.compute_tp(),
      0,
      result_cell_slabs.size(),
      0,
      num_range_threads,
      [&](uint64_t i, uint64_t range_thread_idx) {
        // For easy reference.
        auto& rcs = result_cell_slabs[i];
        auto rt = static_cast<GlobalOrderResultTile<BitmapType>*>(
            result_cell_slabs[i].tile_);

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

        // Get dest buffers.
        auto buffer = (OffType*)query_buffer.buffer_ + dest_cell_offset;
        auto val_buffer =
            query_buffer.validity_vector_.buffer() + dest_cell_offset;
        auto var_data_buffer = &var_data[dest_cell_offset - cell_offsets[0]];

        // Copy full tile.
        if (!use_fill_value) {
          for (uint64_t c = min_pos; c < max_pos; c++) {
            *buffer = (OffType)(src_buff[c + 1] - src_buff[c]) / offset_div;
            buffer++;
            *var_data_buffer = src_var_buff + src_buff[c];
            var_data_buffer++;
          }
        } else {
          for (uint64_t c = min_pos; c < max_pos; c++) {
            *buffer = fill_value_size / offset_div;
            buffer++;
            *var_data_buffer = src_var_buff;
            var_data_buffer++;
          }
        }

        // Copy nullable values.
        if (nullable) {
          if (!use_fill_value) {
            const auto& t_val = tile_tuple->validity_tile();
            const auto src_val_buff = t_val.template data_as<uint8_t>();
            for (uint64_t c = min_pos; c < max_pos; c++) {
              *val_buffer = src_val_buff[c];
              val_buffer++;
            }
          } else {
            uint8_t v = array_schema_.attribute(name)->fill_value_validity();
            for (uint64_t c = min_pos; c < max_pos; c++) {
              *val_buffer = v;
              val_buffer++;
            }
          }
        }

        return Status::Ok();
      }));
}

template <class BitmapType>
template <class OffType>
void SparseGlobalOrderReader<BitmapType>::copy_var_data_tiles(
    const uint64_t num_range_threads,
    const OffType offset_div,
    const uint64_t var_buffer_size,
    const std::vector<ResultCellSlab>& result_cell_slabs,
    const std::vector<uint64_t>& cell_offsets,
    QueryBuffer& query_buffer,
    std::vector<const void*>& var_data) {
  auto timer_se = stats_->start_timer("copy_var_tiles");

  // For easy reference.
  auto var_data_buffer = static_cast<uint8_t*>(query_buffer.buffer_var_);

  // Process all tiles/cells in parallel.
  throw_if_not_ok(parallel_for_2d(
      &resources_.compute_tp(),
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
      }));
}

template <class BitmapType>
void SparseGlobalOrderReader<BitmapType>::copy_fixed_data_tiles(
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
  throw_if_not_ok(parallel_for_2d(
      &resources_.compute_tp(),
      0,
      result_cell_slabs.size(),
      0,
      num_range_threads,
      [&](uint64_t i, uint64_t range_thread_idx) {
        // For easy reference.
        auto& rcs = result_cell_slabs[i];
        auto rt = static_cast<GlobalOrderResultTile<BitmapType>*>(
            result_cell_slabs[i].tile_);

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

        // Get dest buffers.
        auto buffer = static_cast<uint8_t*>(query_buffer.buffer_) +
                      dest_cell_offset * cell_size;
        auto val_buffer =
            query_buffer.validity_vector_.buffer() + dest_cell_offset;

        if (!stores_zipped_coords) {
          if (!use_fill_value) {
            // Copy tile.
            memcpy(
                buffer,
                src_buff + min_pos * cell_size,
                (max_pos - min_pos) * cell_size);
          } else {
            // Copy tile using the fill value.
            for (uint64_t i = 0; i < max_pos - min_pos; i++) {
              memcpy(buffer, src_buff, cell_size);
              buffer += cell_size;
            }
          }
        } else {  // Copy for zipped coords.
          const auto dim_num = rt->domain()->dim_num();
          for (uint64_t c = min_pos; c < max_pos; c++) {
            auto pos = c * dim_num + dim_idx;
            memcpy(buffer, src_buff + pos * cell_size, cell_size);
            buffer += cell_size;
          }
        }

        if (nullable) {
          if (!use_fill_value) {
            // Copy validity values from tile.
            const auto& t_val = tile_tuple->validity_tile();
            const auto src_val_buff = t_val.template data_as<uint8_t>();
            memcpy(val_buffer, src_val_buff + min_pos, max_pos - min_pos);
          } else {
            // Copy the fill value for validity.
            uint8_t v = array_schema_.attribute(name)->fill_value_validity();
            for (uint64_t i = 0; i < max_pos - min_pos; i++) {
              *val_buffer = v;
              val_buffer++;
            }
          }
        }

        return Status::Ok();
      }));
}

template <class BitmapType>
void SparseGlobalOrderReader<BitmapType>::copy_timestamps_tiles(
    const uint64_t num_range_threads,
    const std::vector<ResultCellSlab>& result_cell_slabs,
    const std::vector<uint64_t>& cell_offsets,
    QueryBuffer& query_buffer) {
  auto timer_se = stats_->start_timer("copy_timestamps_tiles");

  // Process all tiles/cells in parallel.
  throw_if_not_ok(parallel_for_2d(
      &resources_.compute_tp(),
      0,
      result_cell_slabs.size(),
      0,
      num_range_threads,
      [&](uint64_t i, uint64_t range_thread_idx) {
        // For easy reference.
        auto& rcs = result_cell_slabs[i];
        auto rt = static_cast<GlobalOrderResultTile<BitmapType>*>(
            result_cell_slabs[i].tile_);
        const uint64_t cell_size = constants::timestamp_size;

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

        // Get dest buffer.
        auto buffer =
            static_cast<uint64_t*>(query_buffer.buffer_) + dest_cell_offset;

        if (fragment_metadata_[rt->frag_idx()]->has_timestamps()) {
          // Copy tile.
          const auto tile_tuple = rt->tile_tuple(constants::timestamps);
          const auto t = &tile_tuple->fixed_tile();
          const auto src_buff = t->template data_as<uint8_t>();
          memcpy(
              buffer,
              src_buff + min_pos * cell_size,
              (max_pos - min_pos) * cell_size);
        } else {
          // Copy fragment timestamp.
          auto timestamp = fragment_timestamp(rt);
          for (uint64_t c = 0; c < (max_pos - min_pos); c++) {
            *buffer = timestamp;
            buffer++;
          }
        }

        return Status::Ok();
      }));
}

template <class BitmapType>
void SparseGlobalOrderReader<BitmapType>::copy_delete_meta_tiles(
    const uint64_t num_range_threads,
    const std::vector<ResultCellSlab>& result_cell_slabs,
    const std::vector<uint64_t>& cell_offsets,
    QueryBuffer& query_buffer) {
  auto timer_se = stats_->start_timer("copy_delete_meta_tiles");

  // Make a map to quickly find the condition index from a marker.
  std::unordered_map<std::string, uint64_t> condition_marker_to_index_map;
  for (auto& condition : delete_and_update_conditions_) {
    condition_marker_to_index_map.emplace(
        condition.condition_marker(), condition.condition_index());
  }

  // Process all tiles/cells in parallel.
  throw_if_not_ok(parallel_for_2d(
      &resources_.compute_tp(),
      0,
      result_cell_slabs.size(),
      0,
      num_range_threads,
      [&](uint64_t i, uint64_t range_thread_idx) {
        // For easy reference.
        auto& rcs = result_cell_slabs[i];
        auto rt = static_cast<GlobalOrderResultTile<BitmapType>*>(
            result_cell_slabs[i].tile_);

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
        auto buffer_delete_ts =
            static_cast<uint64_t*>(
                buffers_[constants::delete_timestamps].buffer_) +
            dest_cell_offset;
        auto buffer_condition_indexes =
            static_cast<size_t*>(query_buffer.buffer_) + dest_cell_offset;

        if (fragment_metadata_[rt->frag_idx()]->has_delete_meta()) {
          // If we have delete metadata, we need to take either the existing
          // delete time, or the one coming from processing the delete
          // conditions not already processed for this fragment.

          // Get source buffers.
          const auto tile_tuple_delete_ts =
              rt->tile_tuple(constants::delete_timestamps);
          const auto t_delete_ts = &tile_tuple_delete_ts->fixed_tile();
          auto src_buff_delete_ts =
              t_delete_ts->template data_as<uint64_t>() + min_pos;
          const auto tile_tuple_condition_indexes =
              rt->tile_tuple(constants::delete_condition_index);
          const auto t_condition_indexes =
              &tile_tuple_condition_indexes->fixed_tile();
          auto src_buff_condition_indexes =
              t_condition_indexes->template data_as<uint64_t>() + min_pos;

          // For all cells, take either the time coming in from the existing
          // metadata or the one computed with the delete conditions not
          // already processed for this fragment, whichever comes first.
          for (uint64_t c = min_pos; c < max_pos; c++) {
            const auto delete_condition_ts = rt->delete_timestamp(c);
            const auto delete_condition_index = rt->delete_condition_index(c);
            if (delete_condition_ts >= *src_buff_delete_ts) {
              *buffer_delete_ts = *src_buff_delete_ts;

              // Convert the source condition index to this fragment's
              // processed condition index.
              uint64_t converted_index = std::numeric_limits<uint64_t>::max();
              if (*src_buff_condition_indexes !=
                  std::numeric_limits<uint64_t>::max()) {
                auto& condition_marker = fragment_metadata_[rt->frag_idx()]
                                             ->loaded_metadata()
                                             ->get_processed_conditions()
                                                 [*src_buff_condition_indexes];
                converted_index =
                    condition_marker_to_index_map[condition_marker];
              }
              *buffer_condition_indexes = converted_index;
            } else {
              *buffer_delete_ts = delete_condition_ts;
              *buffer_condition_indexes = delete_condition_index;
            }

            // Move the source/destination pointers to the next cell.
            buffer_delete_ts++;
            src_buff_delete_ts++;
            buffer_condition_indexes++;
            src_buff_condition_indexes++;
          }
        } else {
          // No delete metadata, just that the computed value.

          // Copy using the delete condition idx vector.
          for (uint64_t c = min_pos; c < max_pos; c++) {
            *buffer_delete_ts = rt->delete_timestamp(c);
            buffer_delete_ts++;
            *buffer_condition_indexes = rt->delete_condition_index(c);
            buffer_condition_indexes++;
          }
        }

        return Status::Ok();
      }));
}

template <class BitmapType>
std::vector<uint64_t>
SparseGlobalOrderReader<BitmapType>::respect_copy_memory_budget(
    const std::vector<std::string>& names,
    std::vector<ResultCellSlab>& result_cell_slabs,
    bool& user_buffers_full) {
  // Process all attributes in parallel.
  const uint64_t memory_budget = available_memory();
  uint64_t max_cs_idx = result_cell_slabs.size();
  std::mutex max_cs_idx_mtx;
  std::vector<uint64_t> total_mem_usage_per_attr(names.size());
  throw_if_not_ok(
      parallel_for(&resources_.compute_tp(), 0, names.size(), [&](uint64_t i) {
        // For easy reference.
        const auto& name = names[i];
        const bool agg_only = aggregate_only(name);
        const auto var_sized = array_schema_.var_size(name);
        uint64_t* mem_usage = &total_mem_usage_per_attr[i];
        const bool is_timestamps = name == constants::timestamps ||
                                   name == constants::delete_timestamps;

        // Keep track of tiles already accounted for.
        std::
            unordered_set<std::pair<uint64_t, uint64_t>, utils::hash::pair_hash>
                accounted_tiles;

        // For dimensions or query condition fields, tiles are already all
        // loaded in memory.
        if (array_schema_.is_dim(name) ||
            qc_loaded_attr_names_set_.count(name) != 0 || is_timestamps ||
            name == constants::count_of_rows) {
          return Status::Ok();
        }

        // Get the size for all tiles.
        uint64_t idx = 0;
        for (; idx < max_cs_idx; idx++) {
          auto rcs = result_cell_slabs[idx];
          if (rcs.length_ == 0) {
            continue;
          }

          // Skip this tile if it's aggregate only and we can aggregate it with
          // the fragment metadata only.
          if (agg_only && can_aggregate_tile_with_frag_md(rcs)) {
            continue;
          }

          auto rt = static_cast<GlobalOrderResultTile<BitmapType>*>(rcs.tile_);
          const auto f = rt->frag_idx();
          const auto t = rt->tile_idx();
          auto id = std::pair<uint64_t, uint64_t>(f, t);

          if (accounted_tiles.count(id) == 0) {
            accounted_tiles.emplace(id);

            // Skip for delete condition name if the fragment doesn't have
            // delete metadata.
            if (name == constants::delete_condition_index &&
                !fragment_metadata_[f]->has_delete_meta()) {
              continue;
            }

            // Skip for fields added in schema evolution.
            if (!fragment_metadata_[f]->array_schema()->is_field(name)) {
              continue;
            }

            // Size of the tile in memory.
            auto tile_size = get_attribute_tile_size(name, f, t);

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
        }

        // Save the minimum result tile index that we saw for all attributes.
        {
          std::unique_lock<std::mutex> ul(max_cs_idx_mtx);
          max_cs_idx = std::min(idx, max_cs_idx);
        }

        return Status::Ok();
      }));

  if (max_cs_idx == 0) {
    throw SparseGlobalOrderReaderException(
        "Unable to copy one slab with current budget/buffers");
  }

  // Resize the result tiles vector.
  user_buffers_full &= max_cs_idx == result_cell_slabs.size();
  while (result_cell_slabs.size() > max_cs_idx) {
    // Revert progress for this slab in read state, and pop it.
    auto& last_rcs = result_cell_slabs.back();
    read_state_.set_frag_idx(
        last_rcs.tile_->frag_idx(),
        FragIdx(last_rcs.tile_->tile_idx(), last_rcs.start_));
    result_cell_slabs.pop_back();
  }

  return total_mem_usage_per_attr;
}

template <class BitmapType>
template <class OffType>
tuple<bool, uint64_t>
SparseGlobalOrderReader<BitmapType>::compute_var_size_offsets(
    stats::Stats* stats,
    std::vector<ResultCellSlab>& result_cell_slabs,
    std::vector<uint64_t>& cell_offsets,
    QueryBuffer& query_buffer) {
  auto timer_se = stats->start_timer("switch_sizes_to_offsets");

  auto new_var_buffer_size = *query_buffer.buffer_var_size_;
  bool user_buffers_full = false;

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
    user_buffers_full = true;

    // Make sure that the start of the last RCS can fit the buffers. If not,
    // pop the last slab until it does.
    auto total_cells = cell_offsets[result_cell_slabs.size() - 1];
    new_var_buffer_size = ((OffType*)query_buffer.buffer_)[total_cells];
    while (query_buffer.original_buffer_var_size_ < new_var_buffer_size) {
      // Revert progress for this slab in read state, and pop it.
      auto& last_rcs = result_cell_slabs.back();
      read_state_.set_frag_idx(
          last_rcs.tile_->frag_idx(),
          FragIdx(last_rcs.tile_->tile_idx(), last_rcs.start_));
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

    // Update the buffer size.
    new_var_buffer_size = ((OffType*)query_buffer.buffer_)[total_cells];

    // Update the cell progress.
    read_state_.set_frag_idx(
        last_rcs.tile_->frag_idx(),
        FragIdx(
            last_rcs.tile_->tile_idx(), last_rcs.start_ + last_rcs.length_));

    // Remove empty cell slab.
    if (last_rcs.length_ == 0) {
      result_cell_slabs.pop_back();
    }
  }

  return {user_buffers_full, new_var_buffer_size};
}

template <class BitmapType>
std::vector<ResultTile*>
SparseGlobalOrderReader<BitmapType>::result_tiles_to_load(
    std::vector<ResultCellSlab>& result_cell_slabs, bool aggregate_only) {
  std::vector<ResultTile*> result_tiles;
  {
    std::unordered_set<ResultTile*> found_tiles;
    for (auto& rcs : result_cell_slabs) {
      if (rcs.length_ != 0) {
        if (found_tiles.count(rcs.tile_) == 0) {
          found_tiles.emplace(rcs.tile_);

          if (!aggregate_only || !can_aggregate_tile_with_frag_md(rcs))
            result_tiles.emplace_back(rcs.tile_);
        }
      }
    }
  }
  std::sort(result_tiles.begin(), result_tiles.end(), result_tile_cmp);
  return result_tiles;
}

template <class BitmapType>
template <class OffType>
void SparseGlobalOrderReader<BitmapType>::process_slabs(
    std::vector<std::string>& names,
    std::vector<ResultCellSlab>& result_cell_slabs,
    bool& user_buffers_full) {
  auto timer_se = stats_->start_timer("process_slabs");

  // Compute parallelization parameters.
  uint64_t num_range_threads = 1;
  const auto num_threads = resources_.compute_tp().concurrency_level();
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
  auto mem_usage_per_attr =
      respect_copy_memory_budget(names, result_cell_slabs, user_buffers_full);

  // There is no space for any tiles in the user buffer, exit.
  if (result_cell_slabs.empty()) {
    return;
  }

  // Read a few attributes a a time.
  std::vector<ResultTile*> result_tiles =
      result_tiles_to_load(result_cell_slabs, false);
  std::optional<std::string> last_field_to_overflow{std::nullopt};
  uint64_t buffer_idx{0};
  optional<std::vector<ResultTile*>> result_tiles_agg_only;
  while (buffer_idx < names.size()) {
    // Generate a list of filtered result tiles for aggregates only fields.
    bool agg_only = aggregate_only(names[buffer_idx]);
    if (agg_only && result_tiles_agg_only == nullopt) {
      result_tiles_agg_only = result_tiles_to_load(result_cell_slabs, true);

      // If we hit an overflow, it might have changed the tiles we need to load.
      // Recompute the memory usage. This is because a tile where we might have
      // included the full tile in a cell slab (0 to 'cell_num()') and not
      // loaded might now be truncated to fit the user buffers. Since we can't
      // use the fragment metadata to compute the aggregates for this tile on
      // this iteration, the memory usage for this attribute needs to be
      // recomputed.
      if (last_field_to_overflow != nullopt) {
        mem_usage_per_attr = respect_copy_memory_budget(
            names, result_cell_slabs, user_buffers_full);
      }
    }

    // Read and unfilter as many attributes as can fit in the budget.
    auto names_to_copy = read_and_unfilter_attributes(
        names,
        mem_usage_per_attr,
        &buffer_idx,
        agg_only ? *result_tiles_agg_only : result_tiles,
        agg_only);

    for (const auto& name : names_to_copy) {
      // For easy reference.
      const auto is_dim = array_schema_.is_dim(name);

      // Delete timestamps will be processed at the same time as the delete
      // condition indexes.
      if (name == constants::delete_timestamps) {
        continue;
      }

      // Copy the data only if the name is in the buffer list.
      if (buffers_.count(name) != 0) {
        user_buffers_full |= copy_tiles<OffType>(
            num_range_threads,
            name,
            is_dim,
            cell_offsets,
            result_cell_slabs,
            last_field_to_overflow);
      }

      // Process aggregates.
      if (aggregates_.count(name) != 0) {
        process_aggregates(
            num_range_threads, name, cell_offsets, result_cell_slabs);
      }

      // Clear tiles from memory.
      if (!is_dim && qc_loaded_attr_names_set_.count(name) == 0 &&
          name != constants::timestamps &&
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
            throw SparseGlobalOrderReaderException(
                "Overflow happened after aggregate was computed, aggregate "
                "recompute pass is not yet implemented");
          }
        }
      }
    }
  }

  logger_->debug("Done copying tiles, buffers full {0}", user_buffers_full);
}

template <class BitmapType>
template <class OffType>
bool SparseGlobalOrderReader<BitmapType>::copy_tiles(
    const uint64_t num_range_threads,
    const std::string name,
    const bool is_dim,
    std::vector<uint64_t>& cell_offsets,
    std::vector<ResultCellSlab>& result_cell_slabs,
    std::optional<std::string>& last_field_to_overflow) {
  const auto var_sized = array_schema_.var_size(name);
  const auto nullable = array_schema_.is_nullable(name);
  const auto cell_size = array_schema_.cell_size(name);
  auto& query_buffer = buffers_[name];

  bool user_buffers_full = false;

  // Pointers to var size data, generated when offsets are processed.
  std::vector<const void*> var_data;
  if (var_sized) {
    var_data.resize(cell_offsets[result_cell_slabs.size()] - cell_offsets[0]);
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
  if (name == constants::timestamps) {
    copy_timestamps_tiles(
        num_range_threads, result_cell_slabs, cell_offsets, query_buffer);
  } else if (name == constants::delete_condition_index) {
    // Copy fixed size data.
    copy_delete_meta_tiles(
        num_range_threads, result_cell_slabs, cell_offsets, query_buffer);
  } else if (var_sized) {
    copy_offsets_tiles<OffType>(
        name,
        num_range_threads,
        nullable,
        offset_div,
        result_cell_slabs,
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
        result_cell_slabs,
        cell_offsets,
        query_buffer);
  }

  uint64_t var_buffer_size = 0;
  if (var_sized) {
    // Adjust the offsets buffer and make sure all data fits.
    auto&& [caused_overflow, var_buff_size] = compute_var_size_offsets<OffType>(
        stats_, result_cell_slabs, cell_offsets, query_buffer);
    user_buffers_full |= caused_overflow;
    var_buffer_size = var_buff_size;

    // Save the last field to overflow.
    if (caused_overflow) {
      last_field_to_overflow = name;
    }

    // Now copy the var size data.
    copy_var_data_tiles(
        num_range_threads,
        offset_div,
        var_buffer_size,
        result_cell_slabs,
        cell_offsets,
        query_buffer,
        var_data);
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

  if (nullable) {
    *buffers_[name].validity_vector_.buffer_size() = total_cells;
  }

  // For delete timestamps, since they get processed at the same time as
  // the delete condition indexes, we need to adjust the buffer size.
  if (name == constants::delete_condition_index) {
    *buffers_[constants::delete_timestamps].buffer_size_ =
        total_cells * constants::timestamp_size;
  }

  return user_buffers_full;
}

template <class BitmapType>
AggregateBuffer SparseGlobalOrderReader<BitmapType>::make_aggregate_buffer(
    const std::string name,
    const bool var_sized,
    const bool nullable,
    const uint64_t cell_size,
    const uint64_t min_cell,
    const uint64_t max_cell,
    ResultTile& rt) {
  return AggregateBuffer(
      min_cell,
      max_cell,
      name == constants::count_of_rows ?
          nullptr :
          rt.tile_tuple(name)->fixed_tile().data(),
      var_sized ?
          std::make_optional(rt.tile_tuple(name)->var_tile().data_as<char>()) :
          nullopt,
      nullable ? std::make_optional(
                     rt.tile_tuple(name)->validity_tile().data_as<uint8_t>()) :
                 nullopt,
      false,
      nullopt,
      cell_size);
}

template <class BitmapType>
void SparseGlobalOrderReader<BitmapType>::process_aggregates(
    const uint64_t num_range_threads,
    const std::string name,
    std::vector<uint64_t>& cell_offsets,
    std::vector<ResultCellSlab>& result_cell_slabs) {
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

  // Process all tiles/cells in parallel.
  throw_if_not_ok(parallel_for_2d(
      &resources_.compute_tp(),
      0,
      result_cell_slabs.size(),
      0,
      num_range_threads,
      [&](uint64_t i, uint64_t range_thread_idx) {
        // For easy reference.
        auto& rcs = result_cell_slabs[i];
        if (can_aggregate_tile_with_frag_md(result_cell_slabs[i])) {
          if (range_thread_idx == 0) {
            auto rt = result_cell_slabs[i].tile_;
            auto md = fragment_metadata_[rt->frag_idx()]->get_tile_metadata(
                name, rt->tile_idx());
            for (auto& aggregate : aggregates) {
              aggregate->aggregate_tile_with_frag_md(md);
            }
          }
        } else {
          // Compute parallelization parameters.
          auto&& [min_pos, max_pos, dest_cell_offset, skip_aggregate] =
              compute_parallelization_parameters(
                  range_thread_idx,
                  num_range_threads,
                  rcs.start_,
                  rcs.length_,
                  cell_offsets[i]);
          if (skip_aggregate) {
            return Status::Ok();
          }

          // Compute aggregate.
          AggregateBuffer aggregate_buffer{make_aggregate_buffer(
              name,
              var_sized && !validity_only,
              nullable,
              cell_val_num,
              min_pos,
              max_pos,
              *result_cell_slabs[i].tile_)};
          for (auto& aggregate : aggregates) {
            aggregate->aggregate_data(aggregate_buffer);
          }
        }

        return Status::Ok();
      }));
}

template <class BitmapType>
void SparseGlobalOrderReader<BitmapType>::remove_result_tile(
    const unsigned frag_idx,
    TileListIt rt,
    std::vector<ResultTilesList>& result_tiles) {
  // Remove coord tile size from memory budget.
  auto tile_idx = rt->tile_idx();
  auto tiles_size =
      get_coord_tiles_size(array_schema_.dim_num(), frag_idx, tile_idx);

  // Adjust total memory usage.
  memory_used_for_coords_total_ -= tiles_size;

  // Delete the tile.
  result_tiles[frag_idx].erase(rt);
}

template <class BitmapType>
void SparseGlobalOrderReader<BitmapType>::end_iteration(
    std::vector<ResultTilesList>& result_tiles) {
  // For easy reference.
  auto fragment_num = fragment_metadata_.size();

  // Clear fully processed tiles in each fragments.
  throw_if_not_ok(
      parallel_for(&resources_.compute_tp(), 0, fragment_num, [&](uint64_t f) {
        while (!result_tiles[f].empty() &&
               result_tiles[f].front().tile_idx() <
                   read_state_.frag_idx()[f].tile_idx_) {
          remove_result_tile(f, result_tiles[f].begin(), result_tiles);
        }

        return Status::Ok();
      }));

  if (!incomplete()) {
    assert(memory_used_for_coords_total_ == 0);
    assert(tmp_read_state_.memory_used_tile_ranges() == 0);
  }

  uint64_t num_rt = 0;
  for (unsigned int f = 0; f < fragment_num; f++) {
    num_rt += result_tiles[f].size();
  }

  logger_->debug("Done with iteration, num result tiles {0}", num_rt);

  array_memory_tracker_->set_budget(std::numeric_limits<uint64_t>::max());
}

// Explicit template instantiations
template SparseGlobalOrderReader<uint8_t>::SparseGlobalOrderReader(
    stats::Stats*, shared_ptr<Logger>, StrategyParams&, bool);
template SparseGlobalOrderReader<uint64_t>::SparseGlobalOrderReader(
    stats::Stats*, shared_ptr<Logger>, StrategyParams&, bool);

}  // namespace tiledb::sm
