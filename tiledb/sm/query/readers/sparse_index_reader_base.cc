/**
 * @file   sparse_index_reader_base.cc
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
 * This file implements class SparseIndexReaderBase.
 */

#include "tiledb/sm/query/readers/sparse_index_reader_base.h"
#include "tiledb/common/logger.h"
#include "tiledb/common/memory_tracker.h"
#include "tiledb/sm/array/array.h"
#include "tiledb/sm/array/array_operations.h"
#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/filesystem/vfs.h"
#include "tiledb/sm/fragment/fragment_metadata.h"
#include "tiledb/sm/misc/parallel_functions.h"
#include "tiledb/sm/misc/resource_pool.h"
#include "tiledb/sm/query/iquery_strategy.h"
#include "tiledb/sm/query/query_buffer.h"
#include "tiledb/sm/query/query_macros.h"
#include "tiledb/sm/query/readers/filtered_data.h"
#include "tiledb/sm/query/strategy_base.h"
#include "tiledb/sm/query/update_value.h"
#include "tiledb/sm/subarray/subarray.h"

#include <numeric>

namespace tiledb::sm {

class SparseIndexReaderBaseException : public StatusException {
 public:
  explicit SparseIndexReaderBaseException(const std::string& message)
      : StatusException("SparseIndexReaderBase", message) {
  }
};

/* ****************************** */
/*          CONSTRUCTORS          */
/* ****************************** */

SparseIndexReaderBase::SparseIndexReaderBase(
    std::string reader_string,
    stats::Stats* stats,
    shared_ptr<Logger> logger,
    StrategyParams& params,
    bool include_coords)
    : ReaderBase(stats, logger, params)
    , read_state_(array_->fragment_metadata().size())
    , tmp_read_state_(array_->fragment_metadata().size())
    , memory_budget_(config_, reader_string, params.memory_budget())
    , include_coords_(include_coords)
    , memory_used_for_coords_total_(0)
    , partial_tile_offsets_loading_(false) {
  // Sanity checks
  if (!params.skip_checks_serialization() && buffers_.empty() &&
      aggregate_buffers_.empty()) {
    throw SparseIndexReaderBaseException(
        "Cannot initialize reader; Buffers not set");
  }

  // Clear preprocess tile order
  preprocess_tile_order_.enabled_ = false;
  preprocess_tile_order_.cursor_ = 0;

  // Check subarray
  check_subarray();

  // Load offset configuration options.
  offsets_format_mode_ =
      config_.get<std::string>("sm.var_offsets.mode", Config::must_find);
  if (offsets_format_mode_ != "bytes" && offsets_format_mode_ != "elements") {
    throw SparseIndexReaderBaseException(
        "Cannot initialize reader; Unsupported offsets format in "
        "configuration");
  }
  elements_mode_ = offsets_format_mode_ == "elements";

  offsets_extra_element_ =
      config_.get<bool>("sm.var_offsets.extra_element", Config::must_find);

  offsets_bitsize_ =
      config_.get<uint32_t>("sm.var_offsets.bitsize", Config::must_find);
  if (offsets_bitsize_ != 32 && offsets_bitsize_ != 64) {
    throw SparseIndexReaderBaseException(
        "Cannot initialize reader; Unsupported offsets bitsize in "
        "configuration");
  }

  // Cache information about dimensions.
  const auto dim_num = array_schema_.dim_num();
  dim_names_.reserve(dim_num);
  is_dim_var_size_.reserve(dim_num);
  for (unsigned d = 0; d < dim_num; ++d) {
    dim_names_.emplace_back(array_schema_.dimension_ptr(d)->name());
    is_dim_var_size_.emplace_back(array_schema_.var_size(dim_names_[d]));
  }

  // Check the validity buffer sizes.
  check_validity_buffer_sizes();
}

/* ****************************** */
/*        PROTECTED METHODS       */
/* ****************************** */

const typename SparseIndexReaderBase::ReadState&
SparseIndexReaderBase::read_state() const {
  return read_state_;
}

void SparseIndexReaderBase::set_read_state(ReadState read_state) {
  read_state_ = std::move(read_state);
}

const PreprocessTileOrder& SparseIndexReaderBase::preprocess_tile_order()
    const {
  return preprocess_tile_order_;
}

void SparseIndexReaderBase::set_preprocess_tile_order_cursor(
    uint64_t, std::vector<ResultTileId>) {
  throw SparseIndexReaderBaseException(
      "Internal error: set_preprocess_tile_order_cursor for unsupported sparse "
      "index reader");
}

uint64_t SparseIndexReaderBase::available_memory() {
  return memory_budget_.total_budget() - memory_used_for_coords_total_ -
         tmp_read_state_.memory_used_tile_ranges() -
         array_memory_tracker_->get_memory_usage();
}

bool SparseIndexReaderBase::has_post_deduplication_conditions(
    FragmentMetadata& frag_meta) {
  return frag_meta.has_delete_meta() || condition_.has_value() ||
         (!delete_and_update_conditions_.empty() &&
          !deletes_consolidation_no_purge_);
}

uint64_t SparseIndexReaderBase::cells_copied(
    const std::vector<std::string>& names) {
  for (auto it = names.rbegin(); it != names.rend(); ++it) {
    auto& name = *it;
    if (buffers_.count(name) != 0) {
      auto buffer_size = *buffers_[name].buffer_size_;
      if (array_schema_.var_size(name)) {
        if (buffer_size == 0) {
          return 0;
        } else {
          return buffer_size / (offsets_bitsize_ / 8) - offsets_extra_element_;
        }
      } else {
        return buffer_size / array_schema_.cell_size(name);
      }
    }
  }

  return 0;
}

template <class BitmapType>
uint64_t SparseIndexReaderBase::get_coord_tiles_size(
    unsigned dim_num, unsigned f, uint64_t t) {
  uint64_t tiles_size = 0;

  // Add the coordinate tiles size.
  if (include_coords_) {
    for (unsigned d = 0; d < dim_num; d++) {
      tiles_size += fragment_metadata_[f]->tile_size(dim_names_[d], t);

      if (is_dim_var_size_[d]) {
        tiles_size += fragment_metadata_[f]->loaded_metadata()->tile_var_size(
            dim_names_[d], t);
      }
    }
  }

  if (include_timestamps(f)) {
    tiles_size +=
        fragment_metadata_[f]->cell_num(t) * constants::timestamp_size;
  }

  if (fragment_metadata_[f]->has_delete_meta()) {
    tiles_size +=
        fragment_metadata_[f]->cell_num(t) * constants::timestamp_size;
  }

  // Compute query condition tile sizes.
  if (!qc_loaded_attr_names_.empty()) {
    for (auto& name : qc_loaded_attr_names_) {
      // Calculate memory consumption for this tile.
      tiles_size += get_attribute_tile_size(name, f, t);
    }
  }

  return tiles_size;
}

Status SparseIndexReaderBase::load_initial_data() {
  if (initial_data_loaded_) {
    return Status::Ok();
  }

  auto timer_se = stats_->start_timer("load_initial_data");
  read_state_.set_done_adding_result_tiles(false);

  // For easy reference.
  const auto dim_num = array_schema_.dim_num();

  // Load delete conditions.
  auto&& [conditions, update_values] =
      load_delete_and_update_conditions(resources_, *array_.get());
  delete_and_update_conditions_ = conditions;
  bool make_timestamped_conditions = need_timestamped_conditions();

  if (make_timestamped_conditions) {
    RETURN_CANCEL_OR_ERROR(generate_timestamped_conditions());
  }

  // Load processed conditions from fragment metadata.
  if (delete_and_update_conditions_.size() > 0) {
    load_processed_conditions();
  }

  // Make a list of dim/attr that will be loaded for query condition.
  if (condition_.has_value()) {
    for (auto& name : condition_->field_names()) {
      if (!array_schema_.is_dim(name) || !include_coords_) {
        qc_loaded_attr_names_set_.insert(name);
      }
    }
  }
  for (auto delete_and_update_condition : delete_and_update_conditions_) {
    for (auto& name : delete_and_update_condition.field_names()) {
      if (!array_schema_.is_dim(name) || !include_coords_) {
        qc_loaded_attr_names_set_.insert(name);
      }
    }
  }

  qc_loaded_attr_names_.reserve(qc_loaded_attr_names_set_.size());
  for (auto& name : qc_loaded_attr_names_set_) {
    qc_loaded_attr_names_.emplace_back(name);
    attr_tile_offsets_to_load_.emplace_back(name);
    if (array_schema_.var_size(name)) {
      var_size_to_load_.emplace_back(name);
    }
  }

  // Calculate ranges of tiles in the subarray, if set.
  if (subarray_.is_set()) {
    // At this point, full memory budget is available.
    if (!array_memory_tracker_->set_budget(memory_budget_.total_budget())) {
      throw SparseIndexReaderBaseException(
          "Cannot set array memory budget (" +
          std::to_string(memory_budget_.total_budget()) +
          ") because it is smaller than the current memory usage (" +
          std::to_string(array_memory_tracker_->get_memory_usage()) + ").");
    }

    // Make sure there is no memory taken by the subarray.
    subarray_.clear_tile_overlap();

    // Tile ranges computation will not stop if it exceeds memory budget.
    // This is ok as it is a soft limit and will be taken into consideration
    // below.
    subarray_.precompute_all_ranges_tile_overlap(
        &resources_.compute_tp(), read_state_.frag_idx(), &tmp_read_state_);

    if (tmp_read_state_.memory_used_tile_ranges() >
        memory_budget_.ratio_tile_ranges() * memory_budget_.total_budget())
      return logger_->status(
          Status_ReaderError("Exceeded memory budget for result tile ranges"));
  } else {
    for (const auto& [name, _] : aggregates_) {
      if (array_schema_.is_dim(name)) {
        subarray_.load_relevant_fragment_rtrees(&resources_.compute_tp());
        break;
      }
    }
  }

  // Compute tile offsets to load and var size to load for attributes.
  for (auto& name : field_names_to_process()) {
    if (array_schema_.is_dim(name) ||
        qc_loaded_attr_names_set_.count(name) != 0 ||
        name == constants::count_of_rows) {
      continue;
    }

    attr_tile_offsets_to_load_.emplace_back(name);

    if (array_schema_.var_size(name)) {
      var_size_to_load_.emplace_back(name);
    }

    if (name == constants::timestamps) {
      user_requested_timestamps_ = true;
    }
  }

  const bool partial_consol_fragment_overlap =
      partial_consolidated_fragment_overlap(subarray_);
  use_timestamps_ = partial_consol_fragment_overlap ||
                    !array_schema_.allows_dups() ||
                    user_requested_timestamps_ || make_timestamped_conditions;

  // Add partial overlap condition, if required.
  if (partial_consol_fragment_overlap) {
    RETURN_CANCEL_OR_ERROR(add_partial_overlap_condition());
  }

  // Add delete timestamps condition.
  RETURN_CANCEL_OR_ERROR(add_delete_timestamps_condition());

  // Load per fragment tile offsets memory usage.
  per_frag_tile_offsets_usage_ = tile_offset_sizes();

  // Set a limit to the array memory.
  if (!array_memory_tracker_->set_budget(
          memory_budget_.total_budget() * memory_budget_.ratio_array_data())) {
    throw SparseIndexReaderBaseException(
        "Cannot set array memory budget (" +
        std::to_string(
            memory_budget_.total_budget() * memory_budget_.ratio_array_data()) +
        ") because it is smaller than the current memory usage (" +
        std::to_string(array_memory_tracker_->get_memory_usage()) + ").");
  }

  // Add var size dimensions to the list of tile var size to load vector.
  for (unsigned d = 0; d < dim_num; ++d) {
    if (is_dim_var_size_[d]) {
      var_size_to_load_.emplace_back(dim_names_[d]);
    }
  }

  // Add timestamps and filter by timestamps condition if required. If the
  // user has requested timestamps the special attribute will already be in
  // the list, so don't include it again
  if (use_timestamps_ && !user_requested_timestamps_) {
    attr_tile_offsets_to_load_.emplace_back(constants::timestamps);
  }

  // Load delete timestamps, always.
  attr_tile_offsets_to_load_.emplace_back(constants::delete_timestamps);

  // Load delete condition marker hashes for delete consolidation.
  if (deletes_consolidation_no_purge_) {
    attr_tile_offsets_to_load_.emplace_back(constants::delete_condition_index);
  }

  logger_->debug("Initial data loaded");
  initial_data_loaded_ = true;
  return Status::Ok();
}

uint64_t SparseIndexReaderBase::tile_offsets_size(
    const RelevantFragments& relevant_fragments) {
  uint64_t total_tile_offset_usage = 0;
  for (auto f : relevant_fragments) {
    total_tile_offset_usage += per_frag_tile_offsets_usage_[f];
  }

  return total_tile_offset_usage;
}

void SparseIndexReaderBase::load_tile_offsets_for_fragments(
    const RelevantFragments& relevant_fragments) {
  // Preload zipped coordinate tile offsets. Note that this will
  // ignore fragments with a version >= 5.
  std::vector<std::string> zipped_coords_names = {constants::coords};
  load_tile_offsets(relevant_fragments, zipped_coords_names);

  // Preload unzipped coordinate tile offsets. Note that this will
  // ignore fragments with a version < 5.
  load_tile_offsets(relevant_fragments, dim_names_);

  // Load tile offsets and var sizes for attributes.
  load_tile_var_sizes(relevant_fragments, var_size_to_load_);
  load_tile_offsets(relevant_fragments, attr_tile_offsets_to_load_);

  // Load tile metadata.
  auto md_names_to_load = attr_tile_offsets_to_load_;
  for (const auto& [name, _] : aggregates_) {
    if (array_schema_.is_dim(name)) {
      md_names_to_load.emplace_back(name);
    }
  }
  load_tile_metadata(relevant_fragments, md_names_to_load);
}

Status SparseIndexReaderBase::read_and_unfilter_coords(
    const std::vector<ResultTile*>& result_tiles) {
  auto timer_se = stats_->start_timer("read_and_unfilter_coords");

  if (include_coords_) {
    // Read and unfilter zipped coordinate tiles. Note that
    // this will ignore fragments with a version >= 5.
    RETURN_CANCEL_OR_ERROR(
        read_and_unfilter_coordinate_tiles({constants::coords}, result_tiles));

    // Read and unfilter unzipped coordinate tiles. Note that
    // this will ignore fragments with a version < 5.
    RETURN_CANCEL_OR_ERROR(
        read_and_unfilter_coordinate_tiles(dim_names_, result_tiles));
  }

  // Compute attributes to load.
  std::vector<std::string> attr_to_load;
  attr_to_load.reserve(
      1 + deletes_consolidation_no_purge_ + use_timestamps_ +
      qc_loaded_attr_names_.size());
  if (use_timestamps_) {
    attr_to_load.emplace_back(constants::timestamps);
  }
  attr_to_load.emplace_back(constants::delete_timestamps);
  if (deletes_consolidation_no_purge_) {
    attr_to_load.emplace_back(constants::delete_condition_index);
  }
  std::copy(
      qc_loaded_attr_names_.begin(),
      qc_loaded_attr_names_.end(),
      std::back_inserter(attr_to_load));

  // Read and unfilter attribute tiles.
  std::vector<ReaderBase::NameToLoad> to_load;
  to_load.reserve(attr_to_load.size());
  for (auto& name : attr_to_load) {
    to_load.emplace_back(name);
  }

  RETURN_CANCEL_OR_ERROR(
      read_and_unfilter_attribute_tiles(to_load, result_tiles));

  logger_->debug("Done reading and unfiltering coords tiles");
  return Status::Ok();
}

template <class BitmapType>
void SparseIndexReaderBase::compute_tile_bitmaps(
    std::vector<ResultTile*>& result_tiles) {
  auto timer_se = stats_->start_timer("compute_tile_bitmaps");

  // For easy reference.
  const auto& domain{array_schema_.domain()};
  const auto dim_num = array_schema_.dim_num();
  const auto cell_order = array_schema_.cell_order();

  // No subarray set or empty result tiles, return.
  if (!subarray_.is_set() || result_tiles.empty()) {
    return;
  }

  // Compute parallelization parameters.
  uint64_t num_range_threads = 1;
  const auto num_threads = resources_.compute_tp().concurrency_level();
  if (result_tiles.size() < num_threads) {
    // Ceil the division between thread_num and tile_num.
    num_range_threads = 1 + ((num_threads - 1) / result_tiles.size());
  }

  // Performance runs have shown that running multiple parallel_for's has a
  // measurable performance impact. So only pre-allocate tile bitmaps if we
  // are going to run multiple range threads.
  if (num_range_threads != 1) {
    // Resize bitmaps to process for each tiles in parallel.
    throw_if_not_ok(parallel_for(
        &resources_.compute_tp(), 0, result_tiles.size(), [&](uint64_t t) {
          static_cast<ResultTileWithBitmap<BitmapType>*>(result_tiles[t])
              ->alloc_bitmap();
          return Status::Ok();
        }));
  }

  // Process all tiles/cells in parallel.
  throw_if_not_ok(parallel_for_2d(
      &resources_.compute_tp(),
      0,
      result_tiles.size(),
      0,
      num_range_threads,
      [&](uint64_t t, uint64_t range_thread_idx) {
        // For easy reference.
        auto rt = (ResultTileWithBitmap<BitmapType>*)result_tiles[t];
        auto cell_num =
            fragment_metadata_[rt->frag_idx()]->cell_num(rt->tile_idx());
        stats_->add_counter("cell_num", cell_num);

        // Allocate the bitmap if not preallocated.
        if (num_range_threads == 1) {
          rt->alloc_bitmap();
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
          tdb::pmr::vector<uint64_t> relevant_ranges(
              query_memory_tracker_->get_resource(MemoryType::DIMENSIONS));
          relevant_ranges.reserve(ranges_for_dim.size());
          domain.dimension_ptr(dim_idx)->relevant_ranges(
              ranges_for_dim, mbr[dim_idx], relevant_ranges);

          // For non overlapping ranges, if we have full overlap on any range
          // there is no need to compute bitmaps.
          const bool non_overlapping = std::is_same<BitmapType, uint8_t>::value;
          if (non_overlapping) {
            auto covered_bitmap = domain.dimension_ptr(dim_idx)->covered_vec(
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
            throw_if_not_ok(rt->compute_results_count_sparse(
                dim_idx,
                ranges_for_dim,
                relevant_ranges,
                rt->bitmap(),
                cell_order,
                min,
                max));
          }
        }

        // Only compute bitmap cells here if we are processing a single cell
        // range. If not, it will be done below.
        if (num_range_threads == 1) {
          rt->count_cells();
        }

        return Status::Ok();
      }));

  // For multiple range threads, bitmap cell count is done in a separate
  // parallel for.
  if (num_range_threads != 1) {
    // Compute number of cells in each bitmaps in parallel.
    throw_if_not_ok(parallel_for(
        &resources_.compute_tp(), 0, result_tiles.size(), [&](uint64_t t) {
          static_cast<ResultTileWithBitmap<BitmapType>*>(result_tiles[t])
              ->count_cells();
          return Status::Ok();
        }));
  }

  logger_->debug("Done computing tile bitmaps");
}

template <class ResultTileType, class BitmapType>
void SparseIndexReaderBase::apply_query_condition(
    std::vector<ResultTile*>& result_tiles) {
  auto timer_se = stats_->start_timer("apply_query_condition");

  if (condition_.has_value() || !delete_and_update_conditions_.empty() ||
      use_timestamps_) {
    // Process all tiles in parallel.
    throw_if_not_ok(parallel_for(
        &resources_.compute_tp(), 0, result_tiles.size(), [&](uint64_t t) {
          // For easy reference.
          auto rt = static_cast<ResultTileType*>(result_tiles[t]);
          const auto frag_meta = fragment_metadata_[rt->frag_idx()];

          // If timestamps are present and fragment is partially included,
          // filter out tiles based on time by applying the query condition
          if (process_partial_timestamps(*frag_meta)) {
            // Make a bitmap, if required.
            if (!rt->has_bmp()) {
              rt->alloc_bitmap();
            }

            // Remove cells with partial overlap from the bitmap.
            QueryCondition::Params params(
                query_memory_tracker_, *(frag_meta->array_schema().get()));
            throw_if_not_ok(partial_overlap_condition_.apply_sparse<BitmapType>(
                params, *rt, rt->bitmap()));
            rt->count_cells();
          }

          // Make sure we have a condition bitmap if needed.
          if (has_post_deduplication_conditions(*frag_meta) ||
              deletes_consolidation_no_purge_) {
            rt->ensure_bitmap_for_query_condition();
          }

          // If the fragment has delete meta, process the delete timestamps.
          if (frag_meta->has_delete_meta() &&
              !deletes_consolidation_no_purge_) {
            // Remove cells deleted cells using the open timestamp.
            QueryCondition::Params params(
                query_memory_tracker_, *(frag_meta->array_schema().get()));
            throw_if_not_ok(
                delete_timestamps_condition_.apply_sparse<BitmapType>(
                    params, *rt, rt->post_dedup_bitmap()));
            if (array_schema_.allows_dups()) {
              rt->count_cells();
            }
          }

          // Compute the result of the query condition for this tile.
          if (condition_.has_value()) {
            QueryCondition::Params params(
                query_memory_tracker_, *(frag_meta->array_schema().get()));
            throw_if_not_ok(condition_->apply_sparse<BitmapType>(
                params, *rt, rt->post_dedup_bitmap()));
            if (array_schema_.allows_dups()) {
              rt->count_cells();
            }
          }

          // Apply delete conditions.
          if (!delete_and_update_conditions_.empty()) {
            // Allocate delete condition idx vector if required. This vector
            // is used to store which delete condition deleted a particular
            // cell.
            if (deletes_consolidation_no_purge_) {
              rt->allocate_per_cell_delete_condition_vector();
            }

            for (uint64_t i = 0; i < delete_and_update_conditions_.size();
                 i++) {
              if (!frag_meta->has_delete_meta() ||
                  frag_meta->loaded_metadata()
                          ->get_processed_conditions_set()
                          .count(delete_and_update_conditions_[i]
                                     .condition_marker()) == 0) {
                auto delete_timestamp =
                    delete_and_update_conditions_[i].condition_timestamp();

                // Check the delete condition timestamp is after the fragment
                // start.
                if (delete_timestamp >= frag_meta->timestamp_range().first) {
                  // Apply timestamped condition or regular condition.
                  QueryCondition::Params params(
                      query_memory_tracker_,
                      *(frag_meta->array_schema().get()));
                  if (!frag_meta->has_timestamps() ||
                      delete_timestamp > frag_meta->timestamp_range().second) {
                    throw_if_not_ok(
                        delete_and_update_conditions_[i]
                            .apply_sparse<BitmapType>(
                                params, *rt, rt->post_dedup_bitmap()));
                  } else {
                    throw_if_not_ok(
                        timestamped_delete_and_update_conditions_[i]
                            .apply_sparse<BitmapType>(
                                params, *rt, rt->post_dedup_bitmap()));
                  }

                  if (deletes_consolidation_no_purge_) {
                    // This is a post processing step during deletes
                    // consolidation to set the delete condition pointer to
                    // the current delete condition if the cells was cleared
                    // by this condition and not any previous conditions.
                    rt->compute_per_cell_delete_condition(
                        &delete_and_update_conditions_[i]);
                  } else {
                    // Count cells is dups are allowed as the regular bitmap was
                    // modified.
                    if (array_schema_.allows_dups()) {
                      rt->count_cells();
                    }
                  }
                }
              }
            }
          }

          return Status::Ok();
        }));
  }

  logger_->debug("Done applying query condition");
}

std::vector<std::string> SparseIndexReaderBase::read_and_unfilter_attributes(
    const std::vector<std::string>& names,
    const std::vector<uint64_t>& mem_usage_per_attr,
    uint64_t* buffer_idx,
    std::vector<ResultTile*>& result_tiles,
    bool agg_only) {
  auto timer_se = stats_->start_timer("read_and_unfilter_attributes");
  const uint64_t memory_budget = available_memory();

  std::vector<ReaderBase::NameToLoad> names_to_read;
  std::vector<std::string> names_to_copy;
  uint64_t memory_used = 0;
  while (*buffer_idx < names.size()) {
    auto& name = names[*buffer_idx];

    // Stop processing if we are doing non aggregates only fields and we hit an
    // aggregates only field. Aggregates only field will pass in a filteted list
    // of tiles to load.
    if (!agg_only && aggregate_only(name)) {
      break;
    }

    auto attr_mem_usage = mem_usage_per_attr[*buffer_idx];
    if (memory_used + attr_mem_usage < memory_budget) {
      memory_used += attr_mem_usage;

      // We only read attributes, so dimensions have 0 cost.
      if (attr_mem_usage != 0) {
        names_to_read.emplace_back(name, null_count_aggregate_only(name));
      }

      names_to_copy.emplace_back(name);
      (*buffer_idx)++;
    } else {
      break;
    }
  }

  // Read and unfilter tiles.
  throw_if_not_ok(
      read_and_unfilter_attribute_tiles(names_to_read, result_tiles));

  return names_to_copy;
}

std::vector<std::string> SparseIndexReaderBase::field_names_to_process() {
  std::vector<std::string> ret;
  std::unordered_set<std::string> added_names;

  // Guarantee the same ordering of buffers over different platform to guarantee
  // that tests have consistent behaviors.
  std::vector<std::string> names;
  names.reserve(buffers_.size());
  for (auto& buffer : buffers_) {
    names.emplace_back(buffer.first);
  }
  std::sort(names.begin(), names.end());

  // First add var fields with no aggregates that need recompute in case of
  // overflow.
  for (auto& name : names) {
    if (!array_schema_.var_size(name)) {
      continue;
    }

    // See if any of the aggregates for this field would need a recompute.
    bool any_need_recompute = false;
    if (aggregates_.count(name) != 0) {
      for (auto& aggregate : aggregates_[name]) {
        any_need_recompute |= aggregate->need_recompute_on_overflow();
      }
    }

    // Only add fields that don't need recompute.
    if (!any_need_recompute) {
      ret.emplace_back(name);
      added_names.emplace(name);
    }
  }

  // Second add the rest of the var fields.
  for (auto& name : names) {
    if (array_schema_.var_size(name) && added_names.count(name) == 0) {
      ret.emplace_back(name);
      added_names.emplace(name);
    }
  }

  // Now for the fixed fields.
  for (auto& name : names) {
    if (!array_schema_.var_size(name)) {
      ret.emplace_back(name);
      added_names.emplace(name);
    }
  }

  // Add field names for aggregates not requested in user buffers.
  for (auto& item : aggregates_) {
    auto name = item.first;
    if (added_names.count(name) == 0) {
      ret.emplace_back(name);
      added_names.emplace(name);
    }
  }

  return ret;
}

void SparseIndexReaderBase::resize_output_buffers(uint64_t cells_copied) {
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
    if (array_schema_.is_nullable(name)) {
      if (it.second.validity_vector_.buffer_size() != nullptr)
        *(it.second.validity_vector_.buffer_size()) =
            cells_copied * constants::cell_validity_size;
    }
  }
}

void SparseIndexReaderBase::add_extra_offset() {
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
      throw std::logic_error(
          "Cannot add extra offset to buffer; Unsupported offsets format");
    }
  }
}

// Explicit template instantiations
template uint64_t SparseIndexReaderBase::get_coord_tiles_size<uint64_t>(
    unsigned, unsigned, uint64_t);
template uint64_t SparseIndexReaderBase::get_coord_tiles_size<uint8_t>(
    unsigned, unsigned, uint64_t);
template void SparseIndexReaderBase::apply_query_condition<
    UnorderedWithDupsResultTile<uint64_t>,
    uint64_t>(std::vector<ResultTile*>&);
template void SparseIndexReaderBase::apply_query_condition<
    UnorderedWithDupsResultTile<uint8_t>,
    uint8_t>(std::vector<ResultTile*>&);
template void SparseIndexReaderBase::apply_query_condition<
    GlobalOrderResultTile<uint64_t>,
    uint64_t>(std::vector<ResultTile*>&);
template void SparseIndexReaderBase::apply_query_condition<
    GlobalOrderResultTile<uint8_t>,
    uint8_t>(std::vector<ResultTile*>&);
template void SparseIndexReaderBase::compute_tile_bitmaps<uint64_t>(
    std::vector<ResultTile*>&);
template void SparseIndexReaderBase::compute_tile_bitmaps<uint8_t>(
    std::vector<ResultTile*>&);

}  // namespace tiledb::sm
