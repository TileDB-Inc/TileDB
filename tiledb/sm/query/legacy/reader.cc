/**
 * @file   reader.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2022 TileDB, Inc.
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
 * This file implements class Reader.
 */

#include "tiledb/sm/query/legacy/reader.h"
#include "tiledb/common/logger.h"
#include "tiledb/sm/array/array.h"
#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/array_schema/dimension.h"
#include "tiledb/sm/fragment/fragment_metadata.h"
#include "tiledb/sm/misc/comparators.h"
#include "tiledb/sm/misc/hilbert.h"
#include "tiledb/sm/misc/parallel_functions.h"
#include "tiledb/sm/misc/types.h"
#include "tiledb/sm/misc/utils.h"
#include "tiledb/sm/query/hilbert_order.h"
#include "tiledb/sm/query/legacy/read_cell_slab_iter.h"
#include "tiledb/sm/query/query_macros.h"
#include "tiledb/sm/query/readers/result_tile.h"
#include "tiledb/sm/query/update_value.h"
#include "tiledb/sm/stats/global_stats.h"
#include "tiledb/sm/storage_manager/storage_manager.h"
#include "tiledb/sm/subarray/cell_slab.h"
#include "tiledb/sm/tile/generic_tile_io.h"

using namespace tiledb;
using namespace tiledb::common;
using namespace tiledb::sm::stats;

namespace tiledb {
namespace sm {

class ReaderStatusException : public StatusException {
 public:
  explicit ReaderStatusException(const std::string& message)
      : StatusException("Reader", message) {
  }
};

namespace {
/**
 * If the given iterator points to an "invalid" element, advance it until the
 * pointed-to element is valid, or `end`. Validity is determined by calling
 * `it->valid()`.
 *
 * Example:
 *
 * @code{.cpp}
 * std::vector<T> vec = ...;
 * // Get an iterator to the first valid vec element, or vec.end() if the
 * // vector is empty or only contains invalid elements.
 * auto it = skip_invalid_elements(vec.begin(), vec.end());
 * // If there was a valid element, now advance the iterator to the next
 * // valid element (or vec.end() if there are no more).
 * it = skip_invalid_elements(++it, vec.end());
 * @endcode
 *
 *
 * @tparam IterT The iterator type
 * @param it The iterator
 * @param end The end iterator value
 * @return Iterator pointing to a valid element, or `end`.
 */
template <typename IterT>
inline IterT skip_invalid_elements(IterT it, const IterT& end) {
  while (it != end && !it->valid()) {
    ++it;
  }
  return it;
}
}  // namespace

/* ****************************** */
/*          CONSTRUCTORS          */
/* ****************************** */

Reader::Reader(
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
    : ReaderBase(
          stats,
          logger->clone("Reader", ++logger_id_),
          storage_manager,
          array,
          config,
          buffers,
          subarray,
          layout,
          condition) {
  // Sanity checks
  if (storage_manager_ == nullptr) {
    throw ReaderStatusException(
        "Cannot initialize reader; Storage manager not set");
  }

  if (!skip_checks_serialization && buffers_.empty()) {
    throw ReaderStatusException("Cannot initialize reader; Buffers not set");
  }

  if (!skip_checks_serialization && array_schema_.dense() &&
      !subarray_.is_set()) {
    throw ReaderStatusException(
        "Cannot initialize reader; Dense reads must have a subarray set");
  }

  // Check subarray
  check_subarray();

  // Initialize the read state
  init_read_state();

  // Check the validity buffer sizes. This must be performed
  // after `init_read_state` to ensure we have set the
  // member state correctly from the config.
  check_validity_buffer_sizes();
}

/* ****************************** */
/*               API              */
/* ****************************** */

Status Reader::finalize() {
  return Status::Ok();
}

bool Reader::incomplete() const {
  return read_state_.overflowed_ || !read_state_.done();
}

QueryStatusDetailsReason Reader::status_incomplete_reason() const {
  return incomplete() ? QueryStatusDetailsReason::REASON_USER_BUFFER_SIZE :
                        QueryStatusDetailsReason::REASON_NONE;
}

Status Reader::initialize_memory_budget() {
  return Status::Ok();
}

const Reader::ReadState* Reader::read_state() const {
  return &read_state_;
}

Reader::ReadState* Reader::read_state() {
  return &read_state_;
}

Status Reader::complete_read_loop() {
  if (offsets_extra_element_) {
    RETURN_NOT_OK(add_extra_offset());
  }

  return Status::Ok();
}

uint64_t Reader::get_timestamp(const ResultCoords& rc) const {
  const auto f = rc.tile_->frag_idx();
  if (fragment_metadata_[f]->has_timestamps()) {
    return rc.tile_->timestamp(rc.pos_);
  } else {
    return fragment_timestamp(rc.tile_);
  }
}

Status Reader::dowork() {
  auto timer_se = stats_->start_timer("dowork");

  // Check that the query condition is valid.
  RETURN_NOT_OK(condition_.check(array_schema_));

  if (buffers_.count(constants::delete_timestamps) != 0) {
    return logger_->status(
        Status_ReaderError("Reader cannot process delete timestamps"));
  }

  get_dim_attr_stats();

  auto dense_mode = array_schema_.dense();

  // Get next partition
  if (!read_state_.unsplittable_)
    RETURN_NOT_OK(read_state_.next());

  // Handle empty array or empty/finished subarray
  if (!dense_mode && fragment_metadata_.empty()) {
    zero_out_buffer_sizes();
    return Status::Ok();
  }

  // Loop until you find results, or unsplittable, or done
  do {
    stats_->add_counter("loop_num", 1);

    read_state_.overflowed_ = false;
    reset_buffer_sizes();

    // Perform read
    if (dense_mode) {
      RETURN_NOT_OK(dense_read());
    } else {
      RETURN_NOT_OK(sparse_read());
    }

    // In the case of overflow, we need to split the current partition
    // without advancing to the next partition
    if (read_state_.overflowed_) {
      zero_out_buffer_sizes();
      RETURN_NOT_OK(read_state_.split_current());

      if (read_state_.unsplittable_) {
        return complete_read_loop();
      }
    } else {
      bool has_results = false;
      for (const auto& it : buffers_) {
        if (*(it.second.buffer_size_) != 0)
          has_results = true;
      }

      // Need to reset unsplittable if the results fit after all
      if (has_results)
        read_state_.unsplittable_ = false;

      if (has_results || read_state_.done()) {
        return complete_read_loop();
      }

      RETURN_NOT_OK(read_state_.next());
    }
  } while (true);

  return Status::Ok();
}

void Reader::reset() {
}

/* ****************************** */
/*         PRIVATE METHODS        */
/* ****************************** */

Status Reader::load_initial_data() {
  if (initial_data_loaded_) {
    return Status::Ok();
  }

  // Load delete conditions.
  auto&& [st, conditions, update_values] =
      storage_manager_->load_delete_and_update_conditions(*array_);
  RETURN_CANCEL_OR_ERROR(st);
  delete_and_update_conditions_ = std::move(*conditions);

  // Set timestamps variables
  user_requested_timestamps_ = buffers_.count(constants::timestamps) != 0 ||
                               delete_and_update_conditions_.size() > 0;
  const bool partial_consol_fragment_overlap =
      partial_consolidated_fragment_overlap();
  use_timestamps_ = partial_consol_fragment_overlap ||
                    !array_schema_.allows_dups() || user_requested_timestamps_;

  // Add partial overlap condition for timestamps, if required.
  if (partial_consol_fragment_overlap) {
    RETURN_NOT_OK(add_partial_overlap_condition());
  }

  // Legacy reader always uses timestamped conditions. As we process all cell
  // slabs at once and they could be from fragments consolidated with
  // timestamps, there is not way to know if we need the regular condition
  // or the timestamped condition. This reader will have worst performance
  // for deletes.
  RETURN_CANCEL_OR_ERROR(generate_timestamped_conditions());

  // Make a list of dim/attr that will be loaded for query condition.
  if (!condition_.empty()) {
    qc_loaded_attr_names_set_.merge(condition_.field_names());
  }
  for (auto delete_and_update_condition : delete_and_update_conditions_) {
    qc_loaded_attr_names_set_.merge(delete_and_update_condition.field_names());
  }

  // Add delete timestamps condition.
  RETURN_NOT_OK(add_delete_timestamps_condition());

  // Load processed conditions from fragment metadata.
  if (delete_and_update_conditions_.size() > 0) {
    load_processed_conditions();
  }

  initial_data_loaded_ = true;

  return Status::Ok();
}

Status Reader::apply_query_condition(
    std::vector<ResultCellSlab>& result_cell_slabs,
    std::vector<ResultTile*>& result_tiles,
    Subarray& subarray,
    uint64_t stride) {
  if ((condition_.empty() && delete_and_update_conditions_.empty()) ||
      result_cell_slabs.empty()) {
    return Status::Ok();
  }

  // To evaluate the query condition, we need to read tiles for the
  // attributes used in the query condition. Build a map of attribute
  // names to read.
  std::unordered_map<std::string, ProcessTileFlags> names;
  for (const auto& condition_name : qc_loaded_attr_names_set_) {
    names[condition_name] = ProcessTileFlag::READ;
  }

  // Each element in `names` has been flagged with `ProcessTileFlag::READ`.
  // This will read the tiles, but will not copy them into the user buffers.
  RETURN_NOT_OK(
      process_tiles(names, result_tiles, result_cell_slabs, subarray, stride));

  // The `UINT64_MAX` is a sentinel value to indicate that we do not
  // use a stride in the cell index calculation. To simplify our logic,
  // assign this to `1`.
  if (stride == UINT64_MAX)
    stride = 1;

  if (!condition_.empty()) {
    RETURN_NOT_OK(condition_.apply(
        array_schema_, fragment_metadata_, result_cell_slabs, stride));
  }

  // Apply delete conditions.
  if (!delete_and_update_conditions_.empty()) {
    for (uint64_t i = 0; i < delete_and_update_conditions_.size(); i++) {
      // For legacy, always run the timestamped condition.
      RETURN_NOT_OK(timestamped_delete_and_update_conditions_[i].apply(
          array_schema_, fragment_metadata_, result_cell_slabs, stride));
    }
  }

  // Process the delete timestamps condition, if required.
  if (!delete_timestamps_condition_.empty()) {
    // Remove cells with partial overlap from the bitmap.
    RETURN_NOT_OK(delete_timestamps_condition_.apply(
        array_schema_, fragment_metadata_, result_cell_slabs, stride));
  }

  return Status::Ok();
}

Status Reader::compute_result_cell_slabs(
    const std::vector<ResultCoords>& result_coords,
    std::vector<ResultCellSlab>& result_cell_slabs) const {
  auto timer_se =
      stats_->start_timer("compute_sparse_result_cell_slabs_sparse");

  // Trivial case
  auto coords_num = (uint64_t)result_coords.size();
  if (coords_num == 0)
    return Status::Ok();

  // Initialize the first range
  auto coords_end = result_coords.end();
  auto it = skip_invalid_elements(result_coords.begin(), coords_end);
  if (it == coords_end) {
    return logger_->status(Status_ReaderError("Unexpected empty cell range."));
  }
  uint64_t start_pos = it->pos_;
  uint64_t end_pos = start_pos;
  ResultTile* tile = it->tile_;

  // Scan the coordinates and compute ranges
  it = skip_invalid_elements(++it, coords_end);
  while (it != coords_end) {
    if (it->tile_ == tile && it->pos_ == end_pos + 1) {
      // Same range - advance end position
      end_pos = it->pos_;
    } else {
      // New range - append previous range
      result_cell_slabs.emplace_back(tile, start_pos, end_pos - start_pos + 1);
      start_pos = it->pos_;
      end_pos = start_pos;
      tile = it->tile_;
    }
    it = skip_invalid_elements(++it, coords_end);
  }

  // Append the last range
  result_cell_slabs.emplace_back(tile, start_pos, end_pos - start_pos + 1);

  return Status::Ok();
}

Status Reader::compute_range_result_coords(
    Subarray& subarray,
    unsigned frag_idx,
    ResultTile* tile,
    uint64_t range_idx,
    std::vector<ResultCoords>& result_coords) {
  auto coords_num = tile->cell_num();
  auto dim_num = array_schema_.dim_num();
  auto cell_order = array_schema_.cell_order();
  auto range_coords = subarray.get_range_coords(range_idx);

  if (array_schema_.dense()) {
    std::vector<uint8_t> result_bitmap(coords_num, 1);
    std::vector<uint8_t> overwritten_bitmap(coords_num, 0);

    // Compute result and overwritten bitmap per dimension
    for (unsigned d = 0; d < dim_num; ++d) {
      const auto& ranges = subarray.ranges_for_dim(d);
      RETURN_NOT_OK(tile->compute_results_dense(
          d,
          ranges[range_coords[d]],
          fragment_metadata_,
          frag_idx,
          &result_bitmap,
          &overwritten_bitmap));
    }

    // Gather results
    for (uint64_t pos = 0; pos < coords_num; ++pos) {
      if (result_bitmap[pos] && !overwritten_bitmap[pos])
        result_coords.emplace_back(tile, pos);
    }
  } else {  // Sparse
    std::vector<uint8_t> result_bitmap(coords_num, 1);

    // Compute result and overwritten bitmap per dimension
    for (unsigned d = 0; d < dim_num; ++d) {
      // For col-major cell ordering, iterate the dimensions
      // in reverse.
      const unsigned dim_idx =
          cell_order == Layout::COL_MAJOR ? dim_num - d - 1 : d;
      if (!subarray.is_default(dim_idx)) {
        const auto& ranges = subarray.ranges_for_dim(dim_idx);
        RETURN_NOT_OK(tile->compute_results_sparse(
            dim_idx,
            ranges[range_coords[dim_idx]],
            &result_bitmap,
            cell_order));
      }
    }

    // Apply partial overlap condition, if required.
    const auto frag_meta = fragment_metadata_[tile->frag_idx()];
    if (process_partial_timestamps(*frag_meta)) {
      RETURN_NOT_OK(partial_overlap_condition_.apply_sparse<uint8_t>(
          *(frag_meta->array_schema().get()), *tile, result_bitmap));
    }

    // Gather results
    for (uint64_t pos = 0; pos < coords_num; ++pos) {
      if (result_bitmap[pos])
        result_coords.emplace_back(tile, pos);
    }
  }

  return Status::Ok();
}

Status Reader::compute_range_result_coords(
    Subarray& subarray,
    const std::vector<bool>& single_fragment,
    const std::map<std::pair<unsigned, uint64_t>, size_t>& result_tile_map,
    std::vector<ResultTile>& result_tiles,
    std::vector<std::vector<ResultCoords>>& range_result_coords) {
  auto timer_se = stats_->start_timer("compute_range_result_coords");

  auto range_num = subarray.range_num();
  range_result_coords.resize(range_num);
  auto cell_order = array_schema_.cell_order();
  auto allows_dups = array_schema_.allows_dups();

  // To de-dupe the ranges, we may need to sort them. If the
  // read layout is UNORDERED, we will sort by the cell layout.
  // If the cell layout is hilbert, we will sort in row-major to
  // avoid the expense of calculating hilbert values.
  Layout sort_layout = layout_;
  if (sort_layout == Layout::UNORDERED) {
    sort_layout = cell_order;
    if (sort_layout == Layout::HILBERT) {
      sort_layout = Layout::ROW_MAJOR;
    }
  }

  auto status = parallel_for(
      storage_manager_->compute_tp(), 0, range_num, [&](uint64_t r) {
        // Compute overlapping coordinates per range
        RETURN_NOT_OK(compute_range_result_coords(
            subarray,
            r,
            result_tile_map,
            result_tiles,
            range_result_coords[r]));

        // Dedup unless there is a single fragment or array schema allows
        // duplicates
        if (!single_fragment[r] && !allows_dups) {
          RETURN_CANCEL_OR_ERROR(sort_result_coords(
              range_result_coords[r].begin(),
              range_result_coords[r].end(),
              range_result_coords[r].size(),
              sort_layout));
          RETURN_CANCEL_OR_ERROR(dedup_result_coords(range_result_coords[r]));
        }

        return Status::Ok();
      });

  RETURN_NOT_OK(status);

  return Status::Ok();
}

Status Reader::compute_range_result_coords(
    Subarray& subarray,
    uint64_t range_idx,
    uint32_t fragment_idx,
    const std::map<std::pair<unsigned, uint64_t>, size_t>& result_tile_map,
    std::vector<ResultTile>& result_tiles,
    std::vector<ResultCoords>& range_result_coords) {
  // Skip dense fragments
  if (fragment_metadata_[fragment_idx]->dense())
    return Status::Ok();

  auto tr =
      subarray.tile_overlap(fragment_idx, range_idx)->tile_ranges_.begin();
  auto tr_end =
      subarray.tile_overlap(fragment_idx, range_idx)->tile_ranges_.end();
  auto t = subarray.tile_overlap(fragment_idx, range_idx)->tiles_.begin();
  auto t_end = subarray.tile_overlap(fragment_idx, range_idx)->tiles_.end();

  while (tr != tr_end || t != t_end) {
    // Handle tile range
    if (tr != tr_end && (t == t_end || tr->first < t->first)) {
      for (uint64_t i = tr->first; i <= tr->second; ++i) {
        auto pair = std::pair<unsigned, uint64_t>(fragment_idx, i);
        auto tile_it = result_tile_map.find(pair);
        assert(tile_it != result_tile_map.end());
        auto tile_idx = tile_it->second;
        auto& tile = result_tiles[tile_idx];

        // Add results only if the sparse tile MBR is not fully
        // covered by a more recent fragment's non-empty domain
        if (!sparse_tile_overwritten(fragment_idx, i))
          RETURN_NOT_OK(get_all_result_coords(&tile, range_result_coords));
      }
      ++tr;
    } else {
      // Handle single tile
      auto pair = std::pair<unsigned, uint64_t>(fragment_idx, t->first);
      auto tile_it = result_tile_map.find(pair);
      assert(tile_it != result_tile_map.end());
      auto tile_idx = tile_it->second;
      auto& tile = result_tiles[tile_idx];
      if (t->second == 1.0) {  // Full overlap
        // Add results only if the sparse tile MBR is not fully
        // covered by a more recent fragment's non-empty domain
        if (!sparse_tile_overwritten(fragment_idx, t->first))
          RETURN_NOT_OK(get_all_result_coords(&tile, range_result_coords));
      } else {  // Partial overlap
        RETURN_NOT_OK(compute_range_result_coords(
            subarray, fragment_idx, &tile, range_idx, range_result_coords));
      }
      ++t;
    }
  }

  return Status::Ok();
}

Status Reader::compute_range_result_coords(
    Subarray& subarray,
    uint64_t range_idx,
    const std::map<std::pair<unsigned, uint64_t>, size_t>& result_tile_map,
    std::vector<ResultTile>& result_tiles,
    std::vector<ResultCoords>& range_result_coords) {
  // Gather result range coordinates per fragment
  auto fragment_num = fragment_metadata_.size();
  std::vector<std::vector<ResultCoords>> range_result_coords_vec(fragment_num);
  auto status = parallel_for(
      storage_manager_->compute_tp(), 0, fragment_num, [&](uint32_t f) {
        return compute_range_result_coords(
            subarray,
            range_idx,
            f,
            result_tile_map,
            result_tiles,
            range_result_coords_vec[f]);
      });
  RETURN_NOT_OK(status);

  // Consolidate the result coordinates in the single result vector
  for (const auto& vec : range_result_coords_vec) {
    for (const auto& r : vec)
      range_result_coords.emplace_back(r);
  }

  return Status::Ok();
}

Status Reader::compute_subarray_coords(
    std::vector<std::vector<ResultCoords>>& range_result_coords,
    std::vector<ResultCoords>& result_coords) {
  auto timer_se = stats_->start_timer("compute_subarray_coords");
  // The input 'result_coords' is already sorted. Save the current size
  // before inserting new elements.
  const size_t result_coords_size = result_coords.size();

  // Add all valid ``range_result_coords`` to ``result_coords``
  for (const auto& rv : range_result_coords) {
    for (const auto& c : rv) {
      if (c.valid())
        result_coords.emplace_back(c.tile_, c.pos_);
    }
  }

  // No need to sort in UNORDERED layout
  if (layout_ == Layout::UNORDERED)
    return Status::Ok();

  // We should not sort if:
  // - there is a single fragment and global order
  // - there is a single fragment and one dimension
  // - there are multiple fragments and a single range and dups are not allowed
  //   (therefore, the coords in that range have already been sorted)
  auto dim_num = array_schema_.dim_num();
  bool must_sort = true;
  auto allows_dups = array_schema_.allows_dups();
  auto single_range = (range_result_coords.size() == 1);
  if (layout_ == Layout::GLOBAL_ORDER || dim_num == 1) {
    must_sort = !belong_to_single_fragment(
        result_coords.begin() + result_coords_size, result_coords.end());
  } else if (single_range && !allows_dups) {
    must_sort = belong_to_single_fragment(
        result_coords.begin() + result_coords_size, result_coords.end());
  }

  if (must_sort) {
    RETURN_NOT_OK(sort_result_coords(
        result_coords.begin() + result_coords_size,
        result_coords.end(),
        result_coords.size() - result_coords_size,
        layout_));
  }

  return Status::Ok();
}

Status Reader::compute_sparse_result_tiles(
    std::vector<ResultTile>& result_tiles,
    std::map<std::pair<unsigned, uint64_t>, size_t>* result_tile_map,
    std::vector<bool>* single_fragment) {
  auto timer_se = stats_->start_timer("compute_sparse_result_tiles");

  // For easy reference
  auto& partitioner = read_state_.partitioner_;
  const auto& subarray = partitioner.current();
  auto range_num = subarray.range_num();
  auto fragment_num = fragment_metadata_.size();
  std::vector<unsigned> first_fragment;
  first_fragment.resize(range_num);
  for (uint64_t r = 0; r < range_num; ++r)
    first_fragment[r] = UINT32_MAX;

  single_fragment->resize(range_num);
  for (uint64_t i = 0; i < range_num; ++i)
    (*single_fragment)[i] = true;

  result_tiles.clear();
  for (unsigned f = 0; f < fragment_num; ++f) {
    // Skip dense fragments
    if (fragment_metadata_[f]->dense())
      continue;

    for (uint64_t r = 0; r < range_num; ++r) {
      // Handle range of tiles (full overlap)
      const auto& tile_ranges = subarray.tile_overlap(f, r)->tile_ranges_;
      for (const auto& tr : tile_ranges) {
        for (uint64_t t = tr.first; t <= tr.second; ++t) {
          auto pair = std::pair<unsigned, uint64_t>(f, t);
          // Add tile only if it does not already exist
          if (result_tile_map->find(pair) == result_tile_map->end()) {
            result_tiles.emplace_back(
                f, t, *(fragment_metadata_[f]->array_schema()).get());
            (*result_tile_map)[pair] = result_tiles.size() - 1;
          }
          // Always check range for multiple fragments or fragments with
          // timestamps.
          if (f > first_fragment[r] || fragment_metadata_[f]->has_timestamps())
            (*single_fragment)[r] = false;
          else
            first_fragment[r] = f;
        }
      }

      // Handle single tiles
      const auto& o_tiles = subarray.tile_overlap(f, r)->tiles_;
      for (const auto& o_tile : o_tiles) {
        auto t = o_tile.first;
        auto pair = std::pair<unsigned, uint64_t>(f, t);
        // Add tile only if it does not already exist
        if (result_tile_map->find(pair) == result_tile_map->end()) {
          result_tiles.emplace_back(
              f, t, *(fragment_metadata_[f]->array_schema()).get());
          (*result_tile_map)[pair] = result_tiles.size() - 1;
        }
        // Always check range for multiple fragments or fragments with
        // timestamps.
        if (f > first_fragment[r] || fragment_metadata_[f]->has_timestamps())
          (*single_fragment)[r] = false;
        else
          first_fragment[r] = f;
      }
    }
  }

  return Status::Ok();
}

Status Reader::copy_coordinates(
    const std::vector<ResultTile*>& result_tiles,
    std::vector<ResultCellSlab>& result_cell_slabs) {
  auto timer_se = stats_->start_timer("copy_coordinates");

  if (result_cell_slabs.empty() && result_tiles.empty()) {
    zero_out_buffer_sizes();
    return Status::Ok();
  }

  const uint64_t stride = UINT64_MAX;

  // Build a list of coordinate names to copy, separating them by
  // whether they are of fixed or variable length. The motivation
  // is that copying fixed and variable cells require two different
  // cell slab partitions. Processing them separately allows us to
  // reduce memory use.
  std::vector<std::string> fixed_names;
  std::vector<std::string> var_names;

  for (const auto& it : buffers_) {
    const auto& name = it.first;
    if (read_state_.overflowed_)
      break;
    if (!(name == constants::coords || array_schema_.is_dim(name)))
      continue;

    if (array_schema_.var_size(name))
      var_names.emplace_back(name);
    else
      fixed_names.emplace_back(name);
  }

  // Copy result cells for fixed-sized coordinates.
  if (!fixed_names.empty()) {
    std::vector<size_t> fixed_cs_partitions;
    compute_fixed_cs_partitions(result_cell_slabs, &fixed_cs_partitions);

    for (const auto& name : fixed_names) {
      RETURN_CANCEL_OR_ERROR(copy_fixed_cells(
          name, stride, result_cell_slabs, &fixed_cs_partitions));
      clear_tiles(name, result_tiles);
    }
  }

  // Copy result cells for var-sized coordinates.
  if (!var_names.empty()) {
    std::vector<std::pair<size_t, size_t>> var_cs_partitions;
    size_t total_var_cs_length;
    compute_var_cs_partitions(
        result_cell_slabs, &var_cs_partitions, &total_var_cs_length);

    for (const auto& name : var_names) {
      RETURN_CANCEL_OR_ERROR(copy_var_cells(
          name,
          stride,
          result_cell_slabs,
          &var_cs_partitions,
          total_var_cs_length));
      clear_tiles(name, result_tiles);
    }
  }

  return Status::Ok();
}

Status Reader::copy_attribute_values(
    const uint64_t stride,
    std::vector<ResultTile*>& result_tiles,
    std::vector<ResultCellSlab>& result_cell_slabs,
    Subarray& subarray) {
  auto timer_se = stats_->start_timer("copy_attr_values");

  if (result_cell_slabs.empty() && result_tiles.empty()) {
    zero_out_buffer_sizes();
    return Status::Ok();
  }

  // Build a set of attribute names to process.
  std::unordered_map<std::string, ProcessTileFlags> names;
  for (const auto& it : buffers_) {
    const auto& name = it.first;

    if (read_state_.overflowed_) {
      break;
    }

    if (name == constants::coords || array_schema_.is_dim(name)) {
      continue;
    }

    // If the query condition has a clause for `name`, we will only
    // flag it to copy because we have already preloaded the offsets
    // and read the tiles in `apply_query_condition`.
    ProcessTileFlags flags = ProcessTileFlag::COPY;
    if (qc_loaded_attr_names_set_.count(name) == 0) {
      flags |= ProcessTileFlag::READ;
    }

    names[name] = flags;
  }

  if (!names.empty()) {
    RETURN_NOT_OK(process_tiles(
        names, result_tiles, result_cell_slabs, subarray, stride));
  }

  return Status::Ok();
}

Status Reader::copy_fixed_cells(
    const std::string& name,
    uint64_t stride,
    const std::vector<ResultCellSlab>& result_cell_slabs,
    std::vector<size_t>* fixed_cs_partitions) {
  auto stat_type = (array_schema_.is_attr(name)) ? "copy_fixed_attr_values" :
                                                   "copy_fixed_coords";
  auto timer_se = stats_->start_timer(stat_type);

  if (result_cell_slabs.empty()) {
    zero_out_buffer_sizes();
    return Status::Ok();
  }

  auto it = buffers_.find(name);
  auto buffer_size = it->second.buffer_size_;
  auto cell_size = array_schema_.cell_size(name);

  // Precompute the cell range destination offsets in the buffer.
  uint64_t buffer_offset = 0;
  std::vector<uint64_t> cs_offsets(result_cell_slabs.size());
  for (uint64_t i = 0; i < cs_offsets.size(); i++) {
    const auto& cs = result_cell_slabs[i];
    auto cs_length = cs.length_;

    auto bytes_to_copy = cs_length * cell_size;
    cs_offsets[i] = buffer_offset;
    buffer_offset += bytes_to_copy;
  }

  // Handle overflow.
  if (buffer_offset > *buffer_size) {
    read_state_.overflowed_ = true;
    return Status::Ok();
  }

  // Copy result cell slabs in parallel.
  std::function<Status(size_t)> copy_fn = std::bind(
      &Reader::copy_partitioned_fixed_cells,
      this,
      std::placeholders::_1,
      &name,
      stride,
      result_cell_slabs,
      &cs_offsets,
      fixed_cs_partitions);
  auto status = parallel_for(
      storage_manager_->compute_tp(),
      0,
      fixed_cs_partitions->size(),
      std::move(copy_fn));

  RETURN_NOT_OK(status);

  // Update buffer offsets
  *(buffers_[name].buffer_size_) = buffer_offset;
  if (array_schema_.is_nullable(name)) {
    *(buffers_[name].validity_vector_.buffer_size()) =
        (buffer_offset / cell_size) * constants::cell_validity_size;
  }

  return Status::Ok();
}

void Reader::compute_fixed_cs_partitions(
    const std::vector<ResultCellSlab>& result_cell_slabs,
    std::vector<size_t>* fixed_cs_partitions) {
  if (result_cell_slabs.empty()) {
    return;
  }

  const auto num_copy_threads =
      storage_manager_->compute_tp()->concurrency_level();

  // Calculate the partition sizes.
  auto num_cs = result_cell_slabs.size();
  const uint64_t num_cs_partitions =
      std::min<uint64_t>(num_copy_threads, num_cs);
  const uint64_t cs_per_partition = num_cs / num_cs_partitions;
  const uint64_t cs_per_partition_carry = num_cs % num_cs_partitions;

  // Calculate the partition offsets.
  uint64_t num_cs_partitioned = 0;
  fixed_cs_partitions->reserve(num_cs_partitions);
  for (uint64_t i = 0; i < num_cs_partitions; ++i) {
    const uint64_t num_cs_in_partition =
        cs_per_partition + ((i < cs_per_partition_carry) ? 1 : 0);
    num_cs_partitioned += num_cs_in_partition;
    fixed_cs_partitions->emplace_back(num_cs_partitioned);
  }
}

Status Reader::copy_partitioned_fixed_cells(
    const size_t partition_idx,
    const std::string* const name,
    const uint64_t stride,
    const std::vector<ResultCellSlab>& result_cell_slabs,
    const std::vector<uint64_t>* cs_offsets,
    const std::vector<size_t>* cs_partitions) {
  assert(name);

  // For easy reference.
  auto nullable = array_schema_.is_nullable(*name);
  auto it = buffers_.find(*name);
  auto buffer = (unsigned char*)it->second.buffer_;
  auto buffer_validity = (unsigned char*)it->second.validity_vector_.buffer();
  auto cell_size = array_schema_.cell_size(*name);
  const auto is_attr = array_schema_.is_attr(*name);
  const auto is_dim = array_schema_.is_dim(*name);
  ByteVecValue fill_value;
  uint8_t fill_value_validity = 0;
  if (is_attr) {
    fill_value = array_schema_.attribute(*name)->fill_value();
    fill_value_validity = array_schema_.attribute(*name)->fill_value_validity();
  }
  uint64_t fill_value_size = (uint64_t)fill_value.size();
  const bool is_timestamps = *name == constants::timestamps;

  // Calculate the partition to operate on.
  const uint64_t cs_idx_start =
      partition_idx == 0 ? 0 : cs_partitions->at(partition_idx - 1);
  const uint64_t cs_idx_end = cs_partitions->at(partition_idx);

  // Copy the cells.
  for (uint64_t cs_idx = cs_idx_start; cs_idx < cs_idx_end; ++cs_idx) {
    const auto& cs = result_cell_slabs[cs_idx];
    uint64_t offset = cs_offsets->at(cs_idx);
    auto cs_length = cs.length_;

    // Copy

    // First we check if this is an older (pre TileDB 2.0) array with zipped
    // coordinates and the user has requested split buffer if so we should
    // proceed to copying the tile. If not, and there is no tile or the tile is
    // empty for the field then this is a read of an older fragment in schema
    // evolution. In that case we want to set the field to fill values for this
    // for this tile.
    const bool split_buffer_for_zipped_coords =
        is_dim && cs.tile_->stores_zipped_coords();
    const bool field_not_present =
        (is_dim || is_attr) && cs.tile_->tile_tuple(*name) == nullptr;
    if ((cs.tile_ == nullptr || field_not_present) &&
        !split_buffer_for_zipped_coords) {  // Empty range or attributed added
                                            // in schema evolution
      auto bytes_to_copy = cs_length * cell_size;
      auto fill_num = bytes_to_copy / fill_value_size;
      for (uint64_t j = 0; j < fill_num; ++j) {
        std::memcpy(buffer + offset, fill_value.data(), fill_value_size);
        if (nullable) {
          std::memset(
              buffer_validity +
                  (offset / cell_size * constants::cell_validity_size),
              fill_value_validity,
              constants::cell_validity_size);
        }
        offset += fill_value_size;
      }
    } else {  // Non-empty range
      // Pass in the fragment timestamp if required.
      uint64_t timestamp = UINT64_MAX;
      if (is_timestamps &&
          !fragment_metadata_[cs.tile_->frag_idx()]->has_timestamps()) {
        timestamp = fragment_timestamp(cs.tile_);
      }

      if (stride == UINT64_MAX) {
        if (!nullable)
          RETURN_NOT_OK(cs.tile_->read(
              *name, buffer, offset, cs.start_, cs_length, timestamp));
        else
          RETURN_NOT_OK(cs.tile_->read_nullable(
              *name, buffer, offset, cs.start_, cs_length, buffer_validity));
      } else {
        auto cell_offset = offset;
        auto start = cs.start_;
        for (uint64_t j = 0; j < cs_length; ++j) {
          if (!nullable)
            RETURN_NOT_OK(cs.tile_->read(
                *name, buffer, cell_offset, start, 1, timestamp));
          else
            RETURN_NOT_OK(cs.tile_->read_nullable(
                *name, buffer, cell_offset, start, 1, buffer_validity));
          cell_offset += cell_size;
          start += stride;
        }
      }
    }
  }

  return Status::Ok();
}

Status Reader::copy_var_cells(
    const std::string& name,
    const uint64_t stride,
    std::vector<ResultCellSlab>& result_cell_slabs,
    std::vector<std::pair<size_t, size_t>>* var_cs_partitions,
    size_t total_cs_length) {
  auto stat_type = (array_schema_.is_attr(name)) ? "copy_var_attr_values" :
                                                   "copy_var_coords";
  auto timer_se = stats_->start_timer(stat_type);

  if (result_cell_slabs.empty()) {
    zero_out_buffer_sizes();
    return Status::Ok();
  }

  std::vector<uint64_t> offset_offsets_per_cs(total_cs_length);
  std::vector<uint64_t> var_offsets_per_cs(total_cs_length);

  // Compute the destinations of offsets and var-len data in the buffers.
  uint64_t total_offset_size, total_var_size, total_validity_size;
  RETURN_NOT_OK(compute_var_cell_destinations(
      name,
      stride,
      result_cell_slabs,
      &offset_offsets_per_cs,
      &var_offsets_per_cs,
      &total_offset_size,
      &total_var_size,
      &total_validity_size));

  // Check for overflow and return early (without copying) in that case.
  if (read_state_.overflowed_) {
    return Status::Ok();
  }

  // Copy result cell slabs in parallel
  std::function<Status(size_t)> copy_fn = std::bind(
      &Reader::copy_partitioned_var_cells,
      this,
      std::placeholders::_1,
      &name,
      stride,
      result_cell_slabs,
      &offset_offsets_per_cs,
      &var_offsets_per_cs,
      var_cs_partitions);
  auto status = parallel_for(
      storage_manager_->compute_tp(), 0, var_cs_partitions->size(), copy_fn);

  RETURN_NOT_OK(status);

  // Update buffer offsets
  *(buffers_[name].buffer_size_) = total_offset_size;
  *(buffers_[name].buffer_var_size_) = total_var_size;
  if (array_schema_.is_nullable(name))
    *(buffers_[name].validity_vector_.buffer_size()) = total_validity_size;

  return Status::Ok();
}

void Reader::compute_var_cs_partitions(
    const std::vector<ResultCellSlab>& result_cell_slabs,
    std::vector<std::pair<size_t, size_t>>* var_cs_partitions,
    size_t* total_var_cs_length) {
  if (result_cell_slabs.empty()) {
    return;
  }

  const auto num_copy_threads =
      storage_manager_->compute_tp()->concurrency_level();

  // Calculate the partition range.
  const uint64_t num_cs = result_cell_slabs.size();
  const uint64_t num_cs_partitions =
      std::min<uint64_t>(num_copy_threads, num_cs);
  const uint64_t cs_per_partition = num_cs / num_cs_partitions;
  const uint64_t cs_per_partition_carry = num_cs % num_cs_partitions;

  // Compute the boundary between each partition. Each boundary
  // is represented by an `std::pair` that contains the total
  // length of each cell slab in the leading partition and an
  // exclusive cell slab index that ends the partition.
  uint64_t next_partition_idx = cs_per_partition;
  if (cs_per_partition_carry > 0)
    ++next_partition_idx;

  *total_var_cs_length = 0;
  var_cs_partitions->reserve(num_cs_partitions);
  for (uint64_t cs_idx = 0; cs_idx < num_cs; cs_idx++) {
    if (cs_idx == next_partition_idx) {
      var_cs_partitions->emplace_back(*total_var_cs_length, cs_idx);

      // The final partition may contain extra cell slabs that did
      // not evenly divide into the partition range. Set the
      // `next_partition_idx` to zero and build the last boundary
      // after this for-loop.
      if (var_cs_partitions->size() == num_cs_partitions) {
        next_partition_idx = 0;
      } else {
        next_partition_idx += cs_per_partition;
        if (cs_idx < (cs_per_partition_carry - 1))
          ++next_partition_idx;
      }
    }

    *total_var_cs_length += result_cell_slabs[cs_idx].length_;
  }

  // Store the final boundary.
  var_cs_partitions->emplace_back(*total_var_cs_length, num_cs);
}

Status Reader::compute_var_cell_destinations(
    const std::string& name,
    uint64_t stride,
    std::vector<ResultCellSlab>& result_cell_slabs,
    std::vector<uint64_t>* offset_offsets_per_cs,
    std::vector<uint64_t>* var_offsets_per_cs,
    uint64_t* total_offset_size,
    uint64_t* total_var_size,
    uint64_t* total_validity_size) {
  // For easy reference
  auto nullable = array_schema_.is_nullable(name);
  auto num_cs = result_cell_slabs.size();
  auto offset_size = offsets_bytesize();
  ByteVecValue fill_value;
  if (array_schema_.is_attr(name))
    fill_value = array_schema_.attribute(name)->fill_value();
  auto fill_value_size = (uint64_t)fill_value.size();

  auto it = buffers_.find(name);
  auto buffer_size = *it->second.buffer_size_;
  auto buffer_var_size = *it->second.buffer_var_size_;
  auto buffer_validity_size = it->second.validity_vector_.buffer_size();

  if (offsets_extra_element_)
    buffer_size -= offset_size;

  // Compute the destinations for all result cell slabs
  *total_offset_size = 0;
  *total_var_size = 0;
  *total_validity_size = 0;
  size_t total_cs_length = 0;
  for (uint64_t cs_idx = 0; cs_idx < num_cs; cs_idx++) {
    const auto& cs = result_cell_slabs.at(cs_idx);
    auto cs_length = cs.length_;

    // Get tile information, if the range is nonempty.
    uint64_t* tile_offsets = nullptr;
    uint64_t tile_cell_num = 0;
    uint64_t tile_var_size = 0;
    if (cs.tile_ != nullptr && cs.tile_->tile_tuple(name) != nullptr) {
      const auto tile_tuple = cs.tile_->tile_tuple(name);
      const auto& tile = tile_tuple->fixed_tile();
      const auto& tile_var = tile_tuple->var_tile();

      // Get the internal buffer to the offset values.
      tile_offsets = (uint64_t*)tile.data();
      tile_cell_num = tile.cell_num();
      tile_var_size = tile_var.size();
    }

    // Compute the destinations for each cell in the range.
    uint64_t dest_vec_idx = 0;
    stride = (stride == UINT64_MAX) ? 1 : stride;

    for (auto cell_idx = cs.start_; dest_vec_idx < cs_length;
         cell_idx += stride, dest_vec_idx++) {
      // Get size of variable-sized cell
      uint64_t cell_var_size = 0;
      if (cs.tile_ == nullptr || cs.tile_->tile_tuple(name) == nullptr) {
        cell_var_size = fill_value_size;
      } else {
        cell_var_size =
            (cell_idx != tile_cell_num - 1) ?
                tile_offsets[cell_idx + 1] - tile_offsets[cell_idx] :
                tile_var_size - (tile_offsets[cell_idx] - tile_offsets[0]);
      }

      if (*total_offset_size + offset_size > buffer_size ||
          *total_var_size + cell_var_size > buffer_var_size ||
          (buffer_validity_size &&
           *total_validity_size + constants::cell_validity_size >
               *buffer_validity_size)) {
        read_state_.overflowed_ = true;

        // In case an extra offset is configured, we need to account memory for
        // it on each read
        *total_offset_size += offsets_extra_element_ ? offset_size : 0;

        return Status::Ok();
      }

      // Record destination offsets.
      (*offset_offsets_per_cs)[total_cs_length + dest_vec_idx] =
          *total_offset_size;
      (*var_offsets_per_cs)[total_cs_length + dest_vec_idx] = *total_var_size;
      *total_offset_size += offset_size;
      *total_var_size += cell_var_size;
      if (nullable)
        *total_validity_size += constants::cell_validity_size;
    }

    total_cs_length += cs_length;
  }

  // In case an extra offset is configured, we need to account memory for it on
  // each read
  *total_offset_size += offsets_extra_element_ ? offset_size : 0;

  return Status::Ok();
}

Status Reader::copy_partitioned_var_cells(
    const size_t partition_idx,
    const std::string* const name,
    uint64_t stride,
    const std::vector<ResultCellSlab>& result_cell_slabs,
    const std::vector<uint64_t>* const offset_offsets_per_cs,
    const std::vector<uint64_t>* const var_offsets_per_cs,
    const std::vector<std::pair<size_t, size_t>>* const cs_partitions) {
  assert(name);

  auto it = buffers_.find(*name);
  auto nullable = array_schema_.is_nullable(*name);
  auto buffer = (unsigned char*)it->second.buffer_;
  auto buffer_var = (unsigned char*)it->second.buffer_var_;
  auto buffer_validity = (unsigned char*)it->second.validity_vector_.buffer();
  auto offset_size = offsets_bytesize();
  ByteVecValue fill_value;
  uint8_t fill_value_validity = 0;
  if (array_schema_.is_attr(*name)) {
    fill_value = array_schema_.attribute(*name)->fill_value();
    fill_value_validity = array_schema_.attribute(*name)->fill_value_validity();
  }
  auto fill_value_size = (uint64_t)fill_value.size();
  auto attr_datatype_size = datatype_size(array_schema_.type(*name));

  // Fetch the starting array offset into both `offset_offsets_per_cs`
  // and `var_offsets_per_cs`.
  size_t arr_offset =
      partition_idx == 0 ? 0 : (*cs_partitions)[partition_idx - 1].first;

  // Fetch the inclusive starting cell slab index and the exclusive ending
  // cell slab index.
  const size_t start_cs_idx =
      partition_idx == 0 ? 0 : (*cs_partitions)[partition_idx - 1].second;
  const size_t end_cs_idx = (*cs_partitions)[partition_idx].second;

  // Copy all cells within the range of cell slabs.
  for (uint64_t cs_idx = start_cs_idx; cs_idx < end_cs_idx; ++cs_idx) {
    const auto& cs = result_cell_slabs[cs_idx];
    auto cs_length = cs.length_;

    // Get tile information, if the range is nonempty.
    uint64_t* tile_offsets = nullptr;
    Tile* tile_var = nullptr;
    Tile* tile_validity = nullptr;
    uint64_t tile_cell_num = 0;
    if (cs.tile_ != nullptr && cs.tile_->tile_tuple(*name) != nullptr) {
      const auto tile_tuple = cs.tile_->tile_tuple(*name);
      Tile* const tile = &tile_tuple->fixed_tile();
      tile_var = &tile_tuple->var_tile();
      tile_validity = nullable ? &tile_tuple->validity_tile() : nullptr;

      // Get the internal buffer to the offset values.
      tile_offsets = (uint64_t*)tile->data();
      tile_cell_num = tile->cell_num();
    }

    // Copy each cell in the range
    uint64_t dest_vec_idx = 0;
    stride = (stride == UINT64_MAX) ? 1 : stride;
    for (auto cell_idx = cs.start_; dest_vec_idx < cs_length;
         cell_idx += stride, dest_vec_idx++) {
      auto offset_offsets = (*offset_offsets_per_cs)[arr_offset + dest_vec_idx];
      auto offset_dest = buffer + offset_offsets;
      auto var_offset = (*var_offsets_per_cs)[arr_offset + dest_vec_idx];
      auto var_dest = buffer_var + var_offset;
      auto validity_dest = buffer_validity + (offset_offsets / offset_size);

      if (offsets_format_mode_ == "elements") {
        var_offset = var_offset / attr_datatype_size;
      }

      // Copy offset
      std::memcpy(offset_dest, &var_offset, offset_size);

      // Copy variable-sized value
      if (cs.tile_ == nullptr || cs.tile_->tile_tuple(*name) == nullptr) {
        std::memcpy(var_dest, fill_value.data(), fill_value_size);
        if (nullable)
          std::memset(
              validity_dest,
              fill_value_validity,
              constants::cell_validity_size);
      } else {
        const uint64_t cell_var_size =
            (cell_idx != tile_cell_num - 1) ?
                tile_offsets[cell_idx + 1] - tile_offsets[cell_idx] :
                tile_var->size() - (tile_offsets[cell_idx] - tile_offsets[0]);
        const uint64_t tile_var_offset =
            tile_offsets[cell_idx] - tile_offsets[0];

        RETURN_NOT_OK(tile_var->read(var_dest, tile_var_offset, cell_var_size));

        if (nullable)
          RETURN_NOT_OK(tile_validity->read(
              validity_dest, cell_idx, constants::cell_validity_size));
      }
    }

    arr_offset += cs_length;
  }

  return Status::Ok();
}

Status Reader::process_tiles(
    const std::unordered_map<std::string, ProcessTileFlags>& names,
    std::vector<ResultTile*>& result_tiles,
    std::vector<ResultCellSlab>& result_cell_slabs,
    Subarray& subarray,
    const uint64_t stride) {
  // If a name needs to be read, we put it on `read_names` vector (it may
  // contain other flags). Otherwise, we put the name on the `copy_names`
  // vector if it needs to be copied back to the user buffer.
  // We can benefit from concurrent reads by processing `read_names`
  // separately from `copy_names`.
  std::vector<std::string> read_names;
  std::vector<std::string> copy_names;
  std::vector<std::string> var_size_read_names;
  read_names.reserve(names.size());
  for (const auto& name_pair : names) {
    const std::string name = name_pair.first;
    const ProcessTileFlags flags = name_pair.second;
    if (flags & ProcessTileFlag::READ) {
      read_names.push_back(name);
      if (array_schema_.var_size(name))
        var_size_read_names.emplace_back(name);
    } else if (flags & ProcessTileFlag::COPY) {
      copy_names.push_back(name);
    }
  }

  // Pre-load all attribute offsets into memory for attributes
  // to be read.
  RETURN_NOT_OK(load_tile_offsets(subarray, read_names));

  // Pre-load all var attribute var tile sizes into memory for attributes
  // to be read.
  RETURN_NOT_OK(load_tile_var_sizes(subarray, var_size_read_names));

  // Get the maximum number of attributes to read and unfilter in parallel.
  // Each attribute requires additional memory to buffer reads into
  // before copying them back into `buffers_`. Cells must be copied
  // before moving onto the next set of concurrent reads to prevent
  // bloating memory. Additionally, the copy cells paths are performed
  // in serial, which will bottleneck the read concurrency. Increasing
  // this number will have diminishing returns on performance.
  const uint64_t concurrent_reads = constants::concurrent_attr_reads;

  // Instantiate partitions for copying fixed and variable cells.
  std::vector<size_t> fixed_cs_partitions;
  compute_fixed_cs_partitions(result_cell_slabs, &fixed_cs_partitions);

  std::vector<std::pair<size_t, size_t>> var_cs_partitions;
  size_t total_var_cs_length;
  compute_var_cs_partitions(
      result_cell_slabs, &var_cs_partitions, &total_var_cs_length);

  // Handle attribute/dimensions that need to be copied but do
  // not need to be read.
  for (const auto& copy_name : copy_names) {
    if (!array_schema_.var_size(copy_name))
      RETURN_CANCEL_OR_ERROR(copy_fixed_cells(
          copy_name, stride, result_cell_slabs, &fixed_cs_partitions));
    else
      RETURN_CANCEL_OR_ERROR(copy_var_cells(
          copy_name,
          stride,
          result_cell_slabs,
          &var_cs_partitions,
          total_var_cs_length));
    clear_tiles(copy_name, result_tiles);
  }

  // Iterate through all of the attribute names. This loop
  // will read, unfilter, and copy tiles back into the `buffers_`.
  uint64_t idx = 0;
  tdb_unique_ptr<ResultCellSlabsIndex> rcs_index = nullptr;
  while (idx < read_names.size()) {
    // We will perform `concurrent_reads` unless we have a smaller
    // number of remaining attributes to process.
    const uint64_t num_reads =
        std::min(concurrent_reads, read_names.size() - idx);

    // Build a vector of the attribute names to process.
    std::vector<std::string> inner_names(
        read_names.begin() + idx, read_names.begin() + idx + num_reads);

    // Read the tiles for the names in `inner_names`. Each attribute
    // name will be read concurrently.
    RETURN_CANCEL_OR_ERROR(read_attribute_tiles(inner_names, result_tiles));

    // Copy the cells into the associated `buffers_`, and then clear the cells
    // from the tiles. The cell copies are not thread safe. Clearing tiles are
    // thread safe, but quick enough that they do not justify scheduling on
    // separate threads.
    for (const auto& inner_name : inner_names) {
      const ProcessTileFlags flags = names.at(inner_name);

      RETURN_CANCEL_OR_ERROR(unfilter_tiles(inner_name, result_tiles));

      if (flags & ProcessTileFlag::COPY) {
        if (!array_schema_.var_size(inner_name)) {
          RETURN_CANCEL_OR_ERROR(copy_fixed_cells(
              inner_name, stride, result_cell_slabs, &fixed_cs_partitions));
        } else {
          RETURN_CANCEL_OR_ERROR(copy_var_cells(
              inner_name,
              stride,
              result_cell_slabs,
              &var_cs_partitions,
              total_var_cs_length));
        }
        clear_tiles(inner_name, result_tiles);
      }
    }

    idx += inner_names.size();
  }

  return Status::Ok();
}

template <class T>
Status Reader::compute_result_cell_slabs(
    const Subarray& subarray,
    std::map<const T*, ResultSpaceTile<T>>& result_space_tiles,
    std::vector<ResultCoords>& result_coords,
    std::vector<ResultTile*>& result_tiles,
    std::vector<ResultCellSlab>& result_cell_slabs) const {
  auto timer_se = stats_->start_timer("compute_sparse_result_cell_slabs_dense");

  auto layout = subarray.layout();
  if (layout == Layout::ROW_MAJOR || layout == Layout::COL_MAJOR) {
    uint64_t result_coords_pos = 0;
    std::set<std::pair<unsigned, uint64_t>> frag_tile_set;
    return compute_result_cell_slabs_row_col<T>(
        subarray,
        result_space_tiles,
        result_coords,
        &result_coords_pos,
        result_tiles,
        frag_tile_set,
        result_cell_slabs);
  } else if (layout == Layout::GLOBAL_ORDER) {
    return compute_result_cell_slabs_global<T>(
        subarray,
        result_space_tiles,
        result_coords,
        result_tiles,
        result_cell_slabs);
  } else {  // UNORDERED
    assert(false);
  }

  return Status::Ok();
}

template <class T>
Status Reader::compute_result_cell_slabs_row_col(
    const Subarray& subarray,
    std::map<const T*, ResultSpaceTile<T>>& result_space_tiles,
    std::vector<ResultCoords>& result_coords,
    uint64_t* result_coords_pos,
    std::vector<ResultTile*>& result_tiles,
    std::set<std::pair<unsigned, uint64_t>>& frag_tile_set,
    std::vector<ResultCellSlab>& result_cell_slabs) const {
  // Compute result space tiles. The result space tiles hold all the
  // relevant result tiles of the dense fragments
  compute_result_space_tiles<T>(
      subarray, read_state_.partitioner_.subarray(), result_space_tiles);

  // Gather result cell slabs and pointers to result tiles
  // `result_tiles` holds pointers to tiles that store actual results,
  // which can be stored either in `sparse_result_tiles` (sparse)
  // or in `result_space_tiles` (dense).
  auto rcs_it = ReadCellSlabIter<T>(
      &subarray, &result_space_tiles, &result_coords, *result_coords_pos);
  for (rcs_it.begin(); !rcs_it.end(); ++rcs_it) {
    // Add result cell slab
    auto result_cell_slab = rcs_it.result_cell_slab();
    result_cell_slabs.push_back(result_cell_slab);
    // Add result tile
    if (result_cell_slab.tile_ != nullptr) {
      auto frag_idx = result_cell_slab.tile_->frag_idx();
      auto tile_idx = result_cell_slab.tile_->tile_idx();
      auto frag_tile_tuple = std::pair<unsigned, uint64_t>(frag_idx, tile_idx);
      auto it = frag_tile_set.find(frag_tile_tuple);
      if (it == frag_tile_set.end()) {
        frag_tile_set.insert(frag_tile_tuple);
        result_tiles.push_back(result_cell_slab.tile_);
      }
    }
  }
  *result_coords_pos = rcs_it.result_coords_pos();

  return Status::Ok();
}

template <class T>
Status Reader::compute_result_cell_slabs_global(
    const Subarray& subarray,
    std::map<const T*, ResultSpaceTile<T>>& result_space_tiles,
    std::vector<ResultCoords>& result_coords,
    std::vector<ResultTile*>& result_tiles,
    std::vector<ResultCellSlab>& result_cell_slabs) const {
  const auto& tile_coords = subarray.tile_coords();
  auto cell_order = array_schema_.cell_order();
  std::vector<Subarray> tile_subarrays;
  tile_subarrays.reserve(tile_coords.size());
  uint64_t result_coords_pos = 0;
  std::set<std::pair<unsigned, uint64_t>> frag_tile_set;

  for (const auto& tc : tile_coords) {
    tile_subarrays.emplace_back(
        subarray.crop_to_tile((const T*)&tc[0], cell_order));
    auto& tile_subarray = tile_subarrays.back();
    tile_subarray.template compute_tile_coords<T>();

    RETURN_NOT_OK(compute_result_cell_slabs_row_col<T>(
        tile_subarray,
        result_space_tiles,
        result_coords,
        &result_coords_pos,
        result_tiles,
        frag_tile_set,
        result_cell_slabs));
  }

  return Status::Ok();
}

Status Reader::compute_result_coords(
    std::vector<ResultTile>& result_tiles,
    std::vector<ResultCoords>& result_coords) {
  auto timer_se = stats_->start_timer("compute_result_coords");

  // Get overlapping tile indexes
  typedef std::pair<unsigned, uint64_t> FragTileTuple;
  std::map<FragTileTuple, size_t> result_tile_map;
  std::vector<bool> single_fragment;

  RETURN_CANCEL_OR_ERROR(compute_sparse_result_tiles(
      result_tiles, &result_tile_map, &single_fragment));

  if (result_tiles.empty())
    return Status::Ok();

  // Create temporary vector with pointers to result tiles, so that
  // `read_tiles`, `unfilter_tiles` below can work without changes
  std::vector<ResultTile*> tmp_result_tiles;
  for (auto& result_tile : result_tiles)
    tmp_result_tiles.push_back(&result_tile);

  // Preload zipped coordinate tile offsets. Note that this will
  // ignore fragments with a version >= 5.
  auto& subarray = read_state_.partitioner_.current();
  std::vector<std::string> zipped_coords_names = {constants::coords};
  RETURN_CANCEL_OR_ERROR(load_tile_offsets(
      read_state_.partitioner_.subarray(), zipped_coords_names));

  // Preload unzipped coordinate tile offsets. Note that this will
  // ignore fragments with a version < 5.
  const auto dim_num = array_schema_.dim_num();
  std::vector<std::string> dim_names;
  std::vector<std::string> var_size_dim_names;
  dim_names.reserve(dim_num);
  for (unsigned d = 0; d < dim_num; ++d) {
    const auto& name{array_schema_.dimension_ptr(d)->name()};
    dim_names.emplace_back(name);
    if (array_schema_.var_size(name))
      var_size_dim_names.emplace_back(name);
  }
  RETURN_CANCEL_OR_ERROR(
      load_tile_offsets(read_state_.partitioner_.subarray(), dim_names));
  RETURN_CANCEL_OR_ERROR(load_tile_var_sizes(
      read_state_.partitioner_.subarray(), var_size_dim_names));

  // Read and unfilter zipped coordinate tiles. Note that
  // this will ignore fragments with a version >= 5.
  RETURN_CANCEL_OR_ERROR(
      read_coordinate_tiles(zipped_coords_names, tmp_result_tiles));
  RETURN_CANCEL_OR_ERROR(unfilter_tiles(constants::coords, tmp_result_tiles));

  // Read and unfilter unzipped coordinate tiles. Note that
  // this will ignore fragments with a version < 5.
  RETURN_CANCEL_OR_ERROR(read_coordinate_tiles(dim_names, tmp_result_tiles));
  for (const auto& dim_name : dim_names) {
    RETURN_CANCEL_OR_ERROR(unfilter_tiles(dim_name, tmp_result_tiles));
  }

  // Read and unfilter timestamps, if required.
  if (use_timestamps_) {
    std::vector<std::string> timestamps = {constants::timestamps};
    RETURN_CANCEL_OR_ERROR(
        load_tile_offsets(read_state_.partitioner_.subarray(), timestamps));

    RETURN_CANCEL_OR_ERROR(read_attribute_tiles(timestamps, tmp_result_tiles));
    RETURN_CANCEL_OR_ERROR(
        unfilter_tiles(constants::timestamps, tmp_result_tiles));
  }

  // Read and unfilter delete timestamps.
  {
    std::vector<std::string> delete_timestamps = {constants::delete_timestamps};
    RETURN_CANCEL_OR_ERROR(load_tile_offsets(
        read_state_.partitioner_.subarray(), delete_timestamps));

    RETURN_CANCEL_OR_ERROR(
        read_attribute_tiles(delete_timestamps, tmp_result_tiles));
    RETURN_CANCEL_OR_ERROR(
        unfilter_tiles(constants::delete_timestamps, tmp_result_tiles));
  }

  // Compute the read coordinates for all fragments for each subarray range.
  std::vector<std::vector<ResultCoords>> range_result_coords;
  RETURN_CANCEL_OR_ERROR(compute_range_result_coords(
      subarray,
      single_fragment,
      result_tile_map,
      result_tiles,
      range_result_coords));
  result_tile_map.clear();

  // Compute final coords (sorted in the result layout) of the whole subarray.
  RETURN_CANCEL_OR_ERROR(
      compute_subarray_coords(range_result_coords, result_coords));
  range_result_coords.clear();

  return Status::Ok();
}

Status Reader::dedup_result_coords(
    std::vector<ResultCoords>& result_coords) const {
  auto coords_end = result_coords.end();
  auto it = skip_invalid_elements(result_coords.begin(), coords_end);
  while (it != coords_end) {
    auto next_it = skip_invalid_elements(std::next(it), coords_end);
    if (next_it != coords_end && it->same_coords(*next_it)) {
      if (get_timestamp(*it) < get_timestamp(*next_it)) {
        it->invalidate();
        it = skip_invalid_elements(++it, coords_end);
      } else {
        next_it->invalidate();
      }
    } else {
      it = skip_invalid_elements(++it, coords_end);
    }
  }
  return Status::Ok();
}

Status Reader::dense_read() {
  auto type{array_schema_.domain().dimension_ptr(0)->type()};
  switch (type) {
    case Datatype::INT8:
      return dense_read<int8_t>();
    case Datatype::UINT8:
      return dense_read<uint8_t>();
    case Datatype::INT16:
      return dense_read<int16_t>();
    case Datatype::UINT16:
      return dense_read<uint16_t>();
    case Datatype::INT32:
      return dense_read<int>();
    case Datatype::UINT32:
      return dense_read<unsigned>();
    case Datatype::INT64:
      return dense_read<int64_t>();
    case Datatype::UINT64:
      return dense_read<uint64_t>();
    case Datatype::DATETIME_YEAR:
    case Datatype::DATETIME_MONTH:
    case Datatype::DATETIME_WEEK:
    case Datatype::DATETIME_DAY:
    case Datatype::DATETIME_HR:
    case Datatype::DATETIME_MIN:
    case Datatype::DATETIME_SEC:
    case Datatype::DATETIME_MS:
    case Datatype::DATETIME_US:
    case Datatype::DATETIME_NS:
    case Datatype::DATETIME_PS:
    case Datatype::DATETIME_FS:
    case Datatype::DATETIME_AS:
    case Datatype::TIME_HR:
    case Datatype::TIME_MIN:
    case Datatype::TIME_SEC:
    case Datatype::TIME_MS:
    case Datatype::TIME_US:
    case Datatype::TIME_NS:
    case Datatype::TIME_PS:
    case Datatype::TIME_FS:
    case Datatype::TIME_AS:
      return dense_read<int64_t>();
    default:
      return logger_->status(Status_ReaderError(
          "Cannot read dense array; Unsupported domain type"));
  }

  return Status::Ok();
}

template <class T>
Status Reader::dense_read() {
  // Sanity checks
  assert(std::is_integral<T>::value);

  // Compute result coordinates from the sparse fragments
  // `sparse_result_tiles` will hold all the relevant result tiles of
  // sparse fragments
  std::vector<ResultCoords> result_coords;
  std::vector<ResultTile> sparse_result_tiles;
  RETURN_NOT_OK(compute_result_coords(sparse_result_tiles, result_coords));

  // Compute result cell slabs.
  // `result_space_tiles` will hold all the relevant result tiles of
  // dense fragments. `result` tiles will hold pointers to the
  // final result tiles for both sparse and dense fragments.
  std::map<const T*, ResultSpaceTile<T>> result_space_tiles;
  std::vector<ResultCellSlab> result_cell_slabs;
  std::vector<ResultTile*> result_tiles;
  auto& subarray = read_state_.partitioner_.current();

  RETURN_NOT_OK(subarray.compute_tile_coords<T>());
  RETURN_NOT_OK(compute_result_cell_slabs<T>(
      subarray,
      result_space_tiles,
      result_coords,
      result_tiles,
      result_cell_slabs));

  auto stride = array_schema_.domain().stride<T>(subarray.layout());
  RETURN_NOT_OK(apply_query_condition(
      result_cell_slabs,
      result_tiles,
      read_state_.partitioner_.subarray(),
      stride));

  get_result_tile_stats(result_tiles);
  get_result_cell_stats(result_cell_slabs);

  // Clear sparse coordinate tiles (not needed any more)
  erase_coord_tiles(sparse_result_tiles);

  // Needed when copying the cells
  RETURN_NOT_OK(copy_attribute_values(
      stride,
      result_tiles,
      result_cell_slabs,
      read_state_.partitioner_.subarray()));

  // Fill coordinates if the user requested them
  if (!read_state_.overflowed_ && has_coords()) {
    auto&& [st, overflowed] = fill_dense_coords<T>(subarray);
    RETURN_CANCEL_OR_ERROR(st);
    read_state_.overflowed_ = *overflowed;
  }

  return Status::Ok();
}

Status Reader::get_all_result_coords(
    ResultTile* tile, std::vector<ResultCoords>& result_coords) {
  auto coords_num = tile->cell_num();

  // Apply partial overlap condition, if required.
  const auto frag_meta = fragment_metadata_[tile->frag_idx()];
  const bool partial_overlap = frag_meta->partial_time_overlap(
      array_->timestamp_start(), array_->timestamp_end_opened_at());
  if (fragment_metadata_[tile->frag_idx()]->has_timestamps() &&
      partial_overlap) {
    std::vector<uint8_t> result_bitmap(coords_num, 1);
    RETURN_NOT_OK(partial_overlap_condition_.apply_sparse<uint8_t>(
        *(frag_meta->array_schema().get()), *tile, result_bitmap));

    for (uint64_t i = 0; i < coords_num; ++i) {
      if (result_bitmap[i]) {
        result_coords.emplace_back(tile, i);
      }
    }
  } else {
    for (uint64_t i = 0; i < coords_num; ++i) {
      result_coords.emplace_back(tile, i);
    }
  }

  return Status::Ok();
}

bool Reader::has_separate_coords() const {
  for (const auto& it : buffers_) {
    if (array_schema_.is_dim(it.first))
      return true;
  }

  return false;
}

void Reader::init_read_state() {
  auto timer_se = stats_->start_timer("init_state");

  // Check subarray
  if (subarray_.layout() == Layout::GLOBAL_ORDER &&
      subarray_.range_num() != 1) {
    throw ReaderStatusException(
        "Cannot initialize read state; Multi-range subarrays do not support "
        "global order");
  }

  // Get config
  bool found = false;
  uint64_t memory_budget = 0;
  if (!config_.get<uint64_t>("sm.memory_budget", &memory_budget, &found).ok()) {
    throw ReaderStatusException("Cannot get setting");
  }
  assert(found);

  uint64_t memory_budget_var = 0;
  if (!config_.get<uint64_t>("sm.memory_budget_var", &memory_budget_var, &found)
           .ok()) {
    throw ReaderStatusException("Cannot get setting");
  }
  assert(found);

  offsets_format_mode_ = config_.get("sm.var_offsets.mode", &found);
  assert(found);
  if (offsets_format_mode_ != "bytes" && offsets_format_mode_ != "elements") {
    throw ReaderStatusException(
        "Cannot initialize reader; Unsupported offsets"
        " format in configuration");
  }

  if (!config_
           .get<bool>(
               "sm.var_offsets.extra_element", &offsets_extra_element_, &found)
           .ok()) {
    throw ReaderStatusException("Cannot get setting");
  }
  assert(found);

  if (!config_
           .get<uint32_t>("sm.var_offsets.bitsize", &offsets_bitsize_, &found)
           .ok()) {
    throw ReaderStatusException("Cannot get setting");
  }
  assert(found);

  if (offsets_bitsize_ != 32 && offsets_bitsize_ != 64) {
    throw ReaderStatusException(
        "Cannot initialize reader; Unsupported offsets"
        " bitsize in configuration");
  }
  assert(found);

  // Consider the validity memory budget to be identical to `sm.memory_budget`
  // because the validity vector is currently a bytemap. When converted to a
  // bitmap, this can be budgeted as `sm.memory_budget` / 8
  uint64_t memory_budget_validity = memory_budget;

  // Create read state
  read_state_.partitioner_ = SubarrayPartitioner(
      &config_,
      subarray_,
      memory_budget,
      memory_budget_var,
      memory_budget_validity,
      storage_manager_->compute_tp(),
      stats_,
      logger_);
  read_state_.overflowed_ = false;
  read_state_.unsplittable_ = false;

  // Set result size budget
  for (const auto& a : buffers_) {
    auto attr_name = a.first;
    auto buffer_size = a.second.buffer_size_;
    auto buffer_var_size = a.second.buffer_var_size_;
    auto buffer_validity_size = a.second.validity_vector_.buffer_size();
    if (!array_schema_.var_size(attr_name)) {
      if (!array_schema_.is_nullable(attr_name)) {
        if (!read_state_.partitioner_
                 .set_result_budget(attr_name.c_str(), *buffer_size)
                 .ok()) {
          throw ReaderStatusException("Cannot set result budget");
        }
      } else {
        if (!read_state_.partitioner_
                 .set_result_budget_nullable(
                     attr_name.c_str(), *buffer_size, *buffer_validity_size)
                 .ok()) {
          throw ReaderStatusException("Cannot set result budget");
        }
      }
    } else {
      if (!array_schema_.is_nullable(attr_name)) {
        if (!read_state_.partitioner_
                 .set_result_budget(
                     attr_name.c_str(), *buffer_size, *buffer_var_size)
                 .ok()) {
          throw ReaderStatusException("Cannot set result budget");
        }
      } else {
        if (!read_state_.partitioner_
                 .set_result_budget_nullable(
                     attr_name.c_str(),
                     *buffer_size,
                     *buffer_var_size,
                     *buffer_validity_size)
                 .ok()) {
          throw ReaderStatusException("Cannot set result budget");
        }
      }
    }
  }

  read_state_.unsplittable_ = false;
  read_state_.overflowed_ = false;
  read_state_.initialized_ = true;
}

Status Reader::sort_result_coords(
    std::vector<ResultCoords>::iterator iter_begin,
    std::vector<ResultCoords>::iterator iter_end,
    size_t coords_num,
    Layout layout) const {
  auto timer_se = stats_->start_timer("sort_result_coords");
  auto& domain{array_schema_.domain()};

  if (layout == Layout::ROW_MAJOR) {
    parallel_sort(
        storage_manager_->compute_tp(), iter_begin, iter_end, RowCmp(domain));
  } else if (layout == Layout::COL_MAJOR) {
    parallel_sort(
        storage_manager_->compute_tp(), iter_begin, iter_end, ColCmp(domain));
  } else if (layout == Layout::GLOBAL_ORDER) {
    if (array_schema_.cell_order() == Layout::HILBERT) {
      std::vector<std::pair<uint64_t, uint64_t>> hilbert_values(coords_num);
      RETURN_NOT_OK(calculate_hilbert_values(iter_begin, &hilbert_values));
      parallel_sort(
          storage_manager_->compute_tp(),
          hilbert_values.begin(),
          hilbert_values.end(),
          HilbertCmpRCI(domain, iter_begin));
      RETURN_NOT_OK(reorganize_result_coords(iter_begin, &hilbert_values));
    } else {
      parallel_sort(
          storage_manager_->compute_tp(),
          iter_begin,
          iter_end,
          GlobalCmp(domain));
    }
  } else {
    assert(false);
  }

  return Status::Ok();
}

Status Reader::sparse_read() {
  // Load initial data.
  RETURN_NOT_OK(load_initial_data());

  // Compute result coordinates from the sparse fragments
  // `sparse_result_tiles` will hold all the relevant result tiles of
  // sparse fragments
  std::vector<ResultCoords> result_coords;
  std::vector<ResultTile> sparse_result_tiles;

  RETURN_NOT_OK(compute_result_coords(sparse_result_tiles, result_coords));
  std::vector<ResultTile*> result_tiles;
  for (auto& srt : sparse_result_tiles) {
    result_tiles.push_back(&srt);
  }

  // Compute result cell slabs
  std::vector<ResultCellSlab> result_cell_slabs;
  RETURN_CANCEL_OR_ERROR(
      compute_result_cell_slabs(result_coords, result_cell_slabs));
  result_coords.clear();

  apply_query_condition(
      result_cell_slabs, result_tiles, read_state_.partitioner_.subarray());
  get_result_tile_stats(result_tiles);
  get_result_cell_stats(result_cell_slabs);

  RETURN_NOT_OK(copy_coordinates(result_tiles, result_cell_slabs));
  RETURN_NOT_OK(copy_attribute_values(
      UINT64_MAX,
      result_tiles,
      result_cell_slabs,
      read_state_.partitioner_.subarray()));

  return Status::Ok();
}

Status Reader::add_extra_offset() {
  for (const auto& it : buffers_) {
    const auto& name = it.first;
    if (!array_schema_.var_size(name))
      continue;

    // Do not apply offset for empty results because we will
    // write backwards and corrupt memory we don't own.
    if (*it.second.buffer_size_ == 0)
      continue;

    // The buffer should always be 0 or divisible by the bytesize.
    assert(!(*it.second.buffer_size_ < offsets_bytesize()));

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
      return logger_->status(Status_ReaderError(
          "Cannot add extra offset to buffer; Unsupported offsets format"));
    }
  }

  return Status::Ok();
}

bool Reader::sparse_tile_overwritten(
    unsigned frag_idx, uint64_t tile_idx) const {
  const auto& mbr = fragment_metadata_[frag_idx]->mbr(tile_idx);
  assert(!mbr.empty());
  auto fragment_num = (unsigned)fragment_metadata_.size();
  auto& domain{array_schema_.domain()};

  for (unsigned f = frag_idx + 1; f < fragment_num; ++f) {
    if (fragment_metadata_[f]->dense() &&
        domain.covered(mbr, fragment_metadata_[f]->non_empty_domain()))
      return true;
  }

  return false;
}

void Reader::erase_coord_tiles(std::vector<ResultTile>& result_tiles) const {
  for (auto& tile : result_tiles) {
    auto dim_num = array_schema_.dim_num();
    for (unsigned d = 0; d < dim_num; ++d)
      tile.erase_tile(array_schema_.dimension_ptr(d)->name());
    tile.erase_tile(constants::coords);
  }
}

void Reader::get_result_cell_stats(
    const std::vector<ResultCellSlab>& result_cell_slabs) const {
  uint64_t result_num = 0;
  for (const auto& rc : result_cell_slabs)
    result_num += rc.length_;
  stats_->add_counter("result_num", result_num);
}

void Reader::get_result_tile_stats(
    const std::vector<ResultTile*>& result_tiles) const {
  stats_->add_counter("overlap_tile_num", result_tiles.size());

  uint64_t cell_num = 0;
  for (const auto& rt : result_tiles) {
    if (!fragment_metadata_[rt->frag_idx()]->dense())
      cell_num += rt->cell_num();
    else
      cell_num += array_schema_.domain().cell_num_per_tile();
  }
  stats_->add_counter("cell_num", cell_num);
}

Status Reader::calculate_hilbert_values(
    std::vector<ResultCoords>::iterator iter_begin,
    std::vector<std::pair<uint64_t, uint64_t>>* hilbert_values) const {
  auto timer_se = stats_->start_timer("calculate_hilbert_values");
  auto dim_num = array_schema_.dim_num();
  Hilbert h(dim_num);
  auto bits = h.bits();
  auto max_bucket_val = ((uint64_t)1 << bits) - 1;
  auto coords_num = (uint64_t)hilbert_values->size();

  // Calculate Hilbert values in parallel
  auto status = parallel_for(
      storage_manager_->compute_tp(), 0, coords_num, [&](uint64_t c) {
        std::vector<uint64_t> coords(dim_num);
        for (uint32_t d = 0; d < dim_num; ++d) {
          auto dim{array_schema_.dimension_ptr(d)};
          coords[d] = hilbert_order::map_to_uint64(
              *dim, *(iter_begin + c), d, bits, max_bucket_val);
        }
        (*hilbert_values)[c] =
            std::pair<uint64_t, uint64_t>(h.coords_to_hilbert(&coords[0]), c);
        return Status::Ok();
      });

  RETURN_NOT_OK_ELSE(status, logger_->status(status));

  return Status::Ok();
}

Status Reader::reorganize_result_coords(
    std::vector<ResultCoords>::iterator iter_begin,
    std::vector<std::pair<uint64_t, uint64_t>>* hilbert_values) const {
  auto timer_se = stats_->start_timer("reorganize_result_coords");
  auto coords_num = hilbert_values->size();
  size_t i_src, i_dst;
  ResultCoords pending;
  for (size_t i_dst_first = 0; i_dst_first < coords_num; ++i_dst_first) {
    // Check if this element needs to be permuted
    i_src = (*hilbert_values)[i_dst_first].second;
    if (i_src == i_dst_first)
      continue;

    i_dst = i_dst_first;
    pending = std::move(*(iter_begin + i_dst));

    // Follow the permutation cycle
    do {
      *(iter_begin + i_dst) = std::move(*(iter_begin + i_src));
      (*hilbert_values)[i_dst].second = i_dst;

      i_dst = i_src;
      i_src = (*hilbert_values)[i_src].second;
    } while (i_src != i_dst_first);

    *(iter_begin + i_dst) = std::move(pending);
    (*hilbert_values)[i_dst].second = i_dst;
  }

  return Status::Ok();
}

bool Reader::belong_to_single_fragment(
    std::vector<ResultCoords>::iterator iter_begin,
    std::vector<ResultCoords>::iterator iter_end) const {
  if (iter_begin == iter_end)
    return true;

  uint32_t last_frag_idx = iter_begin->tile_->frag_idx();
  for (auto it = iter_begin + 1; it != iter_end; ++it) {
    if (it->tile_->frag_idx() != last_frag_idx)
      return false;
  }

  return true;
}

template <class T>
tuple<Status, optional<bool>> Reader::fill_dense_coords(
    const Subarray& subarray) {
  auto timer_se = stats_->start_timer("fill_dense_coords");

  // Reading coordinates with a query condition is currently unsupported.
  // Query conditions mutate the result cell slabs to filter attributes.
  // This path does not use result cell slabs, which will fill coordinates
  // for cells that should be filtered out.
  if (!condition_.empty()) {
    return {logger_->status(Status_ReaderError(
                "Cannot read dense coordinates; dense coordinate "
                "reads are unsupported with a query condition")),
            nullopt};
  }

  // Prepare buffers
  std::vector<unsigned> dim_idx;
  std::vector<QueryBuffer*> buffers;
  auto coords_it = buffers_.find(constants::coords);
  auto dim_num = array_schema_.dim_num();
  if (coords_it != buffers_.end()) {
    buffers.emplace_back(&(coords_it->second));
    dim_idx.emplace_back(dim_num);
  } else {
    for (unsigned d = 0; d < dim_num; ++d) {
      const auto dim{array_schema_.dimension_ptr(d)};
      auto it = buffers_.find(dim->name());
      if (it != buffers_.end()) {
        buffers.emplace_back(&(it->second));
        dim_idx.emplace_back(d);
      }
    }
  }
  std::vector<uint64_t> offsets(buffers.size(), 0);

  bool overflowed = false;
  if (layout_ == Layout::GLOBAL_ORDER) {
    auto&& [st, of] =
        fill_dense_coords_global<T>(subarray, dim_idx, buffers, offsets);
    RETURN_NOT_OK_TUPLE(st, std::nullopt);
    overflowed = *of;
  } else {
    assert(layout_ == Layout::ROW_MAJOR || layout_ == Layout::COL_MAJOR);
    auto&& [st, of] =
        fill_dense_coords_row_col<T>(subarray, dim_idx, buffers, offsets);
    RETURN_NOT_OK_TUPLE(st, std::nullopt);
    overflowed = *of;
  }

  // Update buffer sizes
  for (size_t i = 0; i < buffers.size(); ++i)
    *(buffers[i]->buffer_size_) = offsets[i];

  return {Status::Ok(), overflowed};
}

template <class T>
tuple<Status, optional<bool>> Reader::fill_dense_coords_global(
    const Subarray& subarray,
    const std::vector<unsigned>& dim_idx,
    const std::vector<QueryBuffer*>& buffers,
    std::vector<uint64_t>& offsets) {
  auto tile_coords = subarray.tile_coords();
  auto cell_order = array_schema_.cell_order();

  bool overflowed = false;
  for (const auto& tc : tile_coords) {
    auto tile_subarray = subarray.crop_to_tile((const T*)&tc[0], cell_order);
    auto&& [st, of] =
        fill_dense_coords_row_col<T>(tile_subarray, dim_idx, buffers, offsets);
    RETURN_NOT_OK_TUPLE(st, std::nullopt);
    overflowed |= *of;
  }

  return {Status::Ok(), overflowed};
}

template <class T>
tuple<Status, optional<bool>> Reader::fill_dense_coords_row_col(
    const Subarray& subarray,
    const std::vector<unsigned>& dim_idx,
    const std::vector<QueryBuffer*>& buffers,
    std::vector<uint64_t>& offsets) {
  auto cell_order = array_schema_.cell_order();
  auto dim_num = array_schema_.dim_num();

  // Iterate over all coordinates, retrieved in cell slabs
  CellSlabIter<T> iter(&subarray);
  RETURN_CANCEL_OR_ERROR_TUPLE(iter.begin());
  while (!iter.end()) {
    auto cell_slab = iter.cell_slab();
    auto coords_num = cell_slab.length_;

    // Check for overflow
    for (size_t i = 0; i < buffers.size(); ++i) {
      auto idx = (dim_idx[i] == dim_num) ? 0 : dim_idx[i];
      auto coord_size{array_schema_.domain().dimension_ptr(idx)->coord_size()};
      coord_size = (dim_idx[i] == dim_num) ? coord_size * dim_num : coord_size;
      auto buff_size = *(buffers[i]->buffer_size_);
      auto offset = offsets[i];
      if (coords_num * coord_size + offset > buff_size) {
        return {Status::Ok(), true};
      }
    }

    // Copy slab
    if (layout_ == Layout::ROW_MAJOR ||
        (layout_ == Layout::GLOBAL_ORDER && cell_order == Layout::ROW_MAJOR))
      fill_dense_coords_row_slab(
          &cell_slab.coords_[0], coords_num, dim_idx, buffers, offsets);
    else
      fill_dense_coords_col_slab(
          &cell_slab.coords_[0], coords_num, dim_idx, buffers, offsets);

    ++iter;
  }

  return {Status::Ok(), false};
}

template <class T>
void Reader::fill_dense_coords_row_slab(
    const T* start,
    uint64_t num,
    const std::vector<unsigned>& dim_idx,
    const std::vector<QueryBuffer*>& buffers,
    std::vector<uint64_t>& offsets) const {
  // For easy reference
  auto dim_num = array_schema_.dim_num();

  // Special zipped coordinates
  if (dim_idx.size() == 1 && dim_idx[0] == dim_num) {
    auto c_buff = (char*)buffers[0]->buffer_;
    auto offset = &offsets[0];

    // Fill coordinates
    for (uint64_t i = 0; i < num; ++i) {
      // First dim-1 dimensions are copied as they are
      if (dim_num > 1) {
        auto bytes_to_copy = (dim_num - 1) * sizeof(T);
        std::memcpy(c_buff + *offset, start, bytes_to_copy);
        *offset += bytes_to_copy;
      }

      // Last dimension is incremented by `i`
      auto new_coord = start[dim_num - 1] + i;
      std::memcpy(c_buff + *offset, &new_coord, sizeof(T));
      *offset += sizeof(T);
    }
  } else {  // Set of separate coordinate buffers
    for (uint64_t i = 0; i < num; ++i) {
      for (size_t b = 0; b < buffers.size(); ++b) {
        auto c_buff = (char*)buffers[b]->buffer_;
        auto offset = &offsets[b];

        // First dim-1 dimensions are copied as they are
        if (dim_num > 1 && dim_idx[b] < dim_num - 1) {
          std::memcpy(c_buff + *offset, &start[dim_idx[b]], sizeof(T));
          *offset += sizeof(T);
        } else {
          // Last dimension is incremented by `i`
          auto new_coord = start[dim_num - 1] + i;
          std::memcpy(c_buff + *offset, &new_coord, sizeof(T));
          *offset += sizeof(T);
        }
      }
    }
  }
}

template <class T>
void Reader::fill_dense_coords_col_slab(
    const T* start,
    uint64_t num,
    const std::vector<unsigned>& dim_idx,
    const std::vector<QueryBuffer*>& buffers,
    std::vector<uint64_t>& offsets) const {
  // For easy reference
  auto dim_num = array_schema_.dim_num();

  // Special zipped coordinates
  if (dim_idx.size() == 1 && dim_idx[0] == dim_num) {
    auto c_buff = (char*)buffers[0]->buffer_;
    auto offset = &offsets[0];

    // Fill coordinates
    for (uint64_t i = 0; i < num; ++i) {
      // First dimension is incremented by `i`
      auto new_coord = start[0] + i;
      std::memcpy(c_buff + *offset, &new_coord, sizeof(T));
      *offset += sizeof(T);

      // Last dim-1 dimensions are copied as they are
      if (dim_num > 1) {
        auto bytes_to_copy = (dim_num - 1) * sizeof(T);
        std::memcpy(c_buff + *offset, &start[1], bytes_to_copy);
        *offset += bytes_to_copy;
      }
    }
  } else {  // Separate coordinate buffers
    for (uint64_t i = 0; i < num; ++i) {
      for (size_t b = 0; b < buffers.size(); ++b) {
        auto c_buff = (char*)buffers[b]->buffer_;
        auto offset = &offsets[b];

        // First dimension is incremented by `i`
        if (dim_idx[b] == 0) {
          auto new_coord = start[0] + i;
          std::memcpy(c_buff + *offset, &new_coord, sizeof(T));
          *offset += sizeof(T);
        } else {  // Last dim-1 dimensions are copied as they are
          std::memcpy(c_buff + *offset, &start[dim_idx[b]], sizeof(T));
          *offset += sizeof(T);
        }
      }
    }
  }
}

}  // namespace sm
}  // namespace tiledb
