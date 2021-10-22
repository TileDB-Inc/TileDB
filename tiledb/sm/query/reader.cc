/**
 * @file   reader.cc
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
 * This file implements class Reader.
 */

#include "tiledb/sm/query/reader.h"
#include "tiledb/common/logger.h"
#include "tiledb/sm/array/array.h"
#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/array_schema/dimension.h"
#include "tiledb/sm/fragment/fragment_metadata.h"
#include "tiledb/sm/misc/comparators.h"
#include "tiledb/sm/misc/hilbert.h"
#include "tiledb/sm/misc/parallel_functions.h"
#include "tiledb/sm/misc/utils.h"
#include "tiledb/sm/query/query_macros.h"
#include "tiledb/sm/query/read_cell_slab_iter.h"
#include "tiledb/sm/query/result_tile.h"
#include "tiledb/sm/stats/global_stats.h"
#include "tiledb/sm/storage_manager/storage_manager.h"
#include "tiledb/sm/subarray/cell_slab.h"
#include "tiledb/sm/tile/generic_tile_io.h"

using namespace tiledb;
using namespace tiledb::common;
using namespace tiledb::sm::stats;

namespace tiledb {
namespace sm {

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
          condition) {
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

Status Reader::init() {
  // Sanity checks
  if (storage_manager_ == nullptr)
    return LOG_STATUS(Status::ReaderError(
        "Cannot initialize reader; Storage manager not set"));
  if (array_schema_ == nullptr)
    return LOG_STATUS(Status::ReaderError(
        "Cannot initialize reader; Array metadata not set"));
  if (buffers_.empty())
    return LOG_STATUS(
        Status::ReaderError("Cannot initialize reader; Buffers not set"));
  if (array_schema_->dense() && !subarray_.is_set())
    return LOG_STATUS(Status::ReaderError(
        "Cannot initialize reader; Dense reads must have a subarray set"));

  // Check subarray
  RETURN_NOT_OK(check_subarray());

  // Initialize the read state
  RETURN_NOT_OK(init_read_state());

  // Check the validity buffer sizes. This must be performed
  // after `init_read_state` to ensure we have set the
  // member state correctly from the config.
  RETURN_NOT_OK(check_validity_buffer_sizes());

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

Status Reader::dowork() {
  // Check that the query condition is valid.
  RETURN_NOT_OK(condition_.check(array_schema_));

  get_dim_attr_stats();

  auto timer_se = stats_->start_timer("read");

  auto dense_mode = array_schema_->dense();

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
    copy_overflowed_ = false;
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

Status Reader::compute_result_cell_slabs(
    const std::vector<ResultCoords>& result_coords,
    std::vector<ResultCellSlab>* result_cell_slabs) const {
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
    return LOG_STATUS(Status::ReaderError("Unexpected empty cell range."));
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
      result_cell_slabs->emplace_back(tile, start_pos, end_pos - start_pos + 1);
      start_pos = it->pos_;
      end_pos = start_pos;
      tile = it->tile_;
    }
    it = skip_invalid_elements(++it, coords_end);
  }

  // Append the last range
  result_cell_slabs->emplace_back(tile, start_pos, end_pos - start_pos + 1);

  return Status::Ok();
}

Status Reader::compute_range_result_coords(
    Subarray* subarray,
    unsigned frag_idx,
    ResultTile* tile,
    uint64_t range_idx,
    std::vector<ResultCoords>* result_coords) {
  auto coords_num = tile->cell_num();
  auto dim_num = array_schema_->dim_num();
  auto cell_order = array_schema_->cell_order();
  auto range_coords = subarray->get_range_coords(range_idx);

  if (array_schema_->dense()) {
    std::vector<uint8_t> result_bitmap(coords_num, 1);
    std::vector<uint8_t> overwritten_bitmap(coords_num, 0);

    // Compute result and overwritten bitmap per dimension
    for (unsigned d = 0; d < dim_num; ++d) {
      const auto& ranges = subarray->ranges_for_dim(d);
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
        result_coords->emplace_back(tile, pos);
    }
  } else {  // Sparse
    std::vector<uint8_t> result_bitmap(coords_num, 1);

    // Compute result and overwritten bitmap per dimension
    for (unsigned d = 0; d < dim_num; ++d) {
      // For col-major cell ordering, iterate the dimensions
      // in reverse.
      const unsigned dim_idx =
          cell_order == Layout::COL_MAJOR ? dim_num - d - 1 : d;
      if (!subarray->is_default(dim_idx)) {
        const auto& ranges = subarray->ranges_for_dim(dim_idx);
        RETURN_NOT_OK(tile->compute_results_sparse(
            dim_idx,
            ranges[range_coords[dim_idx]],
            &result_bitmap,
            cell_order));
      }
    }

    // Gather results
    for (uint64_t pos = 0; pos < coords_num; ++pos) {
      if (result_bitmap[pos])
        result_coords->emplace_back(tile, pos);
    }
  }

  return Status::Ok();
}

Status Reader::compute_range_result_coords(
    Subarray* subarray,
    const std::vector<bool>& single_fragment,
    const std::map<std::pair<unsigned, uint64_t>, size_t>& result_tile_map,
    std::vector<ResultTile>* result_tiles,
    std::vector<std::vector<ResultCoords>>* range_result_coords) {
  auto timer_se = stats_->start_timer("compute_range_result_coords");

  auto range_num = subarray->range_num();
  range_result_coords->resize(range_num);
  auto cell_order = array_schema_->cell_order();
  auto allows_dups = array_schema_->allows_dups();

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
            &((*range_result_coords)[r])));

        // Dedup unless there is a single fragment or array schema allows
        // duplicates
        if (!single_fragment[r] && !allows_dups) {
          RETURN_CANCEL_OR_ERROR(sort_result_coords(
              ((*range_result_coords)[r]).begin(),
              ((*range_result_coords)[r]).end(),
              ((*range_result_coords)[r]).size(),
              sort_layout));
          RETURN_CANCEL_OR_ERROR(
              dedup_result_coords(&((*range_result_coords)[r])));
        }

        return Status::Ok();
      });

  RETURN_NOT_OK(status);

  return Status::Ok();
}

Status Reader::compute_range_result_coords(
    Subarray* subarray,
    uint64_t range_idx,
    uint32_t fragment_idx,
    const std::map<std::pair<unsigned, uint64_t>, size_t>& result_tile_map,
    std::vector<ResultTile>* result_tiles,
    std::vector<ResultCoords>* range_result_coords) {
  // Skip dense fragments
  if (fragment_metadata_[fragment_idx]->dense())
    return Status::Ok();

  auto tr =
      subarray->tile_overlap(fragment_idx, range_idx)->tile_ranges_.begin();
  auto tr_end =
      subarray->tile_overlap(fragment_idx, range_idx)->tile_ranges_.end();
  auto t = subarray->tile_overlap(fragment_idx, range_idx)->tiles_.begin();
  auto t_end = subarray->tile_overlap(fragment_idx, range_idx)->tiles_.end();

  while (tr != tr_end || t != t_end) {
    // Handle tile range
    if (tr != tr_end && (t == t_end || tr->first < t->first)) {
      for (uint64_t i = tr->first; i <= tr->second; ++i) {
        auto pair = std::pair<unsigned, uint64_t>(fragment_idx, i);
        auto tile_it = result_tile_map.find(pair);
        assert(tile_it != result_tile_map.end());
        auto tile_idx = tile_it->second;
        auto& tile = (*result_tiles)[tile_idx];

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
      auto& tile = (*result_tiles)[tile_idx];
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
    Subarray* subarray,
    uint64_t range_idx,
    const std::map<std::pair<unsigned, uint64_t>, size_t>& result_tile_map,
    std::vector<ResultTile>* result_tiles,
    std::vector<ResultCoords>* range_result_coords) {
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
            &range_result_coords_vec[f]);
      });
  RETURN_NOT_OK(status);

  // Consolidate the result coordinates in the single result vector
  for (const auto& vec : range_result_coords_vec) {
    for (const auto& r : vec)
      range_result_coords->emplace_back(r);
  }

  return Status::Ok();
}

Status Reader::compute_subarray_coords(
    std::vector<std::vector<ResultCoords>>* range_result_coords,
    std::vector<ResultCoords>* result_coords) {
  auto timer_se = stats_->start_timer("compute_subarray_coords");
  // The input 'result_coords' is already sorted. Save the current size
  // before inserting new elements.
  const size_t result_coords_size = result_coords->size();

  // Add all valid ``range_result_coords`` to ``result_coords``
  for (const auto& rv : *range_result_coords) {
    for (const auto& c : rv) {
      if (c.valid())
        result_coords->emplace_back(c.tile_, c.pos_);
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
  auto dim_num = array_schema_->dim_num();
  bool must_sort = true;
  auto allows_dups = array_schema_->allows_dups();
  auto single_range = (range_result_coords->size() == 1);
  if (layout_ == Layout::GLOBAL_ORDER || dim_num == 1) {
    must_sort = !belong_to_single_fragment(
        result_coords->begin() + result_coords_size, result_coords->end());
  } else if (single_range && !allows_dups) {
    must_sort = belong_to_single_fragment(
        result_coords->begin() + result_coords_size, result_coords->end());
  }

  if (must_sort) {
    RETURN_NOT_OK(sort_result_coords(
        result_coords->begin() + result_coords_size,
        result_coords->end(),
        result_coords->size() - result_coords_size,
        layout_));
  }

  return Status::Ok();
}

Status Reader::compute_sparse_result_tiles(
    std::vector<ResultTile>* result_tiles,
    std::map<std::pair<unsigned, uint64_t>, size_t>* result_tile_map,
    std::vector<bool>* single_fragment) {
  auto timer_se = stats_->start_timer("compute_sparse_result_tiles");

  // For easy reference
  auto domain = array_schema_->domain();
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

  result_tiles->clear();
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
            result_tiles->emplace_back(f, t, domain);
            (*result_tile_map)[pair] = result_tiles->size() - 1;
          }
          // Always check range for multiple fragments
          if (f > first_fragment[r])
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
          result_tiles->emplace_back(f, t, domain);
          (*result_tile_map)[pair] = result_tiles->size() - 1;
        }
        // Always check range for multiple fragments
        if (f > first_fragment[r])
          (*single_fragment)[r] = false;
        else
          first_fragment[r] = f;
      }
    }
  }

  return Status::Ok();
}

template <class T>
Status Reader::compute_result_cell_slabs(
    const Subarray& subarray,
    std::map<const T*, ResultSpaceTile<T>>* result_space_tiles,
    std::vector<ResultCoords>* result_coords,
    std::vector<ResultTile*>* result_tiles,
    std::vector<ResultCellSlab>* result_cell_slabs) const {
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
        &frag_tile_set,
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
    std::map<const T*, ResultSpaceTile<T>>* result_space_tiles,
    std::vector<ResultCoords>* result_coords,
    uint64_t* result_coords_pos,
    std::vector<ResultTile*>* result_tiles,
    std::set<std::pair<unsigned, uint64_t>>* frag_tile_set,
    std::vector<ResultCellSlab>* result_cell_slabs) const {
  // Compute result space tiles. The result space tiles hold all the
  // relevant result tiles of the dense fragments
  compute_result_space_tiles<T>(subarray, result_space_tiles);

  // Gather result cell slabs and pointers to result tiles
  // `result_tiles` holds pointers to tiles that store actual results,
  // which can be stored either in `sparse_result_tiles` (sparse)
  // or in `result_space_tiles` (dense).
  auto rcs_it = ReadCellSlabIter<T>(
      &subarray, result_space_tiles, result_coords, *result_coords_pos);
  for (rcs_it.begin(); !rcs_it.end(); ++rcs_it) {
    // Add result cell slab
    auto result_cell_slab = rcs_it.result_cell_slab();
    result_cell_slabs->push_back(result_cell_slab);
    // Add result tile
    if (result_cell_slab.tile_ != nullptr) {
      auto frag_idx = result_cell_slab.tile_->frag_idx();
      auto tile_idx = result_cell_slab.tile_->tile_idx();
      auto frag_tile_tuple = std::pair<unsigned, uint64_t>(frag_idx, tile_idx);
      auto it = frag_tile_set->find(frag_tile_tuple);
      if (it == frag_tile_set->end()) {
        frag_tile_set->insert(frag_tile_tuple);
        result_tiles->push_back(result_cell_slab.tile_);
      }
    }
  }
  *result_coords_pos = rcs_it.result_coords_pos();

  return Status::Ok();
}

template <class T>
Status Reader::compute_result_cell_slabs_global(
    const Subarray& subarray,
    std::map<const T*, ResultSpaceTile<T>>* result_space_tiles,
    std::vector<ResultCoords>* result_coords,
    std::vector<ResultTile*>* result_tiles,
    std::vector<ResultCellSlab>* result_cell_slabs) const {
  const auto& tile_coords = subarray.tile_coords();
  auto cell_order = array_schema_->cell_order();
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
        &frag_tile_set,
        result_cell_slabs));
  }

  return Status::Ok();
}

Status Reader::compute_result_coords(
    std::vector<ResultTile>* result_tiles,
    std::vector<ResultCoords>* result_coords) {
  auto timer_se = stats_->start_timer("compute_result_coords");

  // Get overlapping tile indexes
  typedef std::pair<unsigned, uint64_t> FragTileTuple;
  std::map<FragTileTuple, size_t> result_tile_map;
  std::vector<bool> single_fragment;

  RETURN_CANCEL_OR_ERROR(compute_sparse_result_tiles(
      result_tiles, &result_tile_map, &single_fragment));

  if (result_tiles->empty())
    return Status::Ok();

  // Create temporary vector with pointers to result tiles, so that
  // `read_tiles`, `unfilter_tiles` below can work without changes
  std::vector<ResultTile*> tmp_result_tiles;
  for (auto& result_tile : *result_tiles)
    tmp_result_tiles.push_back(&result_tile);

  // Preload zipped coordinate tile offsets. Note that this will
  // ignore fragments with a version >= 5.
  auto& subarray = read_state_.partitioner_.current();
  std::vector<std::string> zipped_coords_names = {constants::coords};
  RETURN_CANCEL_OR_ERROR(load_tile_offsets(&subarray, &zipped_coords_names));

  // Preload unzipped coordinate tile offsets. Note that this will
  // ignore fragments with a version < 5.
  const auto dim_num = array_schema_->dim_num();
  std::vector<std::string> dim_names;
  dim_names.reserve(dim_num);
  for (unsigned d = 0; d < dim_num; ++d)
    dim_names.emplace_back(array_schema_->dimension(d)->name());
  RETURN_CANCEL_OR_ERROR(load_tile_offsets(&subarray, &dim_names));

  // Read and unfilter zipped coordinate tiles. Note that
  // this will ignore fragments with a version >= 5.
  RETURN_CANCEL_OR_ERROR(
      read_coordinate_tiles(&zipped_coords_names, &tmp_result_tiles));
  RETURN_CANCEL_OR_ERROR(unfilter_tiles(constants::coords, &tmp_result_tiles));

  // Read and unfilter unzipped coordinate tiles. Note that
  // this will ignore fragments with a version < 5.
  RETURN_CANCEL_OR_ERROR(read_coordinate_tiles(&dim_names, &tmp_result_tiles));
  for (const auto& dim_name : dim_names) {
    RETURN_CANCEL_OR_ERROR(unfilter_tiles(dim_name, &tmp_result_tiles));
  }

  // Compute the read coordinates for all fragments for each subarray range.
  std::vector<std::vector<ResultCoords>> range_result_coords;
  RETURN_CANCEL_OR_ERROR(compute_range_result_coords(
      &subarray,
      single_fragment,
      result_tile_map,
      result_tiles,
      &range_result_coords));
  result_tile_map.clear();

  // Compute final coords (sorted in the result layout) of the whole subarray.
  RETURN_CANCEL_OR_ERROR(
      compute_subarray_coords(&range_result_coords, result_coords));
  range_result_coords.clear();

  return Status::Ok();
}

Status Reader::dedup_result_coords(
    std::vector<ResultCoords>* result_coords) const {
  auto coords_end = result_coords->end();
  auto it = skip_invalid_elements(result_coords->begin(), coords_end);
  while (it != coords_end) {
    auto next_it = skip_invalid_elements(std::next(it), coords_end);
    if (next_it != coords_end && it->same_coords(*next_it)) {
      if (it->tile_->frag_idx() < next_it->tile_->frag_idx()) {
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
  auto type = array_schema_->domain()->dimension(0)->type();
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
      return LOG_STATUS(Status::ReaderError(
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
  RETURN_NOT_OK(compute_result_coords(&sparse_result_tiles, &result_coords));

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
      &result_space_tiles,
      &result_coords,
      &result_tiles,
      &result_cell_slabs));

  auto stride = array_schema_->domain()->stride<T>(subarray.layout());
  apply_query_condition(&result_cell_slabs, &result_tiles, &subarray, stride);

  get_result_tile_stats(result_tiles);
  get_result_cell_stats(result_cell_slabs);

  // Clear sparse coordinate tiles (not needed any more)
  erase_coord_tiles(&sparse_result_tiles);

  // Needed when copying the cells
  RETURN_NOT_OK(copy_attribute_values(
      stride, &result_tiles, &result_cell_slabs, subarray));
  read_state_.overflowed_ = copy_overflowed_;

  // Fill coordinates if the user requested them
  if (!read_state_.overflowed_ && has_coords())
    RETURN_CANCEL_OR_ERROR(fill_dense_coords<T>(subarray));
  read_state_.overflowed_ = copy_overflowed_;

  return Status::Ok();
}

Status Reader::get_all_result_coords(
    ResultTile* tile, std::vector<ResultCoords>* result_coords) const {
  auto coords_num = tile->cell_num();
  for (uint64_t i = 0; i < coords_num; ++i)
    result_coords->emplace_back(tile, i);

  return Status::Ok();
}

bool Reader::has_separate_coords() const {
  for (const auto& it : buffers_) {
    if (array_schema_->is_dim(it.first))
      return true;
  }

  return false;
}

Status Reader::init_read_state() {
  auto timer_se = stats_->start_timer("init_state");

  // Check subarray
  if (subarray_.layout() == Layout::GLOBAL_ORDER && subarray_.range_num() != 1)
    return LOG_STATUS(
        Status::ReaderError("Cannot initialize read "
                            "state; Multi-range "
                            "subarrays do not "
                            "support global order"));

  // Get config
  bool found = false;
  uint64_t memory_budget = 0;
  RETURN_NOT_OK(
      config_.get<uint64_t>("sm.memory_budget", &memory_budget, &found));
  assert(found);
  uint64_t memory_budget_var = 0;
  RETURN_NOT_OK(config_.get<uint64_t>(
      "sm.memory_budget_var", &memory_budget_var, &found));
  assert(found);
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
    return LOG_STATUS(
        Status::ReaderError("Cannot initialize reader; Unsupported offsets "
                            "bitsize in configuration"));
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
      stats_);
  read_state_.overflowed_ = false;
  read_state_.unsplittable_ = false;

  // Set result size budget
  for (const auto& a : buffers_) {
    auto attr_name = a.first;
    auto buffer_size = a.second.buffer_size_;
    auto buffer_var_size = a.second.buffer_var_size_;
    auto buffer_validity_size = a.second.validity_vector_.buffer_size();
    if (!array_schema_->var_size(attr_name)) {
      if (!array_schema_->is_nullable(attr_name)) {
        RETURN_NOT_OK(read_state_.partitioner_.set_result_budget(
            attr_name.c_str(), *buffer_size));
      } else {
        RETURN_NOT_OK(read_state_.partitioner_.set_result_budget_nullable(
            attr_name.c_str(), *buffer_size, *buffer_validity_size));
      }
    } else {
      if (!array_schema_->is_nullable(attr_name)) {
        RETURN_NOT_OK(read_state_.partitioner_.set_result_budget(
            attr_name.c_str(), *buffer_size, *buffer_var_size));
      } else {
        RETURN_NOT_OK(read_state_.partitioner_.set_result_budget_nullable(
            attr_name.c_str(),
            *buffer_size,
            *buffer_var_size,
            *buffer_validity_size));
      }
    }
  }

  read_state_.unsplittable_ = false;
  read_state_.overflowed_ = false;
  copy_overflowed_ = false;
  read_state_.initialized_ = true;

  return Status::Ok();
}

Status Reader::sort_result_coords(
    std::vector<ResultCoords>::iterator iter_begin,
    std::vector<ResultCoords>::iterator iter_end,
    size_t coords_num,
    Layout layout) const {
  auto timer_se = stats_->start_timer("sort_result_coords");
  auto domain = array_schema_->domain();

  if (layout == Layout::ROW_MAJOR) {
    parallel_sort(
        storage_manager_->compute_tp(), iter_begin, iter_end, RowCmp(domain));
  } else if (layout == Layout::COL_MAJOR) {
    parallel_sort(
        storage_manager_->compute_tp(), iter_begin, iter_end, ColCmp(domain));
  } else if (layout == Layout::GLOBAL_ORDER) {
    if (array_schema_->cell_order() == Layout::HILBERT) {
      std::vector<std::pair<uint64_t, uint64_t>> hilbert_values(coords_num);
      RETURN_NOT_OK(calculate_hilbert_values(iter_begin, &hilbert_values));
      parallel_sort(
          storage_manager_->compute_tp(),
          hilbert_values.begin(),
          hilbert_values.end(),
          HilbertCmp(domain, iter_begin));
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
  // Compute result coordinates from the sparse fragments
  // `sparse_result_tiles` will hold all the relevant result tiles of
  // sparse fragments
  std::vector<ResultCoords> result_coords;
  std::vector<ResultTile> sparse_result_tiles;

  RETURN_NOT_OK(compute_result_coords(&sparse_result_tiles, &result_coords));
  std::vector<ResultTile*> result_tiles;
  for (auto& srt : sparse_result_tiles)
    result_tiles.push_back(&srt);

  // Compute result cell slabs
  std::vector<ResultCellSlab> result_cell_slabs;
  RETURN_CANCEL_OR_ERROR(
      compute_result_cell_slabs(result_coords, &result_cell_slabs));
  result_coords.clear();

  auto& subarray = read_state_.partitioner_.current();
  apply_query_condition(&result_cell_slabs, &result_tiles, &subarray);
  get_result_tile_stats(result_tiles);
  get_result_cell_stats(result_cell_slabs);

  RETURN_NOT_OK(copy_coordinates(&result_tiles, &result_cell_slabs));
  RETURN_NOT_OK(copy_attribute_values(
      UINT64_MAX, &result_tiles, &result_cell_slabs, subarray));
  read_state_.overflowed_ = copy_overflowed_;

  return Status::Ok();
}

Status Reader::add_extra_offset() {
  for (const auto& it : buffers_) {
    const auto& name = it.first;
    if (!array_schema_->var_size(name))
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
      auto elements = *it.second.buffer_var_size_ /
                      datatype_size(array_schema_->type(name));
      memcpy(
          buffer + *it.second.buffer_size_ - offsets_bytesize(),
          &elements,
          offsets_bytesize());
    } else {
      return LOG_STATUS(Status::ReaderError(
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
  auto domain = array_schema_->domain();

  for (unsigned f = frag_idx + 1; f < fragment_num; ++f) {
    if (fragment_metadata_[f]->dense() &&
        domain->covered(mbr, fragment_metadata_[f]->non_empty_domain()))
      return true;
  }

  return false;
}

void Reader::erase_coord_tiles(std::vector<ResultTile>* result_tiles) const {
  for (auto& tile : *result_tiles) {
    auto dim_num = array_schema_->dim_num();
    for (unsigned d = 0; d < dim_num; ++d)
      tile.erase_tile(array_schema_->dimension(d)->name());
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
      cell_num += array_schema_->domain()->cell_num_per_tile();
  }
  stats_->add_counter("cell_num", cell_num);
}

Status Reader::calculate_hilbert_values(
    std::vector<ResultCoords>::iterator iter_begin,
    std::vector<std::pair<uint64_t, uint64_t>>* hilbert_values) const {
  auto timer_se = stats_->start_timer("calculate_hilbert_values");
  auto dim_num = array_schema_->dim_num();
  Hilbert h(dim_num);
  auto bits = h.bits();
  auto max_bucket_val = ((uint64_t)1 << bits) - 1;
  auto coords_num = (uint64_t)hilbert_values->size();

  // Calculate Hilbert values in parallel
  auto status = parallel_for(
      storage_manager_->compute_tp(), 0, coords_num, [&](uint64_t c) {
        std::vector<uint64_t> coords(dim_num);
        for (uint32_t d = 0; d < dim_num; ++d) {
          auto dim = array_schema_->dimension(d);
          coords[d] =
              dim->map_to_uint64(*(iter_begin + c), d, bits, max_bucket_val);
        }
        (*hilbert_values)[c] =
            std::pair<uint64_t, uint64_t>(h.coords_to_hilbert(&coords[0]), c);
        return Status::Ok();
      });

  RETURN_NOT_OK_ELSE(status, LOG_STATUS(status));

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

}  // namespace sm
}  // namespace tiledb
