/**
 * @file   reader.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2020 TileDB, Inc.
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
#include "tiledb/sm/array/array.h"
#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/array_schema/dimension.h"
#include "tiledb/sm/filesystem/vfs.h"
#include "tiledb/sm/fragment/fragment_metadata.h"
#include "tiledb/sm/global_state/global_state.h"
#include "tiledb/sm/misc/comparators.h"
#include "tiledb/sm/misc/logger.h"
#include "tiledb/sm/misc/parallel_functions.h"
#include "tiledb/sm/misc/utils.h"
#include "tiledb/sm/query/query_macros.h"
#include "tiledb/sm/query/read_cell_slab_iter.h"
#include "tiledb/sm/query/result_tile.h"
#include "tiledb/sm/stats/stats.h"
#include "tiledb/sm/storage_manager/storage_manager.h"
#include "tiledb/sm/subarray/cell_slab.h"
#include "tiledb/sm/tile/generic_tile_io.h"

#include <iostream>
#include <unordered_set>

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
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

Reader::Reader() {
  array_ = nullptr;
  array_schema_ = nullptr;
  storage_manager_ = nullptr;
  layout_ = Layout::ROW_MAJOR;
  sparse_mode_ = false;
  read_state_.initialized_ = false;
}

Reader::~Reader() = default;

/* ****************************** */
/*               API              */
/* ****************************** */

const Array* Reader::array() const {
  return array_;
}

Status Reader::add_range(unsigned dim_idx, const Range& range) {
  return subarray_.add_range(dim_idx, range);
}

Status Reader::get_range_num(unsigned dim_idx, uint64_t* range_num) const {
  return subarray_.get_range_num(dim_idx, range_num);
}

Status Reader::get_range(
    unsigned dim_idx,
    uint64_t range_idx,
    const void** start,
    const void** end,
    const void** stride) const {
  *stride = nullptr;
  return subarray_.get_range(dim_idx, range_idx, start, end);
}

Status Reader::get_range_var_size(
    unsigned dim_idx,
    uint64_t range_idx,
    uint64_t* start_size,
    uint64_t* end_size) const {
  return subarray_.get_range_var_size(dim_idx, range_idx, start_size, end_size);
}

Status Reader::get_est_result_size(const char* name, uint64_t* size) {
  return subarray_.get_est_result_size(
      name, size, storage_manager_->compute_tp());
}

Status Reader::get_est_result_size(
    const char* name, uint64_t* size_off, uint64_t* size_val) {
  return subarray_.get_est_result_size(
      name, size_off, size_val, storage_manager_->compute_tp());
}

const ArraySchema* Reader::array_schema() const {
  return array_schema_;
}

std::vector<std::string> Reader::buffer_names() const {
  std::vector<std::string> ret;
  ret.reserve(buffers_.size());
  for (const auto& it : buffers_)
    ret.push_back(it.first);

  return ret;
}

QueryBuffer Reader::buffer(const std::string& name) const {
  auto buf = buffers_.find(name);
  if (buf == buffers_.end())
    return QueryBuffer{};
  return buf->second;
}

bool Reader::incomplete() const {
  return read_state_.overflowed_ || !read_state_.done();
}

Status Reader::get_buffer(
    const std::string& name, void** buffer, uint64_t** buffer_size) const {
  auto it = buffers_.find(name);
  if (it == buffers_.end()) {
    *buffer = nullptr;
    *buffer_size = nullptr;
  } else {
    *buffer = it->second.buffer_;
    *buffer_size = it->second.buffer_size_;
  }

  return Status::Ok();
}

Status Reader::get_buffer(
    const std::string& name,
    uint64_t** buffer_off,
    uint64_t** buffer_off_size,
    void** buffer_val,
    uint64_t** buffer_val_size) const {
  auto it = buffers_.find(name);
  if (it == buffers_.end()) {
    *buffer_off = nullptr;
    *buffer_off_size = nullptr;
    *buffer_val = nullptr;
    *buffer_val_size = nullptr;
  } else {
    *buffer_off = (uint64_t*)it->second.buffer_;
    *buffer_off_size = it->second.buffer_size_;
    *buffer_val = it->second.buffer_var_;
    *buffer_val_size = it->second.buffer_var_size_;
  }
  return Status::Ok();
}

Status Reader::init(const Layout& layout) {
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
  if (array_schema_->dense() && !sparse_mode_ && !subarray_.is_set())
    return LOG_STATUS(Status::ReaderError(
        "Cannot initialize reader; Dense reads must have a subarray set"));

  // Set layout
  RETURN_NOT_OK(set_layout(layout));

  // Check subarray
  RETURN_NOT_OK(check_subarray());

  // Get configuration parameters
  const char *memory_budget, *memory_budget_var;
  auto config = storage_manager_->config();
  RETURN_NOT_OK(config.get("sm.memory_budget", &memory_budget));
  RETURN_NOT_OK(config.get("sm.memory_budget_var", &memory_budget_var));
  RETURN_NOT_OK(utils::parse::convert(memory_budget, &memory_budget_));
  RETURN_NOT_OK(utils::parse::convert(memory_budget_var, &memory_budget_var_));
  RETURN_NOT_OK(init_read_state());

  return Status::Ok();
}

URI Reader::first_fragment_uri() const {
  if (fragment_metadata_.empty())
    return URI();
  return fragment_metadata_.front()->fragment_uri();
}

URI Reader::last_fragment_uri() const {
  if (fragment_metadata_.empty())
    return URI();
  return fragment_metadata_.back()->fragment_uri();
}

Layout Reader::layout() const {
  return layout_;
}

bool Reader::no_results() const {
  for (const auto& it : buffers_) {
    if (*(it.second.buffer_size_) != 0)
      return false;
  }
  return true;
}

const Reader::ReadState* Reader::read_state() const {
  return &read_state_;
}

Reader::ReadState* Reader::read_state() {
  return &read_state_;
}

Status Reader::read() {
  get_dim_attr_stats();

  STATS_START_TIMER(stats::Stats::TimerType::READ)
  STATS_ADD_COUNTER(stats::Stats::CounterType::READ_NUM, 1)

  auto dense_mode = array_schema_->dense() && !sparse_mode_;

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
    STATS_ADD_COUNTER(stats::Stats::CounterType::READ_LOOP_NUM, 1)

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

      if (read_state_.unsplittable_)
        return Status::Ok();
    } else {
      bool no_results = this->no_results();

      // Need to reset unsplittable if the results fit after all
      if (!no_results)
        read_state_.unsplittable_ = false;

      if (!no_results || read_state_.done())
        return Status::Ok();

      RETURN_NOT_OK(read_state_.next());
    }
  } while (true);

  return Status::Ok();

  STATS_END_TIMER(stats::Stats::TimerType::READ)
}

void Reader::set_array(const Array* array) {
  array_ = array;
  subarray_ = Subarray(array, Layout::ROW_MAJOR);
}

void Reader::set_array_schema(const ArraySchema* array_schema) {
  array_schema_ = array_schema;
}

Status Reader::set_buffer(
    const std::string& name,
    void* buffer,
    uint64_t* buffer_size,
    bool check_null_buffers) {
  // Check buffer
  if (check_null_buffers && (buffer == nullptr || buffer_size == nullptr))
    return LOG_STATUS(Status::ReaderError(
        "Cannot set buffer; Buffer or buffer size is null"));

  // Array schema must exist
  if (array_schema_ == nullptr)
    return LOG_STATUS(
        Status::ReaderError("Cannot set buffer; Array schema not set"));

  // For easy reference
  bool is_dim = array_schema_->is_dim(name);
  bool is_attr = array_schema_->is_attr(name);

  // Check that attribute/dimension exists
  if (name != constants::coords && !is_dim && !is_attr)
    return LOG_STATUS(Status::ReaderError(
        std::string("Cannot set buffer; Invalid attribute/dimension '") + name +
        "'"));

  // Check that attribute/dimension is fixed-sized
  bool var_size = (name != constants::coords && array_schema_->var_size(name));
  if (var_size)
    return LOG_STATUS(Status::ReaderError(
        std::string("Cannot set buffer; Input attribute/dimension '") + name +
        "' is var-sized"));

  // Check if zipped coordinates coexist with separate coordinate buffers
  if ((is_dim && buffers_.find(constants::coords) != buffers_.end()) ||
      (name == constants::coords && has_separate_coords()))
    return LOG_STATUS(Status::ReaderError(
        std::string("Cannot set separate coordinate buffers and "
                    "a zipped coordinate buffer in the same query")));

  // Error if setting a new attribute/dimension after initialization
  bool exists = buffers_.find(name) != buffers_.end();
  if (read_state_.initialized_ && !exists)
    return LOG_STATUS(Status::ReaderError(
        std::string("Cannot set buffer for new attribute/dimension '") + name +
        "' after initialization"));

  // Set attribute buffer
  buffers_[name] = QueryBuffer(buffer, nullptr, buffer_size, nullptr);

  return Status::Ok();
}

Status Reader::set_buffer(
    const std::string& name,
    uint64_t* buffer_off,
    uint64_t* buffer_off_size,
    void* buffer_val,
    uint64_t* buffer_val_size,
    bool check_null_buffers) {
  // Check buffer
  if (check_null_buffers &&
      (buffer_off == nullptr || buffer_off_size == nullptr ||
       buffer_val == nullptr || buffer_val_size == nullptr))
    return LOG_STATUS(Status::ReaderError(
        "Cannot set buffer; Buffer or buffer size is null"));

  // Array schema must exist
  if (array_schema_ == nullptr)
    return LOG_STATUS(
        Status::ReaderError("Cannot set buffer; Array schema not set"));

  // For easy reference
  bool is_dim = array_schema_->is_dim(name);
  bool is_attr = array_schema_->is_attr(name);

  // Check that attribute/dimension exists
  if (!is_dim && !is_attr)
    return LOG_STATUS(Status::ReaderError(
        std::string("Cannot set buffer; Invalid attribute/dimension '") + name +
        "'"));

  // Check that attribute/dimension is var-sized
  if (!array_schema_->var_size(name))
    return LOG_STATUS(Status::ReaderError(
        std::string("Cannot set buffer; Input attribute/dimension '") + name +
        "' is fixed-sized"));

  // Error if setting a new attribute/dimension after initialization
  bool exists = buffers_.find(name) != buffers_.end();
  if (read_state_.initialized_ && !exists)
    return LOG_STATUS(Status::ReaderError(
        std::string("Cannot set buffer for new attribute/dimension '") + name +
        "' after initialization"));

  // Set attribute/dimension buffer
  buffers_[name] =
      QueryBuffer(buffer_off, buffer_val, buffer_off_size, buffer_val_size);

  return Status::Ok();
}

void Reader::set_fragment_metadata(
    const std::vector<FragmentMetadata*>& fragment_metadata) {
  fragment_metadata_ = fragment_metadata;
}

Status Reader::set_layout(Layout layout) {
  layout_ = layout;
  subarray_.set_layout(layout);

  return Status::Ok();
}

Status Reader::set_sparse_mode(bool sparse_mode) {
  if (!array_schema_->dense())
    return LOG_STATUS(Status::ReaderError(
        "Cannot set sparse mode; Only applicable to dense arrays"));

  bool all_sparse = true;
  for (const auto& f : fragment_metadata_) {
    if (f->dense()) {
      all_sparse = false;
      break;
    }
  }

  if (!all_sparse)
    return LOG_STATUS(
        Status::ReaderError("Cannot set sparse mode; Only applicable to opened "
                            "dense arrays having only sparse fragments"));

  sparse_mode_ = sparse_mode;
  return Status::Ok();
}

void Reader::set_storage_manager(StorageManager* storage_manager) {
  storage_manager_ = storage_manager;
}

Status Reader::set_subarray(const Subarray& subarray) {
  subarray_ = subarray;
  layout_ = subarray.layout();

  return Status::Ok();
}

const Subarray* Reader::subarray() const {
  return &subarray_;
}

/* ********************************* */
/*          STATIC FUNCTIONS         */
/* ********************************* */

template <class T>
void Reader::compute_result_space_tiles(
    const Domain* domain,
    const std::vector<std::vector<uint8_t>>& tile_coords,
    const TileDomain<T>& array_tile_domain,
    const std::vector<TileDomain<T>>& frag_tile_domains,
    std::map<const T*, ResultSpaceTile<T>>* result_space_tiles) {
  auto fragment_num = (unsigned)frag_tile_domains.size();
  auto dim_num = array_tile_domain.dim_num();
  std::vector<T> start_coords;
  const T* coords;
  start_coords.resize(dim_num);

  // For all tile coordinates
  for (const auto& tc : tile_coords) {
    coords = (T*)(&(tc[0]));
    start_coords = array_tile_domain.start_coords(coords);

    // Create result space tile and insert into the map
    auto r = result_space_tiles->emplace(coords, ResultSpaceTile<T>());
    auto& result_space_tile = r.first->second;
    result_space_tile.set_start_coords(start_coords);

    // Add fragment info to the result space tile
    for (unsigned f = 0; f < fragment_num; ++f) {
      // Check if the fragment overlaps with the space tile
      if (!frag_tile_domains[f].in_tile_domain(coords))
        continue;

      // Check if any previous fragment covers this fragment
      // for the tile identified by `coords`
      bool covered = false;
      for (unsigned j = 0; j < f; ++j) {
        if (frag_tile_domains[j].covers(coords, frag_tile_domains[f])) {
          covered = true;
          break;
        }
      }

      // Exclude this fragment from the space tile
      if (covered)
        continue;

      // Include this fragment in the space tile
      auto frag_domain = frag_tile_domains[f].domain_slice();
      auto frag_idx = frag_tile_domains[f].id();
      result_space_tile.append_frag_domain(frag_idx, frag_domain);
      auto tile_idx = frag_tile_domains[f].tile_pos(coords);
      ResultTile result_tile(frag_idx, tile_idx, domain);
      result_space_tile.set_result_tile(frag_idx, result_tile);
    }
  }
}

/* ****************************** */
/*          PRIVATE METHODS       */
/* ****************************** */

Status Reader::check_subarray() const {
  if (subarray_.layout() == Layout::GLOBAL_ORDER && subarray_.range_num() != 1)
    return LOG_STATUS(Status::ReaderError(
        "Cannot initialize reader; Multi-range subarrays with "
        "global order layout are not supported"));

  return Status::Ok();
}

void Reader::clear_tiles(
    const std::string& name,
    const std::vector<ResultTile*>& result_tiles) const {
  for (auto& result_tile : result_tiles)
    result_tile->erase_tile(name);
}

Status Reader::compute_result_cell_slabs(
    const std::vector<ResultCoords>& result_coords,
    std::vector<ResultCellSlab>* result_cell_slabs) const {
  STATS_START_TIMER(
      stats::Stats::TimerType::READ_COMPUTE_SPARSE_RESULT_CELL_SLABS_SPARSE)

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

  STATS_END_TIMER(
      stats::Stats::TimerType::READ_COMPUTE_SPARSE_RESULT_CELL_SLABS_SPARSE)
}

Status Reader::compute_range_result_coords(
    Subarray* subarray,
    unsigned frag_idx,
    ResultTile* tile,
    uint64_t range_idx,
    std::vector<ResultCoords>* result_coords) {
  auto coords_num = tile->cell_num();
  auto dim_num = array_schema_->dim_num();
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
      const auto& ranges = subarray->ranges_for_dim(d);
      RETURN_NOT_OK(tile->compute_results_sparse(
          d, ranges[range_coords[d]], &result_bitmap));
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
  STATS_START_TIMER(stats::Stats::TimerType::READ_COMPUTE_RANGE_RESULT_COORDS)

  auto range_num = subarray->range_num();
  range_result_coords->resize(range_num);
  auto cell_order = array_schema_->cell_order();
  Layout layout =
      (layout_ == Layout::GLOBAL_ORDER || layout_ == Layout::UNORDERED) ?
          cell_order :
          layout_;
  auto allows_dups = array_schema_->allows_dups();

  auto statuses = parallel_for(
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
              layout));
          RETURN_CANCEL_OR_ERROR(
              dedup_result_coords(&((*range_result_coords)[r])));
        }

        return Status::Ok();
      });

  for (auto st : statuses)
    RETURN_NOT_OK(st);

  return Status::Ok();

  STATS_END_TIMER(stats::Stats::TimerType::READ_COMPUTE_RANGE_RESULT_COORDS)
}

Status Reader::compute_range_result_coords(
    Subarray* subarray,
    uint64_t range_idx,
    uint32_t fragment_idx,
    const std::map<std::pair<unsigned, uint64_t>, size_t>& result_tile_map,
    std::vector<ResultTile>* result_tiles,
    std::vector<ResultCoords>* range_result_coords) {
  const auto& overlap = subarray->tile_overlap();

  // Skip dense fragments
  if (fragment_metadata_[fragment_idx]->dense())
    return Status::Ok();

  auto tr = overlap[fragment_idx][range_idx].tile_ranges_.begin();
  auto tr_end = overlap[fragment_idx][range_idx].tile_ranges_.end();
  auto t = overlap[fragment_idx][range_idx].tiles_.begin();
  auto t_end = overlap[fragment_idx][range_idx].tiles_.end();

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
  auto statuses = parallel_for(
      storage_manager_->compute_tp(), 0, fragment_num, [&](uint32_t f) {
        return compute_range_result_coords(
            subarray,
            range_idx,
            f,
            result_tile_map,
            result_tiles,
            &range_result_coords_vec[f]);
      });
  for (const auto& st : statuses)
    RETURN_NOT_OK(st);

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

  // Sort
  auto cell_order = array_schema_->cell_order();
  Layout layout = (layout_ == Layout ::UNORDERED) ? cell_order : layout_;

  RETURN_NOT_OK(sort_result_coords(
      result_coords->begin() + result_coords_size,
      result_coords->end(),
      layout));

  return Status::Ok();
}

Status Reader::compute_sparse_result_tiles(
    std::vector<ResultTile>* result_tiles,
    std::map<std::pair<unsigned, uint64_t>, size_t>* result_tile_map,
    std::vector<bool>* single_fragment) {
  STATS_START_TIMER(stats::Stats::TimerType::READ_COMPUTE_SPARSE_RESULT_TILES)

  // For easy reference
  auto domain = array_schema_->domain();
  const auto& subarray = read_state_.partitioner_.current();
  const auto& overlap = subarray.tile_overlap();
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
      const auto& tile_ranges = overlap[f][r].tile_ranges_;
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
      const auto& o_tiles = overlap[f][r].tiles_;
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

  STATS_END_TIMER(stats::Stats::TimerType::READ_COMPUTE_SPARSE_RESULT_TILES)
}

Status Reader::copy_fixed_cells(
    const std::string& name,
    uint64_t stride,
    const std::vector<ResultCellSlab>& result_cell_slabs,
    CopyFixedCellsContextCache* const ctx_cache) {
  assert(ctx_cache);

  auto stat_type = (array_schema_->is_attr(name)) ?
                       stats::Stats::TimerType::READ_COPY_FIXED_ATTR_VALUES :
                       stats::Stats::TimerType::READ_COPY_FIXED_COORDS;
  STATS_START_TIMER(stat_type)

  if (result_cell_slabs.empty()) {
    zero_out_buffer_sizes();
    return Status::Ok();
  }

  // Perform a lazy initialization of the context cache for copying
  // fixed cells.
  populate_cfc_ctx_cache(result_cell_slabs, ctx_cache);

  auto it = buffers_.find(name);
  auto buffer_size = it->second.buffer_size_;
  auto cell_size = array_schema_->cell_size(name);

  // Precompute the cell range destination offsets in the buffer.
  uint64_t buffer_offset = 0;
  for (uint64_t i = 0; i < ctx_cache->cs_offsets.size(); i++) {
    const auto& cs = result_cell_slabs[i];
    auto bytes_to_copy = cs.length_ * cell_size;
    ctx_cache->cs_offsets[i] = buffer_offset;
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
      &result_cell_slabs,
      ctx_cache);
  auto statuses = parallel_for(
      storage_manager_->compute_tp(),
      0,
      ctx_cache->cs_partitions.size(),
      std::move(copy_fn));

  for (auto st : statuses)
    RETURN_NOT_OK(st);

  // Update buffer offsets
  *(buffers_[name].buffer_size_) = buffer_offset;

  return Status::Ok();

  STATS_END_TIMER(stat_type)
}

void Reader::populate_cfc_ctx_cache(
    const std::vector<ResultCellSlab>& result_cell_slabs,
    CopyFixedCellsContextCache* const ctx_cache) {
  auto cs_offsets = &ctx_cache->cs_offsets;
  auto cs_partitions = &ctx_cache->cs_partitions;

  // If `ctx_cache` is already populated, we're done.
  if (!cs_offsets->empty()) {
    return;
  }

  // Allocate and resize `cs_offsets`.
  auto num_cs = result_cell_slabs.size();
  cs_offsets->resize(num_cs);

  // Partition the range of the `result_cell_slab` into
  // individual cell ranges for each TBB thread. If TBB
  // is disabled, we will use the concurrency level from
  // the compute thread pool.
  const int num_threads =
      global_state::GlobalState::GetGlobalState().tbb_threads() > 0 ?
          global_state::GlobalState::GetGlobalState().tbb_threads() :
          storage_manager_->compute_tp()->concurrency_level();
  const uint64_t num_cs_partitions = std::min<uint64_t>(num_threads, num_cs);
  const uint64_t cs_per_partition = num_cs / num_cs_partitions;
  const uint64_t cs_per_partition_carry = num_cs % num_cs_partitions;

  // Calculate the partition offsets into `cs_offsets`.
  uint64_t num_cs_partitioned = 0;
  for (uint64_t i = 0; i < num_cs_partitions; ++i) {
    const uint64_t num_cs_in_partition =
        cs_per_partition + ((i < cs_per_partition_carry) ? 1 : 0);
    num_cs_partitioned += num_cs_in_partition;
    cs_partitions->emplace_back(num_cs_partitioned);
  }
}

Status Reader::copy_partitioned_fixed_cells(
    const size_t partition_idx,
    const std::string* const name,
    const uint64_t stride,
    const std::vector<ResultCellSlab>* const result_cell_slabs,
    const CopyFixedCellsContextCache* const ctx_cache) {
  assert(name);
  assert(result_cell_slabs);
  assert(ctx_cache);

  // For easy reference.
  auto it = buffers_.find(*name);
  auto buffer = (unsigned char*)it->second.buffer_;
  auto cell_size = array_schema_->cell_size(*name);
  ByteVecValue fill_value;
  if (array_schema_->is_attr(*name))
    fill_value = array_schema_->attribute(*name)->fill_value();
  uint64_t fill_value_size = (uint64_t)fill_value.size();

  // Calculate the partition to operate on.
  const uint64_t cs_idx_start =
      partition_idx == 0 ? 0 : ctx_cache->cs_partitions[partition_idx - 1];
  const uint64_t cs_idx_end = ctx_cache->cs_partitions[partition_idx];

  // Copy the cells.
  for (uint64_t cs_idx = cs_idx_start; cs_idx < cs_idx_end; ++cs_idx) {
    const auto& cs = (*result_cell_slabs)[cs_idx];
    uint64_t offset = ctx_cache->cs_offsets[cs_idx];

    // Copy
    if (cs.tile_ == nullptr) {  // Empty range
      auto bytes_to_copy = cs.length_ * cell_size;
      auto fill_num = bytes_to_copy / fill_value_size;
      for (uint64_t j = 0; j < fill_num; ++j) {
        std::memcpy(buffer + offset, fill_value.data(), fill_value_size);
        offset += fill_value_size;
      }
    } else {  // Non-empty range
      if (stride == UINT64_MAX) {
        RETURN_NOT_OK(
            cs.tile_->read(*name, buffer + offset, cs.start_, cs.length_));
      } else {
        auto cell_offset = offset;
        auto start = cs.start_;
        for (uint64_t j = 0; j < cs.length_; ++j) {
          RETURN_NOT_OK(cs.tile_->read(*name, buffer + cell_offset, start, 1));
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
    uint64_t stride,
    const std::vector<ResultCellSlab>& result_cell_slabs,
    CopyVarCellsContextCache* const ctx_cache) {
  assert(ctx_cache);

  auto stat_type = (array_schema_->is_attr(name)) ?
                       stats::Stats::TimerType::READ_COPY_VAR_ATTR_VALUES :
                       stats::Stats::TimerType::READ_COPY_VAR_COORDS;
  STATS_START_TIMER(stat_type);

  if (result_cell_slabs.empty()) {
    zero_out_buffer_sizes();
    return Status::Ok();
  }

  // Perform a lazy initialization of the context cache for copying
  // fixed cells.
  populate_cvc_ctx_cache(result_cell_slabs, ctx_cache);

  // Compute the destinations of offsets and var-len data in the buffers.
  uint64_t total_offset_size, total_var_size;
  RETURN_NOT_OK(compute_var_cell_destinations(
      name,
      stride,
      result_cell_slabs,
      &ctx_cache->offset_offsets_per_cs,
      &ctx_cache->var_offsets_per_cs,
      &total_offset_size,
      &total_var_size));

  auto it = buffers_.find(name);
  auto buffer_size = it->second.buffer_size_;
  auto buffer_var_size = it->second.buffer_var_size_;

  // Check for overflow and return early (without copying) in that case.
  if (total_offset_size > *buffer_size || total_var_size > *buffer_var_size) {
    read_state_.overflowed_ = true;
    return Status::Ok();
  }

  // Copy result cell slabs in parallel
  std::function<Status(size_t)> copy_fn = std::bind(
      &Reader::copy_partitioned_var_cells,
      this,
      std::placeholders::_1,
      &name,
      stride,
      &result_cell_slabs,
      ctx_cache);
  auto statuses = parallel_for(
      storage_manager_->compute_tp(),
      0,
      ctx_cache->cs_partitions.size(),
      copy_fn);

  // Check all statuses
  for (auto st : statuses)
    RETURN_NOT_OK(st);

  // Update buffer offsets
  *(buffers_[name].buffer_size_) = total_offset_size;
  *(buffers_[name].buffer_var_size_) = total_var_size;

  return Status::Ok();

  STATS_END_TIMER(stat_type);
}

void Reader::populate_cvc_ctx_cache(
    const std::vector<ResultCellSlab>& result_cell_slabs,
    CopyVarCellsContextCache* const ctx_cache) {
  assert(ctx_cache);

  auto cs_partitions = &ctx_cache->cs_partitions;
  auto offset_offsets_per_cs = &ctx_cache->offset_offsets_per_cs;
  auto var_offsets_per_cs = &ctx_cache->var_offsets_per_cs;

  // If `ctx_cache` is already populated, we're done.
  if (!cs_partitions->empty()) {
    return;
  }

  // Calculate the partition range.
  const size_t num_cs = result_cell_slabs.size();
  const int num_threads =
      global_state::GlobalState::GetGlobalState().tbb_threads() > 0 ?
          global_state::GlobalState::GetGlobalState().tbb_threads() :
          storage_manager_->compute_tp()->concurrency_level();
  const uint64_t num_cs_partitions = std::min<uint64_t>(num_threads, num_cs);
  const uint64_t cs_per_partition = num_cs / num_cs_partitions;
  const uint64_t cs_per_partition_carry = num_cs % num_cs_partitions;

  // Allocate space for all of the partitions.
  cs_partitions->reserve(num_cs_partitions);

  // Compute the boundary between each partition. Each boundary
  // is represented by an `std::pair` that contains the total
  // length of each cell slab in the leading partition and an
  // exclusive cell slab index that ends the partition.
  size_t total_cs_length = 0;
  uint64_t next_partition_idx = cs_per_partition;
  if (cs_per_partition_carry > 0)
    ++next_partition_idx;

  for (uint64_t cs_idx = 0; cs_idx < num_cs; cs_idx++) {
    if (cs_idx == next_partition_idx) {
      cs_partitions->emplace_back(total_cs_length, cs_idx);

      // The final partition may contain extra cell slabs that did
      // not evenly divide into the partition range. Set the
      // `next_partition_idx` to zero and build the last boundary
      // after this for-loop.
      if (cs_partitions->size() == num_cs_partitions) {
        next_partition_idx = 0;
      } else {
        next_partition_idx += cs_per_partition;
        if (cs_idx < (cs_per_partition_carry - 1))
          ++next_partition_idx;
      }
    }

    total_cs_length += result_cell_slabs[cs_idx].length_;
  }

  // Store the final boundary.
  cs_partitions->emplace_back(total_cs_length, num_cs);

  // Allocate and size both `offset_offsets_per_cs` and
  // `var_offsets_per_cs` to contain enough elements for
  // storing any result cell.
  offset_offsets_per_cs->resize(total_cs_length);
  var_offsets_per_cs->resize(total_cs_length);
}

Status Reader::compute_var_cell_destinations(
    const std::string& name,
    uint64_t stride,
    const std::vector<ResultCellSlab>& result_cell_slabs,
    std::vector<uint64_t>* offset_offsets_per_cs,
    std::vector<uint64_t>* var_offsets_per_cs,
    uint64_t* total_offset_size,
    uint64_t* total_var_size) const {
  // For easy reference
  auto num_cs = result_cell_slabs.size();
  auto offset_size = constants::cell_var_offset_size;
  ByteVecValue fill_value;
  if (array_schema_->is_attr(name))
    fill_value = array_schema_->attribute(name)->fill_value();
  auto fill_value_size = (uint64_t)fill_value.size();

  // Compute the destinations for all result cell slabs
  *total_offset_size = 0;
  *total_var_size = 0;
  size_t total_cs_length = 0;
  for (uint64_t cs_idx = 0; cs_idx < num_cs; cs_idx++) {
    const auto& cs = result_cell_slabs[cs_idx];

    // Get tile information, if the range is nonempty.
    uint64_t* tile_offsets = nullptr;
    uint64_t tile_cell_num = 0;
    uint64_t tile_var_size = 0;
    if (cs.tile_ != nullptr) {
      const auto tile_pair = cs.tile_->tile_pair(name);
      const auto& tile = tile_pair->first;
      const auto& tile_var = tile_pair->second;

      // Get the internal buffer to the offset values.
      ChunkedBuffer* const chunked_buffer = tile.chunked_buffer();

      // Offset tiles are always contiguously allocated.
      assert(
          chunked_buffer->buffer_addressing() ==
          ChunkedBuffer::BufferAddressing::CONTIGUOUS);

      tile_offsets = (uint64_t*)chunked_buffer->get_contiguous_unsafe();
      tile_cell_num = tile.cell_num();
      tile_var_size = tile_var.size();
    }

    // Compute the destinations for each cell in the range.
    uint64_t dest_vec_idx = 0;
    stride = (stride == UINT64_MAX) ? 1 : stride;
    for (auto cell_idx = cs.start_; dest_vec_idx < cs.length_;
         cell_idx += stride, dest_vec_idx++) {
      // Get size of variable-sized cell
      uint64_t cell_var_size = 0;
      if (cs.tile_ == nullptr) {
        cell_var_size = fill_value_size;
      } else {
        cell_var_size =
            (cell_idx != tile_cell_num - 1) ?
                tile_offsets[cell_idx + 1] - tile_offsets[cell_idx] :
                tile_var_size - (tile_offsets[cell_idx] - tile_offsets[0]);
      }

      // Record destination offsets.
      (*offset_offsets_per_cs)[total_cs_length + dest_vec_idx] =
          *total_offset_size;
      (*var_offsets_per_cs)[total_cs_length + dest_vec_idx] = *total_var_size;
      *total_offset_size += offset_size;
      *total_var_size += cell_var_size;
    }

    total_cs_length += cs.length_;
  }

  return Status::Ok();
}

Status Reader::copy_partitioned_var_cells(
    const size_t partition_idx,
    const std::string* const name,
    uint64_t stride,
    const std::vector<ResultCellSlab>* const result_cell_slabs,
    CopyVarCellsContextCache* const ctx_cache) {
  assert(name);
  assert(result_cell_slabs);
  assert(ctx_cache);

  auto it = buffers_.find(*name);
  auto buffer = (unsigned char*)it->second.buffer_;
  auto buffer_var = (unsigned char*)it->second.buffer_var_;
  uint64_t offset_size = constants::cell_var_offset_size;
  ByteVecValue fill_value;
  if (array_schema_->is_attr(*name))
    fill_value = array_schema_->attribute(*name)->fill_value();
  auto fill_value_size = (uint64_t)fill_value.size();
  auto cs_partitions = &ctx_cache->cs_partitions;
  auto offset_offsets_per_cs = &ctx_cache->offset_offsets_per_cs;
  auto var_offsets_per_cs = &ctx_cache->var_offsets_per_cs;

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
    const auto& cs = (*result_cell_slabs)[cs_idx];

    // Get tile information, if the range is nonempty.
    uint64_t* tile_offsets = nullptr;
    Tile* tile_var = nullptr;
    uint64_t tile_cell_num = 0;
    if (cs.tile_ != nullptr) {
      const auto tile_pair = cs.tile_->tile_pair(*name);
      Tile* const tile = &tile_pair->first;
      tile_var = &tile_pair->second;
      // Get the internal buffer to the offset values.
      ChunkedBuffer* const chunked_buffer = tile->chunked_buffer();

      // Offset tiles are always contiguously allocated.
      assert(
          chunked_buffer->buffer_addressing() ==
          ChunkedBuffer::BufferAddressing::CONTIGUOUS);

      tile_offsets = (uint64_t*)chunked_buffer->get_contiguous_unsafe();
      tile_cell_num = tile->cell_num();
    }

    // Copy each cell in the range
    uint64_t dest_vec_idx = 0;
    stride = (stride == UINT64_MAX) ? 1 : stride;
    for (auto cell_idx = cs.start_; dest_vec_idx < cs.length_;
         cell_idx += stride, dest_vec_idx++) {
      auto offset_dest =
          buffer + (*offset_offsets_per_cs)[arr_offset + dest_vec_idx];
      // offset_offsets[dest_vec_idx];
      auto var_offset = (*var_offsets_per_cs)[arr_offset + dest_vec_idx];
      auto var_dest = buffer_var + var_offset;

      // Copy offset
      std::memcpy(offset_dest, &var_offset, offset_size);

      // Copy variable-sized value
      if (cs.tile_ == nullptr) {
        std::memcpy(var_dest, fill_value.data(), fill_value_size);
      } else {
        const uint64_t cell_var_size =
            (cell_idx != tile_cell_num - 1) ?
                tile_offsets[cell_idx + 1] - tile_offsets[cell_idx] :
                tile_var->size() - (tile_offsets[cell_idx] - tile_offsets[0]);
        const uint64_t tile_var_offset =
            tile_offsets[cell_idx] - tile_offsets[0];
        RETURN_NOT_OK(tile_var->read(var_dest, cell_var_size, tile_var_offset));
      }
    }

    arr_offset += cs.length_;
  }

  return Status::Ok();
}

template <class T>
void Reader::compute_result_space_tiles(
    const Subarray& subarray,
    std::map<const T*, ResultSpaceTile<T>>* result_space_tiles) const {
  // For easy reference
  auto domain = array_schema_->domain()->domain();
  auto tile_extents = array_schema_->domain()->tile_extents();
  auto tile_order = array_schema_->tile_order();

  // Compute fragment tile domains
  std::vector<TileDomain<T>> frag_tile_domains;
  auto fragment_num = (int)fragment_metadata_.size();
  if (fragment_num > 0) {
    for (int i = fragment_num - 1; i >= 0; --i) {
      if (fragment_metadata_[i]->dense()) {
        frag_tile_domains.emplace_back(
            i,
            domain,
            fragment_metadata_[i]->non_empty_domain(),
            tile_extents,
            tile_order);
      }
    }
  }

  // Get tile coords and array domain
  const auto& tile_coords = subarray.tile_coords();
  TileDomain<T> array_tile_domain(
      UINT32_MAX, domain, domain, tile_extents, tile_order);

  // Compute result space tiles
  compute_result_space_tiles<T>(
      array_schema_->domain(),
      tile_coords,
      array_tile_domain,
      frag_tile_domains,
      result_space_tiles);
}

template <class T>
Status Reader::compute_result_cell_slabs(
    const Subarray& subarray,
    std::map<const T*, ResultSpaceTile<T>>* result_space_tiles,
    std::vector<ResultCoords>* result_coords,
    std::vector<ResultTile*>* result_tiles,
    std::vector<ResultCellSlab>* result_cell_slabs) const {
  STATS_START_TIMER(
      stats::Stats::TimerType::READ_COMPUTE_SPARSE_RESULT_CELL_SLABS_DENSE)

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

  STATS_END_TIMER(
      stats::Stats::TimerType::READ_COMPUTE_SPARSE_RESULT_CELL_SLABS_DENSE)
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
      auto frag_tile_pair = std::pair<unsigned, uint64_t>(frag_idx, tile_idx);
      auto it = frag_tile_set->find(frag_tile_pair);
      if (it == frag_tile_set->end()) {
        frag_tile_set->insert(frag_tile_pair);
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
  STATS_START_TIMER(stats::Stats::TimerType::READ_COMPUTE_RESULT_COORDS)

  // Get overlapping tile indexes
  typedef std::pair<unsigned, uint64_t> FragTilePair;
  std::map<FragTilePair, size_t> result_tile_map;
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
  RETURN_CANCEL_OR_ERROR(load_offsets({constants::coords}));

  // Preload unzipped coordinate tile offsets. Note that this will
  // ignore fragments with a version < 5.
  const auto dim_num = array_schema_->dim_num();
  std::vector<std::string> dim_names;
  dim_names.reserve(dim_num);
  for (unsigned d = 0; d < dim_num; ++d)
    dim_names.emplace_back(array_schema_->dimension(d)->name());
  RETURN_CANCEL_OR_ERROR(load_offsets(dim_names));

  // Read and unfilter zipped coordinate tiles. Note that
  // this will ignore fragments with a version >= 5.
  RETURN_CANCEL_OR_ERROR(read_tiles(constants::coords, tmp_result_tiles));
  RETURN_CANCEL_OR_ERROR(unfilter_tiles(constants::coords, tmp_result_tiles));

  // Read and unfilter unzipped coordinate tiles. Note that
  // this will ignore fragments with a version < 5.
  for (const auto& dim_name : dim_names) {
    RETURN_CANCEL_OR_ERROR(read_tiles(dim_name, tmp_result_tiles));
    RETURN_CANCEL_OR_ERROR(unfilter_tiles(dim_name, tmp_result_tiles));
  }

  // Fetch the sub partitioner's memory budget.
  bool found = false;
  auto config = storage_manager_->config();
  uint64_t cfg_sub_memory_budget = 0;
  RETURN_NOT_OK(config.get<uint64_t>(
      "sm.sub_partitioner_memory_budget", &cfg_sub_memory_budget, &found));
  assert(found);

  // The a 0-value memory budget is used to bypass the sub-partitioner. The
  // sub-partitioner is only useful for scenarios where the sorting time is much
  // larger than the partitioning time.
  if (cfg_sub_memory_budget == 0) {
    // Compute the read coordinates for all fragments for each subarray range.
    std::vector<std::vector<ResultCoords>> range_result_coords;
    RETURN_CANCEL_OR_ERROR(compute_range_result_coords(
        &read_state_.partitioner_.current(),
        single_fragment,
        result_tile_map,
        result_tiles,
        &range_result_coords));
    result_tile_map.clear();

    // Compute final coords (sorted in the result layout) of the whole subarray.
    RETURN_CANCEL_OR_ERROR(
        compute_subarray_coords(&range_result_coords, result_coords));
    range_result_coords.clear();
  } else {
    // If the configured memory budget is too low (e.g. unsplittable), it
    // will be corrected in a retry.
    uint64_t sub_partitioner_memory_budget = cfg_sub_memory_budget;
    uint64_t sub_partitioner_memory_budget_var = cfg_sub_memory_budget;

    // Create a sub-partitioner to partition the current subarray in
    // `read_state_.partitioner_`. This allows us to compute the range
    // result coords and subarray coords on a smaller set of elements.
    // The motiviation for this is primarily to avoid sorting a large
    // number of elements within a `parallel_sort` because it has a
    // time complexity of O(N*log(N)).
    SubarrayPartitioner* const partitioner = &read_state_.partitioner_;
    SubarrayPartitioner sub_partitioner(
        partitioner->current(),
        sub_partitioner_memory_budget,
        sub_partitioner_memory_budget,
        storage_manager_->compute_tp());

    // Set the individual attribute budgets in the sub-partitioner
    // to the same values as in the parent partitioner.
    for (const auto& kv : *partitioner->get_result_budgets()) {
      const std::string& attr_name = kv.first;
      const SubarrayPartitioner::ResultBudget& result_budget = kv.second;
      if (!array_schema_->var_size(attr_name)) {
        RETURN_NOT_OK(sub_partitioner.set_result_budget(
            attr_name.c_str(), result_budget.size_fixed_));
      } else {
        RETURN_NOT_OK(sub_partitioner.set_result_budget(
            attr_name.c_str(),
            result_budget.size_fixed_,
            result_budget.size_var_));
      }
    }

    // Move to the first partition.
    RETURN_NOT_OK(sub_partitioner.next(&read_state_.unsplittable_));

    while (true) {
      // If the sub-partitioners memory budget was too low, we may
      // have been unable to split. In this scenario, double the
      // budget and retry. In the worst-case scenario, the budget
      // will equal the parent partitioner's budget.
      while (read_state_.unsplittable_) {
        uint64_t partitioner_memory_budget;
        uint64_t partitioner_memory_budget_var;
        RETURN_NOT_OK(partitioner->get_memory_budget(
            &partitioner_memory_budget, &partitioner_memory_budget_var));

        sub_partitioner_memory_budget = std::min(
            partitioner_memory_budget, sub_partitioner_memory_budget * 2);
        sub_partitioner_memory_budget_var = std::min(
            partitioner_memory_budget_var,
            sub_partitioner_memory_budget_var * 2);

        RETURN_NOT_OK(sub_partitioner.set_memory_budget(
            sub_partitioner_memory_budget, sub_partitioner_memory_budget));

        RETURN_NOT_OK(sub_partitioner.next(&read_state_.unsplittable_));
      }

      std::vector<std::vector<ResultCoords>> range_result_coords;
      RETURN_CANCEL_OR_ERROR(compute_range_result_coords(
          &sub_partitioner.current(),
          single_fragment,
          result_tile_map,
          result_tiles,
          &range_result_coords));

      RETURN_CANCEL_OR_ERROR(
          compute_subarray_coords(&range_result_coords, result_coords));
      range_result_coords.clear();

      // We're done when we have processed all sub-partitions.
      if (sub_partitioner.done())
        break;

      RETURN_NOT_OK(sub_partitioner.next(&read_state_.unsplittable_));
    }
  }

  return Status::Ok();

  STATS_END_TIMER(stats::Stats::TimerType::READ_COMPUTE_RESULT_COORDS)
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

  get_result_tile_stats(result_tiles);
  get_result_cell_stats(result_cell_slabs);

  // Clear sparse coordinate tiles (not needed any more)
  erase_coord_tiles(&sparse_result_tiles);

  // Needed when copying the cells
  auto stride = array_schema_->domain()->stride<T>(subarray.layout());
  RETURN_NOT_OK(copy_attribute_values(stride, result_tiles, result_cell_slabs));

  // Fill coordinates if the user requested them
  if (!read_state_.overflowed_ && has_coords())
    RETURN_CANCEL_OR_ERROR(fill_dense_coords<T>(subarray));

  return Status::Ok();
}

template <class T>
Status Reader::fill_dense_coords(const Subarray& subarray) {
  STATS_START_TIMER(stats::Stats::TimerType::READ_FILL_DENSE_COORDS)

  // Prepare buffers
  std::vector<unsigned> dim_idx;
  std::vector<QueryBuffer*> buffers;
  auto coords_it = buffers_.find(constants::coords);
  auto dim_num = array_schema_->dim_num();
  if (coords_it != buffers_.end()) {
    buffers.emplace_back(&(coords_it->second));
    dim_idx.emplace_back(dim_num);
  } else {
    for (unsigned d = 0; d < dim_num; ++d) {
      const auto& dim = array_schema_->dimension(d);
      auto it = buffers_.find(dim->name());
      if (it != buffers_.end()) {
        buffers.emplace_back(&(it->second));
        dim_idx.emplace_back(d);
      }
    }
  }
  std::vector<uint64_t> offsets(buffers.size(), 0);

  if (layout_ == Layout::GLOBAL_ORDER) {
    RETURN_NOT_OK(
        fill_dense_coords_global<T>(subarray, dim_idx, buffers, &offsets));
  } else {
    assert(layout_ == Layout::ROW_MAJOR || layout_ == Layout::COL_MAJOR);
    RETURN_NOT_OK(
        fill_dense_coords_row_col<T>(subarray, dim_idx, buffers, &offsets));
  }

  // Update buffer sizes
  for (size_t i = 0; i < buffers.size(); ++i)
    *(buffers[i]->buffer_size_) = offsets[i];

  return Status::Ok();

  STATS_END_TIMER(stats::Stats::TimerType::READ_FILL_DENSE_COORDS)
}

template <class T>
Status Reader::fill_dense_coords_global(
    const Subarray& subarray,
    const std::vector<unsigned>& dim_idx,
    const std::vector<QueryBuffer*>& buffers,
    std::vector<uint64_t>* offsets) {
  auto tile_coords = subarray.tile_coords();
  auto cell_order = array_schema_->cell_order();

  for (const auto& tc : tile_coords) {
    auto tile_subarray = subarray.crop_to_tile((const T*)&tc[0], cell_order);
    RETURN_NOT_OK(
        fill_dense_coords_row_col<T>(tile_subarray, dim_idx, buffers, offsets));
  }

  return Status::Ok();
}

template <class T>
Status Reader::fill_dense_coords_row_col(
    const Subarray& subarray,
    const std::vector<unsigned>& dim_idx,
    const std::vector<QueryBuffer*>& buffers,
    std::vector<uint64_t>* offsets) {
  auto cell_order = array_schema_->cell_order();
  auto dim_num = array_schema_->dim_num();

  // Iterate over all coordinates, retrieved in cell slabs
  CellSlabIter<T> iter(&subarray);
  RETURN_CANCEL_OR_ERROR(iter.begin());
  while (!iter.end()) {
    auto cell_slab = iter.cell_slab();
    auto coords_num = cell_slab.length_;

    // Check for overflow
    for (size_t i = 0; i < buffers.size(); ++i) {
      auto idx = (dim_idx[i] == dim_num) ? 0 : dim_idx[i];
      auto dim = array_schema_->domain()->dimension(idx);
      auto coord_size = dim->coord_size();
      coord_size = (dim_idx[i] == dim_num) ? coord_size * dim_num : coord_size;
      auto buff_size = *(buffers[i]->buffer_size_);
      auto offset = (*offsets)[i];
      if (coords_num * coord_size + offset > buff_size) {
        read_state_.overflowed_ = true;
        return Status::Ok();
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

  return Status::Ok();
}

template <class T>
void Reader::fill_dense_coords_row_slab(
    const T* start,
    uint64_t num,
    const std::vector<unsigned>& dim_idx,
    const std::vector<QueryBuffer*>& buffers,
    std::vector<uint64_t>* offsets) const {
  // For easy reference
  auto dim_num = array_schema_->dim_num();

  // Special zipped coordinates
  if (dim_idx.size() == 1 && dim_idx[0] == dim_num) {
    auto c_buff = (char*)buffers[0]->buffer_;
    auto offset = &(*offsets)[0];

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
        auto offset = &(*offsets)[b];

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
    std::vector<uint64_t>* offsets) const {
  // For easy reference
  auto dim_num = array_schema_->dim_num();

  // Special zipped coordinates
  if (dim_idx.size() == 1 && dim_idx[0] == dim_num) {
    auto c_buff = (char*)buffers[0]->buffer_;
    auto offset = &(*offsets)[0];

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
        auto offset = &(*offsets)[b];

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

Status Reader::unfilter_tiles(
    const std::string& name,
    const std::vector<ResultTile*>& result_tiles,
    const std::unordered_map<
        ResultTile*,
        std::vector<std::pair<uint64_t, uint64_t>>>* const cs_ranges) const {
  auto stat_type = (array_schema_->is_attr(name)) ?
                       stats::Stats::TimerType::READ_UNFILTER_ATTR_TILES :
                       stats::Stats::TimerType::READ_UNFILTER_COORD_TILES;
  STATS_START_TIMER(stat_type);

  auto var_size = array_schema_->var_size(name);
  auto num_tiles = static_cast<uint64_t>(result_tiles.size());
  auto encryption_key = array_->encryption_key();

  auto statuses = parallel_for(
      storage_manager_->compute_tp(), 0, num_tiles, [&, this](uint64_t i) {
        auto& tile = result_tiles[i];
        auto& fragment = fragment_metadata_[tile->frag_idx()];
        auto format_version = fragment->format_version();

        // Applicable for zipped coordinates only to versions < 5
        // Applicable for separate coordinates only to version >= 5
        if (name != constants::coords ||
            (name == constants::coords && format_version < 5) ||
            (array_schema_->is_dim(name) && format_version >= 5)) {
          auto tile_pair = tile->tile_pair(name);

          // Skip non-existent attributes/dimensions (e.g. coords in the
          // dense case).
          if (tile_pair == nullptr ||
              tile_pair->first.filtered_buffer()->size() == 0)
            return Status::Ok();

          // Get information about the tile in its fragment
          auto tile_attr_uri = fragment->uri(name);
          auto tile_idx = tile->tile_idx();
          uint64_t tile_attr_offset;
          RETURN_NOT_OK(fragment->file_offset(
              *encryption_key, name, tile_idx, &tile_attr_offset));

          auto& t = tile_pair->first;
          auto& t_var = tile_pair->second;

          // If we're performing selective unfiltering, lookup the result
          // cell slab ranges associated with this tile. If we do not have
          // any ranges, use an empty list to indicate that this tile doesn't
          // contain any results.
          const std::vector<std::pair<uint64_t, uint64_t>>*
              result_cell_slab_ranges = nullptr;
          static const std::vector<std::pair<uint64_t, uint64_t>> empty_ranges;
          if (cs_ranges) {
            result_cell_slab_ranges =
                cs_ranges->find(tile) != cs_ranges->end() ?
                    &cs_ranges->at(tile) :
                    &empty_ranges;
          }

          // Cache 't'.
          if (t.filtered()) {
            // Store the filtered buffer in the tile cache.
            RETURN_NOT_OK(storage_manager_->write_to_cache(
                tile_attr_uri, tile_attr_offset, t.filtered_buffer()));
          }

          // Cache 't_var'.
          if (var_size && t_var.filtered()) {
            auto tile_attr_var_uri = fragment->var_uri(name);
            uint64_t tile_attr_var_offset;
            RETURN_NOT_OK(fragment->file_var_offset(
                *encryption_key, name, tile_idx, &tile_attr_var_offset));

            // Store the filtered buffer in the tile cache.
            RETURN_NOT_OK(storage_manager_->write_to_cache(
                tile_attr_var_uri,
                tile_attr_var_offset,
                t_var.filtered_buffer()));
          }

          // Unfilter 't' for fixed-sized tiles, otherwise unfilter both 't' and
          // 't_var' for var-sized tiles.
          if (!var_size) {
            RETURN_NOT_OK(unfilter_tile(name, &t, result_cell_slab_ranges));
          } else {
            RETURN_NOT_OK(
                unfilter_tile(name, &t, &t_var, result_cell_slab_ranges));
          }
        }

        return Status::Ok();
      });

  for (const auto& st : statuses)
    RETURN_CANCEL_OR_ERROR(st);

  return Status::Ok();

  STATS_END_TIMER(stat_type);
}

Status Reader::unfilter_tile(
    const std::string& name,
    Tile* tile,
    const std::vector<std::pair<uint64_t, uint64_t>>* result_cell_slab_ranges)
    const {
  FilterPipeline filters = array_schema_->filters(name);

  // Append an encryption unfilter when necessary.
  RETURN_NOT_OK(FilterPipeline::append_encryption_filter(
      &filters, array_->get_encryption_key()));

  // Skip selective unfiltering on coordinate tiles.
  if (name == constants::coords || tile->stores_coords()) {
    result_cell_slab_ranges = nullptr;
  }

  // Reverse the tile filters.
  RETURN_NOT_OK(filters.run_reverse(
      tile,
      storage_manager_->compute_tp(),
      storage_manager_->config(),
      result_cell_slab_ranges));

  return Status::Ok();
}

Status Reader::unfilter_tile(
    const std::string& name,
    Tile* tile,
    Tile* tile_var,
    const std::vector<std::pair<uint64_t, uint64_t>>* result_cell_slab_ranges)
    const {
  FilterPipeline offset_filters = array_schema_->cell_var_offsets_filters();
  FilterPipeline filters = array_schema_->filters(name);

  // Append an encryption unfilter when necessary.
  RETURN_NOT_OK(FilterPipeline::append_encryption_filter(
      &offset_filters, array_->get_encryption_key()));
  RETURN_NOT_OK(FilterPipeline::append_encryption_filter(
      &filters, array_->get_encryption_key()));

  // Skip selective unfiltering on coordinate tiles.
  if (name == constants::coords || tile->stores_coords()) {
    result_cell_slab_ranges = nullptr;
  }

  // Reverse the tile filters, but do not use selective
  // unfiltering for offset tiles.
  RETURN_NOT_OK(offset_filters.run_reverse(
      tile,
      storage_manager_->compute_tp(),
      storage_manager_->config(),
      nullptr));
  RETURN_NOT_OK(filters.run_reverse(
      tile,
      tile_var,
      storage_manager_->compute_tp(),
      storage_manager_->config(),
      result_cell_slab_ranges));

  return Status::Ok();
}

Status Reader::get_all_result_coords(
    ResultTile* tile, std::vector<ResultCoords>* result_coords) const {
  auto coords_num = tile->cell_num();
  for (uint64_t i = 0; i < coords_num; ++i)
    result_coords->emplace_back(tile, i);

  return Status::Ok();
}

bool Reader::has_coords() const {
  for (const auto& it : buffers_) {
    if (it.first == constants::coords || array_schema_->is_dim(it.first))
      return true;
  }

  return false;
}

bool Reader::has_separate_coords() const {
  for (const auto& it : buffers_) {
    if (array_schema_->is_dim(it.first))
      return true;
  }

  return false;
}

Status Reader::init_read_state() {
  STATS_START_TIMER(stats::Stats::TimerType::READ_INIT_STATE)

  // Check subarray
  if (subarray_.layout() == Layout::GLOBAL_ORDER && subarray_.range_num() != 1)
    return LOG_STATUS(
        Status::ReaderError("Cannot initialize read state; Multi-range "
                            "subarrays do not support global order"));

  // Get config
  bool found = false;
  auto config = storage_manager_->config();
  uint64_t memory_budget = 0;
  RETURN_NOT_OK(
      config.get<uint64_t>("sm.memory_budget", &memory_budget, &found));
  assert(found);
  uint64_t memory_budget_var = 0;
  RETURN_NOT_OK(
      config.get<uint64_t>("sm.memory_budget_var", &memory_budget_var, &found));
  assert(found);

  // Create read state
  read_state_.partitioner_ = SubarrayPartitioner(
      subarray_,
      memory_budget,
      memory_budget_var,
      storage_manager_->compute_tp());
  read_state_.overflowed_ = false;
  read_state_.unsplittable_ = false;

  // Set result size budget
  for (const auto& a : buffers_) {
    auto attr_name = a.first;
    auto buffer_size = a.second.buffer_size_;
    auto buffer_var_size = a.second.buffer_var_size_;
    if (!array_schema_->var_size(attr_name)) {
      RETURN_NOT_OK(read_state_.partitioner_.set_result_budget(
          attr_name.c_str(), *buffer_size));
    } else {
      RETURN_NOT_OK(read_state_.partitioner_.set_result_budget(
          attr_name.c_str(), *buffer_size, *buffer_var_size));
    }
  }

  read_state_.unsplittable_ = false;
  read_state_.overflowed_ = false;
  read_state_.initialized_ = true;

  return Status::Ok();

  STATS_END_TIMER(stats::Stats::TimerType::READ_INIT_STATE)
}

Status Reader::init_tile(
    uint32_t format_version, const std::string& name, Tile* tile) const {
  // For easy reference
  auto cell_size = array_schema_->cell_size(name);
  auto type = array_schema_->type(name);
  auto is_coords = (name == constants::coords);
  auto dim_num = (is_coords) ? array_schema_->dim_num() : 0;

  // Initialize
  RETURN_NOT_OK(tile->init_filtered(format_version, type, cell_size, dim_num));

  return Status::Ok();
}

Status Reader::init_tile(
    uint32_t format_version,
    const std::string& name,
    Tile* tile,
    Tile* tile_var) const {
  // For easy reference
  auto type = array_schema_->type(name);

  // Initialize
  RETURN_NOT_OK(tile->init_filtered(
      format_version,
      constants::cell_var_offset_type,
      constants::cell_var_offset_size,
      0));
  RETURN_NOT_OK(
      tile_var->init_filtered(format_version, type, datatype_size(type), 0));
  return Status::Ok();
}

Status Reader::load_offsets(const std::vector<std::string>& names) {
  const auto encryption_key = array_->encryption_key();

  const auto statuses = parallel_for(
      storage_manager_->compute_tp(),
      0,
      fragment_metadata_.size(),
      [&](const uint64_t i) {
        auto& fragment = fragment_metadata_[i];
        const auto format_version = fragment->format_version();

        // Filter the 'names' for format-specific names.
        std::vector<std::string> filtered_names;
        filtered_names.reserve(names.size());
        for (const auto& name : names) {
          // Applicable for zipped coordinates only to versions < 5
          if (name == constants::coords && format_version >= 5)
            continue;

          // Applicable to separate coordinates only to versions >= 5
          const auto is_dim = array_schema_->is_dim(name);
          if (is_dim && format_version < 5)
            continue;

          filtered_names.emplace_back(name);
        }

        fragment->load_tile_offsets(*encryption_key, std::move(filtered_names));
        return Status::Ok();
      });

  for (const auto& st : statuses)
    RETURN_NOT_OK(st);

  return Status::Ok();
}

Status Reader::read_tiles(
    const std::string& name,
    const std::vector<ResultTile*>& result_tiles) const {
  auto stat_type = (array_schema_->is_attr(name)) ?
                       stats::Stats::TimerType::READ_ATTR_TILES :
                       stats::Stats::TimerType::READ_COORD_TILES;
  STATS_START_TIMER(stat_type);

  // Shortcut for empty tile vec
  if (result_tiles.empty())
    return Status::Ok();

  // Read the tiles asynchronously
  std::vector<ThreadPool::Task> tasks;
  RETURN_CANCEL_OR_ERROR(read_tiles(name, result_tiles, &tasks));

  // Wait for the reads to finish and check statuses.
  auto statuses = storage_manager_->io_tp()->wait_all_status(tasks);
  for (const auto& st : statuses)
    RETURN_CANCEL_OR_ERROR(st);

  return Status::Ok();

  STATS_END_TIMER(stat_type);
}

Status Reader::read_tiles(
    const std::string& name,
    const std::vector<ResultTile*>& result_tiles,
    std::vector<ThreadPool::Task>* const tasks) const {
  // Shortcut for empty tile vec
  if (result_tiles.empty())
    return Status::Ok();

  // For each tile, read from its fragment.
  const bool var_size = array_schema_->var_size(name);
  const auto encryption_key = array_->encryption_key();

  // Gather the unique fragments indexes for which there are tiles
  std::unordered_set<uint32_t> fragment_idxs_set;
  for (const auto& tile : result_tiles)
    fragment_idxs_set.emplace(tile->frag_idx());

  // Put fragment indexes in a vector
  std::vector<uint32_t> fragment_idxs_vec;
  fragment_idxs_vec.reserve(fragment_idxs_set.size());
  for (const auto& idx : fragment_idxs_set)
    fragment_idxs_vec.emplace_back(idx);

  // Populate the list of regions per file to be read.
  std::map<URI, std::vector<std::tuple<uint64_t, void*, uint64_t>>> all_regions;
  for (const auto& tile : result_tiles) {
    FragmentMetadata* const fragment = fragment_metadata_[tile->frag_idx()];
    const uint32_t format_version = fragment->format_version();

    // Applicable for zipped coordinates only to versions < 5
    if (name == constants::coords && format_version >= 5)
      continue;

    // Applicable to separate coordinates only to versions >= 5
    const bool is_dim = array_schema_->is_dim(name);
    if (is_dim && format_version < 5)
      continue;

    // Initialize the tile(s)
    if (is_dim) {
      const uint64_t dim_num = array_schema_->dim_num();
      for (uint64_t d = 0; d < dim_num; ++d) {
        if (array_schema_->dimension(d)->name() == name) {
          tile->init_coord_tile(name, d);
          break;
        }
      }
    } else {
      tile->init_attr_tile(name);
    }

    ResultTile::TilePair* const tile_pair = tile->tile_pair(name);
    assert(tile_pair != nullptr);
    Tile* const t = &tile_pair->first;
    Tile* const t_var = &tile_pair->second;
    if (!var_size) {
      RETURN_NOT_OK(init_tile(format_version, name, t));
    } else {
      RETURN_NOT_OK(init_tile(format_version, name, t, t_var));
    }

    // Get information about the tile in its fragment
    auto tile_attr_uri = fragment->uri(name);
    auto tile_idx = tile->tile_idx();
    uint64_t tile_attr_offset;
    RETURN_NOT_OK(fragment->file_offset(
        *encryption_key, name, tile_idx, &tile_attr_offset));
    uint64_t tile_persisted_size;
    RETURN_NOT_OK(fragment->persisted_tile_size(
        *encryption_key, name, tile_idx, &tile_persisted_size));

    // Try the cache first.
    bool cache_hit;
    RETURN_NOT_OK(storage_manager_->read_from_cache(
        tile_attr_uri,
        tile_attr_offset,
        t->filtered_buffer(),
        tile_persisted_size,
        &cache_hit));
    if (!cache_hit) {
      // Add the region of the fragment to be read.
      RETURN_NOT_OK(t->filtered_buffer()->realloc(tile_persisted_size));
      t->filtered_buffer()->set_size(tile_persisted_size);
      t->filtered_buffer()->reset_offset();
      all_regions[tile_attr_uri].emplace_back(
          tile_attr_offset, t->filtered_buffer()->data(), tile_persisted_size);
    }

    if (var_size) {
      auto tile_attr_var_uri = fragment->var_uri(name);
      uint64_t tile_attr_var_offset;
      RETURN_NOT_OK(fragment->file_var_offset(
          *encryption_key, name, tile_idx, &tile_attr_var_offset));
      uint64_t tile_var_persisted_size;
      RETURN_NOT_OK(fragment->persisted_tile_var_size(
          *encryption_key, name, tile_idx, &tile_var_persisted_size));

      Buffer cached_var_buffer;
      RETURN_NOT_OK(storage_manager_->read_from_cache(
          tile_attr_var_uri,
          tile_attr_var_offset,
          t_var->filtered_buffer(),
          tile_var_persisted_size,
          &cache_hit));

      if (!cache_hit) {
        // Add the region of the fragment to be read.
        RETURN_NOT_OK(
            t_var->filtered_buffer()->realloc(tile_var_persisted_size));
        t_var->filtered_buffer()->set_size(tile_var_persisted_size);
        t_var->filtered_buffer()->reset_offset();
        all_regions[tile_attr_var_uri].emplace_back(
            tile_attr_var_offset,
            t_var->filtered_buffer()->data(),
            tile_var_persisted_size);
      }
    }
  }

  // Do not use the read-ahead cache because tiles will be
  // cached in the tile cache.
  const bool use_read_ahead = false;

  // Enqueue all regions to be read.
  for (const auto& item : all_regions) {
    RETURN_NOT_OK(storage_manager_->vfs()->read_all(
        item.first,
        item.second,
        storage_manager_->io_tp(),
        tasks,
        use_read_ahead));
  }

  return Status::Ok();
}

void Reader::reset_buffer_sizes() {
  for (auto& it : buffers_) {
    *(it.second.buffer_size_) = it.second.original_buffer_size_;
    if (it.second.buffer_var_size_ != nullptr)
      *(it.second.buffer_var_size_) = it.second.original_buffer_var_size_;
  }
}

Status Reader::sort_result_coords(
    std::vector<ResultCoords>::iterator iter_begin,
    std::vector<ResultCoords>::iterator iter_end,
    Layout layout) const {
  // TODO: do not sort if it is single fragment and
  // (i) it is single dimension, or (ii) it is global order

  auto domain = array_schema_->domain();

  if (layout == Layout::ROW_MAJOR) {
    parallel_sort(
        storage_manager_->compute_tp(), iter_begin, iter_end, RowCmp(domain));
  } else if (layout == Layout::COL_MAJOR) {
    parallel_sort(
        storage_manager_->compute_tp(), iter_begin, iter_end, ColCmp(domain));
  } else if (layout == Layout::GLOBAL_ORDER) {
    parallel_sort(
        storage_manager_->compute_tp(),
        iter_begin,
        iter_end,
        GlobalCmp(domain));
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

  get_result_tile_stats(result_tiles);
  get_result_cell_stats(result_cell_slabs);

  RETURN_NOT_OK(copy_coordinates(result_tiles, result_cell_slabs));
  RETURN_NOT_OK(
      copy_attribute_values(UINT64_MAX, result_tiles, result_cell_slabs));

  return Status::Ok();
}

Status Reader::copy_coordinates(
    const std::vector<ResultTile*>& result_tiles,
    const std::vector<ResultCellSlab>& result_cell_slabs) {
  STATS_START_TIMER(stats::Stats::TimerType::READ_COPY_COORDS);

  const uint64_t stride = UINT64_MAX;

  // Build a lists of coordinate names to copy, separating them by
  // whether they are of fixed or variable length. The motivation
  // is that copying fixed and variable cells require two different
  // context caches. Processing them separately allows us to maintain
  // a single context cache at the same time to reduce memory use.
  std::vector<std::string> fixed_names;
  std::vector<std::string> var_names;
  for (const auto& it : buffers_) {
    const auto& name = it.first;
    if (read_state_.overflowed_)
      break;
    if (!(name == constants::coords || array_schema_->is_dim(name)))
      continue;

    if (array_schema_->var_size(name))
      var_names.emplace_back(name);
    else
      fixed_names.emplace_back(name);
  }

  // Copy result cells for fixed-sized coordinates.
  if (!fixed_names.empty()) {
    CopyFixedCellsContextCache ctx_cache;
    for (const auto& name : fixed_names) {
      RETURN_CANCEL_OR_ERROR(
          copy_fixed_cells(name, stride, result_cell_slabs, &ctx_cache));
      clear_tiles(name, result_tiles);
    }
  }

  // Copy result cells for var-sized coordinates.
  if (!var_names.empty()) {
    CopyVarCellsContextCache ctx_cache;
    for (const auto& name : var_names) {
      RETURN_CANCEL_OR_ERROR(
          copy_var_cells(name, stride, result_cell_slabs, &ctx_cache));
      clear_tiles(name, result_tiles);
    }
  }

  return Status::Ok();

  STATS_END_TIMER(stats::Stats::TimerType::READ_COPY_COORDS);
}

Status Reader::copy_attribute_values(
    uint64_t stride,
    const std::vector<ResultTile*>& result_tiles,
    const std::vector<ResultCellSlab>& result_cell_slabs) {
  STATS_START_TIMER(stats::Stats::TimerType::READ_COPY_ATTR_VALUES);

  // Build an association from the result tile to the cell slab ranges
  // that it contains.
  std::unordered_map<ResultTile*, std::vector<std::pair<uint64_t, uint64_t>>>
      cs_ranges;
  std::vector<ResultCellSlab>::const_iterator it = result_cell_slabs.cbegin();
  while (it != result_cell_slabs.cend()) {
    std::pair<uint64_t, uint64_t> range =
        std::make_pair(it->start_, it->start_ + it->length_);
    if (cs_ranges.find(it->tile_) == cs_ranges.end()) {
      std::vector<std::pair<uint64_t, uint64_t>> ranges(1, std::move(range));
      cs_ranges.insert(std::make_pair(it->tile_, std::move(ranges)));
    } else {
      cs_ranges[it->tile_].emplace_back(std::move(range));
    }
    ++it;
  }

  // The result cell slab ranges must be sorted in ascending order for the
  // selective decompression intersection algorithm. For 1-dimensional arrays,
  // the result cell slab ranges are guaranteed to be sorted in ascending
  // order. For arrays with more than one dimension, we must sort them.
  if (array_schema_->dim_num() > 1) {
    struct RangeCompare {
      inline bool operator()(
          const std::pair<uint64_t, uint64_t>& a,
          const std::pair<uint64_t, uint64_t>& b) const {
        return a.first < b.first;
      }
    };

    // Sort each range, per tile.
    for (auto& kv : cs_ranges) {
      parallel_sort(
          storage_manager_->compute_tp(),
          kv.second.begin(),
          kv.second.end(),
          RangeCompare());
    }
  }

  // Build a list of attribute names to copy.
  std::vector<std::string> names;
  for (const auto& it : buffers_) {
    const auto& name = it.first;
    if (read_state_.overflowed_)
      break;
    if (name == constants::coords || array_schema_->is_dim(name))
      continue;

    names.emplace_back(name);
  }

  // Pre-load all attribute offsets into memory.
  load_offsets(names);

  // Read, unfilter, and copy all attributes into memory.
  CopyFixedCellsContextCache fixed_ctx_cache;
  CopyVarCellsContextCache var_ctx_cache;
  for (const auto& name : names) {
    RETURN_CANCEL_OR_ERROR(read_tiles(name, result_tiles));
    RETURN_CANCEL_OR_ERROR(unfilter_tiles(name, result_tiles, &cs_ranges));
    if (!array_schema_->var_size(name))
      RETURN_CANCEL_OR_ERROR(
          copy_fixed_cells(name, stride, result_cell_slabs, &fixed_ctx_cache));
    else
      RETURN_CANCEL_OR_ERROR(
          copy_var_cells(name, stride, result_cell_slabs, &var_ctx_cache));
    clear_tiles(name, result_tiles);
  }

  return Status::Ok();
  STATS_END_TIMER(stats::Stats::TimerType::READ_COPY_ATTR_VALUES);
}

void Reader::zero_out_buffer_sizes() {
  for (auto& buffer : buffers_) {
    if (buffer.second.buffer_size_ != nullptr)
      *(buffer.second.buffer_size_) = 0;
    if (buffer.second.buffer_var_size_ != nullptr)
      *(buffer.second.buffer_var_size_) = 0;
  }
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

void Reader::get_dim_attr_stats() const {
  for (const auto& it : buffers_) {
    const auto& name = it.first;
    auto var_size = array_schema_->var_size(name);
    if (array_schema_->is_attr(name)) {
      STATS_ADD_COUNTER(stats::Stats::CounterType::READ_ATTR_NUM, 1);
      if (var_size) {
        STATS_ADD_COUNTER(stats::Stats::CounterType::READ_ATTR_VAR_NUM, 1);
      } else {
        STATS_ADD_COUNTER(stats::Stats::CounterType::READ_ATTR_FIXED_NUM, 1);
      }
    } else {
      STATS_ADD_COUNTER(stats::Stats::CounterType::READ_DIM_NUM, 1);
      if (var_size) {
        STATS_ADD_COUNTER(stats::Stats::CounterType::READ_DIM_VAR_NUM, 1);
      } else {
        if (name == constants::coords) {
          STATS_ADD_COUNTER(stats::Stats::CounterType::READ_DIM_ZIPPED_NUM, 1);
        } else {
          STATS_ADD_COUNTER(stats::Stats::CounterType::READ_DIM_FIXED_NUM, 1);
        }
      }
    }
  }
}

void Reader::get_result_cell_stats(
    const std::vector<ResultCellSlab>& result_cell_slabs) const {
  uint64_t result_num = 0;
  for (const auto& rc : result_cell_slabs)
    result_num += rc.length_;
  STATS_ADD_COUNTER(stats::Stats::CounterType::READ_RESULT_NUM, result_num);
}

void Reader::get_result_tile_stats(
    const std::vector<ResultTile*>& result_tiles) const {
  STATS_ADD_COUNTER(
      stats::Stats::CounterType::READ_OVERLAP_TILE_NUM, result_tiles.size());

  uint64_t cell_num = 0;
  for (const auto& rt : result_tiles) {
    if (!fragment_metadata_[rt->frag_idx()]->dense())
      cell_num += rt->cell_num();
    else
      cell_num += array_schema_->domain()->cell_num_per_tile();
  }
  STATS_ADD_COUNTER(stats::Stats::CounterType::READ_CELL_NUM, cell_num);
}

Status Reader::set_est_result_size(
    std::unordered_map<std::string, Subarray::ResultSize>& est_result_size,
    std::unordered_map<std::string, Subarray::MemorySize>& max_mem_size) {
  return subarray_.set_est_result_size(est_result_size, max_mem_size);
}

std::unordered_map<std::string, Subarray::ResultSize>
Reader::get_est_result_size_map() {
  return subarray_.get_est_result_size_map(storage_manager_->compute_tp());
}

bool Reader::est_result_size_computed() {
  return subarray_.est_result_size_computed();
}

std::unordered_map<std::string, Subarray::MemorySize>
Reader::get_max_mem_size_map() {
  return subarray_.get_max_mem_size_map(storage_manager_->compute_tp());
}

}  // namespace sm
}  // namespace tiledb
