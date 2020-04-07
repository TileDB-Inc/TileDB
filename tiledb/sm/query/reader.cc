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
#include "tiledb/sm/misc/comparators.h"
#include "tiledb/sm/misc/logger.h"
#include "tiledb/sm/misc/parallel_functions.h"
#include "tiledb/sm/misc/stats.h"
#include "tiledb/sm/misc/utils.h"
#include "tiledb/sm/query/query_macros.h"
#include "tiledb/sm/query/read_cell_slab_iter.h"
#include "tiledb/sm/query/result_tile.h"
#include "tiledb/sm/storage_manager/storage_manager.h"
#include "tiledb/sm/subarray/cell_slab.h"
#include "tiledb/sm/tile/tile_io.h"

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

Status Reader::get_est_result_size(const char* name, uint64_t* size) {
  return subarray_.get_est_result_size(name, size);
}

Status Reader::get_est_result_size(
    const char* name, uint64_t* size_off, uint64_t* size_val) {
  return subarray_.get_est_result_size(name, size_off, size_val);
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
  STATS_FUNC_IN(reader_read);

  // Get next partition
  if (!read_state_.unsplittable_)
    RETURN_NOT_OK(read_state_.next());

  // Handle empty array or empty/finished subarray
  if (fragment_metadata_.empty()) {
    zero_out_buffer_sizes();
    return Status::Ok();
  }

  // Loop until you find results, or unsplittable, or done
  do {
    read_state_.overflowed_ = false;
    reset_buffer_sizes();

    // Perform read
    if (array_schema_->dense() && !sparse_mode_) {
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

  STATS_FUNC_OUT(reader_read);
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
  STATS_FUNC_IN(reader_compute_cell_ranges);

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

  STATS_FUNC_OUT(reader_compute_cell_ranges);
}

Status Reader::compute_range_result_coords(
    unsigned frag_idx,
    ResultTile* tile,
    const NDRange& ndrange,
    std::vector<ResultCoords>* result_coords) const {
  auto coords_num = tile->cell_num();
  auto fragment_num = fragment_metadata_.size();

  for (uint64_t pos = 0; pos < coords_num; ++pos) {
    // Check if the coordinates are in the range
    if (!tile->coord_in_rect(pos, ndrange))
      continue;

    // Check if the coordinates are overwritten by a future dense fragment
    bool overwritten = false;
    for (unsigned f = frag_idx + 1; !overwritten && f < fragment_num; ++f) {
      if (fragment_metadata_[f]->dense()) {
        overwritten =
            tile->coord_in_rect(pos, fragment_metadata_[f]->non_empty_domain());
        if (overwritten)
          break;
      }
    }

    if (!overwritten)
      result_coords->emplace_back(tile, pos);
  }

  return Status::Ok();
}

Status Reader::compute_range_result_coords(
    const std::vector<bool>& single_fragment,
    const std::map<std::pair<unsigned, uint64_t>, size_t>& result_tile_map,
    std::vector<ResultTile>* result_tiles,
    std::vector<std::vector<ResultCoords>>* range_result_coords) {
  auto range_num = read_state_.partitioner_.current().range_num();
  range_result_coords->resize(range_num);
  auto cell_order = array_schema_->cell_order();

  auto statuses = parallel_for(0, range_num, [&](uint64_t r) {
    // Compute overlapping coordinates per range
    RETURN_NOT_OK(compute_range_result_coords(
        r, result_tile_map, result_tiles, &((*range_result_coords)[r])));

    // Sort
    Layout layout =
        (layout_ == Layout::GLOBAL_ORDER || layout_ == Layout ::UNORDERED) ?
            cell_order :
            layout_;
    RETURN_CANCEL_OR_ERROR(
        sort_result_coords(&((*range_result_coords)[r]), layout));

    // Dedup unless there is a single fragment or array schema allows duplicates
    if (!single_fragment[r] && !array_schema_->allows_dups()) {
      RETURN_CANCEL_OR_ERROR(dedup_result_coords(&((*range_result_coords)[r])));
    }

    // Compute tile coordinate
    return Status::Ok();
  });

  for (auto st : statuses)
    RETURN_NOT_OK(st);

  return Status::Ok();
}

Status Reader::compute_range_result_coords(
    uint64_t range_idx,
    uint32_t fragment_idx,
    const std::map<std::pair<unsigned, uint64_t>, size_t>& result_tile_map,
    std::vector<ResultTile>* result_tiles,
    std::vector<ResultCoords>* range_result_coords) {
  const auto& subarray = read_state_.partitioner_.current();
  const auto& overlap = subarray.tile_overlap();

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
        auto ndrange = subarray.ndrange(range_idx);
        RETURN_NOT_OK(compute_range_result_coords(
            fragment_idx, &tile, ndrange, range_result_coords));
      }
      ++t;
    }
  }

  return Status::Ok();
}

Status Reader::compute_range_result_coords(
    uint64_t range_idx,
    const std::map<std::pair<unsigned, uint64_t>, size_t>& result_tile_map,
    std::vector<ResultTile>* result_tiles,
    std::vector<ResultCoords>* range_result_coords) {
  // Gather result range coordinates per fragment
  auto fragment_num = fragment_metadata_.size();
  std::vector<std::vector<ResultCoords>> range_result_coords_vec(fragment_num);
  auto statuses = parallel_for(0, fragment_num, [&](uint32_t f) {
    return compute_range_result_coords(
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

  RETURN_NOT_OK(sort_result_coords(result_coords, layout));

  return Status::Ok();
}

Status Reader::compute_sparse_result_tiles(
    std::vector<ResultTile>* result_tiles,
    std::map<std::pair<unsigned, uint64_t>, size_t>* result_tile_map,
    std::vector<bool>* single_fragment) {
  STATS_FUNC_IN(reader_compute_overlapping_tiles);

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
            if (f > first_fragment[r])
              (*single_fragment)[r] = false;
            else
              first_fragment[r] = f;
          }
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
          if (f > first_fragment[r])
            (*single_fragment)[r] = false;
          else
            first_fragment[r] = f;
        }
      }
    }
  }

  return Status::Ok();

  STATS_FUNC_OUT(reader_compute_overlapping_tiles);
}

Status Reader::copy_cells(
    const std::string& attribute,
    uint64_t stride,
    const std::vector<ResultCellSlab>& result_cell_slabs) {
  if (result_cell_slabs.empty()) {
    zero_out_buffer_sizes();
    return Status::Ok();
  }

  if (array_schema_->var_size(attribute))
    return copy_var_cells(attribute, stride, result_cell_slabs);
  return copy_fixed_cells(attribute, stride, result_cell_slabs);
}

Status Reader::copy_fixed_cells(
    const std::string& name,
    uint64_t stride,
    const std::vector<ResultCellSlab>& result_cell_slabs) {
  STATS_FUNC_IN(reader_copy_fixed_cells);

  // For easy reference
  auto it = buffers_.find(name);
  auto buffer = (unsigned char*)it->second.buffer_;
  auto buffer_size = it->second.buffer_size_;
  auto cell_size = array_schema_->cell_size(name);

  // Precompute the cell range destination offsets in the buffer
  auto num_cs = result_cell_slabs.size();
  uint64_t buffer_offset = 0;
  std::vector<uint64_t> cs_offsets(num_cs);
  for (uint64_t i = 0; i < num_cs; i++) {
    const auto& cs = result_cell_slabs[i];
    auto bytes_to_copy = cs.length_ * cell_size;
    cs_offsets[i] = buffer_offset;
    buffer_offset += bytes_to_copy;
  }

  // Handle overflow
  if (buffer_offset > *buffer_size) {
    read_state_.overflowed_ = true;
    return Status::Ok();
  }

  // Copy cell ranges in parallel.
  auto statuses = parallel_for(0, num_cs, [&](uint64_t i) {
    const auto& cs = result_cell_slabs[i];
    uint64_t offset = cs_offsets[i];
    // Check for overflow
    assert(offset + cs.length_ * cell_size <= *buffer_size);

    // Copy
    if (cs.tile_ == nullptr) {  // Empty range
      auto type = array_schema_->type(name);
      auto fill_size = datatype_size(type);
      auto fill_value = constants::fill_value(type);
      assert(fill_value != nullptr);
      auto bytes_to_copy = cs.length_ * cell_size;
      auto fill_num = bytes_to_copy / fill_size;
      for (uint64_t j = 0; j < fill_num; ++j) {
        std::memcpy(buffer + offset, fill_value, fill_size);
        offset += fill_size;
      }
    } else {  // Non-empty range
      if (stride == UINT64_MAX) {
        RETURN_NOT_OK(
            cs.tile_->read(name, buffer + offset, cs.start_, cs.length_));
      } else {
        auto cell_offset = offset;
        auto start = cs.start_;
        for (uint64_t j = 0; j < cs.length_; ++j) {
          RETURN_NOT_OK(cs.tile_->read(name, buffer + cell_offset, start, 1));
          cell_offset += cell_size;
          start += stride;
        }
      }
    }
    return Status::Ok();
  });

  for (auto st : statuses)
    RETURN_NOT_OK(st);

  // Update buffer offsets
  *(buffers_[name].buffer_size_) = buffer_offset;
  STATS_COUNTER_ADD(reader_num_fixed_cell_bytes_copied, buffer_offset);

  return Status::Ok();

  STATS_FUNC_OUT(reader_copy_fixed_cells);
}

Status Reader::copy_var_cells(
    const std::string& name,
    uint64_t stride,
    const std::vector<ResultCellSlab>& result_cell_slabs) {
  STATS_FUNC_IN(reader_copy_var_cells);

  // For easy reference
  auto it = buffers_.find(name);
  auto buffer = (unsigned char*)it->second.buffer_;
  auto buffer_var = (unsigned char*)it->second.buffer_var_;
  auto buffer_size = it->second.buffer_size_;
  auto buffer_var_size = it->second.buffer_var_size_;
  uint64_t offset_size = constants::cell_var_offset_size;
  auto type = array_schema_->type(name);
  auto fill_size = datatype_size(type);
  auto fill_value = constants::fill_value(type);
  assert(fill_value != nullptr);

  // Compute the destinations of offsets and var-len data in the buffers.
  std::vector<std::vector<uint64_t>> offset_offsets_per_cs;
  std::vector<std::vector<uint64_t>> var_offsets_per_cs;
  uint64_t total_offset_size, total_var_size;
  RETURN_NOT_OK(compute_var_cell_destinations(
      name,
      stride,
      result_cell_slabs,
      &offset_offsets_per_cs,
      &var_offsets_per_cs,
      &total_offset_size,
      &total_var_size));

  // Check for overflow and return early (without copying) in that case.
  if (total_offset_size > *buffer_size || total_var_size > *buffer_var_size) {
    read_state_.overflowed_ = true;
    return Status::Ok();
  }

  // Copy result cell slabs in parallel
  const auto num_cs = result_cell_slabs.size();
  auto statuses = parallel_for(0, num_cs, [&](uint64_t cs_idx) {
    const auto& cs = result_cell_slabs[cs_idx];
    const auto& offset_offsets = offset_offsets_per_cs[cs_idx];
    const auto& var_offsets = var_offsets_per_cs[cs_idx];

    // Get tile information, if the range is nonempty.
    std::vector<uint64_t> tile_offsets;
    Tile* tile_var = nullptr;
    uint64_t tile_cell_num = 0;
    if (cs.tile_ != nullptr) {
      std::pair<tiledb::sm::Tile, tiledb::sm::Tile>* const tile_pair =
          cs.tile_->tile_pair(name);
      const auto& tile = &tile_pair->first;
      tile_var = &tile_pair->second;
      tile_cell_num = tile->cell_num();

      // Build the tile offsets.
      tile_offsets.reserve(tile_cell_num);
      uint64_t tile_offset;
      uint64_t read_offset = 0;
      for (size_t cell_idx = 0; cell_idx < tile_cell_num; ++cell_idx) {
        RETURN_NOT_OK(tile->read(&tile_offset, sizeof(uint64_t), read_offset));
        read_offset += sizeof(uint64_t);
        tile_offsets.emplace_back(tile_offset);
      }
    }

    // Copy each cell in the range
    uint64_t dest_vec_idx = 0;
    stride = (stride == UINT64_MAX) ? 1 : stride;
    for (auto cell_idx = cs.start_; dest_vec_idx < cs.length_;
         cell_idx += stride, dest_vec_idx++) {
      auto offset_dest = buffer + offset_offsets[dest_vec_idx];
      auto var_offset = var_offsets[dest_vec_idx];
      auto var_dest = buffer_var + var_offset;

      // Copy offset
      std::memcpy(offset_dest, &var_offset, offset_size);

      // Copy variable-sized value
      if (cs.tile_ == nullptr) {
        std::memcpy(var_dest, &fill_value, fill_size);
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

    return Status::Ok();
  });

  // Check all statuses
  for (auto st : statuses)
    RETURN_NOT_OK(st);

  // Update buffer offsets
  *(buffers_[name].buffer_size_) = total_offset_size;
  *(buffers_[name].buffer_var_size_) = total_var_size;
  STATS_COUNTER_ADD(
      reader_num_var_cell_bytes_copied, total_offset_size + total_var_size);

  return Status::Ok();

  STATS_FUNC_OUT(reader_copy_var_cells);
}

Status Reader::compute_var_cell_destinations(
    const std::string& name,
    uint64_t stride,
    const std::vector<ResultCellSlab>& result_cell_slabs,
    std::vector<std::vector<uint64_t>>* offset_offsets_per_cs,
    std::vector<std::vector<uint64_t>>* var_offsets_per_cs,
    uint64_t* total_offset_size,
    uint64_t* total_var_size) const {
  // For easy reference
  auto num_cs = result_cell_slabs.size();
  auto offset_size = constants::cell_var_offset_size;
  auto type = array_schema_->type(name);
  auto fill_size = datatype_size(type);

  // Resize the output vectors
  offset_offsets_per_cs->resize(num_cs);
  var_offsets_per_cs->resize(num_cs);

  // Compute the destinations for all result cell slabs
  *total_offset_size = 0;
  *total_var_size = 0;
  for (uint64_t cs_idx = 0; cs_idx < num_cs; cs_idx++) {
    const auto& cs = result_cell_slabs[cs_idx];
    (*offset_offsets_per_cs)[cs_idx].resize(cs.length_);
    (*var_offsets_per_cs)[cs_idx].resize(cs.length_);

    // Get tile information, if the range is nonempty.
    std::vector<uint64_t> tile_offsets;
    uint64_t tile_cell_num = 0;
    uint64_t tile_var_size = 0;
    if (cs.tile_ != nullptr) {
      std::pair<tiledb::sm::Tile, tiledb::sm::Tile>* const tile_pair =
          cs.tile_->tile_pair(name);
      const auto& tile = &tile_pair->first;
      const auto& tile_var = &tile_pair->second;
      tile_cell_num = tile->cell_num();
      tile_var_size = tile_var->size();

      // Build the tile offsets.
      tile_offsets.reserve(tile_cell_num);
      uint64_t tile_offset;
      uint64_t read_offset = 0;
      for (size_t cell_idx = 0; cell_idx < tile_cell_num; ++cell_idx) {
        RETURN_NOT_OK(tile->read(&tile_offset, sizeof(uint64_t), read_offset));
        read_offset += sizeof(uint64_t);
        tile_offsets.emplace_back(tile_offset);
      }
    }

    // Compute the destinations for each cell in the range.
    uint64_t dest_vec_idx = 0;
    stride = (stride == UINT64_MAX) ? 1 : stride;
    for (auto cell_idx = cs.start_; dest_vec_idx < cs.length_;
         cell_idx += stride, dest_vec_idx++) {
      // Get size of variable-sized cell
      uint64_t cell_var_size = 0;
      if (cs.tile_ == nullptr) {
        cell_var_size = fill_size;
      } else {
        cell_var_size =
            (cell_idx != tile_cell_num - 1) ?
                tile_offsets[cell_idx + 1] - tile_offsets[cell_idx] :
                tile_var_size - (tile_offsets[cell_idx] - tile_offsets[0]);
      }

      // Record destination offsets.
      (*offset_offsets_per_cs)[cs_idx][dest_vec_idx] = *total_offset_size;
      (*var_offsets_per_cs)[cs_idx][dest_vec_idx] = *total_var_size;
      *total_offset_size += offset_size;
      *total_var_size += cell_var_size;
    }
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
void Reader::compute_result_cell_slabs(
    const Subarray& subarray,
    std::map<const T*, ResultSpaceTile<T>>* result_space_tiles,
    std::vector<ResultCoords>* result_coords,
    std::vector<ResultTile*>* result_tiles,
    std::vector<ResultCellSlab>* result_cell_slabs) const {
  auto layout = subarray.layout();
  if (layout == Layout::ROW_MAJOR || layout == Layout::COL_MAJOR) {
    uint64_t result_coords_pos = 0;
    std::set<std::pair<unsigned, uint64_t>> frag_tile_set;
    compute_result_cell_slabs_row_col<T>(
        subarray,
        result_space_tiles,
        result_coords,
        &result_coords_pos,
        result_tiles,
        &frag_tile_set,
        result_cell_slabs);
  } else if (layout == Layout::GLOBAL_ORDER) {
    compute_result_cell_slabs_global<T>(
        subarray,
        result_space_tiles,
        result_coords,
        result_tiles,
        result_cell_slabs);
  } else {  // UNORDERED
    assert(false);
  }
}

template <class T>
void Reader::compute_result_cell_slabs_row_col(
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
}

template <class T>
void Reader::compute_result_cell_slabs_global(
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

    compute_result_cell_slabs_row_col<T>(
        tile_subarray,
        result_space_tiles,
        result_coords,
        &result_coords_pos,
        result_tiles,
        &frag_tile_set,
        result_cell_slabs);
  }
}

Status Reader::compute_result_coords(
    std::vector<ResultTile>* result_tiles,
    std::vector<ResultCoords>* result_coords) {
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

  // Read and unfilter coordinate tiles
  // NOTE: these will ignore tiles of fragments with format version >=5
  RETURN_CANCEL_OR_ERROR(read_tiles(constants::coords, tmp_result_tiles));
  RETURN_CANCEL_OR_ERROR(unfilter_tiles(constants::coords, tmp_result_tiles));

  // Read and unfilter coordinate tiles
  // NOTE: these will ignore tiles of fragments with format version <5
  auto dim_num = array_schema_->dim_num();
  for (unsigned d = 0; d < dim_num; ++d) {
    const auto& dim_name = array_schema_->dimension(d)->name();
    RETURN_CANCEL_OR_ERROR(read_tiles(dim_name, tmp_result_tiles));
    RETURN_CANCEL_OR_ERROR(unfilter_tiles(dim_name, tmp_result_tiles));
  }

  // Compute the read coordinates for all fragments for each subarray range
  std::vector<std::vector<ResultCoords>> range_result_coords;
  RETURN_CANCEL_OR_ERROR(compute_range_result_coords(
      single_fragment, result_tile_map, result_tiles, &range_result_coords));
  result_tile_map.clear();

  // Compute final coords (sorted in the result layout) of the whole subarray.
  RETURN_CANCEL_OR_ERROR(
      compute_subarray_coords(&range_result_coords, result_coords));
  range_result_coords.clear();

  return Status::Ok();
}

Status Reader::dedup_result_coords(
    std::vector<ResultCoords>* result_coords) const {
  STATS_FUNC_IN(reader_dedup_coords);

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

  STATS_FUNC_OUT(reader_dedup_coords);
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
  STATS_FUNC_IN(reader_dense_read);

  // Sanity checks
  assert(std::is_integral<T>::value);
  assert(!fragment_metadata_.empty());

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

  subarray.compute_tile_coords<T>();
  compute_result_cell_slabs<T>(
      subarray,
      &result_space_tiles,
      &result_coords,
      &result_tiles,
      &result_cell_slabs);

  // Clear sparse coordinate tiles (not needed any more)
  erase_coord_tiles(&sparse_result_tiles);

  // Needed when copying the cells
  auto stride = array_schema_->domain()->stride<T>(subarray.layout());

  // Copy cells
  for (const auto& it : buffers_) {
    const auto& name = it.first;
    if (read_state_.overflowed_)
      break;
    if (name == constants::coords || array_schema_->is_dim(name))
      continue;

    RETURN_CANCEL_OR_ERROR(read_tiles(name, result_tiles));
    RETURN_CANCEL_OR_ERROR(
        unfilter_tiles(name, result_tiles, &result_cell_slabs));
    RETURN_CANCEL_OR_ERROR(copy_cells(name, stride, result_cell_slabs));
    clear_tiles(name, result_tiles);
  }

  // Fill coordinates if the user requested them
  if (!read_state_.overflowed_ && has_coords())
    RETURN_CANCEL_OR_ERROR(fill_dense_coords<T>(subarray));

  return Status::Ok();

  STATS_FUNC_OUT(reader_dense_read);
}

template <class T>
Status Reader::fill_dense_coords(const Subarray& subarray) {
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
  STATS_FUNC_IN(reader_fill_coords);

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

  STATS_FUNC_OUT(reader_fill_coords);
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
    const std::vector<ResultCellSlab>* const result_cell_slabs) const {
  STATS_FUNC_IN(reader_unfilter_tiles);

  auto var_size = array_schema_->var_size(name);
  auto num_tiles = static_cast<uint64_t>(result_tiles.size());
  auto encryption_key = array_->encryption_key();

  // Build an association from the result tile to the cell slab ranges
  // that it contains.
  std::unordered_map<
      ResultTile*,
      std::forward_list<std::pair<uint64_t, uint64_t>>>
      cs_ranges;

  // If 'result_cell_slabs' is NULL, the caller intends to bypass
  // selective unfiltering.
  if (result_cell_slabs) {
    std::vector<ResultCellSlab>::const_reverse_iterator rit =
        result_cell_slabs->rbegin();
    while (rit != result_cell_slabs->rend()) {
      std::pair<uint64_t, uint64_t> range =
          std::make_pair(rit->start_, rit->start_ + rit->length_);
      if (cs_ranges.find(rit->tile_) == cs_ranges.end()) {
        std::forward_list<std::pair<uint64_t, uint64_t>> ranges(
            1, std::move(range));
        cs_ranges.insert(std::make_pair(rit->tile_, std::move(ranges)));
      } else {
        cs_ranges[rit->tile_].emplace_front(std::move(range));
      }
      ++rit;
    }
  }

  auto statuses = parallel_for(0, num_tiles, [&, this](uint64_t i) {
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
      const std::forward_list<std::pair<uint64_t, uint64_t>>*
          result_cell_slab_ranges = nullptr;
      static const std::forward_list<std::pair<uint64_t, uint64_t>>
          empty_ranges;
      if (result_cell_slabs) {
        result_cell_slab_ranges = cs_ranges.find(tile) != cs_ranges.end() ?
                                      &cs_ranges[tile] :
                                      &empty_ranges;
      }

      if (t.filtered()) {
        // Store the filtered buffer in the tile cache.
        RETURN_NOT_OK(storage_manager_->write_to_cache(
            tile_attr_uri, tile_attr_offset, t.filtered_buffer()));

        // Unfilter the tile buffer within the 't' instance.
        RETURN_NOT_OK(
            unfilter_tile(name, &t, var_size, result_cell_slab_ranges));
      }

      if (var_size && t_var.filtered()) {
        auto tile_attr_var_uri = fragment->var_uri(name);
        uint64_t tile_attr_var_offset;
        RETURN_NOT_OK(fragment->file_var_offset(
            *encryption_key, name, tile_idx, &tile_attr_var_offset));

        // Store the filtered buffer in the tile cache.
        RETURN_NOT_OK(storage_manager_->write_to_cache(
            tile_attr_var_uri, tile_attr_var_offset, t_var.filtered_buffer()));

        // Unfilter the tile buffer within the 't_var' instance.
        RETURN_NOT_OK(
            unfilter_tile(name, &t_var, false, result_cell_slab_ranges));
      }
    }

    return Status::Ok();
  });

  for (const auto& st : statuses)
    RETURN_CANCEL_OR_ERROR(st);

  return Status::Ok();

  STATS_FUNC_OUT(reader_unfilter_tiles);
}

Status Reader::unfilter_tile(
    const std::string& name,
    Tile* tile,
    bool offsets,
    const std::forward_list<std::pair<uint64_t, uint64_t>>* const
        result_cell_slab_ranges) const {
  // Get a copy of the appropriate filter pipeline.
  FilterPipeline filters =
      (offsets ? array_schema_->cell_var_offsets_filters() :
                 array_schema_->filters(name));

  // Append an encryption unfilter when necessary.
  RETURN_NOT_OK(FilterPipeline::append_encryption_filter(
      &filters, array_->get_encryption_key()));

  RETURN_NOT_OK(filters.run_reverse(
      tile, storage_manager_->config(), result_cell_slab_ranges));

  STATS_COUNTER_ADD(reader_num_bytes_after_unfiltering, tile->size());

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
  read_state_.partitioner_ =
      SubarrayPartitioner(subarray_, memory_budget, memory_budget_var);
  read_state_.overflowed_ = false;
  read_state_.unsplittable_ = false;

  // Set result size budget
  for (const auto& a : buffers_) {
    auto attr_name = a.first;
    auto buffer_size = a.second.buffer_size_;
    auto buffer_var_size = a.second.buffer_var_size_;
    if (!array_schema_->var_size(a.first)) {
      RETURN_NOT_OK(read_state_.partitioner_.set_result_budget(
          attr_name.c_str(), *buffer_size));
    } else {
      RETURN_NOT_OK(read_state_.partitioner_.set_result_budget(
          attr_name.c_str(), *buffer_size, *buffer_var_size));
    }
  }

  // Set memory budget
  RETURN_NOT_OK(read_state_.partitioner_.set_memory_budget(
      memory_budget_, memory_budget_var_));

  read_state_.unsplittable_ = false;
  read_state_.overflowed_ = false;
  read_state_.initialized_ = true;

  return Status::Ok();
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

Status Reader::read_tiles(
    const std::string& name,
    const std::vector<ResultTile*>& result_tiles) const {
  // Shortcut for empty tile vec
  if (result_tiles.empty())
    return Status::Ok();

  // Read the tiles asynchronously
  std::vector<std::future<Status>> tasks;
  RETURN_CANCEL_OR_ERROR(read_tiles(name, result_tiles, &tasks));

  // Wait for the reads to finish and check statuses.
  auto statuses =
      storage_manager_->reader_thread_pool()->wait_all_status(tasks);
  for (const auto& st : statuses)
    RETURN_CANCEL_OR_ERROR(st);

  return Status::Ok();
}

Status Reader::read_tiles(
    const std::string& name,
    const std::vector<ResultTile*>& result_tiles,
    std::vector<std::future<Status>>* const tasks) const {
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

  // In parallel, force the required fragment metadata to be loaded
  auto statuses = parallel_for(0, fragment_idxs_vec.size(), [&](uint64_t i) {
    auto& fragment = fragment_metadata_[fragment_idxs_vec[i]];
    auto format_version = fragment->format_version();

    // Applicable for zipped coordinates only to versions < 5
    if (name == constants::coords && format_version >= 5)
      return Status::Ok();

    // Applicable to separate coordinates only to versions >= 5
    auto is_dim = array_schema_->is_dim(name);
    if (is_dim && format_version < 5)
      return Status::Ok();

    uint64_t tmp = 0;
    RETURN_NOT_OK(fragment->file_offset(*encryption_key, name, 0, &tmp));
    return Status::Ok();
  });
  for (const auto& st : statuses)
    RETURN_NOT_OK(st);

  if (var_size) {
    auto statuses = parallel_for(0, fragment_idxs_vec.size(), [&](uint64_t i) {
      auto& fragment = fragment_metadata_[fragment_idxs_vec[i]];
      auto format_version = fragment->format_version();

      // Applicable for zipped coordinates only to versions < 5
      if (name == constants::coords && format_version >= 5)
        return Status::Ok();

      // Applicable to separate coordinates only to versions >= 5
      auto is_dim = array_schema_->is_dim(name);
      if (is_dim && format_version < 5)
        return Status::Ok();

      uint64_t tmp = 0;
      RETURN_NOT_OK(fragment->file_var_offset(*encryption_key, name, 0, &tmp));
      return Status::Ok();
    });
    for (auto st : statuses)
      RETURN_NOT_OK(st);
  }

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
    if (cache_hit) {
      STATS_COUNTER_ADD(reader_attr_tile_cache_hits, 1);
    } else {
      // Add the region of the fragment to be read.
      RETURN_NOT_OK(t->filtered_buffer()->realloc(tile_persisted_size));
      t->filtered_buffer()->set_size(tile_persisted_size);
      t->filtered_buffer()->reset_offset();
      all_regions[tile_attr_uri].emplace_back(
          tile_attr_offset, t->filtered_buffer()->data(), tile_persisted_size);

      STATS_COUNTER_ADD(reader_num_tile_bytes_read, tile_persisted_size);
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

      if (cache_hit) {
        STATS_COUNTER_ADD(reader_attr_tile_cache_hits, 1);
      } else {
        // Add the region of the fragment to be read.
        RETURN_NOT_OK(
            t_var->filtered_buffer()->realloc(tile_var_persisted_size));
        t_var->filtered_buffer()->set_size(tile_var_persisted_size);
        t_var->filtered_buffer()->reset_offset();
        all_regions[tile_attr_var_uri].emplace_back(
            tile_attr_var_offset,
            t_var->filtered_buffer()->data(),
            tile_var_persisted_size);

        STATS_COUNTER_ADD(reader_num_tile_bytes_read, tile_var_persisted_size);
        STATS_COUNTER_ADD(
            reader_num_var_cell_bytes_read, tile_var_persisted_size);
        STATS_COUNTER_ADD(
            reader_num_var_cell_bytes_read, tile_var_persisted_size);
      }
    } else {
      STATS_COUNTER_ADD_IF(
          !cache_hit, reader_num_fixed_cell_bytes_read, tile_persisted_size);
    }
  }

  // Enqueue all regions to be read.
  for (const auto& item : all_regions) {
    RETURN_NOT_OK(storage_manager_->vfs()->read_all(
        item.first,
        item.second,
        storage_manager_->reader_thread_pool(),
        tasks));
  }

  STATS_COUNTER_ADD(
      reader_num_attr_tiles_touched,
      ((var_size ? 2 : 1) * result_tiles.size()));

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
    std::vector<ResultCoords>* result_coords, Layout layout) const {
  STATS_FUNC_IN(reader_sort_coords);

  // TODO: do not sort if it is single fragment and
  // (i) it is single dimension, or (ii) it is global order

  auto domain = array_schema_->domain();

  if (layout == Layout::ROW_MAJOR) {
    parallel_sort(result_coords->begin(), result_coords->end(), RowCmp(domain));
  } else if (layout == Layout::COL_MAJOR) {
    parallel_sort(result_coords->begin(), result_coords->end(), ColCmp(domain));
  } else if (layout == Layout::GLOBAL_ORDER) {
    parallel_sort(
        result_coords->begin(), result_coords->end(), GlobalCmp(domain));
  } else {
    assert(false);
  }

  return Status::Ok();

  STATS_FUNC_OUT(reader_sort_coords);
}

Status Reader::sparse_read() {
  STATS_FUNC_IN(reader_sparse_read);

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

  uint64_t stride = UINT64_MAX;

  // Copy zipped coordinates
  if (buffers_.find(constants::coords) != buffers_.end())
    RETURN_CANCEL_OR_ERROR(
        copy_cells(constants::coords, stride, result_cell_slabs));

  // Clear sparse coordinate tiles (not needed any more)
  erase_coord_tiles(&sparse_result_tiles);

  // Copy cells
  for (const auto& it : buffers_) {
    const auto& name = it.first;
    if (read_state_.overflowed_)
      break;
    if (name == constants::coords)
      continue;

    RETURN_CANCEL_OR_ERROR(read_tiles(name, result_tiles));
    RETURN_CANCEL_OR_ERROR(
        unfilter_tiles(name, result_tiles, &result_cell_slabs));
    RETURN_CANCEL_OR_ERROR(copy_cells(name, stride, result_cell_slabs));
    clear_tiles(name, result_tiles);
  }

  return Status::Ok();

  STATS_FUNC_OUT(reader_sparse_read);
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

}  // namespace sm
}  // namespace tiledb
