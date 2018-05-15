/**
 * @file   reader.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2018 TileDB, Inc.
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
#include "tiledb/sm/misc/comparators.h"
#include "tiledb/sm/misc/logger.h"
#include "tiledb/sm/misc/parallel_functions.h"
#include "tiledb/sm/misc/utils.h"
#include "tiledb/sm/query/query_macros.h"
#include "tiledb/sm/storage_manager/storage_manager.h"
#include "tiledb/sm/tile/tile_io.h"

#include <iostream>

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
  array_schema_ = nullptr;
  layout_ = Layout::ROW_MAJOR;
  read_state_.subarray_ = nullptr;
  storage_manager_ = nullptr;
}

Reader::~Reader() {
  clear_read_state();
}

/* ****************************** */
/*               API              */
/* ****************************** */

const ArraySchema* Reader::array_schema() const {
  return array_schema_;
}

Status Reader::compute_subarray_partitions(
    void* subarray, std::vector<void*>* subarray_partitions) const {
  // Prepare subarray
  auto domain = array_schema_->domain();
  uint64_t subarray_size = 2 * array_schema_->coords_size();
  void* first_subarray = std::malloc(subarray_size);
  if (first_subarray == nullptr)
    return LOG_STATUS(Status::ReaderError(
        "Cannot compute subarrays; Failed to allocate memory"));
  auto to_copy = (subarray != nullptr) ? subarray : domain->domain();
  std::memcpy(first_subarray, to_copy, subarray_size);

  // Prepare buffer sizes
  std::unordered_map<std::string, std::pair<uint64_t, uint64_t>>
      max_buffer_sizes;

  // Compute subarrays
  std::list<void*> my_subarrays;
  my_subarrays.emplace_back(first_subarray);
  auto it = my_subarrays.begin();
  Status st = Status::Ok();
  do {
    // Compute max buffer sizes for the current subarray
    for (const auto& attr : attributes_)
      max_buffer_sizes[attr] = std::pair<uint64_t, uint64_t>(0, 0);
    st = storage_manager_->array_compute_max_read_buffer_sizes(
        array_schema_, fragment_metadata_, *it, &max_buffer_sizes);
    if (!st.ok())
      break;

    // Handle case of no results
    auto no_results = true;
    for (auto& item : max_buffer_sizes) {
      if (item.second.first != 0) {
        no_results = false;
        break;
      }
    }
    if (no_results) {
      std::free(*it);
      it = my_subarrays.erase(it);
      continue;
    }

    // Handle case of split
    auto must_split = false;
    for (auto& item : max_buffer_sizes) {
      auto buffer_size = attr_buffers_.find(item.first)->second.buffer_size_;
      auto buffer_var_size =
          attr_buffers_.find(item.first)->second.buffer_var_size_;
      auto var_size = array_schema_->var_size(item.first);

      if (item.second.first > *buffer_size ||
          (var_size && item.second.second > *buffer_var_size)) {
        must_split = true;
        break;
      }
    }
    if (must_split) {
      void *subarray_1 = nullptr, *subarray_2 = nullptr;
      st = domain->split_subarray(*it, layout_, &subarray_1, &subarray_2);
      if (!st.ok())
        break;

      // Not splittable, return the original subarray as result
      if (subarray_1 == nullptr || subarray_2 == nullptr) {
        for (auto s : my_subarrays)
          std::free(s);
        return Status::Ok();
      }
      my_subarrays.insert(std::next(it), subarray_2);
      my_subarrays.insert(std::next(it), subarray_1);
      it = my_subarrays.erase(it);
      continue;
    }

    ++it;
  } while (it != my_subarrays.end());

  // Clean up upon error
  if (!st.ok()) {
    for (auto s : my_subarrays)
      std::free(s);
    return st;
  }

  // Prepare the result
  for (auto s : my_subarrays)
    subarray_partitions->emplace_back(s);

  return Status::Ok();
}

bool Reader::done() const {
  return read_state_.idx_ >= read_state_.subarray_partitions_.size();
}

Status Reader::finalize() {
  clear_read_state();
  return Status::Ok();
}

unsigned Reader::fragment_num() const {
  return (unsigned int)fragment_metadata_.size();
}

std::vector<URI> Reader::fragment_uris() const {
  std::vector<URI> uris;
  for (auto meta : fragment_metadata_)
    uris.emplace_back(meta->fragment_uri());

  return uris;
}

Status Reader::init() {
  // Sanity checks
  if (storage_manager_ == nullptr)
    return LOG_STATUS(Status::ReaderError(
        "Cannot initialize query; Storage manager not set"));
  if (array_schema_ == nullptr)
    return LOG_STATUS(
        Status::ReaderError("Cannot initialize query; Array metadata not set"));
  if (attr_buffers_.empty())
    return LOG_STATUS(
        Status::ReaderError("Cannot initialize query; Buffers not set"));
  if (attributes_.empty())
    return LOG_STATUS(
        Status::ReaderError("Cannot initialize query; Attributes not set"));

  if (read_state_.subarray_ == nullptr)
    RETURN_NOT_OK(set_subarray(nullptr));

  RETURN_NOT_OK(compute_subarray_partitions(
      read_state_.subarray_, &read_state_.subarray_partitions_));
  if (read_state_.subarray_partitions_.empty())
    read_state_.subarray_partitions_.push_back(read_state_.subarray_);

  read_state_.idx_ = 0;
  cur_subarray_ = read_state_.subarray_partitions_[0];
  assert(cur_subarray_ != nullptr);

  return Status::Ok();
}

URI Reader::last_fragment_uri() const {
  if (fragment_metadata_.empty())
    return URI();
  return fragment_metadata_.back()->fragment_uri();
}

Layout Reader::layout() const {
  return layout_;
}

void Reader::next_subarray_partition() {
  if (read_state_.idx_ >= read_state_.subarray_partitions_.size())
    return;

  ++read_state_.idx_;
  if (read_state_.idx_ >= read_state_.subarray_partitions_.size())
    return;

  cur_subarray_ = read_state_.subarray_partitions_[read_state_.idx_];
}

Status Reader::read() {
  // Handle case of no fragments
  if (fragment_metadata_.empty()) {
    zero_out_buffer_sizes();
    return Status::Ok();
  }

  // Perform dense or sparse read
  if (array_schema_->dense())
    return dense_read();
  return sparse_read();
}

void Reader::set_array_schema(const ArraySchema* array_schema) {
  array_schema_ = array_schema;
  if (array_schema->is_kv())
    layout_ = Layout::GLOBAL_ORDER;
}

Status Reader::set_buffers(
    const char** attributes,
    unsigned int attribute_num,
    void** buffers,
    uint64_t* buffer_sizes) {
  if (buffers == nullptr || buffer_sizes == nullptr)
    return LOG_STATUS(
        Status::ReaderError("Cannot set buffers; Buffers not provided"));

  if (array_schema_ == nullptr)
    return LOG_STATUS(
        Status::ReaderError("Cannot set buffers; Array schema not set"));

  RETURN_NOT_OK(set_attributes(attributes, attribute_num));
  set_buffers(buffers, buffer_sizes);

  return Status::Ok();
}

Status Reader::set_buffers(void** buffers, uint64_t* buffer_sizes) {
  if (buffers == nullptr || buffer_sizes == nullptr)
    return LOG_STATUS(
        Status::ReaderError("Cannot set buffers; Buffers not provided"));

  if (array_schema_ == nullptr)
    return LOG_STATUS(
        Status::ReaderError("Cannot set buffers; Array schema not set"));

  // Necessary check in case this is a reset
  if (!attr_buffers_.empty())
    RETURN_NOT_OK(check_reset_buffer_sizes(buffer_sizes));

  // Reset attribute buffers
  attr_buffers_.clear();
  unsigned bid = 0;
  for (const auto& attr : attributes_) {
    if (!array_schema_->var_size(attr)) {
      attr_buffers_.emplace(
          attr,
          AttributeBuffer(buffers[bid], nullptr, &buffer_sizes[bid], nullptr));
      ++bid;
    } else {
      attr_buffers_.emplace(
          attr,
          AttributeBuffer(
              buffers[bid],
              buffers[bid + 1],
              &buffer_sizes[bid],
              &buffer_sizes[bid + 1]));
      bid += 2;
    }
  }

  return Status::Ok();
}

void Reader::set_fragment_metadata(
    const std::vector<FragmentMetadata*>& fragment_metadata) {
  fragment_metadata_ = fragment_metadata;
}

Status Reader::set_layout(Layout layout) {
  // Check if the array is a key-value store
  if (array_schema_->is_kv())
    return LOG_STATUS(Status::ReaderError(
        "Cannot set layout; The array is defined as a key-value store"));

  layout_ = layout;

  return Status::Ok();
}

void Reader::set_storage_manager(StorageManager* storage_manager) {
  storage_manager_ = storage_manager;
}

Status Reader::set_subarray(const void* subarray) {
  if (read_state_.subarray_ != nullptr)
    clear_read_state();

  auto subarray_size = 2 * array_schema_->coords_size();
  read_state_.subarray_ = std::malloc(subarray_size);
  if (read_state_.subarray_ == nullptr)
    return LOG_STATUS(Status::ReaderError(
        "Memory allocation for read state subarray failed"));

  if (subarray != nullptr)
    std::memcpy(read_state_.subarray_, subarray, subarray_size);
  else
    std::memcpy(
        read_state_.subarray_,
        array_schema_->domain()->domain(),
        subarray_size);

  return Status::Ok();
}

/* ****************************** */
/*          PRIVATE METHODS       */
/* ****************************** */

Status Reader::check_reset_buffer_sizes(const uint64_t* buffer_sizes) const {
  unsigned bid = 0;
  for (const auto& attr : attributes_) {
    auto attr_buffer_it = attr_buffers_.find(attr);
    assert(attr_buffer_it != attr_buffers_.end());
    if (array_schema_->var_size(attr)) {
      if (buffer_sizes[bid] < *(attr_buffer_it->second.buffer_size_) ||
          buffer_sizes[bid + 1] < (*attr_buffer_it->second.buffer_var_size_))
        return LOG_STATUS(Status::ReaderError(
            "Cannot reset buffers; New buffer sizes are smaller than the ones "
            "set upon initialization"));
      bid += 2;
    } else {
      if (buffer_sizes[bid] < *(attr_buffer_it->second.buffer_size_))
        return LOG_STATUS(Status::ReaderError(
            "Cannot reset buffers; New buffer sizes are smaller than the ones "
            "set upon initialization"));
      ++bid;
    }
  }

  return Status::Ok();
}

void Reader::clear_read_state() {
  for (auto p : read_state_.subarray_partitions_) {
    if (p != nullptr && p != read_state_.subarray_)
      std::free(p);
  }
  read_state_.subarray_partitions_.clear();

  if (read_state_.subarray_ != nullptr) {
    std::free(read_state_.subarray_);
    read_state_.subarray_ = nullptr;
  }
}

template <class T>
Status Reader::compute_cell_ranges(
    const OverlappingCoordsList<T>& coords,
    OverlappingCellRangeList* cell_ranges) const {
  // Trivial case
  auto coords_num = (uint64_t)coords.size();
  if (coords_num == 0)
    return Status::Ok();

  // Initialize the first range
  auto coords_end = coords.end();
  auto it = skip_invalid_elements(coords.begin(), coords_end);
  if (it == coords_end) {
    return LOG_STATUS(Status::ReaderError("Unexpected empty cell range."));
  }
  uint64_t start_pos = it->pos_;
  uint64_t end_pos = start_pos;
  auto tile = it->tile_;

  // Scan the coordinates and compute ranges
  it = skip_invalid_elements(++it, coords_end);
  while (it != coords_end) {
    if (it->tile_ == tile && it->pos_ == end_pos + 1) {
      // Same range - advance end position
      end_pos = it->pos_;
    } else {
      // New range - append previous range
      cell_ranges->push_back(std::unique_ptr<OverlappingCellRange>(
          new OverlappingCellRange(tile, start_pos, end_pos)));
      start_pos = it->pos_;
      end_pos = start_pos;
      tile = it->tile_;
    }
    it = skip_invalid_elements(++it, coords_end);
  }

  // Append the last range
  cell_ranges->push_back(std::unique_ptr<OverlappingCellRange>(
      new OverlappingCellRange(tile, start_pos, end_pos)));

  return Status::Ok();
}

template <class T>
Status Reader::compute_dense_cell_ranges(
    const T* tile_coords,
    std::vector<DenseCellRangeIter<T>>& frag_its,
    uint64_t start,
    uint64_t end,
    std::list<DenseCellRange<T>>* dense_cell_ranges) {
  // NOTE: `start` will always get updated as results are inserted
  // in `dense_cell_ranges`.

  // For easy reference
  auto fragment_num = fragment_metadata_.size();

  // Populate queue - stores pairs of (start, fragment_num-fragment_id)
  std::priority_queue<
      DenseCellRange<T>,
      std::vector<DenseCellRange<T>>,
      DenseCellRangeCmp<T>>
      pq;
  for (unsigned i = 0; i < fragment_num; ++i) {
    if (!frag_its[i].end())
      pq.emplace(
          i, tile_coords, frag_its[i].range_start(), frag_its[i].range_end());
  }

  // Iterate over the queue and create dense cell ranges
  while (!pq.empty()) {
    // Get top range
    const auto& top = pq.top();

    // Top must be ignored and a new range must be fetched
    if (top.end_ < start) {
      auto fidx = top.fragment_idx_;
      ++frag_its[fidx];
      pq.pop();
      if (!frag_its[fidx].end())
        pq.emplace(
            fidx,
            tile_coords,
            frag_its[fidx].range_start(),
            frag_its[fidx].range_end());
      continue;
    }

    // The search needs to stop - add current range to result
    if (top.start_ > end) {
      dense_cell_ranges->emplace_back(-1, tile_coords, start, end);
      break;
    }

    // At this point, there is intersection between the top of the
    // queue and the input range. We need to create dense range results.
    if (top.start_ <= start) {
      auto new_end = MIN(end, top.end_);
      dense_cell_ranges->emplace_back(
          top.fragment_idx_, tile_coords, start, new_end);
      start = new_end + 1;
      if (new_end == top.end_) {
        auto fidx = top.fragment_idx_;
        ++frag_its[fidx];
        pq.pop();
        if (!frag_its[fidx].end())
          pq.emplace(
              fidx,
              tile_coords,
              frag_its[fidx].range_start(),
              frag_its[fidx].range_end());
      }
    } else {  // top.start_ > start
      auto new_end = MIN(end, top.start_ - 1);
      dense_cell_ranges->emplace_back(-1, tile_coords, start, new_end);
      start = new_end + 1;
    }

    // Terminating condition
    if (start > end)
      break;
  }

  // Insert an empty cell range if the input range has not been filled
  if (start <= end)
    dense_cell_ranges->emplace_back(-1, tile_coords, start, end);

  return Status::Ok();
}

template <class T>
Status Reader::compute_dense_overlapping_tiles_and_cell_ranges(
    const std::list<DenseCellRange<T>>& dense_cell_ranges,
    const OverlappingCoordsList<T>& coords,
    OverlappingTileVec* tiles,
    OverlappingCellRangeList* overlapping_cell_ranges) {
  // Trivial case = no dense cell ranges
  if (dense_cell_ranges.empty())
    return Status::Ok();

  // For easy reference
  auto domain = array_schema_->domain();
  auto dim_num = array_schema_->dim_num();
  auto coords_size = array_schema_->coords_size();

  // This maps a (fragment, tile coords) pair to an overlapping tile position
  std::map<std::pair<unsigned, const T*>, uint64_t> tile_coords_map;

  // Prepare first range
  auto cr_it = dense_cell_ranges.begin();
  const OverlappingTile* cur_tile = nullptr;
  const T* cur_tile_coords = nullptr;
  if (cr_it->fragment_idx_ != -1) {
    auto tile_idx = fragment_metadata_[cr_it->fragment_idx_]->get_tile_pos(
        cr_it->tile_coords_);
    auto cur_tile_ptr = std::unique_ptr<OverlappingTile>(new OverlappingTile(
        (unsigned)cr_it->fragment_idx_, tile_idx, attributes_));
    tile_coords_map[std::pair<unsigned, const T*>(
        (unsigned)cr_it->fragment_idx_, cr_it->tile_coords_)] =
        (uint64_t)tiles->size();
    cur_tile = cur_tile_ptr.get();
    cur_tile_coords = cr_it->tile_coords_;
    tiles->push_back(std::move(cur_tile_ptr));
  }
  auto start = cr_it->start_;
  auto end = cr_it->end_;

  // Initialize coords info
  auto coords_end = coords.end();
  auto coords_it = skip_invalid_elements(coords.begin(), coords_end);
  std::vector<T> coords_tile_coords;
  coords_tile_coords.resize(dim_num);
  uint64_t coords_pos = 0;
  unsigned coords_fidx = 0;
  const OverlappingTile* coords_tile = nullptr;
  if (coords_it != coords_end) {
    domain->get_tile_coords(coords_it->coords_, &coords_tile_coords[0]);
    RETURN_NOT_OK(domain->get_cell_pos<T>(coords_it->coords_, &coords_pos));
    coords_fidx = coords_it->tile_->fragment_idx_;
    coords_tile = coords_it->tile_;
  }

  // Compute overlapping tiles and cell ranges
  for (++cr_it; cr_it != dense_cell_ranges.end(); ++cr_it) {
    // Find tile
    const OverlappingTile* tile = nullptr;
    if (cr_it->fragment_idx_ != -1) {  // Non-empty
      auto tile_coords_map_it =
          tile_coords_map.find(std::pair<unsigned, const T*>(
              (unsigned)cr_it->fragment_idx_, cr_it->tile_coords_));
      if (tile_coords_map_it != tile_coords_map.end()) {
        tile = (*tiles)[tile_coords_map_it->second].get();
      } else {
        auto tile_idx = fragment_metadata_[cr_it->fragment_idx_]->get_tile_pos(
            cr_it->tile_coords_);
        auto tile_ptr = std::unique_ptr<OverlappingTile>(new OverlappingTile(
            (unsigned)cr_it->fragment_idx_, tile_idx, attributes_));
        tile_coords_map[std::pair<unsigned, const T*>(
            (unsigned)cr_it->fragment_idx_, cr_it->tile_coords_)] =
            (uint64_t)tiles->size();
        tile = tile_ptr.get();
        tiles->push_back(std::move(tile_ptr));
      }
    }

    // Check if the range must be appended to the current one
    if (tile == cur_tile && cr_it->start_ == end + 1) {
      end = cr_it->end_;
      continue;
    }

    // Handle the coordinates that fall between `start` and `end`.
    // This function will either skip the coordinates if they belong to an
    // older fragment, or include them as results and split the dense cell
    // range.
    RETURN_NOT_OK(handle_coords_in_dense_cell_range(
        cur_tile,
        cur_tile_coords,
        &start,
        end,
        coords_size,
        coords,
        &coords_it,
        coords_tile,
        &coords_pos,
        &coords_fidx,
        &coords_tile_coords,
        overlapping_cell_ranges));

    // Push remaining range to the result
    if (start <= end)
      overlapping_cell_ranges->push_back(std::unique_ptr<OverlappingCellRange>(
          new OverlappingCellRange(cur_tile, start, end)));

    // Update state
    cur_tile = tile;
    start = cr_it->start_;
    end = cr_it->end_;
    cur_tile_coords = cr_it->tile_coords_;
  }

  // Handle the coordinates that fall between `start` and `end`.
  // This function will either skip the coordinates if they belong to an
  // older fragment, or include them as results and split the dense cell
  // range.
  RETURN_NOT_OK(handle_coords_in_dense_cell_range(
      cur_tile,
      cur_tile_coords,
      &start,
      end,
      coords_size,
      coords,
      &coords_it,
      coords_tile,
      &coords_pos,
      &coords_fidx,
      &coords_tile_coords,
      overlapping_cell_ranges));

  // Push remaining range to the result
  if (start <= end)
    overlapping_cell_ranges->push_back(std::unique_ptr<OverlappingCellRange>(
        new OverlappingCellRange(cur_tile, start, end)));

  return Status::Ok();
}

template <class T>
Status Reader::compute_overlapping_coords(
    const OverlappingTileVec& tiles, OverlappingCoordsList<T>* coords) const {
  for (const auto& tile : tiles) {
    if (tile->full_overlap_) {
      RETURN_NOT_OK(get_all_coords<T>(tile.get(), coords));
    } else {
      RETURN_NOT_OK(compute_overlapping_coords<T>(tile.get(), coords));
    }
  }

  return Status::Ok();
}

template <class T>
Status Reader::compute_overlapping_coords(
    const OverlappingTile* tile, OverlappingCoordsList<T>* coords) const {
  auto dim_num = array_schema_->dim_num();
  const auto& t = tile->attr_tiles_.find(constants::coords)->second.first;
  auto coords_num = t.cell_num();
  auto subarray = (T*)cur_subarray_;
  auto c = (T*)t.data();

  for (uint64_t i = 0, pos = 0; i < coords_num; ++i, pos += dim_num) {
    if (utils::coords_in_rect<T>(&c[pos], &subarray[0], dim_num))
      coords->emplace_back(tile, &c[pos], i);
  }

  return Status::Ok();
}

template <class T>
Status Reader::compute_overlapping_tiles(OverlappingTileVec* tiles) const {
  // For easy reference
  auto subarray = (T*)cur_subarray_;
  auto dim_num = array_schema_->dim_num();
  auto fragment_num = fragment_metadata_.size();
  bool full_overlap;

  // Find overlapping tile indexes for each fragment
  tiles->clear();
  for (unsigned i = 0; i < fragment_num; ++i) {
    // Applicable only to sparse fragments
    if (fragment_metadata_[i]->dense())
      continue;

    auto mbrs = fragment_metadata_[i]->mbrs();
    auto mbr_num = (uint64_t)mbrs.size();
    for (uint64_t j = 0; j < mbr_num; ++j) {
      if (overlap(&subarray[0], (const T*)(mbrs[j]), dim_num, &full_overlap)) {
        auto tile = std::unique_ptr<OverlappingTile>(
            new OverlappingTile(i, j, attributes_, full_overlap));
        tiles->push_back(std::move(tile));
      }
    }
  }

  return Status::Ok();
}

template <class T>
Status Reader::compute_tile_coordinates(
    std::unique_ptr<T[]>* all_tile_coords,
    OverlappingCoordsList<T>* coords) const {
  if (coords->empty() || array_schema_->domain()->tile_extents() == nullptr)
    return Status::Ok();

  auto domain = reinterpret_cast<const T*>(array_schema_->domain()->domain());
  auto tile_extents =
      reinterpret_cast<const T*>(array_schema_->domain()->tile_extents());
  auto dim_num = array_schema_->dim_num();
  const auto num_coords = (uint64_t)coords->size();

  // Allocate space for all OverlappingCoords' tile coordinate tuples.
  all_tile_coords->reset(new (std::nothrow) T[num_coords * dim_num]);
  if (all_tile_coords == nullptr) {
    return LOG_STATUS(
        Status::ReaderError("Could not allocate tile coords array."));
  }

  // Compute the tile coordinates for each OverlappingCoords.
  for (uint64_t i = 0; i < num_coords; i++) {
    auto& c = (*coords)[i];
    T* tile_coords = all_tile_coords->get() + i * dim_num;
    for (unsigned j = 0; j < dim_num; j++) {
      tile_coords[j] = (c.coords_[j] - domain[2 * j]) / tile_extents[j];
    }
    c.tile_coords_ = tile_coords;
  }

  return Status::Ok();
}

Status Reader::copy_cells(
    const std::string& attribute,
    const OverlappingCellRangeList& cell_ranges) const {
  if (array_schema_->var_size(attribute))
    return copy_var_cells(attribute, cell_ranges);
  return copy_fixed_cells(attribute, cell_ranges);
}

Status Reader::copy_fixed_cells(
    const std::string& attribute,
    const OverlappingCellRangeList& cell_ranges) const {
  // For easy reference
  auto it = attr_buffers_.find(attribute);
  auto buffer = (unsigned char*)it->second.buffer_;
  auto buffer_size = it->second.buffer_size_;
  uint64_t buffer_offset = 0;
  auto cell_size = array_schema_->cell_size(attribute);
  auto type = array_schema_->type(attribute);
  auto fill_size = datatype_size(type);
  auto fill_value = utils::fill_value(type);
  assert(fill_value != nullptr);

  // Copy cells
  for (const auto& cr : cell_ranges) {
    // Check for overflow
    auto bytes_to_copy = (cr->end_ - cr->start_ + 1) * cell_size;
    if (buffer_offset + bytes_to_copy > *buffer_size)
      return LOG_STATUS(Status::ReaderError(
          std::string("Cannot copy cells for attribute '") + attribute +
          "'; Result buffer overflowed"));

    // Copy
    if (cr->tile_ == nullptr) {  // Empty range
      auto fill_num = bytes_to_copy / fill_size;
      for (uint64_t i = 0; i < fill_num; ++i) {
        std::memcpy(buffer + buffer_offset, fill_value, fill_size);
        buffer_offset += fill_size;
      }
    } else {  // Non-empty range
      const auto& tile = cr->tile_->attr_tiles_.find(attribute)->second.first;
      auto data = (unsigned char*)tile.data();
      std::memcpy(
          buffer + buffer_offset, data + cr->start_ * cell_size, bytes_to_copy);
      buffer_offset += bytes_to_copy;
    }
  }

  // Update buffer sizes
  *buffer_size = buffer_offset;

  return Status::Ok();
}

Status Reader::copy_var_cells(
    const std::string& attribute,
    const OverlappingCellRangeList& cell_ranges) const {
  // For easy reference
  auto it = attr_buffers_.find(attribute);
  auto buffer = (unsigned char*)it->second.buffer_;
  auto buffer_var = (unsigned char*)it->second.buffer_var_;
  auto buffer_size = it->second.buffer_size_;
  auto buffer_var_size = it->second.buffer_var_size_;
  uint64_t buffer_offset = 0;
  uint64_t buffer_var_offset = 0;
  uint64_t offset_size = constants::cell_var_offset_size;
  uint64_t cell_var_size;
  auto type = array_schema_->type(attribute);
  auto fill_size = datatype_size(type);
  auto fill_value = utils::fill_value(type);
  assert(fill_value != nullptr);

  // Copy cells
  for (const auto& cr : cell_ranges) {
    auto cell_num_in_range = cr->end_ - cr->start_ + 1;
    // Check if offset buffers can fit the result
    if (buffer_offset + cell_num_in_range * offset_size > *buffer_size)
      return LOG_STATUS(Status::ReaderError(
          std::string("Cannot copy cell offsets for var-sized attribute '") +
          attribute + "'; Result buffer overflow"));

    // Handle empty range
    if (cr->tile_ == nullptr) {
      // Check if result can fit in the buffer
      if (buffer_var_offset + cell_num_in_range * fill_size > *buffer_var_size)
        return LOG_STATUS(Status::ReaderError(
            std::string("Cannot copy cell data for var-sized attribute '") +
            attribute + "'; Result buffer overflowed"));

      // Fill with empty
      for (auto i = cr->start_; i <= cr->end_; ++i) {
        // Offsets
        std::memcpy(buffer + buffer_offset, &buffer_var_offset, offset_size);
        buffer_offset += offset_size;

        // Values
        std::memcpy(buffer_var + buffer_var_offset, fill_value, fill_size);
        buffer_var_offset += fill_size;
      }

      continue;
    }

    // Non-empty range
    const auto& tile_pair = cr->tile_->attr_tiles_.find(attribute)->second;
    const auto& tile = tile_pair.first;
    const auto& tile_var = tile_pair.second;
    const auto offsets = (uint64_t*)tile.data();
    auto data = (unsigned char*)tile_var.data();
    auto cell_num = tile.cell_num();
    auto tile_var_size = tile_var.size();

    for (auto i = cr->start_; i <= cr->end_; ++i) {
      // Copy offsets
      std::memcpy(buffer + buffer_offset, &buffer_var_offset, offset_size);
      buffer_offset += offset_size;

      // Check if next variable-sized cell fits in the result buffer
      cell_var_size = (i != cell_num - 1) ?
                          offsets[i + 1] - offsets[i] :
                          tile_var_size - (offsets[i] - offsets[0]);

      if (buffer_var_offset + cell_var_size > *buffer_var_size)
        return LOG_STATUS(Status::ReaderError(
            std::string("Cannot copy cell data for var-sized attribute '") +
            attribute + "'; Result buffer overflowed"));

      // Copy variable-sized values
      std::memcpy(
          buffer_var + buffer_var_offset,
          &data[offsets[i] - offsets[0]],
          cell_var_size);
      buffer_var_offset += cell_var_size;
    }
  }

  // Update buffer sizes
  *buffer_size = buffer_offset;
  *buffer_var_size = buffer_var_offset;

  return Status::Ok();
}

template <class T>
Status Reader::dedup_coords(OverlappingCoordsList<T>* coords) const {
  auto coords_size = array_schema_->coords_size();
  auto coords_end = coords->end();
  auto it = skip_invalid_elements(coords->begin(), coords_end);
  while (it != coords_end) {
    auto next_it = skip_invalid_elements(std::next(it), coords_end);
    if (next_it != coords_end &&
        !std::memcmp(it->coords_, next_it->coords_, coords_size)) {
      if (it->tile_->fragment_idx_ < next_it->tile_->fragment_idx_) {
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
  auto coords_type = array_schema_->coords_type();
  switch (coords_type) {
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
    default:
      return LOG_STATUS(
          Status::ReaderError("Cannot read; Unsupported domain type"));
  }

  return Status::Ok();
}

template <class T>
Status Reader::dense_read() {
  // For easy reference
  auto domain = array_schema_->domain();
  auto subarray_len = 2 * array_schema_->dim_num();
  std::vector<T> subarray;
  subarray.resize(subarray_len);
  for (size_t i = 0; i < subarray_len; ++i)
    subarray[i] = ((T*)cur_subarray_)[i];

  // Get overlapping sparse tile indexes
  OverlappingTileVec sparse_tiles;
  RETURN_CANCEL_OR_ERROR(compute_overlapping_tiles<T>(&sparse_tiles));

  // Read sparse tiles
  RETURN_CANCEL_OR_ERROR(read_all_tiles(&sparse_tiles));

  // Compute the read coordinates for all sparse fragments
  OverlappingCoordsList<T> coords;
  RETURN_CANCEL_OR_ERROR(compute_overlapping_coords<T>(sparse_tiles, &coords));

  // Compute the tile coordinates for all overlapping coordinates (for sorting).
  std::unique_ptr<T[]> tile_coords(nullptr);
  RETURN_CANCEL_OR_ERROR(compute_tile_coordinates<T>(&tile_coords, &coords));

  // Sort and dedup the coordinates (not applicable to the global order
  // layout for a single fragment)
  if (!(fragment_metadata_.size() == 1 && layout_ == Layout::GLOBAL_ORDER)) {
    RETURN_CANCEL_OR_ERROR(sort_coords<T>(&coords));
    RETURN_CANCEL_OR_ERROR(dedup_coords<T>(&coords));
  }
  tile_coords.reset(nullptr);

  // For each tile, initialize a dense cell range iterator per
  // (dense) fragment
  std::vector<std::vector<DenseCellRangeIter<T>>> dense_frag_its;
  std::unordered_map<uint64_t, std::pair<uint64_t, std::vector<T>>>
      overlapping_tile_idx_coords;
  RETURN_CANCEL_OR_ERROR(init_tile_fragment_dense_cell_range_iters(
      &dense_frag_its, &overlapping_tile_idx_coords));

  // Get the cell ranges
  std::list<DenseCellRange<T>> dense_cell_ranges;
  DenseCellRangeIter<T> it(domain, subarray, layout_);
  RETURN_CANCEL_OR_ERROR(it.begin());
  while (!it.end()) {
    auto o_it = overlapping_tile_idx_coords.find(it.tile_idx());
    assert(o_it != overlapping_tile_idx_coords.end());
    RETURN_CANCEL_OR_ERROR(compute_dense_cell_ranges<T>(
        &(o_it->second.second)[0],
        dense_frag_its[o_it->second.first],
        it.range_start(),
        it.range_end(),
        &dense_cell_ranges));
    ++it;
  }

  // Compute overlapping dense tile indexes
  OverlappingTileVec dense_tiles;
  OverlappingCellRangeList overlapping_cell_ranges;
  RETURN_CANCEL_OR_ERROR(compute_dense_overlapping_tiles_and_cell_ranges<T>(
      dense_cell_ranges, coords, &dense_tiles, &overlapping_cell_ranges));
  coords.clear();
  dense_cell_ranges.clear();
  overlapping_tile_idx_coords.clear();

  // Read dense tiles
  RETURN_CANCEL_OR_ERROR(read_all_tiles(&dense_tiles, false));

  // Copy cells
  for (const auto& attr : attributes_) {
    if (attr != constants::coords)  // Skip coordinates
      RETURN_CANCEL_OR_ERROR(copy_cells(attr, overlapping_cell_ranges));
  }

  // Fill coordinates if the user requested them
  if (has_coords())
    RETURN_CANCEL_OR_ERROR(fill_coords<T>());

  return Status::Ok();
}

template <class T>
Status Reader::fill_coords() const {
  // For easy reference
  auto it = attr_buffers_.find(constants::coords);
  assert(it != attr_buffers_.end());
  auto coords_buff = it->second.buffer_;
  auto coords_buff_offset = (uint64_t)0;
  auto coords_buff_size = it->second.buffer_size_;
  auto domain = array_schema_->domain();
  auto cell_order = array_schema_->cell_order();
  auto subarray_len = 2 * array_schema_->dim_num();
  std::vector<T> subarray;
  subarray.resize(subarray_len);
  for (size_t i = 0; i < subarray_len; ++i)
    subarray[i] = ((T*)cur_subarray_)[i];

  // Iterate over all coordinates, retrieved in cell slabs
  DenseCellRangeIter<T> cell_it(domain, subarray, layout_);
  RETURN_CANCEL_OR_ERROR(cell_it.begin());
  while (!cell_it.end()) {
    auto coords_num = cell_it.range_end() - cell_it.range_start() + 1;
    if (layout_ == Layout::ROW_MAJOR ||
        (layout_ == Layout::GLOBAL_ORDER && cell_order == Layout::ROW_MAJOR))
      fill_coords_row_slab(
          cell_it.coords_start(), coords_num, coords_buff, &coords_buff_offset);
    else
      fill_coords_col_slab(
          cell_it.coords_start(), coords_num, coords_buff, &coords_buff_offset);
    ++cell_it;
  }

  // Update the coords buffer size
  *coords_buff_size = coords_buff_offset;

  return Status::Ok();
}

template <class T>
void Reader::fill_coords_row_slab(
    const T* start, uint64_t num, void* buff, uint64_t* offset) const {
  // For easy reference
  auto dim_num = array_schema_->dim_num();
  assert(dim_num > 0);
  auto c_buff = (char*)buff;

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
}

template <class T>
void Reader::fill_coords_col_slab(
    const T* start, uint64_t num, void* buff, uint64_t* offset) const {
  // For easy reference
  auto dim_num = array_schema_->dim_num();
  assert(dim_num > 0);
  auto c_buff = (char*)buff;

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
}

template <class T>
Status Reader::get_all_coords(
    const OverlappingTile* tile, OverlappingCoordsList<T>* coords) const {
  auto dim_num = array_schema_->dim_num();
  const auto& t = tile->attr_tiles_.find(constants::coords)->second.first;
  auto coords_num = t.cell_num();
  auto c = (T*)t.data();

  for (uint64_t i = 0; i < coords_num; ++i)
    coords->emplace_back(tile, &c[i * dim_num], i);

  return Status::Ok();
}

template <class T>
Status Reader::handle_coords_in_dense_cell_range(
    const OverlappingTile* cur_tile,
    const T* cur_tile_coords,
    uint64_t* start,
    uint64_t end,
    uint64_t coords_size,
    const OverlappingCoordsList<T>& coords,
    typename OverlappingCoordsList<T>::const_iterator* coords_it,
    const OverlappingTile* coords_tile,
    uint64_t* coords_pos,
    unsigned* coords_fidx,
    std::vector<T>* coords_tile_coords,
    OverlappingCellRangeList* overlapping_cell_ranges) const {
  auto domain = array_schema_->domain();
  auto coords_end = coords.end();

  // While the coords are within the same dense cell range
  while (*coords_it != coords_end &&
         !memcmp(&(*coords_tile_coords)[0], cur_tile_coords, coords_size) &&
         *coords_pos >= *start && *coords_pos <= end) {
    if (*coords_fidx < cur_tile->fragment_idx_) {  // Skip coords
      *coords_it = skip_invalid_elements(++(*coords_it), coords_end);
      if (*coords_it != coords_end) {
        domain->get_tile_coords(
            (*coords_it)->coords_, &(*coords_tile_coords)[0]);
        RETURN_NOT_OK(
            domain->get_cell_pos<T>((*coords_it)->coords_, coords_pos));
        *coords_fidx = (*coords_it)->tile_->fragment_idx_;
        coords_tile = (*coords_it)->tile_;
      }
      continue;
    } else {  // Break dense range
      // Left range
      if (*coords_pos > *start) {
        overlapping_cell_ranges->push_back(
            std::unique_ptr<OverlappingCellRange>(
                new OverlappingCellRange(cur_tile, *start, *coords_pos - 1)));
      }
      // Coords unary range
      overlapping_cell_ranges->push_back(
          std::unique_ptr<OverlappingCellRange>(new OverlappingCellRange(
              coords_tile, (*coords_it)->pos_, (*coords_it)->pos_)));
      // Update start
      *start = *coords_pos + 1;

      // Advance coords
      *coords_it = skip_invalid_elements(++(*coords_it), coords_end);
      if (*coords_it != coords_end) {
        domain->get_tile_coords(
            (*coords_it)->coords_, &(*coords_tile_coords)[0]);
        RETURN_NOT_OK(
            domain->get_cell_pos<T>((*coords_it)->coords_, coords_pos));
        *coords_fidx = (*coords_it)->tile_->fragment_idx_;
        coords_tile = (*coords_it)->tile_;
      }
    }
  }

  return Status::Ok();
}

bool Reader::has_coords() const {
  return attr_buffers_.find(constants::coords) != attr_buffers_.end();
}

Status Reader::init_tile(const std::string& attribute, Tile* tile) const {
  // For easy reference
  auto domain = array_schema_->domain();
  auto cell_size = array_schema_->cell_size(attribute);
  auto capacity = array_schema_->capacity();
  auto type = array_schema_->type(attribute);
  auto is_coords = (attribute == constants::coords);
  auto dim_num = (is_coords) ? array_schema_->dim_num() : 0;
  auto compressor = array_schema_->compression(attribute);
  auto compression_level = array_schema_->compression_level(attribute);
  auto cell_num_per_tile =
      (has_coords()) ? capacity : domain->cell_num_per_tile();
  auto tile_size = cell_num_per_tile * cell_size;

  // Initialize
  RETURN_NOT_OK(tile->init(
      type, compressor, compression_level, tile_size, cell_size, dim_num));

  return Status::Ok();
}

Status Reader::init_tile(
    const std::string& attribute, Tile* tile, Tile* tile_var) const {
  // For easy reference
  auto domain = array_schema_->domain();
  auto capacity = array_schema_->capacity();
  auto type = array_schema_->type(attribute);
  auto compressor = array_schema_->compression(attribute);
  auto compression_level = array_schema_->compression_level(attribute);
  auto cell_num_per_tile =
      (has_coords()) ? capacity : domain->cell_num_per_tile();
  auto tile_size = cell_num_per_tile * constants::cell_var_offset_size;

  // Initialize
  RETURN_NOT_OK(tile->init(
      constants::cell_var_offset_type,
      array_schema_->cell_var_offsets_compression(),
      array_schema_->cell_var_offsets_compression_level(),
      tile_size,
      constants::cell_var_offset_size,
      0));
  RETURN_NOT_OK(tile_var->init(
      type, compressor, compression_level, tile_size, datatype_size(type), 0));
  return Status::Ok();
}

template <class T>
Status Reader::init_tile_fragment_dense_cell_range_iters(
    std::vector<std::vector<DenseCellRangeIter<T>>>* iters,
    std::unordered_map<uint64_t, std::pair<uint64_t, std::vector<T>>>*
        overlapping_tile_idx_coords) {
  // For easy reference
  auto domain = array_schema_->domain();
  auto dim_num = domain->dim_num();
  auto fragment_num = fragment_metadata_.size();
  std::vector<T> subarray;
  subarray.resize(2 * dim_num);
  for (unsigned i = 0; i < 2 * dim_num; ++i)
    subarray[i] = ((T*)cur_subarray_)[i];

  // Compute tile domain and current tile coords
  std::vector<T> tile_domain, tile_coords;
  tile_domain.resize(2 * dim_num);
  tile_coords.resize(dim_num);
  domain->get_tile_domain(&subarray[0], &tile_domain[0]);
  for (unsigned i = 0; i < dim_num; ++i)
    tile_coords[i] = tile_domain[2 * i];
  auto tile_num = domain->tile_num<T>(&subarray[0]);

  // Iterate over all tiles in the tile domain
  iters->clear();
  overlapping_tile_idx_coords->clear();
  std::vector<T> tile_subarray, subarray_in_tile;
  std::vector<T> frag_subarray, frag_subarray_in_tile;
  tile_subarray.resize(2 * dim_num);
  subarray_in_tile.resize(2 * dim_num);
  frag_subarray_in_tile.resize(2 * dim_num);
  frag_subarray.resize(2 * dim_num);
  bool tile_overlap, in;
  uint64_t tile_idx;
  for (uint64_t i = 0; i < tile_num; ++i) {
    // Compute subarray overlap with tile
    domain->get_tile_subarray(&tile_coords[0], &tile_subarray[0]);
    domain->subarray_overlap(
        &subarray[0], &tile_subarray[0], &subarray_in_tile[0], &tile_overlap);
    tile_idx = domain->get_tile_pos(&tile_coords[0]);
    (*overlapping_tile_idx_coords)[tile_idx] =
        std::pair<uint64_t, std::vector<T>>(i, tile_coords);

    // Initialize fragment iterators. For sparse fragments, the constructed
    // iterator will always be at its end.
    std::vector<DenseCellRangeIter<T>> frag_iters;
    for (unsigned j = 0; j < fragment_num; ++j) {
      if (!fragment_metadata_[j]->dense()) {  // Sparse fragment
        frag_iters.emplace_back();
      } else {  // Dense fragment
        auto frag_domain = (T*)fragment_metadata_[j]->non_empty_domain();
        for (unsigned k = 0; k < 2 * dim_num; ++k)
          frag_subarray[k] = frag_domain[k];
        domain->subarray_overlap(
            &subarray_in_tile[0],
            &frag_subarray[0],
            &frag_subarray_in_tile[0],
            &tile_overlap);

        if (tile_overlap) {
          frag_iters.emplace_back(domain, frag_subarray_in_tile, layout_);
          RETURN_NOT_OK(frag_iters.back().begin());
        } else {
          frag_iters.emplace_back();
        }
      }
    }
    iters->push_back(std::move(frag_iters));

    // Get next tile coordinates
    domain->get_next_tile_coords(&tile_domain[0], &tile_coords[0], &in);
    assert((i != tile_num - 1 && in) || (i == tile_num - 1 && !in));
  }

  return Status::Ok();
}

template <class T>
bool Reader::overlap(
    const T* a, const T* b, unsigned dim_num, bool* a_contains_b) const {
  for (unsigned i = 0; i < dim_num; ++i) {
    if (a[2 * i] > b[2 * i + 1] || a[2 * i + 1] < b[2 * i])
      return false;
  }

  *a_contains_b = true;
  for (unsigned i = 0; i < dim_num; ++i) {
    if (a[2 * i] > b[2 * i] || a[2 * i + 1] < b[2 * i + 1]) {
      *a_contains_b = false;
      break;
    }
  }

  return true;
}

Status Reader::read_all_tiles(
    OverlappingTileVec* tiles, bool ensure_coords) const {
  // Shortcut for empty tile vec
  if (tiles->empty())
    return Status::Ok();

  // Prepare attributes
  std::set<std::string> all_attributes;
  for (const auto& attr : attributes_) {
    if (array_schema_->dense() && attr == constants::coords)
      continue;  // Skip coords in dense case - no actual tiles to read
    all_attributes.insert(attr);
  }

  // Make sure the coordinate tiles are read if specified.
  if (ensure_coords)
    all_attributes.insert(constants::coords);

  // Read the tiles in parallel over the attributes.
  auto statuses = parallel_for_each(
      all_attributes.begin(),
      all_attributes.end(),
      [this, &tiles](const std::string& attr) {
        RETURN_CANCEL_OR_ERROR(read_tiles(attr, tiles));
        return Status::Ok();
      });

  for (const auto& st : statuses)
    RETURN_CANCEL_OR_ERROR(st);

  return Status::Ok();
}

Status Reader::read_tiles(
    const std::string& attribute, OverlappingTileVec* tiles) const {
  // Prepare tile IO
  auto var_size = array_schema_->var_size(attribute);
  auto num_tiles = static_cast<uint64_t>(tiles->size());
  std::vector<std::vector<std::unique_ptr<TileIO>>> tile_io_vec(num_tiles);
  std::vector<std::vector<std::unique_ptr<TileIO>>> tile_io_var_vec(num_tiles);

  // Create one TileIO instance per tile, per fragment.
  for (uint64_t i = 0; i < num_tiles; i++) {
    auto& tile_io = tile_io_vec[i];
    auto& tile_io_var = tile_io_var_vec[i];
    for (const auto& f : fragment_metadata_) {
      tile_io.push_back(std::unique_ptr<TileIO>(new TileIO(
          storage_manager_, f->attr_uri(attribute), f->file_sizes(attribute))));
      if (var_size)
        tile_io_var.push_back(std::unique_ptr<TileIO>(new TileIO(
            storage_manager_,
            f->attr_var_uri(attribute),
            f->file_var_sizes(attribute))));
      else
        tile_io_var.push_back(std::unique_ptr<TileIO>(nullptr));
    }
  }

  // For each tile, read from its fragment.
  auto statuses = parallel_for(0, num_tiles, [&, this](uint64_t i) {
    auto& tile = (*tiles)[i];
    auto& tile_io = tile_io_vec[i];
    auto& tile_io_var = tile_io_var_vec[i];
    auto it = tile->attr_tiles_.find(attribute);
    if (it == tile->attr_tiles_.end()) {
      return LOG_STATUS(
          Status::ReaderError("Invalid tile map for attribute " + attribute));
    }

    auto& tile_pair = it->second;
    auto& t = tile_pair.first;
    auto& t_var = tile_pair.second;
    // Initialize
    if (!var_size) {
      RETURN_NOT_OK(init_tile(attribute, &t));
    } else {
      RETURN_NOT_OK(init_tile(attribute, &t, &t_var));
    }

    // Read
    RETURN_NOT_OK(tile_io[tile->fragment_idx_]->read(
        &t,
        fragment_metadata_[tile->fragment_idx_]->file_offset(
            attribute, tile->tile_idx_),
        fragment_metadata_[tile->fragment_idx_]->compressed_tile_size(
            attribute, tile->tile_idx_),
        fragment_metadata_[tile->fragment_idx_]->tile_size(
            attribute, tile->tile_idx_)));
    if (var_size) {
      RETURN_NOT_OK(tile_io_var[tile->fragment_idx_]->read(
          &t_var,
          fragment_metadata_[tile->fragment_idx_]->file_var_offset(
              attribute, tile->tile_idx_),
          fragment_metadata_[tile->fragment_idx_]->compressed_tile_var_size(
              attribute, tile->tile_idx_),
          fragment_metadata_[tile->fragment_idx_]->tile_var_size(
              attribute, tile->tile_idx_)));
    }

    return Status::Ok();
  });

  for (const auto& st : statuses)
    RETURN_CANCEL_OR_ERROR(st);

  return Status::Ok();
}

Status Reader::set_attributes(
    const char** attributes, unsigned int attribute_num) {
  // Get attributes
  std::vector<std::string> attributes_vec;
  if (attributes == nullptr) {  // Default: all attributes
    attributes_vec = array_schema_->attribute_names();
    if (!array_schema_->dense())
      attributes_vec.emplace_back(constants::coords);
  } else {  // Custom attributes
    // Get attributes
    unsigned uri_max_len = constants::uri_max_len;
    for (unsigned int i = 0; i < attribute_num; ++i) {
      // Check attribute name length
      if (attributes[i] == nullptr || strlen(attributes[i]) > uri_max_len)
        return LOG_STATUS(Status::ReaderError("Invalid attribute name length"));
      attributes_vec.emplace_back(attributes[i]);
    }

    // Sanity check on duplicates
    if (utils::has_duplicates(attributes_vec))
      return LOG_STATUS(
          Status::ReaderError("Cannot set attributes; Duplicate attributes"));
  }

  // Set attribute names
  attributes_.clear();
  for (const auto& attr : attributes_vec)
    attributes_.emplace_back(attr);

  return Status::Ok();
}

template <class T>
Status Reader::sort_coords(OverlappingCoordsList<T>* coords) const {
  if (layout_ == Layout::GLOBAL_ORDER) {
    auto domain = array_schema_->domain();
    parallel_sort(coords->begin(), coords->end(), GlobalCmp<T>(domain));
  } else {
    auto dim_num = array_schema_->dim_num();
    if (layout_ == Layout::ROW_MAJOR)
      parallel_sort(coords->begin(), coords->end(), RowCmp<T>(dim_num));
    else if (layout_ == Layout::COL_MAJOR)
      parallel_sort(coords->begin(), coords->end(), ColCmp<T>(dim_num));
  }

  return Status::Ok();
}

Status Reader::sparse_read() {
  auto coords_type = array_schema_->coords_type();
  switch (coords_type) {
    case Datatype::INT8:
      return sparse_read<int8_t>();
    case Datatype::UINT8:
      return sparse_read<uint8_t>();
    case Datatype::INT16:
      return sparse_read<int16_t>();
    case Datatype::UINT16:
      return sparse_read<uint16_t>();
    case Datatype::INT32:
      return sparse_read<int>();
    case Datatype::UINT32:
      return sparse_read<unsigned>();
    case Datatype::INT64:
      return sparse_read<int64_t>();
    case Datatype::UINT64:
      return sparse_read<uint64_t>();
    case Datatype::FLOAT32:
      return sparse_read<float>();
    case Datatype::FLOAT64:
      return sparse_read<double>();
    default:
      return LOG_STATUS(
          Status::ReaderError("Cannot read; Unsupported domain type"));
  }

  return Status::Ok();
}

template <class T>
Status Reader::sparse_read() {
  // Get overlapping tile indexes
  OverlappingTileVec tiles;
  RETURN_CANCEL_OR_ERROR(compute_overlapping_tiles<T>(&tiles));

  // Read tiles
  RETURN_CANCEL_OR_ERROR(read_all_tiles(&tiles));

  // Compute the read coordinates for all fragments
  OverlappingCoordsList<T> coords;
  RETURN_CANCEL_OR_ERROR(compute_overlapping_coords<T>(tiles, &coords));

  // Compute the tile coordinates for all overlapping coordinates (for sorting).
  std::unique_ptr<T[]> tile_coords(nullptr);
  RETURN_CANCEL_OR_ERROR(compute_tile_coordinates<T>(&tile_coords, &coords));

  // Sort and dedup the coordinates (not applicable to the global order
  // layout for a single fragment)
  if (!(fragment_metadata_.size() == 1 && layout_ == Layout::GLOBAL_ORDER)) {
    RETURN_CANCEL_OR_ERROR(sort_coords<T>(&coords));
    RETURN_CANCEL_OR_ERROR(dedup_coords<T>(&coords));
  }
  tile_coords.reset(nullptr);

  // Compute the maximal cell ranges
  OverlappingCellRangeList cell_ranges;
  RETURN_CANCEL_OR_ERROR(compute_cell_ranges(coords, &cell_ranges));
  coords.clear();

  // Copy cells
  for (const auto& attr : attributes_)
    RETURN_CANCEL_OR_ERROR(copy_cells(attr, cell_ranges));

  return Status::Ok();
}

void Reader::zero_out_buffer_sizes() {
  for (auto& attr_buffer : attr_buffers_) {
    if (attr_buffer.second.buffer_size_ != nullptr)
      *(attr_buffer.second.buffer_size_) = 0;
    if (attr_buffer.second.buffer_var_size_ != nullptr)
      *(attr_buffer.second.buffer_var_size_) = 0;
  }
}

}  // namespace sm
}  // namespace tiledb
