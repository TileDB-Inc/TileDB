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
#include "tiledb/sm/misc/utils.h"
#include "tiledb/sm/storage_manager/storage_manager.h"
#include "tiledb/sm/tile/tile_io.h"

/* ****************************** */
/*             MACROS             */
/* ****************************** */

#define MIN(a, b) ((a) < (b) ? (a) : (b))
#define MAX(a, b) ((a) > (b) ? (a) : (b))

namespace tiledb {
namespace sm {

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

Reader::Reader() {
  array_schema_ = nullptr;
  layout_ = Layout::ROW_MAJOR;
  storage_manager_ = nullptr;
  subarray_ = nullptr;
}

Reader::~Reader() {
  if (subarray_ != nullptr)
    std::free(subarray_);
}

/* ****************************** */
/*               API              */
/* ****************************** */

const ArraySchema* Reader::array_schema() const {
  return array_schema_;
}

Status Reader::compute_subarrays(
    void* subarray, std::vector<void*>* subarrays) const {
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
  for (const auto& attr : attributes_)
    max_buffer_sizes[attr] = std::pair<uint64_t, uint64_t>(0, 0);

  // Compute subarrays
  std::list<void*> my_subarrays;
  my_subarrays.emplace_back(first_subarray);
  auto it = my_subarrays.begin();
  Status st = Status::Ok();
  do {
    // Compute max buffer sizes for the current subarray
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
      if (subarray_1 == nullptr || subarray_2 == nullptr) {
        st = LOG_STATUS(Status::ReaderError(
            "Cannot compute subarrays; Subarray is not splittable"));
        break;
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
    subarrays->emplace_back(s);

  return Status::Ok();
}

Status Reader::finalize() {
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

  if (subarray_ == nullptr)
    RETURN_NOT_OK(set_subarray(nullptr));
  RETURN_NOT_OK(check_subarray(subarray_));

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

  RETURN_NOT_OK(set_attributes(attributes, attribute_num));
  set_buffers(buffers, buffer_sizes);

  return Status::Ok();
}

void Reader::set_buffers(void** buffers, uint64_t* buffer_sizes) {
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
  RETURN_NOT_OK(check_subarray(subarray));

  uint64_t subarray_size = 2 * array_schema_->coords_size();

  if (subarray_ == nullptr)
    subarray_ = std::malloc(subarray_size);
  if (subarray_ == nullptr)
    return LOG_STATUS(
        Status::ReaderError("Memory allocation for subarray failed"));

  if (subarray == nullptr)
    std::memcpy(subarray_, array_schema_->domain()->domain(), subarray_size);
  else
    std::memcpy(subarray_, subarray, subarray_size);

  return Status::Ok();
}

/* ****************************** */
/*          PRIVATE METHODS       */
/* ****************************** */

Status Reader::check_subarray(const void* subarray) const {
  if (subarray == nullptr)
    return Status::Ok();

  switch (array_schema_->domain()->type()) {
    case Datatype::INT8:
      return check_subarray<int8_t>(static_cast<const int8_t*>(subarray));
    case Datatype::UINT8:
      return check_subarray<uint8_t>(static_cast<const uint8_t*>(subarray));
    case Datatype::INT16:
      return check_subarray<int16_t>(static_cast<const int16_t*>(subarray));
    case Datatype::UINT16:
      return check_subarray<uint16_t>(static_cast<const uint16_t*>(subarray));
    case Datatype::INT32:
      return check_subarray<int32_t>(static_cast<const int32_t*>(subarray));
    case Datatype::UINT32:
      return check_subarray<uint32_t>(static_cast<const uint32_t*>(subarray));
    case Datatype::INT64:
      return check_subarray<int64_t>(static_cast<const int64_t*>(subarray));
    case Datatype::UINT64:
      return check_subarray<uint64_t>(static_cast<const uint64_t*>(subarray));
    case Datatype::FLOAT32:
      return check_subarray<float>(static_cast<const float*>(subarray));
    case Datatype::FLOAT64:
      return check_subarray<double>(static_cast<const double*>(subarray));
    case Datatype::CHAR:
    case Datatype::STRING_ASCII:
    case Datatype::STRING_UTF8:
    case Datatype::STRING_UTF16:
    case Datatype::STRING_UTF32:
    case Datatype::STRING_UCS2:
    case Datatype::STRING_UCS4:
    case Datatype::ANY:
      // Not supported domain type
      assert(false);
      break;
  }

  return Status::Ok();
}

template <class T>
Status Reader::check_subarray(const T* subarray) const {
  // Check subarray bounds
  auto domain = array_schema_->domain();
  auto dim_num = domain->dim_num();
  for (unsigned int i = 0; i < dim_num; ++i) {
    auto dim_domain = static_cast<const T*>(domain->dimension(i)->domain());
    if (subarray[2 * i] < dim_domain[0] || subarray[2 * i + 1] > dim_domain[1])
      return LOG_STATUS(Status::ReaderError("Subarray out of bounds"));
    if (subarray[2 * i] > subarray[2 * i + 1])
      return LOG_STATUS(Status::ReaderError(
          "Subarray lower bound is larger than upper bound"));
  }

  return Status::Ok();
}

template <class T>
Status Reader::compute_cell_ranges(
    const std::list<std::shared_ptr<OverlappingCoords<T>>>& coords,
    OverlappingCellRangeList* cell_ranges) const {
  // Trivial case
  auto coords_num = (uint64_t)coords.size();
  if (coords_num == 0)
    return Status::Ok();

  // Initialize the first range
  auto it = coords.begin();
  uint64_t start_pos = it->get()->pos_;
  uint64_t end_pos = start_pos;
  auto tile = it->get()->tile_;

  // Scan the coordinates and compute ranges
  for (++it; it != coords.end(); ++it) {
    if (it->get()->tile_.get() == tile.get() &&
        it->get()->pos_ == end_pos + 1) {
      // Same range - advance end position
      end_pos = it->get()->pos_;
    } else {
      // New range - append previous range
      cell_ranges->emplace_back(
          std::make_shared<OverlappingCellRange>(tile, start_pos, end_pos));
      start_pos = it->get()->pos_;
      end_pos = start_pos;
      tile = it->get()->tile_;
    }
  }

  // Append the last range
  cell_ranges->emplace_back(
      std::make_shared<OverlappingCellRange>(tile, start_pos, end_pos));

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
    const std::list<std::shared_ptr<OverlappingCoords<T>>>& coords,
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
  std::shared_ptr<OverlappingTile> cur_tile = nullptr;
  const T* cur_tile_coords = nullptr;
  if (cr_it->fragment_idx_ != -1) {
    auto tile_idx = fragment_metadata_[cr_it->fragment_idx_]->get_tile_pos(
        cr_it->tile_coords_);
    cur_tile = std::make_shared<OverlappingTile>(
        (unsigned)cr_it->fragment_idx_, tile_idx);
    tile_coords_map[std::pair<unsigned, const T*>(
        (unsigned)cr_it->fragment_idx_, cr_it->tile_coords_)] =
        (uint64_t)tiles->size();
    tiles->push_back(cur_tile);
    cur_tile_coords = cr_it->tile_coords_;
  }
  auto start = cr_it->start_;
  auto end = cr_it->end_;

  // Initialize coords info
  auto coords_it = coords.begin();
  std::vector<T> coords_tile_coords;
  coords_tile_coords.resize(dim_num);
  uint64_t coords_pos = 0;
  unsigned coords_fidx = 0;
  std::shared_ptr<OverlappingTile> coords_tile = nullptr;
  if (coords_it != coords.end()) {
    domain->get_tile_coords(coords_it->get()->coords_, &coords_tile_coords[0]);
    RETURN_NOT_OK(
        domain->get_cell_pos<T>(coords_it->get()->coords_, &coords_pos));
    coords_fidx = coords_it->get()->tile_->fragment_idx_;
    coords_tile = coords_it->get()->tile_;
  }

  // Compute overlapping tiles and cell ranges
  for (++cr_it; cr_it != dense_cell_ranges.end(); ++cr_it) {
    // Find tile
    std::shared_ptr<OverlappingTile> tile = nullptr;
    if (cr_it->fragment_idx_ != -1) {  // Non-empty
      auto tile_coords_map_it =
          tile_coords_map.find(std::pair<unsigned, const T*>(
              (unsigned)cr_it->fragment_idx_, cr_it->tile_coords_));
      if (tile_coords_map_it != tile_coords_map.end()) {
        tile = (*tiles)[tile_coords_map_it->second];
      } else {
        auto tile_idx = fragment_metadata_[cr_it->fragment_idx_]->get_tile_pos(
            cr_it->tile_coords_);
        tile = std::make_shared<OverlappingTile>(
            (unsigned)cr_it->fragment_idx_, tile_idx);
        tile_coords_map[std::pair<unsigned, const T*>(
            (unsigned)cr_it->fragment_idx_, cr_it->tile_coords_)] =
            (uint64_t)tiles->size();
        tiles->push_back(tile);
      }
    }

    // Check if the range must be appended to the current one
    if (tile.get() == cur_tile.get() && cr_it->start_ == end + 1) {
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
        &coords_tile,
        &coords_pos,
        &coords_fidx,
        &coords_tile_coords,
        overlapping_cell_ranges));

    // Push remaining range to the result
    if (start <= end)
      overlapping_cell_ranges->emplace_back(
          std::make_shared<OverlappingCellRange>(cur_tile, start, end));

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
      &coords_tile,
      &coords_pos,
      &coords_fidx,
      &coords_tile_coords,
      overlapping_cell_ranges));

  // Push remaining range to the result
  if (start <= end)
    overlapping_cell_ranges->emplace_back(
        std::make_shared<OverlappingCellRange>(cur_tile, start, end));

  return Status::Ok();
}

template <class T>
Status Reader::compute_overlapping_coords(
    const OverlappingTileVec& tiles,
    std::list<std::shared_ptr<OverlappingCoords<T>>>* coords) const {
  for (const auto& tile : tiles) {
    std::list<std::shared_ptr<OverlappingCoords<T>>> tile_coords;
    if (tile->full_overlap_) {
      RETURN_NOT_OK(get_all_coords<T>(tile, &tile_coords));
    } else {
      RETURN_NOT_OK(compute_overlapping_coords<T>(tile, &tile_coords));
    }
    coords->splice(coords->end(), tile_coords);
  }

  return Status::Ok();
}

template <class T>
Status Reader::compute_overlapping_coords(
    const std::shared_ptr<OverlappingTile>& tile,
    std::list<std::shared_ptr<OverlappingCoords<T>>>* coords) const {
  auto dim_num = array_schema_->dim_num();
  const auto t = tile->attr_tiles_.find(constants::coords)->second.first;
  auto t_ptr = t.get();
  auto coords_num = t_ptr->cell_num();
  auto subarray = (T*)subarray_;
  auto c = (T*)t_ptr->data();

  for (uint64_t i = 0, pos = 0; i < coords_num; ++i, pos += dim_num) {
    if (utils::coords_in_rect<T>(&c[pos], &subarray[0], dim_num))
      coords->emplace_back(
          std::make_shared<OverlappingCoords<T>>(tile, &c[pos], i));
  }

  return Status::Ok();
}

template <class T>
Status Reader::compute_overlapping_tiles(OverlappingTileVec* tiles) const {
  // For easy reference
  auto subarray = (T*)subarray_;
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
        auto tile = std::make_shared<OverlappingTile>(i, j, full_overlap);
        tiles->emplace_back(tile);
      }
    }
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
      auto data = (unsigned char*)tile->data();
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
    const auto offsets = (uint64_t*)tile->data();
    auto data = (unsigned char*)tile_var->data();
    auto cell_num = tile->cell_num();
    auto tile_var_size = tile_var->size();

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
Status Reader::dedup_coords(
    std::list<std::shared_ptr<OverlappingCoords<T>>>* coords) const {
  auto coords_size = array_schema_->coords_size();
  auto it = coords->begin();
  while (it != coords->end()) {
    auto next_it = std::next(it);
    if (next_it != coords->end() &&
        !std::memcmp(
            it->get()->coords_, next_it->get()->coords_, coords_size)) {
      if (it->get()->tile_.get()->fragment_idx_ <
          next_it->get()->tile_.get()->fragment_idx_) {
        it = coords->erase(it);
      } else {
        coords->erase(next_it);
      }
    } else {
      ++it;
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
    subarray[i] = ((T*)subarray_)[i];

  // Get overlapping sparse tile indexes
  OverlappingTileVec sparse_tiles;
  RETURN_NOT_OK(compute_overlapping_tiles<T>(&sparse_tiles));

  // Read sparse tiles
  RETURN_NOT_OK(read_tiles(constants::coords, &sparse_tiles));
  for (const auto& attr : attributes_) {
    if (attr != constants::coords)
      RETURN_NOT_OK(read_tiles(attr, &sparse_tiles));
  }

  // Compute the read coordinates for all sparse fragments
  std::list<std::shared_ptr<OverlappingCoords<T>>> coords;
  RETURN_NOT_OK(compute_overlapping_coords<T>(sparse_tiles, &coords));

  // Sort and dedup the coordinates (not applicable to the global order
  // layout for a single fragment)
  if (!(fragment_metadata_.size() == 1 && layout_ == Layout::GLOBAL_ORDER)) {
    RETURN_NOT_OK(sort_coords<T>(&coords));
    RETURN_NOT_OK(dedup_coords<T>(&coords));
  }

  // For each tile, initialize a dense cell range iterator per
  // (dense) fragment
  std::vector<std::vector<DenseCellRangeIter<T>>> dense_frag_its;
  std::unordered_map<uint64_t, std::pair<uint64_t, std::vector<T>>>
      overlapping_tile_idx_coords;
  RETURN_NOT_OK(init_tile_fragment_dense_cell_range_iters(
      &dense_frag_its, &overlapping_tile_idx_coords));

  // Get the cell ranges
  std::list<DenseCellRange<T>> dense_cell_ranges;
  DenseCellRangeIter<T> it(domain, subarray, layout_);
  RETURN_NOT_OK(it.begin());
  while (!it.end()) {
    auto o_it = overlapping_tile_idx_coords.find(it.tile_idx());
    assert(o_it != overlapping_tile_idx_coords.end());
    RETURN_NOT_OK(compute_dense_cell_ranges<T>(
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
  RETURN_NOT_OK(compute_dense_overlapping_tiles_and_cell_ranges<T>(
      dense_cell_ranges, coords, &dense_tiles, &overlapping_cell_ranges));
  coords.clear();
  dense_cell_ranges.clear();
  overlapping_tile_idx_coords.clear();

  // Read dense tiles
  for (const auto& attr : attributes_)
    RETURN_NOT_OK(read_tiles(attr, &dense_tiles));

  // Copy cells
  for (const auto& attr : attributes_)
    RETURN_NOT_OK(copy_cells(attr, overlapping_cell_ranges));

  return Status::Ok();
}

template <class T>
Status Reader::get_all_coords(
    const std::shared_ptr<OverlappingTile>& tile,
    std::list<std::shared_ptr<OverlappingCoords<T>>>* coords) const {
  auto dim_num = array_schema_->dim_num();
  const auto& t = tile->attr_tiles_.find(constants::coords)->second.first;
  auto t_ptr = t.get();
  auto coords_num = t_ptr->cell_num();
  auto c = (T*)t_ptr->data();

  for (uint64_t i = 0; i < coords_num; ++i)
    coords->emplace_back(
        std::make_shared<OverlappingCoords<T>>(tile, &c[i * dim_num], i));

  return Status::Ok();
}

template <class T>
Status Reader::handle_coords_in_dense_cell_range(
    const std::shared_ptr<OverlappingTile>& cur_tile,
    const T* cur_tile_coords,
    uint64_t* start,
    uint64_t end,
    uint64_t coords_size,
    const std::list<std::shared_ptr<OverlappingCoords<T>>>& coords,
    typename std::list<std::shared_ptr<OverlappingCoords<T>>>::const_iterator*
        coords_it,
    std::shared_ptr<OverlappingTile>* coords_tile,
    uint64_t* coords_pos,
    unsigned* coords_fidx,
    std::vector<T>* coords_tile_coords,
    OverlappingCellRangeList* overlapping_cell_ranges) const {
  auto domain = array_schema_->domain();

  // While the coords are within the same dense cell range
  while (*coords_it != coords.end() &&
         !memcmp(&(*coords_tile_coords)[0], cur_tile_coords, coords_size) &&
         *coords_pos >= *start && *coords_pos <= end) {
    if (*coords_fidx < cur_tile->fragment_idx_) {  // Skip coords
      ++(*coords_it);
      if (*coords_it != coords.end()) {
        domain->get_tile_coords(
            (*coords_it)->get()->coords_, &(*coords_tile_coords)[0]);
        RETURN_NOT_OK(
            domain->get_cell_pos<T>((*coords_it)->get()->coords_, coords_pos));
        *coords_fidx = (*coords_it)->get()->tile_->fragment_idx_;
        *coords_tile = (*coords_it)->get()->tile_;
      }
      continue;
    } else {  // Break dense range
      // Left range
      if (*coords_pos > *start) {
        overlapping_cell_ranges->emplace_back(
            std::make_shared<OverlappingCellRange>(
                cur_tile, *start, *coords_pos - 1));
      }
      // Coords unary range
      overlapping_cell_ranges->emplace_back(
          std::make_shared<OverlappingCellRange>(
              *coords_tile,
              (*coords_it)->get()->pos_,
              (*coords_it)->get()->pos_));
      // Update start
      *start = *coords_pos + 1;

      // Advance coords
      ++(*coords_it);
      if (*coords_it != coords.end()) {
        domain->get_tile_coords(
            (*coords_it)->get()->coords_, &(*coords_tile_coords)[0]);
        RETURN_NOT_OK(
            domain->get_cell_pos<T>((*coords_it)->get()->coords_, coords_pos));
        *coords_fidx = (*coords_it)->get()->tile_->fragment_idx_;
        *coords_tile = (*coords_it)->get()->tile_;
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
    subarray[i] = ((T*)subarray_)[i];

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

Status Reader::read_tiles(
    const std::string& attribute, OverlappingTileVec* tiles) const {
  // Prepare tile IO
  auto var_size = array_schema_->var_size(attribute);
  std::vector<std::shared_ptr<TileIO>> tile_io;
  std::vector<std::shared_ptr<TileIO>> tile_io_var;
  for (const auto& f : fragment_metadata_) {
    tile_io.emplace_back(std::make_shared<TileIO>(
        storage_manager_, f->attr_uri(attribute), f->file_sizes(attribute)));
    if (var_size)
      tile_io_var.emplace_back(std::make_shared<TileIO>(
          storage_manager_,
          f->attr_var_uri(attribute),
          f->file_var_sizes(attribute)));
    else
      tile_io_var.emplace_back();
  }
  // For each fragment, read the tiles
  for (auto& tile : *tiles) {
    auto& tile_pair = tile->attr_tiles_[attribute];

    // Initialize
    auto t = std::make_shared<Tile>();
    auto t_var = std::shared_ptr<Tile>(nullptr);
    if (!var_size) {
      RETURN_NOT_OK(init_tile(attribute, t.get()));
    } else {
      t_var = std::make_shared<Tile>();
      RETURN_NOT_OK(init_tile(attribute, t.get(), t_var.get()));
    }

    // Read
    RETURN_NOT_OK(tile_io[tile->fragment_idx_]->read(
        t.get(),
        fragment_metadata_[tile->fragment_idx_]->file_offset(
            attribute, tile->tile_idx_),
        fragment_metadata_[tile->fragment_idx_]->compressed_tile_size(
            attribute, tile->tile_idx_),
        fragment_metadata_[tile->fragment_idx_]->tile_size(
            attribute, tile->tile_idx_)));
    tile_pair.first = t;
    if (var_size) {
      RETURN_NOT_OK(tile_io_var[tile->fragment_idx_]->read(
          t_var.get(),
          fragment_metadata_[tile->fragment_idx_]->file_var_offset(
              attribute, tile->tile_idx_),
          fragment_metadata_[tile->fragment_idx_]->compressed_tile_var_size(
              attribute, tile->tile_idx_),
          fragment_metadata_[tile->fragment_idx_]->tile_var_size(
              attribute, tile->tile_idx_)));
    }
    tile_pair.second = t_var;
  }

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
  for (const auto& attr : attributes_vec)
    attributes_.emplace_back(attr);

  return Status::Ok();
}

template <class T>
Status Reader::sort_coords(
    std::list<std::shared_ptr<OverlappingCoords<T>>>* coords) const {
  if (layout_ == Layout::GLOBAL_ORDER) {
    auto domain = array_schema_->domain();
    coords->sort(GlobalCmp<T>(domain));
  } else {
    auto dim_num = array_schema_->dim_num();
    if (layout_ == Layout::ROW_MAJOR)
      coords->sort(RowCmp<T>(dim_num));
    else if (layout_ == Layout::COL_MAJOR)
      coords->sort(ColCmp<T>(dim_num));
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
  RETURN_NOT_OK(compute_overlapping_tiles<T>(&tiles));

  // Read tiles
  RETURN_NOT_OK(read_tiles(constants::coords, &tiles));
  for (const auto& attr : attributes_) {
    if (attr != constants::coords)
      RETURN_NOT_OK(read_tiles(attr, &tiles));
  }

  // Compute the read coordinates for all fragments
  std::list<std::shared_ptr<OverlappingCoords<T>>> coords;
  RETURN_NOT_OK(compute_overlapping_coords<T>(tiles, &coords));

  // Sort and dedup the coordinates (not applicable to the global order
  // layout for a single fragment)
  if (!(fragment_metadata_.size() == 1 && layout_ == Layout::GLOBAL_ORDER)) {
    RETURN_NOT_OK(sort_coords<T>(&coords));
    RETURN_NOT_OK(dedup_coords<T>(&coords));
  }

  // Compute the maximal cell ranges
  OverlappingCellRangeList cell_ranges;
  RETURN_NOT_OK(compute_cell_ranges(coords, &cell_ranges));
  coords.clear();

  // Copy cells
  for (const auto& attr : attributes_)
    RETURN_NOT_OK(copy_cells(attr, cell_ranges));

  return Status::Ok();
}

void Reader::zero_out_buffer_sizes() {
  for (auto& attr_buffer : attr_buffers_) {
    *(attr_buffer.second.buffer_size_) = 0;
    *(attr_buffer.second.buffer_var_size_) = 0;
  }
}

}  // namespace sm
}  // namespace tiledb
