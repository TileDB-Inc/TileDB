/**
 * @file   read_state.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017 TileDB, Inc.
 * @copyright Copyright (c) 2016 MIT and Intel Corporation
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
 * This file implements the ReadState class.
 */

#include "logger.h"
#include "posix_filesystem.h"
#include "query.h"
#include "utils.h"

/* ****************************** */
/*             MACROS             */
/* ****************************** */

#define MIN(a, b) ((a) < (b) ? (a) : (b))
#define MAX(a, b) ((a) > (b) ? (a) : (b))

namespace tiledb {

/* ****************************** */
/*        STATIC CONSTANTS        */
/* ****************************** */

const uint64_t ReadState::INVALID_UINT64 = UINT64_MAX;

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

ReadState::ReadState(
    const Fragment* fragment, Query* query, FragmentMetadata* metadata)
    : fragment_(fragment)
    , metadata_(metadata)
    , query_(query) {
  array_metadata_ = query_->array_metadata();
  attribute_num_ = array_metadata_->attribute_num();
  coords_size_ = array_metadata_->coords_size();
  done_ = false;
  last_tile_coords_ = nullptr;
  search_tile_overlap_subarray_ = std::malloc(2 * coords_size_);
  search_tile_pos_ = INVALID_UINT64;

  tile_coords_aux_ = std::malloc(coords_size_);

  init_tiles();
  init_tile_io();
  init_overflow();
  init_fetched_tiles();
  init_empty_attributes();
  compute_tile_search_range();
}

ReadState::~ReadState() {
  if (last_tile_coords_ != nullptr)
    std::free(last_tile_coords_);

  if (tile_coords_aux_ != nullptr)
    std::free(tile_coords_aux_);

  for (auto& tile : tiles_)
    delete tile;

  for (auto& tile_var : tiles_var_)
    delete tile_var;

  for (auto& tile_io : tile_io_)
    delete tile_io;

  for (auto& tile_io_var : tile_io_var_)
    delete tile_io_var;

  if (search_tile_overlap_subarray_ != nullptr)
    std::free(search_tile_overlap_subarray_);
}

/* ****************************** */
/*              API               */
/* ****************************** */

Status ReadState::copy_cells(
    unsigned int attribute_id,
    uint64_t tile_i,
    void* buffer,
    uint64_t buffer_size,
    uint64_t* buffer_offset,
    const CellPosRange& cell_pos_range) {
  // Sanity check
  assert(!array_metadata_->var_size(attribute_id));

  // Trivial case
  if (is_empty_attribute(attribute_id))
    return Status::Ok();

  // For easy reference
  uint64_t cell_size = array_metadata_->cell_size(attribute_id);

  // Prepare attribute tile
  RETURN_NOT_OK(read_tile(attribute_id, tile_i));

  auto tile = tiles_[attribute_id];

  // Calculate start and end offset in the tile
  uint64_t start_offset = cell_pos_range.first * cell_size;
  uint64_t end_offset = (cell_pos_range.second + 1) * cell_size - 1;

  // Potentially set the tile offset to the beginning of the current range
  if (tile->offset() < start_offset)
    tile->set_offset(start_offset);
  else if (tile->offset() > end_offset)  // This range is written
    return Status::Ok();

  // Calculate the total size to copy
  uint64_t buffer_free_space = buffer_size - *buffer_offset;
  buffer_free_space = (buffer_free_space / cell_size) * cell_size;
  uint64_t bytes_left_to_copy = end_offset - tile->offset() + 1;
  uint64_t bytes_to_copy = MIN(bytes_left_to_copy, buffer_free_space);

  // Copy and update current buffer and tile offsets
  char* buffer_c = static_cast<char*>(buffer) + *buffer_offset;
  if (bytes_to_copy != 0) {
    RETURN_NOT_OK(tile->read(buffer_c, bytes_to_copy));
    *buffer_offset += bytes_to_copy;
  }

  // Handle buffer overflow
  if (tile->offset() != end_offset + 1)
    overflow_[attribute_id] = true;

  // Success
  return Status::Ok();
}

Status ReadState::copy_cells_var(
    unsigned int attribute_id,
    uint64_t tile_i,
    void* buffer,
    uint64_t buffer_size,
    uint64_t* buffer_offset,
    void* buffer_var,
    uint64_t buffer_var_size,
    uint64_t* buffer_var_offset,
    const CellPosRange& cell_pos_range) {
  // Sanity check
  assert(array_metadata_->var_size(attribute_id));

  // For easy reference
  uint64_t cell_size = constants::cell_var_offset_size;

  // Prepare attribute tile
  RETURN_NOT_OK(read_tile_var(attribute_id, tile_i));

  auto tile = tiles_[attribute_id];
  auto tile_var = tiles_var_[attribute_id];

  // Calculate start and end offset in the tile
  uint64_t start_offset = cell_pos_range.first * cell_size;
  uint64_t end_offset = (cell_pos_range.second + 1) * cell_size - 1;

  // Potentially set the tile offset to the beginning of the current range
  if (tile->offset() < start_offset)
    tile->set_offset(start_offset);
  else if (tile->offset() > end_offset)  // This range is written
    return Status::Ok();

  // Calculate the total size to copy
  uint64_t buffer_free_space = buffer_size - *buffer_offset;
  buffer_free_space = (buffer_free_space / cell_size) * cell_size;
  uint64_t buffer_var_free_space = buffer_var_size - *buffer_var_offset;
  uint64_t bytes_left_to_copy = end_offset - tile->offset() + 1;
  uint64_t bytes_to_copy = MIN(bytes_left_to_copy, buffer_free_space);
  uint64_t bytes_var_to_copy;

  // Compute actual cells to copy
  uint64_t start_cell_pos = tile->offset() / cell_size;
  uint64_t end_cell_pos = start_cell_pos + bytes_to_copy / cell_size - 1;
  uint64_t tile_var_size = metadata_->tile_var_sizes()[attribute_id][tile_i];

  RETURN_NOT_OK(compute_bytes_to_copy(
      attribute_id,
      tile_var_size,
      start_cell_pos,
      &end_cell_pos,
      buffer_free_space,
      buffer_var_free_space,
      &bytes_to_copy,
      &bytes_var_to_copy));

  // For easy reference
  void* buffer_start = static_cast<char*>(buffer) + *buffer_offset;

  // Potentially update tile offset to the beginning of the overlap range
  const uint64_t* tile_var_start;
  RETURN_NOT_OK(get_offset(attribute_id, start_cell_pos, &tile_var_start));
  if (tile_var->offset() < *tile_var_start)
    tile_var->set_offset(*tile_var_start);

  // Copy and update current buffer and tile offsets
  if (bytes_to_copy != 0) {
    RETURN_NOT_OK(tile->read(buffer_start, bytes_to_copy));
    *buffer_offset += bytes_to_copy;

    // Shift variable offsets
    shift_var_offsets(
        buffer_start, end_cell_pos - start_cell_pos + 1, *buffer_var_offset);

    char* buffer_var_c = static_cast<char*>(buffer_var) + *buffer_var_offset;
    RETURN_NOT_OK(tile_var->read(buffer_var_c, bytes_var_to_copy));
    *buffer_var_offset += bytes_var_to_copy;
  }

  // Check for overflow
  if (tile->offset() != end_offset + 1)
    overflow_[attribute_id] = true;

  // Entering this if condition implies that the var data in this cell is so
  // large that the allocated buffer cannot hold it
  if (*buffer_offset == 0u && bytes_to_copy == 0u)
    overflow_[attribute_id] = true;

  return Status::Ok();
}

bool ReadState::dense() const {
  return fragment_->dense();
}

bool ReadState::done() const {
  return done_;
}

void ReadState::get_bounding_coords(void* bounding_coords) const {
  // For easy reference
  uint64_t pos = search_tile_pos_;
  assert(pos != INVALID_UINT64);
  std::memcpy(
      bounding_coords, metadata_->bounding_coords()[pos], 2 * coords_size_);
}

template <class T>
Status ReadState::get_coords_after(
    const T* coords, T* coords_after, bool* coords_retrieved) {
  // For easy reference
  uint64_t cell_num = metadata_->cell_num(search_tile_pos_);

  // Prepare attribute tile
  RETURN_NOT_OK(read_tile(attribute_num_ + 1, search_tile_pos_));

  // Compute the cell position at or after the coords
  uint64_t coords_after_pos = INVALID_UINT64;
  RETURN_NOT_OK(get_cell_pos_after(coords, &coords_after_pos));

  // Invalid position
  if (coords_after_pos >= cell_num) {
    *coords_retrieved = false;
    return Status::Ok();
  }

  // Copy result
  RETURN_NOT_OK(read_from_tile(
      attribute_num_ + 1,
      coords_after,
      coords_after_pos * coords_size_,
      coords_size_));
  *coords_retrieved = true;

  // Success
  return Status::Ok();
}

template <class T>
Status ReadState::get_enclosing_coords(
    uint64_t tile_i,
    const T* target_coords,
    const T* start_coords,
    const T* end_coords,
    T* left_coords,
    T* right_coords,
    bool* left_retrieved,
    bool* right_retrieved,
    bool* target_exists) {
  // Prepare attribute tile
  RETURN_NOT_OK(read_tile(attribute_num_ + 1, tile_i));

  // Compute the appropriate cell positions
  uint64_t start_pos = INVALID_UINT64;
  uint64_t end_pos = INVALID_UINT64;
  uint64_t target_pos = INVALID_UINT64;
  RETURN_NOT_OK(get_cell_pos_at_or_after(start_coords, &start_pos));
  RETURN_NOT_OK(get_cell_pos_at_or_before(end_coords, &end_pos));
  RETURN_NOT_OK(get_cell_pos_at_or_before(target_coords, &target_pos));

  // Check if target exists
  if (target_pos != INVALID_UINT64 && target_pos >= start_pos &&
      end_pos != INVALID_UINT64 && target_pos <= end_pos) {
    RETURN_NOT_OK(cmp_coords_to_search_tile(
        target_coords, target_pos * coords_size_, target_exists));
  } else {
    *target_exists = false;
  }

  // Calculate left and right pos
  uint64_t left_pos;
  if (*target_exists)
    left_pos = (target_pos == 0 || target_pos == INVALID_UINT64) ?
                   INVALID_UINT64 :
                   target_pos - 1;
  else
    left_pos = target_pos;
  uint64_t right_pos =
      (target_pos == INVALID_UINT64) ? INVALID_UINT64 : target_pos + 1;

  // Copy left if it exists
  if (left_pos != INVALID_UINT64 && left_pos >= start_pos &&
      end_pos != INVALID_UINT64 && left_pos <= end_pos) {
    RETURN_NOT_OK(read_from_tile(
        attribute_num_ + 1,
        left_coords,
        left_pos * coords_size_,
        coords_size_));
    *left_retrieved = true;
  } else {
    *left_retrieved = false;
  }

  // Copy right if it exists
  if (right_pos != INVALID_UINT64 && right_pos >= start_pos &&
      end_pos != INVALID_UINT64 && right_pos <= end_pos) {
    RETURN_NOT_OK(read_from_tile(
        attribute_num_ + 1,
        right_coords,
        right_pos * coords_size_,
        coords_size_));
    *right_retrieved = true;
  } else {
    *right_retrieved = false;
  }

  // Success
  return Status::Ok();
}

template <class T>
Status ReadState::get_fragment_cell_pos_range_sparse(
    const FragmentInfo& fragment_info,
    const T* cell_range,
    FragmentCellPosRange* fragment_cell_pos_range) {
  // For easy reference
  unsigned int dim_num = array_metadata_->dim_num();
  uint64_t tile_i = fragment_info.second;

  // Prepare attribute tile
  RETURN_NOT_OK(read_tile(attribute_num_ + 1, tile_i));

  // Compute the appropriate cell positions
  uint64_t start_pos = INVALID_UINT64;
  uint64_t end_pos = INVALID_UINT64;
  RETURN_NOT_OK(get_cell_pos_at_or_after(cell_range, &start_pos));
  RETURN_NOT_OK(get_cell_pos_at_or_before(&cell_range[dim_num], &end_pos));

  // Create the result
  fragment_cell_pos_range->first = fragment_info;
  if (end_pos != UINT64_MAX && start_pos <= end_pos)  // There are results
    fragment_cell_pos_range->second = CellPosRange(start_pos, end_pos);
  else  // There are NO results
    fragment_cell_pos_range->second =
        CellPosRange(INVALID_UINT64, INVALID_UINT64);

  // Success
  return Status::Ok();
}

template <class T>
Status ReadState::get_fragment_cell_ranges_dense(
    unsigned int fragment_i, FragmentCellRanges* fragment_cell_ranges) {
  // Trivial cases
  if (done_ || !search_tile_overlap_)
    return Status::Ok();

  // For easy reference
  unsigned int dim_num = array_metadata_->dim_num();
  Layout cell_order = array_metadata_->cell_order();
  uint64_t cell_range_size = 2 * coords_size_;
  auto search_tile_overlap_subarray =
      static_cast<const T*>(search_tile_overlap_subarray_);
  FragmentInfo fragment_info = FragmentInfo(fragment_i, search_tile_pos_);

  // Contiguous cells, single cell range
  if (search_tile_overlap_ == 1 || search_tile_overlap_ == 3) {
    void* cell_range = std::malloc(cell_range_size);
    auto cell_range_T = static_cast<T*>(cell_range);
    for (unsigned int i = 0; i < dim_num; ++i) {
      cell_range_T[i] = search_tile_overlap_subarray[2 * i];
      cell_range_T[dim_num + i] = search_tile_overlap_subarray[2 * i + 1];
    }
    // Insert the new range into the result vector
    fragment_cell_ranges->emplace_back(fragment_info, cell_range);
  } else {  // Non-contiguous cells, multiple ranges
    // Initialize the coordinates at the beginning of the global range
    auto coords = new T[dim_num];
    for (unsigned int i = 0; i < dim_num; ++i)
      coords[i] = search_tile_overlap_subarray[2 * i];

    // Handle the different cell orders
    unsigned int i = 0;
    if (cell_order == Layout::ROW_MAJOR) {  // ROW
      while (coords[0] <= search_tile_overlap_subarray[1]) {
        // Make a cell range representing a slab
        void* cell_range = std::malloc(cell_range_size);
        auto cell_range_T = static_cast<T*>(cell_range);
        for (unsigned int j = 0; j < dim_num - 1; ++j) {
          cell_range_T[j] = coords[j];
          cell_range_T[dim_num + j] = coords[j];
        }
        cell_range_T[dim_num - 1] =
            search_tile_overlap_subarray[2 * (dim_num - 1)];
        cell_range_T[2 * dim_num - 1] =
            search_tile_overlap_subarray[2 * (dim_num - 1) + 1];

        // Insert the new range into the result vector
        fragment_cell_ranges->emplace_back(fragment_info, cell_range);

        // Advance coordinates
        if (dim_num == 1) {
          break;
        } else {
          i = dim_num - 2;
          ++coords[i];
          while (i > 0 && coords[i] > search_tile_overlap_subarray[2 * i + 1]) {
            coords[i] = search_tile_overlap_subarray[2 * i];
            ++coords[--i];
          }
        }
      }
    } else if (cell_order == Layout::COL_MAJOR) {  // COLUMN
      while (coords[dim_num - 1] <=
             search_tile_overlap_subarray[2 * (dim_num - 1) + 1]) {
        // Make a cell range representing a slab
        void* cell_range = std::malloc(cell_range_size);
        auto cell_range_T = static_cast<T*>(cell_range);
        for (unsigned int j = dim_num - 1; j > 0; --j) {
          cell_range_T[j] = coords[j];
          cell_range_T[dim_num + j] = coords[j];
        }
        cell_range_T[0] = search_tile_overlap_subarray[0];
        cell_range_T[dim_num] = search_tile_overlap_subarray[1];

        // Insert the new range into the result vector
        fragment_cell_ranges->emplace_back(fragment_info, cell_range);

        if (dim_num == 1) {
          break;
        } else {
          // Advance coordinates
          i = 1;
          ++coords[i];
          while (i < dim_num - 1 &&
                 coords[i] > search_tile_overlap_subarray[2 * i + 1]) {
            coords[i] = search_tile_overlap_subarray[2 * i];
            ++coords[++i];
          }
        }
      }
    } else {
      assert(0);
    }

    // Clean up
    delete[] coords;
  }

  // Success
  return Status::Ok();
}

template <class T>
Status ReadState::get_fragment_cell_ranges_sparse(
    unsigned int fragment_i, FragmentCellRanges* fragment_cell_ranges) {
  // Trivial cases
  if (done_ || !search_tile_overlap_ || !mbr_tile_overlap_)
    return Status::Ok();

  // For easy reference
  auto dim_num = array_metadata_->dim_num();
  auto search_tile_overlap_subarray =
      static_cast<const T*>(search_tile_overlap_subarray_);

  // Create start and end coordinates for the overlap
  auto start_coords = new T[dim_num];
  auto end_coords = new T[dim_num];
  for (unsigned int i = 0; i < dim_num; ++i) {
    start_coords[i] = search_tile_overlap_subarray[2 * i];
    end_coords[i] = search_tile_overlap_subarray[2 * i + 1];
  }

  // Get fragment cell ranges inside range [start_coords, end_coords]
  Status st = get_fragment_cell_ranges_sparse(
      fragment_i, start_coords, end_coords, fragment_cell_ranges);

  delete[] start_coords;
  delete[] end_coords;

  return st;
}

template <class T>
Status ReadState::get_fragment_cell_ranges_sparse(
    unsigned int fragment_i,
    const T* start_coords,
    const T* end_coords,
    FragmentCellRanges* fragment_cell_ranges) {
  // Sanity checks
  assert(
      search_tile_pos_ >= tile_search_range_[0] &&
      search_tile_pos_ <= tile_search_range_[1]);
  assert(search_tile_overlap_);

  // For easy reference
  unsigned int dim_num = array_metadata_->dim_num();
  auto subarray = static_cast<const T*>(query_->subarray());

  // Handle full overlap
  if (search_tile_overlap_ == 1) {
    FragmentCellRange fragment_cell_range;
    fragment_cell_range.first = FragmentInfo(fragment_i, search_tile_pos_);
    fragment_cell_range.second = std::malloc(2 * coords_size_);
    auto cell_range = static_cast<T*>(fragment_cell_range.second);
    std::memcpy(cell_range, start_coords, coords_size_);
    std::memcpy(&cell_range[dim_num], end_coords, coords_size_);
    fragment_cell_ranges->emplace_back(fragment_cell_range);
    return Status::Ok();
  }

  // Prepare attribute tile
  RETURN_NOT_OK(read_tile(attribute_num_ + 1, search_tile_pos_));

  // Get cell positions for the cell range
  uint64_t start_pos = INVALID_UINT64;
  uint64_t end_pos = INVALID_UINT64;
  RETURN_NOT_OK(get_cell_pos_at_or_after(start_coords, &start_pos));
  RETURN_NOT_OK(get_cell_pos_at_or_before(end_coords, &end_pos));

  // Get the cell ranges
  const void* cell;
  uint64_t current_start_pos = start_pos;
  uint64_t current_end_pos = INVALID_UINT64;
  if (end_pos != INVALID_UINT64) {
    for (uint64_t i = start_pos; i <= end_pos; ++i) {
      RETURN_NOT_OK(get_coords_from_search_tile(i, &cell));

      if (utils::cell_in_subarray<T>(
              static_cast<const T*>(cell), subarray, dim_num)) {
        if (i > 0 && i - 1 == current_end_pos) {  // The range is expanded
          ++current_end_pos;
        } else {  // A new range starts
          current_start_pos = i;
          current_end_pos = i;
        }
      } else {
        if (i > 0 && i - 1 == current_end_pos) {
          // The range needs to be added to the list
          FragmentCellRange fragment_cell_range;
          fragment_cell_range.first =
              FragmentInfo(fragment_i, search_tile_pos_);
          fragment_cell_range.second = std::malloc(2 * coords_size_);
          auto cell_range = static_cast<T*>(fragment_cell_range.second);

          RETURN_NOT_OK(read_from_tile(
              attribute_num_ + 1,
              cell_range,
              current_start_pos * coords_size_,
              coords_size_));

          RETURN_NOT_OK(read_from_tile(
              attribute_num_ + 1,
              &cell_range[dim_num],
              current_end_pos * coords_size_,
              coords_size_));

          fragment_cell_ranges->emplace_back(fragment_cell_range);
          current_end_pos = INVALID_UINT64;  // No active range
        }
      }
    }
  }

  // Add last cell range
  if (current_end_pos != INVALID_UINT64) {
    FragmentCellRange fragment_cell_range;
    fragment_cell_range.first = FragmentInfo(fragment_i, search_tile_pos_);
    fragment_cell_range.second = std::malloc(2 * coords_size_);
    auto cell_range = static_cast<T*>(fragment_cell_range.second);

    RETURN_NOT_OK(read_from_tile(
        attribute_num_ + 1,
        cell_range,
        current_start_pos * coords_size_,
        coords_size_));

    RETURN_NOT_OK(read_from_tile(
        attribute_num_ + 1,
        &cell_range[dim_num],
        current_end_pos * coords_size_,
        coords_size_));

    fragment_cell_ranges->emplace_back(fragment_cell_range);
  }

  // Success
  return Status::Ok();
}

template <class T>
void ReadState::get_next_overlapping_tile_dense(const T* tile_coords) {
  // Trivial case
  if (done_)
    return;

  // For easy reference
  unsigned int dim_num = array_metadata_->dim_num();
  auto domain = array_metadata_->domain();
  auto array_domain = static_cast<const T*>(domain->domain());
  auto subarray = static_cast<const T*>(query_->subarray());
  auto metadata_domain = static_cast<const T*>(metadata_->domain());
  auto non_empty_domain = static_cast<const T*>(metadata_->non_empty_domain());

  // Compute the tile subarray
  auto tile_subarray = new T[2 * dim_num];
  domain->get_tile_subarray(tile_coords, tile_subarray);

  // Compute overlap of tile subarray with non-empty fragment domain
  auto tile_domain_overlap_subarray = new T[2 * dim_num];
  auto tile_domain_overlap = (bool)domain->subarray_overlap(
      tile_subarray, non_empty_domain, tile_domain_overlap_subarray);

  if (!tile_domain_overlap) {  // No overlap with the input tile
    search_tile_overlap_ = 0;
    subarray_area_covered_ = false;
  } else {  // Overlap with the input tile
    // Find the search tile position
    auto tile_extents = static_cast<const T*>(domain->tile_extents());
    auto tile_coords_norm = new T[dim_num];
    for (unsigned int i = 0; i < dim_num; ++i)
      tile_coords_norm[i] =
          tile_coords[i] -
          (metadata_domain[2 * i] - array_domain[2 * i]) / tile_extents[i];
    search_tile_pos_ = domain->get_tile_pos(metadata_domain, tile_coords_norm);
    delete[] tile_coords_norm;

    // Compute overlap of the query subarray with tile
    auto query_tile_overlap_subarray = new T[2 * dim_num];
    domain->subarray_overlap(
        subarray, tile_subarray, query_tile_overlap_subarray);

    // Compute the overlap of the previous results with the non-empty domain
    auto search_tile_overlap_subarray = (T*)search_tile_overlap_subarray_;
    auto overlap = (bool)domain->subarray_overlap(
        query_tile_overlap_subarray,
        tile_domain_overlap_subarray,
        search_tile_overlap_subarray);

    if (!overlap) {
      search_tile_overlap_ = 0;
      subarray_area_covered_ = false;
    } else {
      // Find the type of the search tile overlap
      auto temp = new T[2 * dim_num];
      search_tile_overlap_ = domain->subarray_overlap(
          search_tile_overlap_subarray, tile_subarray, temp);

      // Check if fragment fully covers the tile
      subarray_area_covered_ = utils::is_contained<T>(
          query_tile_overlap_subarray, tile_domain_overlap_subarray, dim_num);

      // Clean up
      delete[] temp;
    }

    // Clean up
    delete[] query_tile_overlap_subarray;
  }

  // Clean up
  delete[] tile_subarray;
  delete[] tile_domain_overlap_subarray;
}

template <class T>
void ReadState::get_next_overlapping_tile_sparse() {
  // Trivial case
  if (done_)
    return;

  // For easy reference
  const std::vector<void*>& mbrs = metadata_->mbrs();
  auto subarray = static_cast<const T*>(query_->subarray());

  // Update the search tile position
  if (search_tile_pos_ == INVALID_UINT64)
    search_tile_pos_ = tile_search_range_[0];
  else
    ++search_tile_pos_;

  // Find the position to the next overlapping tile with the query range
  for (;;) {
    // No overlap - exit
    if (search_tile_pos_ > tile_search_range_[1]) {
      done_ = true;
      return;
    }

    auto mbr = static_cast<const T*>(mbrs[search_tile_pos_]);
    search_tile_overlap_ = array_metadata_->domain()->subarray_overlap(
        subarray, mbr, static_cast<T*>(search_tile_overlap_subarray_));

    if (!search_tile_overlap_)
      ++search_tile_pos_;
    else
      return;
  }
}

template <class T>
void ReadState::get_next_overlapping_tile_sparse(const T* tile_coords) {
  // Trivial case
  if (done_)
    return;

  // For easy reference
  unsigned int dim_num = array_metadata_->dim_num();
  const std::vector<void*>& mbrs = metadata_->mbrs();
  auto subarray = static_cast<const T*>(query_->subarray());
  auto domain = array_metadata_->domain();

  // Compute the tile subarray
  auto tile_subarray = new T[2 * dim_num];
  auto mbr_tile_overlap_subarray = new T[2 * dim_num];
  auto tile_subarray_end = new T[dim_num];
  domain->get_tile_subarray(tile_coords, tile_subarray);
  for (unsigned int i = 0; i < dim_num; ++i)
    tile_subarray_end[i] = tile_subarray[2 * i + 1];

  // Update the search tile position
  if (search_tile_pos_ == INVALID_UINT64)
    search_tile_pos_ = tile_search_range_[0];

  // Reset overlaps
  search_tile_overlap_ = 0;
  mbr_tile_overlap_ = 0;

  // Check against last coordinates
  if (last_tile_coords_ == nullptr) {
    last_tile_coords_ = std::malloc(coords_size_);
    std::memcpy(last_tile_coords_, tile_coords, coords_size_);
  } else {
    if (!std::memcmp(last_tile_coords_, tile_coords, coords_size_)) {
      // Advance only if the MBR does not exceed the tile
      auto bounding_coords =
          static_cast<const T*>(metadata_->bounding_coords()[search_tile_pos_]);
      if (domain->tile_cell_order_cmp(
              &bounding_coords[dim_num],
              tile_subarray_end,
              (T*)tile_coords_aux_) <= 0) {
        ++search_tile_pos_;
      } else {
        delete[] tile_subarray;
        delete[] tile_subarray_end;
        delete[] mbr_tile_overlap_subarray;
        return;
      }
    } else {
      std::memcpy(last_tile_coords_, tile_coords, coords_size_);
    }
  }

  // Find the position to the next overlapping tile with the input tile
  for (;;) {
    // No overlap - exit
    if (search_tile_pos_ > tile_search_range_[1]) {
      done_ = true;
      break;
    }

    // Get overlap between MBR and tile subarray
    auto mbr = static_cast<const T*>(mbrs[search_tile_pos_]);
    mbr_tile_overlap_ =
        domain->subarray_overlap(tile_subarray, mbr, mbr_tile_overlap_subarray);

    // No overlap with the tile
    if (!mbr_tile_overlap_) {
      // Check if we need to break or continue
      auto bounding_coords =
          static_cast<const T*>(metadata_->bounding_coords()[search_tile_pos_]);
      if (domain->tile_cell_order_cmp(
              &bounding_coords[dim_num],
              tile_subarray_end,
              (T*)tile_coords_aux_) > 0)
        break;
      ++search_tile_pos_;
      continue;
    }

    // Get overlap of MBR with the query inside the tile subarray
    search_tile_overlap_ = domain->subarray_overlap(
        subarray,
        mbr_tile_overlap_subarray,
        static_cast<T*>(search_tile_overlap_subarray_));

    // Update the search tile overlap if necessary
    if (search_tile_overlap_) {
      // The overlap is full only when both the MBR-tile and MBR-tile-subarray
      // overlaps are full
      search_tile_overlap_ =
          (mbr_tile_overlap_ == 1 && search_tile_overlap_ == 1) ? 1 : 2;
    }

    // The MBR overlaps with the tile. Regardless of overlap with the
    // query in the tile, break.
    break;
  }

  // Clean up
  delete[] tile_subarray;
  delete[] tile_subarray_end;
  delete[] mbr_tile_overlap_subarray;
}

bool ReadState::mbr_overlaps_tile() const {
  return (bool)mbr_tile_overlap_;
}

bool ReadState::overflow(unsigned int attribute_id) const {
  return overflow_[attribute_id];
}

void ReadState::reset_overflow() {
  for (auto&& i : overflow_)
    i = false;
}

bool ReadState::subarray_area_covered() const {
  return subarray_area_covered_;
}

/* ****************************** */
/*         PRIVATE METHODS        */
/* ****************************** */

Status ReadState::compute_bytes_to_copy(
    unsigned int attribute_id,
    uint64_t tile_var_size,
    uint64_t start_cell_pos,
    uint64_t* end_cell_pos,
    uint64_t buffer_free_space,
    uint64_t buffer_var_free_space,
    uint64_t* bytes_to_copy,
    uint64_t* bytes_var_to_copy) {
  // Trivial case
  if (buffer_free_space == 0 || buffer_var_free_space == 0) {
    *bytes_to_copy = 0;
    *bytes_var_to_copy = 0;
    return Status::Ok();
  }

  // Calculate number of cells in the current tile for this attribute
  uint64_t cell_num = metadata_->cell_num(fetched_tile_[attribute_id]);

  assert(cell_num != 0);

  // Calculate bytes to copy from the variable tile
  const uint64_t* start_offset;
  const uint64_t* end_offset;
  const uint64_t* med_offset;
  RETURN_NOT_OK(get_offset(attribute_id, start_cell_pos, &start_offset));

  if (*end_cell_pos + 1 < cell_num) {
    RETURN_NOT_OK(get_offset(attribute_id, *end_cell_pos + 1, &end_offset));
    *bytes_var_to_copy = *end_offset - *start_offset;
  } else {
    *bytes_var_to_copy = tile_var_size - *start_offset;
  }

  // If bytes do not fit in variable buffer, we need to adjust
  if (*bytes_var_to_copy > buffer_var_free_space) {
    // Perform binary search
    uint64_t min = start_cell_pos + 1;
    uint64_t max = *end_cell_pos;
    uint64_t med = min + ((max - min) / 2);
    // Invariants:
    // (tile[min-1] - tile[start_cell_pos]) < buffer_var_free_space AND
    // (tile[max+1] - tile[start_cell_pos]) > buffer_var_free_space
    while (max != INVALID_UINT64 && min <= max) {
      med = min + ((max - min) / 2);

      // Calculate variable bytes to copy
      RETURN_NOT_OK(get_offset(attribute_id, med, &med_offset));
      *bytes_var_to_copy = *med_offset - *start_offset;

      // Check condition
      if (*bytes_var_to_copy > buffer_var_free_space)
        max = (med > 0) ? (med - 1) : INVALID_UINT64;
      else if (*bytes_var_to_copy < buffer_var_free_space)
        min = med + 1;
      else
        break;
    }

    // Determine the end position of the range
    if (min > max || max == INVALID_UINT64)
      *end_cell_pos = (min > 1) ? min - 2 : INVALID_UINT64;
    else
      *end_cell_pos = med - 1;

    // Update variable bytes to copy
    if (*end_cell_pos != INVALID_UINT64) {
      RETURN_NOT_OK(get_offset(attribute_id, *end_cell_pos + 1, &end_offset));
      *bytes_var_to_copy = *end_offset - *start_offset;
    } else {
      *bytes_var_to_copy = 0;
    }
  }

  // Update bytes to copy
  uint64_t bytes_to_copy_tmp = (*end_cell_pos != INVALID_UINT64) ?
                                   (*end_cell_pos - start_cell_pos + 1) *
                                       constants::cell_var_offset_size :
                                   0;
  *bytes_to_copy = MIN(*bytes_to_copy, bytes_to_copy_tmp);

  // Sanity checks
  assert(*bytes_to_copy <= buffer_free_space);
  assert(*bytes_var_to_copy <= buffer_var_free_space);

  return Status::Ok();
}

Status ReadState::compute_tile_compressed_size(
    uint64_t tile_i,
    unsigned int attribute_id,
    uint64_t file_size,
    uint64_t* tile_compressed_size) const {
  auto& tile_offsets = metadata_->tile_offsets();
  uint64_t tile_num = metadata_->tile_num();

  assert(tile_num != 0);
  assert(file_size != 0);

  *tile_compressed_size = (tile_i == tile_num - 1) ?
                              file_size - tile_offsets[attribute_id][tile_i] :
                              tile_offsets[attribute_id][tile_i + 1] -
                                  tile_offsets[attribute_id][tile_i];

  return Status::Ok();
}

Status ReadState::compute_tile_compressed_var_size(
    uint64_t tile_i,
    unsigned int attribute_id,
    uint64_t file_size,
    uint64_t* tile_compressed_size) const {
  auto& tile_var_offsets = metadata_->tile_var_offsets();
  uint64_t tile_num = metadata_->tile_num();

  assert(tile_num != 0);
  assert(file_size != 0);

  *tile_compressed_size =
      (tile_i == tile_num - 1) ?
          file_size - tile_var_offsets[attribute_id][tile_i] :
          tile_var_offsets[attribute_id][tile_i + 1] -
              tile_var_offsets[attribute_id][tile_i];

  return Status::Ok();
}

void ReadState::compute_tile_search_range() {
  // For easy reference
  Datatype coords_type = array_metadata_->coords_type();

  // Applicable only to sparse fragments
  if (fragment_->dense())
    return;

  // Invoke the proper templated function
  if (coords_type == Datatype::INT32) {
    compute_tile_search_range<int>();
  } else if (coords_type == Datatype::INT64) {
    compute_tile_search_range<int64_t>();
  } else if (coords_type == Datatype::FLOAT32) {
    compute_tile_search_range<float>();
  } else if (coords_type == Datatype::FLOAT64) {
    compute_tile_search_range<double>();
  } else if (coords_type == Datatype::INT8) {
    compute_tile_search_range<int8_t>();
  } else if (coords_type == Datatype::UINT8) {
    compute_tile_search_range<uint8_t>();
  } else if (coords_type == Datatype::INT16) {
    compute_tile_search_range<int16_t>();
  } else if (coords_type == Datatype::UINT16) {
    compute_tile_search_range<uint16_t>();
  } else if (coords_type == Datatype::UINT32) {
    compute_tile_search_range<uint32_t>();
  } else if (coords_type == Datatype::UINT64) {
    compute_tile_search_range<uint64_t>();
  } else {
    // The code should never reach here
    assert(0);
  }
}

template <class T>
void ReadState::compute_tile_search_range() {
  // Initialize the tile search range
  compute_tile_search_range_col_or_row<T>();

  // Handle no overlap
  if (tile_search_range_[0] == INVALID_UINT64 ||
      tile_search_range_[1] == INVALID_UINT64)
    done_ = true;
}

template <class T>
void ReadState::compute_tile_search_range_col_or_row() {
  // For easy reference
  unsigned int dim_num = array_metadata_->dim_num();
  auto subarray = static_cast<const T*>(query_->subarray());
  uint64_t tile_num = metadata_->tile_num();
  auto& bounding_coords = metadata_->bounding_coords();
  auto domain = array_metadata_->domain();

  // Calculate subarray coordinates
  auto subarray_min_coords = new T[dim_num];
  auto subarray_max_coords = new T[dim_num];
  for (unsigned int i = 0; i < dim_num; ++i) {
    subarray_min_coords[i] = subarray[2 * i];
    subarray_max_coords[i] = subarray[2 * i + 1];
  }

  // --- Find the start tile in search range

  assert(tile_num != 0);

  // Perform binary search
  uint64_t min = 0;
  uint64_t max = tile_num - 1;
  uint64_t med = min + ((max - min) / 2);
  const T* tile_start_coords;
  const T* tile_end_coords;
  while (max != INVALID_UINT64 && min <= max) {
    med = min + ((max - min) / 2);

    // Get info for bounding coordinates
    tile_start_coords = static_cast<const T*>(bounding_coords[med]);
    tile_end_coords = &(static_cast<const T*>(bounding_coords[med])[dim_num]);

    // Calculate precedence
    if (domain->tile_cell_order_cmp(
            subarray_min_coords, tile_start_coords, (T*)tile_coords_aux_) <
        0) {  // Subarray min precedes MBR
      max = (med > 0) ? med - 1 : INVALID_UINT64;
    } else if (
        domain->tile_cell_order_cmp(
            subarray_min_coords, tile_end_coords, (T*)tile_coords_aux_) >
        0) {  // Subarray min succeeds MBR
      min = med + 1;
    } else {  // Subarray min in MBR
      break;
    }
  }

  bool is_unary = utils::is_unary_subarray(subarray, dim_num);

  // Determine the start position of the range
  if (max == INVALID_UINT64 || max < min)
    // Subarray min precedes the tile at position min
    tile_search_range_[0] = (is_unary) ? INVALID_UINT64 : min;
  else
    // Subarray min included in a tile
    tile_search_range_[0] = med;

  if (is_unary) {  // Unary range
    // The end position is the same as the start
    tile_search_range_[1] = tile_search_range_[0];
  } else {  // Need to find the end position
    // --- Finding the end tile in search range

    // Perform binary search
    min = 0;
    max = tile_num - 1;
    while (max != INVALID_UINT64 && min <= max) {
      med = min + ((max - min) / 2);

      // Get info for bounding coordinates
      tile_start_coords = static_cast<const T*>(bounding_coords[med]);
      tile_end_coords = &(static_cast<const T*>(bounding_coords[med])[dim_num]);

      // Calculate precedence
      if (domain->tile_cell_order_cmp(
              subarray_max_coords, tile_start_coords, (T*)tile_coords_aux_) <
          0) {  // Subarray max precedes MBR
        max = (med > 0) ? med - 1 : INVALID_UINT64;
      } else if (
          domain->tile_cell_order_cmp(
              subarray_max_coords, tile_end_coords, (T*)tile_coords_aux_) >
          0) {  // Subarray max succeeds MBR
        min = med + 1;
      } else {  // Subarray max in MBR
        break;
      }
    }

    // Determine the start position of the range
    if (max == INVALID_UINT64 || max < min)
      // Subarray max succeeds the tile at position max
      tile_search_range_[1] = max;
    else  // Subarray max included in a tile
      tile_search_range_[1] = med;
  }

  // No overlap
  if (tile_search_range_[0] == INVALID_UINT64 ||
      tile_search_range_[1] == INVALID_UINT64 ||
      tile_search_range_[1] < tile_search_range_[0]) {
    tile_search_range_[0] = INVALID_UINT64;
    tile_search_range_[1] = INVALID_UINT64;
  }

  // Clean up
  delete[] subarray_min_coords;
  delete[] subarray_max_coords;
}

Status ReadState::cmp_coords_to_search_tile(
    const void* buffer, uint64_t tile_offset, bool* isequal) {
  auto tile = tiles_[attribute_num_ + 1];

  *isequal =
      std::memcmp(buffer, (char*)tile->data() + tile_offset, coords_size_) == 0;
  return Status::Ok();
}

template <class T>
Status ReadState::get_cell_pos_after(const T* coords, uint64_t* pos) {
  // For easy reference
  uint64_t cell_num = metadata_->cell_num(fetched_tile_[attribute_num_ + 1]);
  assert(cell_num > 0);

  // Perform binary search to find the position of coords in the tile
  uint64_t min = 0;
  uint64_t max = cell_num - 1;
  uint64_t med = min + ((max - min) / 2);
  const void* coords_t;
  while (min <= max && max != INVALID_UINT64) {
    med = min + ((max - min) / 2);

    // Update search range
    RETURN_NOT_OK(get_coords_from_search_tile(med, &coords_t));

    // Compute order
    int cmp = array_metadata_->domain()->tile_cell_order_cmp<T>(
        coords, static_cast<const T*>(coords_t), (T*)tile_coords_aux_);
    if (cmp < 0)
      max = (med > 0) ? med - 1 : INVALID_UINT64;
    else if (cmp > 0)
      min = med + 1;
    else
      break;
  }

  // Return
  if (max == INVALID_UINT64 || max < min)
    *pos = min;  // After
  else
    *pos = med + 1;  // After (med is at)

  return Status::Ok();
}

template <class T>
Status ReadState::get_cell_pos_at_or_after(const T* coords, uint64_t* pos) {
  // For easy reference
  uint64_t cell_num = metadata_->cell_num(fetched_tile_[attribute_num_ + 1]);
  assert(cell_num > 0);

  // Perform binary search to find the position of coords in the tile
  uint64_t min = 0;
  uint64_t max = cell_num - 1;
  uint64_t med = min + ((max - min) / 2);
  const void* coords_t;

  while (min <= max && max != INVALID_UINT64) {
    med = min + ((max - min) / 2);

    // Update search range
    RETURN_NOT_OK(get_coords_from_search_tile(med, &coords_t))

    // Compute order
    int cmp = array_metadata_->domain()->tile_cell_order_cmp<T>(
        coords, static_cast<const T*>(coords_t), (T*)tile_coords_aux_);

    if (cmp < 0)
      max = (med > 0) ? med - 1 : INVALID_UINT64;
    else if (cmp > 0)
      min = med + 1;
    else
      break;
  }

  // Return
  if (max == INVALID_UINT64 || max < min)
    *pos = min;  // After
  else
    *pos = med;  // At
  return Status::Ok();
}

template <class T>
Status ReadState::get_cell_pos_at_or_before(
    const T* coords, uint64_t* end_pos) {
  // For easy reference
  uint64_t cell_num = metadata_->cell_num(fetched_tile_[attribute_num_ + 1]);
  assert(cell_num > 0);

  // Perform binary search to find the position of coords in the tile
  uint64_t min = 0;
  uint64_t max = cell_num - 1;
  uint64_t med = min + ((max - min) / 2);
  const void* coords_t;
  while (min <= max && max != INVALID_UINT64) {
    med = min + ((max - min) / 2);

    // Update search range
    RETURN_NOT_OK(get_coords_from_search_tile(med, &coords_t));

    // Compute order
    int cmp = array_metadata_->domain()->tile_cell_order_cmp<T>(
        coords, static_cast<const T*>(coords_t), (T*)tile_coords_aux_);
    if (cmp < 0)
      max = (med > 0) ? med - 1 : INVALID_UINT64;
    else if (cmp > 0)
      min = med + 1;
    else
      break;
  }

  // Return
  if (max == INVALID_UINT64 || max < min)
    *end_pos = max;  // Before
  else
    *end_pos = med;  // At

  return Status::Ok();
}

Status ReadState::get_coords_from_search_tile(uint64_t i, const void** coords) {
  // For easy reference
  auto tile = tiles_[attribute_num_ + 1];

  *coords = (char*)tile->data() + i * coords_size_;
  return Status::Ok();
}

Status ReadState::get_offset(
    unsigned int attribute_id, uint64_t i, const uint64_t** offset) {
  auto tile = tiles_[attribute_id];

  *offset =
      (const uint64_t*)((char*)tile->data() + i * constants::cell_var_offset_size);
  return Status::Ok();
}

void ReadState::init_empty_attributes() {
  URI uri;
  is_empty_attribute_.resize(attribute_num_ + 1);
  for (unsigned int i = 0; i < attribute_num_; ++i) {
    uri = fragment_->fragment_uri().join_path(
        array_metadata_->attribute_name(i) + constants::file_suffix);
    is_empty_attribute_[i] = !query_->storage_manager()->is_file(uri);
  }

  uri = fragment_->fragment_uri().join_path(
      std::string(constants::coords) + constants::file_suffix);
  is_empty_attribute_[attribute_num_] =
      !query_->storage_manager()->is_file(uri);
}

void ReadState::init_fetched_tiles() {
  fetched_tile_.resize(attribute_num_ + 2);
  for (unsigned int i = 0; i < attribute_num_ + 2; ++i)
    fetched_tile_[i] = INVALID_UINT64;
}

void ReadState::init_overflow() {
  overflow_.resize(attribute_num_ + 1);
  for (unsigned int i = 0; i < attribute_num_ + 1; ++i)
    overflow_[i] = false;
}

void ReadState::init_tiles() {
  auto dim_num = array_metadata_->domain()->dim_num();

  for (unsigned int i = 0; i < attribute_num_; ++i) {
    const Attribute* attr = array_metadata_->attribute(i);
    bool var_size = attr->var_size();

    tiles_.emplace_back(new Tile(
        (var_size) ? constants::cell_var_offset_type : attr->type(),
        (var_size) ? array_metadata_->cell_var_offsets_compression() :
                     attr->compressor(),
        (var_size) ? constants::cell_var_offset_size : attr->cell_size(),
        0));

    if (var_size)
      tiles_var_.emplace_back(new Tile(
          attr->type(), attr->compressor(), datatype_size(attr->type()), 0));
    else
      tiles_var_.emplace_back(nullptr);
  }
  tiles_.emplace_back(new Tile(
      array_metadata_->coords_type(),
      array_metadata_->coords_compression(),
      array_metadata_->coords_size(),
      dim_num));
  tiles_.emplace_back(new Tile(
      array_metadata_->coords_type(),
      array_metadata_->coords_compression(),
      array_metadata_->coords_size(),
      dim_num));
}

void ReadState::init_tile_io() {
  for (unsigned int i = 0; i < attribute_num_; ++i) {
    const Attribute* attr = array_metadata_->attribute(i);
    bool var_size = attr->var_size();
    tile_io_.emplace_back(new TileIO(
        query_->storage_manager(),
        fragment_->attr_uri(i),
        fragment_->file_size(i)));
    if (var_size)
      tile_io_var_.emplace_back(new TileIO(
          query_->storage_manager(),
          fragment_->attr_var_uri(i),
          fragment_->file_var_size(i)));
    else
      tile_io_var_.emplace_back(nullptr);
  }
  tile_io_.emplace_back(new TileIO(
      query_->storage_manager(),
      fragment_->coords_uri(),
      fragment_->file_coords_size()));
  tile_io_.emplace_back(new TileIO(
      query_->storage_manager(),
      fragment_->coords_uri(),
      fragment_->file_coords_size()));
}

bool ReadState::is_empty_attribute(unsigned int attribute_id) const {
  // Special case for search coordinate tiles
  if (attribute_id == attribute_num_ + 1)
    attribute_id = attribute_num_;

  return is_empty_attribute_[attribute_id];
}

Status ReadState::read_from_tile(
    unsigned int attribute_id,
    void* buffer,
    uint64_t tile_offset,
    uint64_t nbytes) {
  // For easy reference
  auto tile = tiles_[attribute_id];

  std::memcpy(buffer, (char*)tile->data() + tile_offset, nbytes);
  return Status::Ok();
}

Status ReadState::read_tile(unsigned int attribute_id, uint64_t tile_i) {
  // Return if the tile has already been fetched
  if (tile_i == fetched_tile_[attribute_id])
    return Status::Ok();

  auto tile = tiles_[attribute_id];
  auto tile_io = tile_io_[attribute_id];

  // To handle the special case of the search tile
  // The real attribute id corresponds to an actual attribute or coordinates
  unsigned int attribute_id_real =
      (attribute_id == attribute_num_ + 1) ? attribute_num_ : attribute_id;

  uint64_t tile_compressed_size;
  RETURN_NOT_OK(compute_tile_compressed_size(
      tile_i, attribute_id_real, tile_io->file_size(), &tile_compressed_size));

  uint64_t file_offset = metadata_->tile_offsets()[attribute_id_real][tile_i];
  uint64_t tile_size = metadata_->cell_num(tile_i) *
                       array_metadata_->cell_size(attribute_id_real);

  Status st = tile_io->read(tile, file_offset, tile_compressed_size, tile_size);

  // Mark as fetched
  if (st.ok())
    fetched_tile_[attribute_id] = tile_i;

  return st;
}

Status ReadState::read_tile_var(unsigned int attribute_id, uint64_t tile_i) {
  // Return if the tile has already been fetched
  if (tile_i == fetched_tile_[attribute_id])
    return Status::Ok();

  // Sanity check
  assert(
      attribute_id < attribute_num_ && array_metadata_->var_size(attribute_id));

  auto tile = tiles_[attribute_id];
  auto tile_io = tile_io_[attribute_id];

  uint64_t tile_compressed_size;
  RETURN_NOT_OK(compute_tile_compressed_size(
      tile_i, attribute_id, tile_io->file_size(), &tile_compressed_size));
  uint64_t file_offset = metadata_->tile_offsets()[attribute_id][tile_i];
  uint64_t tile_size =
      metadata_->cell_num(tile_i) * constants::cell_var_offset_size;

  RETURN_NOT_OK(
      tile_io->read(tile, file_offset, tile_compressed_size, tile_size));

  auto tile_var = tiles_var_[attribute_id];
  auto tile_io_var = tile_io_var_[attribute_id];

  // Get size of decompressed tile
  uint64_t tile_compressed_var_size;
  RETURN_NOT_OK(compute_tile_compressed_var_size(
      tile_i,
      attribute_id,
      tile_io_var->file_size(),
      &tile_compressed_var_size));
  uint64_t tile_var_size = metadata_->tile_var_sizes()[attribute_id][tile_i];
  uint64_t file_var_offset =
      metadata_->tile_var_offsets()[attribute_id][tile_i];

  RETURN_NOT_OK(tile_io_var->read(
      tile_var, file_var_offset, tile_compressed_var_size, tile_var_size));

  // Shift variable cell offsets
  shift_var_offsets(attribute_id);

  // Mark as fetched
  fetched_tile_[attribute_id] = tile_i;

  // Success
  return Status::Ok();
}

void ReadState::shift_var_offsets(unsigned int attribute_id) {
  // For easy reference
  uint64_t cell_num =
      tiles_[attribute_id]->size() / constants::cell_var_offset_size;
  auto tile_s = static_cast<uint64_t*>(tiles_[attribute_id]->data());
  uint64_t first_offset = tile_s[0];

  // Shift offsets
  for (uint64_t i = 0; i < cell_num; ++i)
    tile_s[i] -= first_offset;
}

void ReadState::shift_var_offsets(
    void* buffer, uint64_t offset_num, uint64_t new_start_offset) {
  // For easy reference
  auto buffer_s = static_cast<uint64_t*>(buffer);
  uint64_t start_offset = buffer_s[0];

  // Shift offsets
  for (uint64_t i = 0; i < offset_num; ++i)
    buffer_s[i] = buffer_s[i] - start_offset + new_start_offset;
}

// Explicit template instantiations
template Status ReadState::get_coords_after<int>(
    const int* coords, int* coords_after, bool* coords_retrieved);
template Status ReadState::get_coords_after<int64_t>(
    const int64_t* coords, int64_t* coords_after, bool* coords_retrieved);
template Status ReadState::get_coords_after<float>(
    const float* coords, float* coords_after, bool* coords_retrieved);
template Status ReadState::get_coords_after<double>(
    const double* coords, double* coords_after, bool* coords_retrieved);
template Status ReadState::get_coords_after<int8_t>(
    const int8_t* coords, int8_t* coords_after, bool* coords_retrieved);
template Status ReadState::get_coords_after<uint8_t>(
    const uint8_t* coords, uint8_t* coords_after, bool* coords_retrieved);
template Status ReadState::get_coords_after<int16_t>(
    const int16_t* coords, int16_t* coords_after, bool* coords_retrieved);
template Status ReadState::get_coords_after<uint16_t>(
    const uint16_t* coords, uint16_t* coords_after, bool* coords_retrieved);
template Status ReadState::get_coords_after<uint32_t>(
    const uint32_t* coords, uint32_t* coords_after, bool* coords_retrieved);
template Status ReadState::get_coords_after<uint64_t>(
    const uint64_t* coords, uint64_t* coords_after, bool* coords_retrieved);

template Status ReadState::get_enclosing_coords<int>(
    uint64_t tile_i,
    const int* target_coords,
    const int* start_coords,
    const int* end_coords,
    int* left_coords,
    int* right_coords,
    bool* left_retrieved,
    bool* right_retrieved,
    bool* target_exists);
template Status ReadState::get_enclosing_coords<int64_t>(
    uint64_t tile_i,
    const int64_t* target_coords,
    const int64_t* start_coords,
    const int64_t* end_coords,
    int64_t* left_coords,
    int64_t* right_coords,
    bool* left_retrieved,
    bool* right_retrieved,
    bool* target_exists);
template Status ReadState::get_enclosing_coords<float>(
    uint64_t tile_i,
    const float* target_coords,
    const float* start_coords,
    const float* end_coords,
    float* left_coords,
    float* right_coords,
    bool* left_retrieved,
    bool* right_retrieved,
    bool* target_exists);
template Status ReadState::get_enclosing_coords<double>(
    uint64_t tile_i,
    const double* target_coords,
    const double* start_coords,
    const double* end_coords,
    double* left_coords,
    double* right_coords,
    bool* left_retrieved,
    bool* right_retrieved,
    bool* target_exists);
template Status ReadState::get_enclosing_coords<int8_t>(
    uint64_t tile_i,
    const int8_t* target_coords,
    const int8_t* start_coords,
    const int8_t* end_coords,
    int8_t* left_coords,
    int8_t* right_coords,
    bool* left_retrieved,
    bool* right_retrieved,
    bool* target_exists);
template Status ReadState::get_enclosing_coords<uint8_t>(
    uint64_t tile_i,
    const uint8_t* target_coords,
    const uint8_t* start_coords,
    const uint8_t* end_coords,
    uint8_t* left_coords,
    uint8_t* right_coords,
    bool* left_retrieved,
    bool* right_retrieved,
    bool* target_exists);
template Status ReadState::get_enclosing_coords<int16_t>(
    uint64_t tile_i,
    const int16_t* target_coords,
    const int16_t* start_coords,
    const int16_t* end_coords,
    int16_t* left_coords,
    int16_t* right_coords,
    bool* left_retrieved,
    bool* right_retrieved,
    bool* target_exists);
template Status ReadState::get_enclosing_coords<uint16_t>(
    uint64_t tile_i,
    const uint16_t* target_coords,
    const uint16_t* start_coords,
    const uint16_t* end_coords,
    uint16_t* left_coords,
    uint16_t* right_coords,
    bool* left_retrieved,
    bool* right_retrieved,
    bool* target_exists);
template Status ReadState::get_enclosing_coords<uint32_t>(
    uint64_t tile_i,
    const uint32_t* target_coords,
    const uint32_t* start_coords,
    const uint32_t* end_coords,
    uint32_t* left_coords,
    uint32_t* right_coords,
    bool* left_retrieved,
    bool* right_retrieved,
    bool* target_exists);
template Status ReadState::get_enclosing_coords<uint64_t>(
    uint64_t tile_i,
    const uint64_t* target_coords,
    const uint64_t* start_coords,
    const uint64_t* end_coords,
    uint64_t* left_coords,
    uint64_t* right_coords,
    bool* left_retrieved,
    bool* right_retrieved,
    bool* target_exists);

template Status ReadState::get_fragment_cell_pos_range_sparse<int>(
    const FragmentInfo& fragment_info,
    const int* cell_range,
    FragmentCellPosRange* fragment_cell_pos_range);
template Status ReadState::get_fragment_cell_pos_range_sparse<int64_t>(
    const FragmentInfo& fragment_info,
    const int64_t* cell_range,
    FragmentCellPosRange* fragment_cell_pos_range);
template Status ReadState::get_fragment_cell_pos_range_sparse<float>(
    const FragmentInfo& fragment_info,
    const float* cell_range,
    FragmentCellPosRange* fragment_cell_pos_range);
template Status ReadState::get_fragment_cell_pos_range_sparse<double>(
    const FragmentInfo& fragment_info,
    const double* cell_range,
    FragmentCellPosRange* fragment_cell_pos_range);
template Status ReadState::get_fragment_cell_pos_range_sparse<int8_t>(
    const FragmentInfo& fragment_info,
    const int8_t* cell_range,
    FragmentCellPosRange* fragment_cell_pos_range);
template Status ReadState::get_fragment_cell_pos_range_sparse<uint8_t>(
    const FragmentInfo& fragment_info,
    const uint8_t* cell_range,
    FragmentCellPosRange* fragment_cell_pos_range);
template Status ReadState::get_fragment_cell_pos_range_sparse<int16_t>(
    const FragmentInfo& fragment_info,
    const int16_t* cell_range,
    FragmentCellPosRange* fragment_cell_pos_range);
template Status ReadState::get_fragment_cell_pos_range_sparse<uint16_t>(
    const FragmentInfo& fragment_info,
    const uint16_t* cell_range,
    FragmentCellPosRange* fragment_cell_pos_range);
template Status ReadState::get_fragment_cell_pos_range_sparse<uint32_t>(
    const FragmentInfo& fragment_info,
    const uint32_t* cell_range,
    FragmentCellPosRange* fragment_cell_pos_range);
template Status ReadState::get_fragment_cell_pos_range_sparse<uint64_t>(
    const FragmentInfo& fragment_info,
    const uint64_t* cell_range,
    FragmentCellPosRange* fragment_cell_pos_range);

template Status ReadState::get_fragment_cell_ranges_sparse<int>(
    unsigned int fragment_i,
    const int* start_coords,
    const int* end_coords,
    FragmentCellRanges* fragment_cell_ranges);
template Status ReadState::get_fragment_cell_ranges_sparse<int64_t>(
    unsigned int fragment_i,
    const int64_t* start_coords,
    const int64_t* end_coords,
    FragmentCellRanges* fragment_cell_ranges);
template Status ReadState::get_fragment_cell_ranges_sparse<float>(
    unsigned int fragment_i,
    const float* start_coords,
    const float* end_coords,
    FragmentCellRanges* fragment_cell_ranges);
template Status ReadState::get_fragment_cell_ranges_sparse<double>(
    unsigned int fragment_i,
    const double* start_coords,
    const double* end_coords,
    FragmentCellRanges* fragment_cell_ranges);
template Status ReadState::get_fragment_cell_ranges_sparse<int8_t>(
    unsigned int fragment_i,
    const int8_t* start_coords,
    const int8_t* end_coords,
    FragmentCellRanges* fragment_cell_ranges);
template Status ReadState::get_fragment_cell_ranges_sparse<uint8_t>(
    unsigned int fragment_i,
    const uint8_t* start_coords,
    const uint8_t* end_coords,
    FragmentCellRanges* fragment_cell_ranges);
template Status ReadState::get_fragment_cell_ranges_sparse<int16_t>(
    unsigned int fragment_i,
    const int16_t* start_coords,
    const int16_t* end_coords,
    FragmentCellRanges* fragment_cell_ranges);
template Status ReadState::get_fragment_cell_ranges_sparse<uint16_t>(
    unsigned int fragment_i,
    const uint16_t* start_coords,
    const uint16_t* end_coords,
    FragmentCellRanges* fragment_cell_ranges);
template Status ReadState::get_fragment_cell_ranges_sparse<uint32_t>(
    unsigned int fragment_i,
    const uint32_t* start_coords,
    const uint32_t* end_coords,
    FragmentCellRanges* fragment_cell_ranges);
template Status ReadState::get_fragment_cell_ranges_sparse<uint64_t>(
    unsigned int fragment_i,
    const uint64_t* start_coords,
    const uint64_t* end_coords,
    FragmentCellRanges* fragment_cell_ranges);

template Status ReadState::get_fragment_cell_ranges_sparse<int>(
    unsigned int fragment_i, FragmentCellRanges* fragment_cell_ranges);
template Status ReadState::get_fragment_cell_ranges_sparse<int64_t>(
    unsigned int fragment_i, FragmentCellRanges* fragment_cell_ranges);
template Status ReadState::get_fragment_cell_ranges_sparse<int8_t>(
    unsigned int fragment_i, FragmentCellRanges* fragment_cell_ranges);
template Status ReadState::get_fragment_cell_ranges_sparse<uint8_t>(
    unsigned int fragment_i, FragmentCellRanges* fragment_cell_ranges);
template Status ReadState::get_fragment_cell_ranges_sparse<int16_t>(
    unsigned int fragment_i, FragmentCellRanges* fragment_cell_ranges);
template Status ReadState::get_fragment_cell_ranges_sparse<uint16_t>(
    unsigned int fragment_i, FragmentCellRanges* fragment_cell_ranges);
template Status ReadState::get_fragment_cell_ranges_sparse<uint32_t>(
    unsigned int fragment_i, FragmentCellRanges* fragment_cell_ranges);
template Status ReadState::get_fragment_cell_ranges_sparse<uint64_t>(
    unsigned int fragment_i, FragmentCellRanges* fragment_cell_ranges);

template Status ReadState::get_fragment_cell_ranges_dense<int>(
    unsigned int fragment_i, FragmentCellRanges* fragment_cell_ranges);
template Status ReadState::get_fragment_cell_ranges_dense<int64_t>(
    unsigned int fragment_i, FragmentCellRanges* fragment_cell_ranges);
template Status ReadState::get_fragment_cell_ranges_dense<int8_t>(
    unsigned int fragment_i, FragmentCellRanges* fragment_cell_ranges);
template Status ReadState::get_fragment_cell_ranges_dense<uint8_t>(
    unsigned int fragment_i, FragmentCellRanges* fragment_cell_ranges);
template Status ReadState::get_fragment_cell_ranges_dense<int16_t>(
    unsigned int fragment_i, FragmentCellRanges* fragment_cell_ranges);
template Status ReadState::get_fragment_cell_ranges_dense<uint16_t>(
    unsigned int fragment_i, FragmentCellRanges* fragment_cell_ranges);
template Status ReadState::get_fragment_cell_ranges_dense<uint32_t>(
    unsigned int fragment_i, FragmentCellRanges* fragment_cell_ranges);
template Status ReadState::get_fragment_cell_ranges_dense<uint64_t>(
    unsigned int fragment_i, FragmentCellRanges* fragment_cell_ranges);

template void ReadState::get_next_overlapping_tile_dense<int>(
    const int* tile_coords);
template void ReadState::get_next_overlapping_tile_dense<int64_t>(
    const int64_t* tile_coords);
template void ReadState::get_next_overlapping_tile_dense<int8_t>(
    const int8_t* tile_coords);
template void ReadState::get_next_overlapping_tile_dense<uint8_t>(
    const uint8_t* tile_coords);
template void ReadState::get_next_overlapping_tile_dense<int16_t>(
    const int16_t* tile_coords);
template void ReadState::get_next_overlapping_tile_dense<uint16_t>(
    const uint16_t* tile_coords);
template void ReadState::get_next_overlapping_tile_dense<uint32_t>(
    const uint32_t* tile_coords);
template void ReadState::get_next_overlapping_tile_dense<uint64_t>(
    const uint64_t* tile_coords);

template void ReadState::get_next_overlapping_tile_sparse<int>(
    const int* tile_coords);
template void ReadState::get_next_overlapping_tile_sparse<int64_t>(
    const int64_t* tile_coords);
template void ReadState::get_next_overlapping_tile_sparse<int8_t>(
    const int8_t* tile_coords);
template void ReadState::get_next_overlapping_tile_sparse<uint8_t>(
    const uint8_t* tile_coords);
template void ReadState::get_next_overlapping_tile_sparse<int16_t>(
    const int16_t* tile_coords);
template void ReadState::get_next_overlapping_tile_sparse<uint16_t>(
    const uint16_t* tile_coords);
template void ReadState::get_next_overlapping_tile_sparse<uint32_t>(
    const uint32_t* tile_coords);
template void ReadState::get_next_overlapping_tile_sparse<uint64_t>(
    const uint64_t* tile_coords);

template void ReadState::get_next_overlapping_tile_sparse<int>();
template void ReadState::get_next_overlapping_tile_sparse<int64_t>();
template void ReadState::get_next_overlapping_tile_sparse<float>();
template void ReadState::get_next_overlapping_tile_sparse<double>();
template void ReadState::get_next_overlapping_tile_sparse<int8_t>();
template void ReadState::get_next_overlapping_tile_sparse<uint8_t>();
template void ReadState::get_next_overlapping_tile_sparse<int16_t>();
template void ReadState::get_next_overlapping_tile_sparse<uint16_t>();
template void ReadState::get_next_overlapping_tile_sparse<uint32_t>();
template void ReadState::get_next_overlapping_tile_sparse<uint64_t>();

}  // namespace tiledb
