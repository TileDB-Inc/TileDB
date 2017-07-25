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

#include "read_state.h"
// TODO: read state should not depend of mman
#include <sys/mman.h>
#include <cmath>

#include "filesystem.h"
#include "logger.h"
#include "utils.h"

#include "blosc_compressor.h"
#include "bzip_compressor.h"
#include "gzip_compressor.h"
#include "lz4_compressor.h"
#include "rle_compressor.h"
#include "zstd_compressor.h"

/* ****************************** */
/*             MACROS             */
/* ****************************** */

namespace tiledb {

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

/*

ReadState::ReadState(const Fragment* fragment, BookKeeping* book_keeping)
: book_keeping_(book_keeping)
, fragment_(fragment) {
array_ = fragment_->array();
array_schema_ = array_->array_schema();
attribute_num_ = array_schema_->attribute_num();
coords_size_ = array_schema_->coords_size();

done_ = false;
fetched_tile_.resize(attribute_num_ + 2);
overflow_.resize(attribute_num_ + 1);
last_tile_coords_ = nullptr;
map_addr_.resize(attribute_num_ + 2);
map_addr_lengths_.resize(attribute_num_ + 2);
map_addr_compressed_ = nullptr;
map_addr_compressed_length_ = 0;
map_addr_var_.resize(attribute_num_);
map_addr_var_lengths_.resize(attribute_num_);
search_tile_overlap_subarray_ = malloc(2 * coords_size_);
search_tile_pos_ = -1;
tile_compressed_ = nullptr;
tile_compressed_allocated_size_ = 0;
tiles_.resize(attribute_num_ + 2);
tiles_offsets_.resize(attribute_num_ + 2);
tiles_file_offsets_.resize(attribute_num_ + 2);
tiles_sizes_.resize(attribute_num_ + 2);
tiles_var_.resize(attribute_num_);
tiles_var_offsets_.resize(attribute_num_);
tiles_var_file_offsets_.resize(attribute_num_);
tiles_var_sizes_.resize(attribute_num_);
tiles_var_allocated_size_.resize(attribute_num_);
tmp_coords_ = malloc(coords_size_);

for (int i = 0; i < attribute_num_; ++i) {
map_addr_var_[i] = nullptr;
map_addr_var_lengths_[i] = 0;
tiles_var_[i] = nullptr;
tiles_var_offsets_[i] = 0;
tiles_var_sizes_[i] = 0;
tiles_var_allocated_size_[i] = 0;
}

for (int i = 0; i < attribute_num_ + 1; ++i) {
overflow_[i] = false;
}

for (int i = 0; i < attribute_num_ + 2; ++i) {
fetched_tile_[i] = -1;
map_addr_[i] = nullptr;
map_addr_lengths_[i] = 0;
tiles_[i] = nullptr;
tiles_offsets_[i] = 0;
tiles_file_offsets_[i] = 0;
tiles_sizes_[i] = 0;
}

compute_tile_search_range();

// Check empty attributes
std::string fragment_name = fragment_->fragment_name();
std::string filename;
is_empty_attribute_.resize(attribute_num_ + 1);
for (int i = 0; i < attribute_num_ + 1; ++i) {
filename = fragment_name + "/" + array_schema_->attribute(i) +
           Configurator::file_suffix();
is_empty_attribute_[i] = !filesystem::is_file(filename);
}
}

ReadState::~ReadState() {
if (last_tile_coords_ != nullptr)
free(last_tile_coords_);

for (int i = 0; i < int(tiles_.size()); ++i) {
if (map_addr_[i] == nullptr && tiles_[i] != nullptr)
  free(tiles_[i]);
}

for (int i = 0; i < int(tiles_var_.size()); ++i) {
if (map_addr_var_[i] == nullptr && tiles_var_[i] != nullptr)
  free(tiles_var_[i]);
}

if (map_addr_compressed_ == nullptr && tile_compressed_ != nullptr)
free(tile_compressed_);

for (int i = 0; i < int(map_addr_.size()); ++i) {
if (map_addr_[i] != nullptr && munmap(map_addr_[i], map_addr_lengths_[i])) {
  LOG_ERROR("Problem in finalizing ReadState; Memory unmap error");
}
}

for (int i = 0; i < int(map_addr_var_.size()); ++i) {
if (map_addr_var_[i] != nullptr &&
    munmap(map_addr_var_[i], map_addr_var_lengths_[i])) {
  LOG_ERROR("Problem in finalizing ReadState; Memory unmap error");
}
}

if (map_addr_compressed_ != nullptr &&
  munmap(map_addr_compressed_, map_addr_compressed_length_)) {
LOG_ERROR("Problem in finalizing ReadState; Memory unmap error");
}

if (search_tile_overlap_subarray_ != nullptr)
free(search_tile_overlap_subarray_);

free(tmp_coords_);
}

 */

/* ****************************** */
/*              API               */
/* ****************************** */

/*

bool ReadState::dense() const {
return fragment_->dense();
}

bool ReadState::done() const {
return done_;
}

void ReadState::get_bounding_coords(void* bounding_coords) const {
// For easy reference
int64_t pos = search_tile_pos_;
assert(pos != -1);
memcpy(
  bounding_coords, book_keeping_->bounding_coords()[pos], 2 * coords_size_);
}

bool ReadState::mbr_overlaps_tile() const {
return mbr_tile_overlap_;
}

bool ReadState::overflow(int attribute_id) const {
return overflow_[attribute_id];
}

bool ReadState::subarray_area_covered() const {
return subarray_area_covered_;
}

void ReadState::reset() {
if (last_tile_coords_ != nullptr) {
free(last_tile_coords_);
last_tile_coords_ = nullptr;
}

reset_overflow();
done_ = false;
search_tile_pos_ = -1;
compute_tile_search_range();

for (int i = 0; i < attribute_num_ + 2; ++i)
tiles_offsets_[i] = 0;

for (int i = 0; i < attribute_num_; ++i)
tiles_var_offsets_[i] = 0;
}

void ReadState::reset_overflow() {
for (int i = 0; i < overflow_.size(); ++i)
overflow_[i] = false;
}

Status ReadState::copy_cells(
int attribute_id,
int tile_i,
void* buffer,
size_t buffer_size,
size_t& buffer_offset,
const CellPosRange& cell_pos_range) {
// Trivial case
if (is_empty_attribute(attribute_id))
return Status::Ok();

// For easy reference
size_t cell_size = array_schema_->cell_size(attribute_id);

// Prepare attribute tile
RETURN_NOT_OK(prepare_tile_for_reading(attribute_id, tile_i));

// Calculate free space in buffer
size_t buffer_free_space = buffer_size - buffer_offset;
buffer_free_space = (buffer_free_space / cell_size) * cell_size;
if (buffer_free_space == 0) {  // Overflow
overflow_[attribute_id] = true;
return Status::Ok();
}

// Sanity check
assert(!array_schema_->var_size(attribute_id));

// For each cell position range, copy the respective cells to the buffer
size_t start_offset, end_offset;
size_t bytes_left_to_copy, bytes_to_copy;

// Calculate start and end offset in the tile
start_offset = cell_pos_range.first * cell_size;
end_offset = (cell_pos_range.second + 1) * cell_size - 1;

// Potentially set the tile offset to the beginning of the current range
if (tiles_offsets_[attribute_id] < start_offset)
tiles_offsets_[attribute_id] = start_offset;
else if (tiles_offsets_[attribute_id] > end_offset)  // This range is written
return Status::Ok();

// Calculate the total size to copy
bytes_left_to_copy = end_offset - tiles_offsets_[attribute_id] + 1;
bytes_to_copy = std::min(bytes_left_to_copy, buffer_free_space);

// Copy and update current buffer and tile offsets
if (bytes_to_copy != 0) {
RETURN_NOT_OK(READ_FROM_TILE(
    attribute_id,
    static_cast<char*>(buffer) + buffer_offset,
    tiles_offsets_[attribute_id],
    bytes_to_copy));
buffer_offset += bytes_to_copy;
tiles_offsets_[attribute_id] += bytes_to_copy;
buffer_free_space = buffer_size - buffer_offset;
}

// Handle buffer overflow
if (tiles_offsets_[attribute_id] != end_offset + 1)
overflow_[attribute_id] = true;

// Success
return Status::Ok();
}

Status ReadState::copy_cells_var(
int attribute_id,
int tile_i,
void* buffer,
size_t buffer_size,
size_t& buffer_offset,
void* buffer_var,
size_t buffer_var_size,
size_t& buffer_var_offset,
const CellPosRange& cell_pos_range) {
// For easy reference
size_t cell_size = Configurator::cell_var_offset_size();

// Calculate free space in buffer
size_t buffer_free_space = buffer_size - buffer_offset;
buffer_free_space = (buffer_free_space / cell_size) * cell_size;
size_t buffer_var_free_space = buffer_var_size - buffer_var_offset;

// Handle overflow
if (buffer_free_space == 0 || buffer_var_free_space == 0) {  // Overflow
overflow_[attribute_id] = true;
return Status::Ok();
}

// Prepare attribute tile
RETURN_NOT_OK(prepare_tile_for_reading_var(attribute_id, tile_i));

// Sanity check
assert(array_schema_->var_size(attribute_id));

// For each cell position range, copy the respective cells to the buffer
size_t start_offset, end_offset;
int64_t start_cell_pos, end_cell_pos;
size_t bytes_left_to_copy, bytes_to_copy, bytes_var_to_copy;

// Calculate start and end offset in the tile
start_offset = cell_pos_range.first * cell_size;
end_offset = (cell_pos_range.second + 1) * cell_size - 1;

// Potentially set the tile offset to the beginning of the current range
if (tiles_offsets_[attribute_id] < start_offset)
tiles_offsets_[attribute_id] = start_offset;
else if (tiles_offsets_[attribute_id] > end_offset)  // This range is written
return Status::Ok();

// Calculate the total size to copy
bytes_left_to_copy = end_offset - tiles_offsets_[attribute_id] + 1;
bytes_to_copy = std::min(bytes_left_to_copy, buffer_free_space);

// Compute actual bytes to copy
start_cell_pos = tiles_offsets_[attribute_id] / cell_size;
end_cell_pos = start_cell_pos + bytes_to_copy / cell_size - 1;
RETURN_NOT_OK(compute_bytes_to_copy(
  attribute_id,
  start_cell_pos,
  end_cell_pos,
  buffer_free_space,
  buffer_var_free_space,
  bytes_to_copy,
  bytes_var_to_copy));

// For easy reference
void* buffer_start = static_cast<char*>(buffer) + buffer_offset;

// Potentially update tile offset to the beginning of the overlap range
const size_t* tile_var_start;
RETURN_NOT_OK(GET_CELL_PTR_FROM_OFFSET_TILE(
  attribute_id, start_cell_pos, tile_var_start));
if (tiles_var_offsets_[attribute_id] < *tile_var_start)
tiles_var_offsets_[attribute_id] = *tile_var_start;

// Copy and update current buffer and tile offsets
if (bytes_to_copy != 0) {
RETURN_NOT_OK(READ_FROM_TILE(
    attribute_id,
    buffer_start,
    tiles_offsets_[attribute_id],
    bytes_to_copy));
buffer_offset += bytes_to_copy;
tiles_offsets_[attribute_id] += bytes_to_copy;
buffer_free_space = buffer_size - buffer_offset;

// Shift variable offsets
shift_var_offsets(
    buffer_start, end_cell_pos - start_cell_pos + 1, buffer_var_offset);

// Copy and update current variable buffer and tile offsets
RETURN_NOT_OK(READ_FROM_TILE_VAR(
    attribute_id,
    static_cast<char*>(buffer_var) + buffer_var_offset,
    tiles_var_offsets_[attribute_id],
    bytes_var_to_copy));
buffer_var_offset += bytes_var_to_copy;
tiles_var_offsets_[attribute_id] += bytes_var_to_copy;
buffer_var_free_space = buffer_var_size - buffer_var_offset;
}

// Check for overflow
if (tiles_offsets_[attribute_id] != end_offset + 1) {
overflow_[attribute_id] = true;
}

// Entering this if condition implies that the var data in this cell is so
// large that the allocated buffer cannot hold it
if (buffer_offset == 0u && bytes_to_copy == 0u) {
overflow_[attribute_id] = true;
}
return Status::Ok();
}

template <class T>
Status ReadState::get_coords_after(
const T* coords, T* coords_after, bool& coords_retrieved) {
// For easy reference
int64_t cell_num = book_keeping_->cell_num(search_tile_pos_);

// Prepare attribute tile
RETURN_NOT_OK(prepare_tile_for_reading(attribute_num_ + 1, search_tile_pos_));

// Compute the cell position at or after the coords
int64_t coords_after_pos = -1;
get_cell_pos_after(coords, &coords_after_pos);

// Invalid position
if (coords_after_pos < 0 || coords_after_pos >= cell_num) {
coords_retrieved = false;
return Status::Ok();
}
// Copy result
RETURN_NOT_OK(READ_FROM_TILE(
  attribute_num_ + 1,
  coords_after,
  coords_after_pos * coords_size_,
  coords_size_));
coords_retrieved = true;

// Success
return Status::Ok();
}

template <class T>
Status ReadState::get_enclosing_coords(
int tile_i,
const T* target_coords,
const T* start_coords,
const T* end_coords,
T* left_coords,
T* right_coords,
bool& left_retrieved,
bool& right_retrieved,
bool& target_exists) {
// Prepare attribute tile
RETURN_NOT_OK(prepare_tile_for_reading(attribute_num_ + 1, tile_i));

// Compute the appropriate cell positions
int64_t start_pos = -1;
int64_t end_pos = -1;
int64_t target_pos = -1;
get_cell_pos_at_or_after(start_coords, &start_pos);
get_cell_pos_at_or_before(end_coords, &end_pos);
get_cell_pos_at_or_before(target_coords, &target_pos);

// Check if target exists
// TODO: Check if target exists <JDFKJDFJLDFJ>
if (target_pos >= start_pos && target_pos <= end_pos) {
RETURN_NOT_OK(CMP_COORDS_TO_SEARCH_TILE(
    target_coords, target_pos * coords_size_, target_exists));
} else {
target_exists = false;
}

// Calculate left and right pos
int64_t left_pos = (target_exists) ? target_pos - 1 : target_pos;
int64_t right_pos = target_pos + 1;

// Copy left if it exists
if (left_pos >= start_pos && left_pos <= end_pos) {
RETURN_NOT_OK(READ_FROM_TILE(
    attribute_num_ + 1,
    left_coords,
    left_pos * coords_size_,
    coords_size_));
left_retrieved = true;
} else {
left_retrieved = false;
}

// Copy right if it exists
if (right_pos >= start_pos && right_pos <= end_pos) {
RETURN_NOT_OK(READ_FROM_TILE(
    attribute_num_ + 1,
    right_coords,
    right_pos * coords_size_,
    coords_size_));
right_retrieved = true;
} else {
right_retrieved = false;
}

// Success
return Status::Ok();
}

template <class T>
Status ReadState::get_fragment_cell_pos_range_sparse(
const FragmentInfo& fragment_info,
const T* cell_range,
FragmentCellPosRange& fragment_cell_pos_range) {
// For easy reference
int dim_num = array_schema_->dim_num();
int64_t tile_i = fragment_info.second;

// Prepare attribute tile
RETURN_NOT_OK(prepare_tile_for_reading(attribute_num_ + 1, tile_i));

// Compute the appropriate cell positions
int64_t start_pos = -1;
int64_t end_pos = -1;
RETURN_NOT_OK(get_cell_pos_at_or_after(cell_range, &start_pos));
RETURN_NOT_OK(get_cell_pos_at_or_before(&cell_range[dim_num], &end_pos));

// Create the result
fragment_cell_pos_range.first = fragment_info;
if (start_pos <= end_pos)  // There are results
fragment_cell_pos_range.second = CellPosRange(start_pos, end_pos);
else  // There are NO rsults
fragment_cell_pos_range.second = CellPosRange(-1, -1);

// Success
return Status::Ok();
}

template <class T>
Status ReadState::get_fragment_cell_ranges_dense(
int fragment_i, FragmentCellRanges& fragment_cell_ranges) {
// Trivial cases
if (done_ || !search_tile_overlap_)
return Status::Ok();

// For easy reference
int dim_num = array_schema_->dim_num();
Layout cell_order = array_schema_->cell_order();
size_t cell_range_size = 2 * coords_size_;
const T* search_tile_overlap_subarray =
  static_cast<const T*>(search_tile_overlap_subarray_);
FragmentInfo fragment_info = FragmentInfo(fragment_i, search_tile_pos_);

// Contiguous cells, single cell range
if (search_tile_overlap_ == 1 || search_tile_overlap_ == 3) {
void* cell_range = malloc(cell_range_size);
T* cell_range_T = static_cast<T*>(cell_range);
for (int i = 0; i < dim_num; ++i) {
  cell_range_T[i] = search_tile_overlap_subarray[2 * i];
  cell_range_T[dim_num + i] = search_tile_overlap_subarray[2 * i + 1];
}

// Insert the new range into the result vector
fragment_cell_ranges.emplace_back(fragment_info, cell_range);
} else {  // Non-contiguous cells, multiple ranges
// Initialize the coordinates at the beginning of the global range
T* coords = new T[dim_num];
for (int i = 0; i < dim_num; ++i)
  coords[i] = search_tile_overlap_subarray[2 * i];

// Handle the different cell orders
int i;
if (cell_order == Layout::ROW_MAJOR) {  // ROW
  while (coords[0] <= search_tile_overlap_subarray[1]) {
    // Make a cell range representing a slab
    void* cell_range = malloc(cell_range_size);
    T* cell_range_T = static_cast<T*>(cell_range);
    for (int i = 0; i < dim_num - 1; ++i) {
      cell_range_T[i] = coords[i];
      cell_range_T[dim_num + i] = coords[i];
    }
    cell_range_T[dim_num - 1] =
        search_tile_overlap_subarray[2 * (dim_num - 1)];
    cell_range_T[2 * dim_num - 1] =
        search_tile_overlap_subarray[2 * (dim_num - 1) + 1];

    // Insert the new range into the result vector
    fragment_cell_ranges.emplace_back(fragment_info, cell_range);

    // Advance coordinates
    i = dim_num - 2;
    ++coords[i];
    while (i > 0 && coords[i] > search_tile_overlap_subarray[2 * i + 1]) {
      coords[i] = search_tile_overlap_subarray[2 * i];
      ++coords[--i];
    }
  }
} else if (cell_order == Layout::COL_MAJOR) {  // COLUMN
  while (coords[dim_num - 1] <=
         search_tile_overlap_subarray[2 * (dim_num - 1) + 1]) {
    // Make a cell range representing a slab
    void* cell_range = malloc(cell_range_size);
    T* cell_range_T = static_cast<T*>(cell_range);
    for (int i = dim_num - 1; i > 0; --i) {
      cell_range_T[i] = coords[i];
      cell_range_T[dim_num + i] = coords[i];
    }
    cell_range_T[0] = search_tile_overlap_subarray[0];
    cell_range_T[dim_num] = search_tile_overlap_subarray[1];

    // Insert the new range into the result vector
    fragment_cell_ranges.emplace_back(fragment_info, cell_range);

    // Advance coordinates
    i = 1;
    ++coords[i];
    while (i < dim_num - 1 &&
           coords[i] > search_tile_overlap_subarray[2 * i + 1]) {
      coords[i] = search_tile_overlap_subarray[2 * i];
      ++coords[++i];
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
int fragment_i, FragmentCellRanges& fragment_cell_ranges) {
// Trivial cases
if (done_ || !search_tile_overlap_ || !mbr_tile_overlap_)
return Status::Ok();

// For easy reference
int dim_num = array_schema_->dim_num();
const T* search_tile_overlap_subarray =
  static_cast<const T*>(search_tile_overlap_subarray_);

// Create start and end coordinates for the overlap
T* start_coords = new T[dim_num];
T* end_coords = new T[dim_num];
for (int i = 0; i < dim_num; ++i) {
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
int fragment_i,
const T* start_coords,
const T* end_coords,
FragmentCellRanges& fragment_cell_ranges) {
// Sanity checks
assert(
  search_tile_pos_ >= tile_search_range_[0] &&
  search_tile_pos_ <= tile_search_range_[1]);
assert(search_tile_overlap_);

// For easy reference
int dim_num = array_schema_->dim_num();
const T* subarray = static_cast<const T*>(array_->subarray());

// Handle full overlap
if (search_tile_overlap_ == 1) {
FragmentCellRange fragment_cell_range;
fragment_cell_range.first = FragmentInfo(fragment_i, search_tile_pos_);
fragment_cell_range.second = malloc(2 * coords_size_);
T* cell_range = static_cast<T*>(fragment_cell_range.second);
memcpy(cell_range, start_coords, coords_size_);
memcpy(&cell_range[dim_num], end_coords, coords_size_);
fragment_cell_ranges.push_back(fragment_cell_range);
return Status::Ok();
}

// Prepare attribute tile
RETURN_NOT_OK(prepare_tile_for_reading(attribute_num_ + 1, search_tile_pos_));

// Get cell positions for the cell range
int64_t start_pos = -1;
int64_t end_pos = -1;
get_cell_pos_at_or_after(start_coords, &start_pos);
get_cell_pos_at_or_before(end_coords, &end_pos);

// Get the cell ranges
const void* cell;
int64_t current_start_pos, current_end_pos = -2;
for (int64_t i = start_pos; i <= end_pos; ++i) {
RETURN_NOT_OK(GET_COORDS_PTR_FROM_SEARCH_TILE(i, cell));

if (utils::cell_in_subarray<T>(
        static_cast<const T*>(cell), subarray, dim_num)) {
  if (i - 1 == current_end_pos) {  // The range is expanded
    ++current_end_pos;
  } else {  // A new range starts
    current_start_pos = i;
    current_end_pos = i;
  }
} else {
  if (i - 1 ==
      current_end_pos) {  // The range needs to be added to the list
    FragmentCellRange fragment_cell_range;
    fragment_cell_range.first = FragmentInfo(fragment_i, search_tile_pos_);
    fragment_cell_range.second = malloc(2 * coords_size_);
    T* cell_range = static_cast<T*>(fragment_cell_range.second);

    RETURN_NOT_OK(READ_FROM_TILE(
        attribute_num_ + 1,
        cell_range,
        current_start_pos * coords_size_,
        coords_size_));

    RETURN_NOT_OK(READ_FROM_TILE(
        attribute_num_ + 1,
        &cell_range[dim_num],
        current_end_pos * coords_size_,
        coords_size_));

    fragment_cell_ranges.push_back(fragment_cell_range);
    current_end_pos = -2;  // This indicates that there is no active range
  }
}
}

// Add last cell range
if (current_end_pos != -2) {
FragmentCellRange fragment_cell_range;
fragment_cell_range.first = FragmentInfo(fragment_i, search_tile_pos_);
fragment_cell_range.second = malloc(2 * coords_size_);
T* cell_range = static_cast<T*>(fragment_cell_range.second);

RETURN_NOT_OK(READ_FROM_TILE(
    attribute_num_ + 1,
    cell_range,
    current_start_pos * coords_size_,
    coords_size_));

RETURN_NOT_OK(READ_FROM_TILE(
    attribute_num_ + 1,
    &cell_range[dim_num],
    current_end_pos * coords_size_,
    coords_size_));

fragment_cell_ranges.push_back(fragment_cell_range);
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
int dim_num = array_schema_->dim_num();
const T* tile_extents = static_cast<const T*>(array_schema_->tile_extents());
const T* array_domain = static_cast<const T*>(array_schema_->domain());
const T* subarray = static_cast<const T*>(array_->subarray());
const T* domain = static_cast<const T*>(book_keeping_->domain());
const T* non_empty_domain =
  static_cast<const T*>(book_keeping_->non_empty_domain());

// Compute the tile subarray
T* tile_subarray = new T[2 * dim_num];
array_schema_->get_tile_subarray(tile_coords, tile_subarray);

// Compute overlap of tile subarray with non-empty fragment domain
T* tile_domain_overlap_subarray = new T[2 * dim_num];
bool tile_domain_overlap = array_schema_->subarray_overlap(
  tile_subarray, non_empty_domain, tile_domain_overlap_subarray);

if (!tile_domain_overlap) {  // No overlap with the input tile
search_tile_overlap_ = 0;
subarray_area_covered_ = false;
} else {  // Overlap with the input tile
// Find the search tile position
T* tile_coords_norm = new T[dim_num];
for (int i = 0; i < dim_num; ++i)
  tile_coords_norm[i] =
      tile_coords[i] -
      (domain[2 * i] - array_domain[2 * i]) / tile_extents[i];
search_tile_pos_ = array_schema_->get_tile_pos(domain, tile_coords_norm);
delete[] tile_coords_norm;

// Compute overlap of the query subarray with tile
T* query_tile_overlap_subarray = new T[2 * dim_num];
array_schema_->subarray_overlap(
    subarray, tile_subarray, query_tile_overlap_subarray);

// Compute the overlap of the previous results with the non-empty domain
T* search_tile_overlap_subarray = (T*)search_tile_overlap_subarray_;
bool overlap = array_schema_->subarray_overlap(
    query_tile_overlap_subarray,
    tile_domain_overlap_subarray,
    search_tile_overlap_subarray);

if (!overlap) {
  search_tile_overlap_ = 0;
  subarray_area_covered_ = false;
} else {
  // Find the type of the search tile overlap
  T* temp = new T[2 * dim_num];
  search_tile_overlap_ = array_schema_->subarray_overlap(
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
const std::vector<void*>& mbrs = book_keeping_->mbrs();
const T* subarray = static_cast<const T*>(array_->subarray());

// Update the search tile position
if (search_tile_pos_ == -1)
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

const T* mbr = static_cast<const T*>(mbrs[search_tile_pos_]);
search_tile_overlap_ = array_schema_->subarray_overlap(
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
int dim_num = array_schema_->dim_num();
const std::vector<void*>& mbrs = book_keeping_->mbrs();
const T* subarray = static_cast<const T*>(array_->subarray());

// Compute the tile subarray
T* tile_subarray = new T[2 * dim_num];
T* mbr_tile_overlap_subarray = new T[2 * dim_num];
T* tile_subarray_end = new T[dim_num];
array_schema_->get_tile_subarray(tile_coords, tile_subarray);
for (int i = 0; i < dim_num; ++i)
tile_subarray_end[i] = tile_subarray[2 * i + 1];

// Update the search tile position
if (search_tile_pos_ == -1)
search_tile_pos_ = tile_search_range_[0];

// Reset overlaps
search_tile_overlap_ = 0;
mbr_tile_overlap_ = 0;

// Check against last coordinates
if (last_tile_coords_ == nullptr) {
last_tile_coords_ = malloc(coords_size_);
memcpy(last_tile_coords_, tile_coords, coords_size_);
} else {
if (!memcmp(last_tile_coords_, tile_coords, coords_size_)) {
  // Advance only if the MBR does not exceed the tile
  const T* bounding_coords = static_cast<const T*>(
      book_keeping_->bounding_coords()[search_tile_pos_]);
  if (array_schema_->tile_cell_order_cmp(
          &bounding_coords[dim_num], tile_subarray_end) <= 0) {
    ++search_tile_pos_;
  } else {
    delete[] tile_subarray;
    delete[] tile_subarray_end;
    delete[] mbr_tile_overlap_subarray;
    return;
  }
} else {
  memcpy(last_tile_coords_, tile_coords, coords_size_);
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
const T* mbr = static_cast<const T*>(mbrs[search_tile_pos_]);
mbr_tile_overlap_ = array_schema_->subarray_overlap(
    tile_subarray, mbr, mbr_tile_overlap_subarray);

// No overlap with the tile
if (!mbr_tile_overlap_) {
  // Check if we need to break or continue
  const T* bounding_coords = static_cast<const T*>(
      book_keeping_->bounding_coords()[search_tile_pos_]);
  if (array_schema_->tile_cell_order_cmp(
          &bounding_coords[dim_num], tile_subarray_end) > 0) {
    break;
  } else {
    ++search_tile_pos_;
    continue;
  }
}

// Get overlap of MBR with the query inside the tile subarray
search_tile_overlap_ = array_schema_->subarray_overlap(
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
return;
}

*/

/* ****************************** */
/*         PRIVATE METHODS        */
/* ****************************** */

/*

Status ReadState::CMP_COORDS_TO_SEARCH_TILE(
const void* buffer, size_t tile_offset, bool& isequal) {
// For easy reference
char* tile = static_cast<char*>(tiles_[attribute_num_ + 1]);

isequal = false;
// The tile is in main memory
if (tile != nullptr) {
isequal = !memcmp(buffer, tile + tile_offset, coords_size_);
return Status::Ok();
}

// We need to read from the disk
std::string filename = fragment_->fragment_name() + "/" +
                     Configurator::coords() + Configurator::file_suffix();
Status st;
IOMethod read_method = array_->config()->read_method();
#ifdef HAVE_MPI
MPI_Comm* mpi_comm = array_->config()->mpi_comm();
#endif

if (read_method == IOMethod::READ) {
st = filesystem::read_from_file(
    filename,
    tiles_file_offsets_[attribute_num_ + 1] + tile_offset,
    tmp_coords_,
    coords_size_);
} else if (read_method == IOMethod::MPI) {
#ifdef HAVE_MPI
st = filesystem::mpi_io_read_from_file(
    mpi_comm,
    filename,
    tiles_file_offsets_[attribute_num_ + 1] + tile_offset,
    tmp_coords_,
    coords_size_);
#else
// Error: MPI not supported
return LOG_STATUS(Status::Error(
    "Cannot compare coordinates to search tile; MPI not supported"));
#endif
}
if (st.ok()) {
isequal = !memcmp(buffer, tmp_coords_, coords_size_);
}
return st;
}

Status ReadState::compute_bytes_to_copy(
int attribute_id,
int64_t start_cell_pos,
int64_t& end_cell_pos,
size_t buffer_free_space,
size_t buffer_var_free_space,
size_t& bytes_to_copy,
size_t& bytes_var_to_copy) {
// Trivial case
if (buffer_free_space == 0 || buffer_var_free_space == 0) {
bytes_to_copy = 0;
bytes_var_to_copy = 0;
return Status::Ok();
}

// Calculate number of cells in the current tile for this attribute
int64_t cell_num = book_keeping_->cell_num(fetched_tile_[attribute_id]);

// Calculate bytes to copy from the variable tile
const size_t* start_offset;
const size_t* end_offset;
const size_t* med_offset;
RETURN_NOT_OK(GET_CELL_PTR_FROM_OFFSET_TILE(
  attribute_id, start_cell_pos, start_offset));

if (end_cell_pos + 1 < cell_num) {
RETURN_NOT_OK(GET_CELL_PTR_FROM_OFFSET_TILE(
    attribute_id, end_cell_pos + 1, end_offset));
bytes_var_to_copy = *end_offset - *start_offset;
} else {
bytes_var_to_copy = tiles_var_sizes_[attribute_id] - *start_offset;
}

// If bytes do not fit in variable buffer, we need to adjust
if (bytes_var_to_copy > buffer_var_free_space) {
// Perform binary search
int64_t min = start_cell_pos + 1;
int64_t max = end_cell_pos;
int64_t med;
// Invariants:
// (tile[min-1] - tile[start_cell_pos]) < buffer_var_free_space AND
// (tile[max+1] - tile[start_cell_pos]) > buffer_var_free_space
while (min <= max) {
  med = min + ((max - min) / 2);

  // Calculate variable bytes to copy
  RETURN_NOT_OK(
      GET_CELL_PTR_FROM_OFFSET_TILE(attribute_id, med, med_offset));
  bytes_var_to_copy = *med_offset - *start_offset;

  // Check condition
  if (bytes_var_to_copy > buffer_var_free_space)
    max = med - 1;
  else if (bytes_var_to_copy < buffer_var_free_space)
    min = med + 1;
  else
    break;
}

// Determine the end position of the range
int64_t tmp_end = -1;
if (min > max)
  tmp_end = min - 2;
else
  tmp_end = med - 1;

end_cell_pos = std::max(tmp_end, start_cell_pos - 1);

// Update variable bytes to copy
RETURN_NOT_OK(GET_CELL_PTR_FROM_OFFSET_TILE(
    attribute_id, end_cell_pos + 1, end_offset));
bytes_var_to_copy = *end_offset - *start_offset;
}

// Update bytes to copy
bytes_to_copy = (end_cell_pos - start_cell_pos + 1) *
              Configurator::cell_var_offset_size();

// Sanity checks
assert(bytes_to_copy <= buffer_free_space);
assert(bytes_var_to_copy <= buffer_var_free_space);

return Status::Ok();
}

void ReadState::compute_tile_search_range() {
// For easy reference
Datatype coords_type = array_schema_->coords_type();

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
if (tile_search_range_[0] == -1 || tile_search_range_[1] == -1)
done_ = true;
}

template <class T>
void ReadState::compute_tile_search_range_col_or_row() {
// For easy reference
int dim_num = array_schema_->dim_num();
const T* subarray = static_cast<const T*>(array_->subarray());
int64_t tile_num = book_keeping_->tile_num();
const std::vector<void*>& bounding_coords = book_keeping_->bounding_coords();

// Calculate subarray coordinates
T* subarray_min_coords = new T[dim_num];
T* subarray_max_coords = new T[dim_num];
for (int i = 0; i < dim_num; ++i) {
subarray_min_coords[i] = subarray[2 * i];
subarray_max_coords[i] = subarray[2 * i + 1];
}

// --- Find the start tile in search range

// Perform binary search
int64_t min = 0;
int64_t max = tile_num - 1;
int64_t med;
const T* tile_start_coords;
const T* tile_end_coords;
while (min <= max) {
med = min + ((max - min) / 2);

// Get info for bounding coordinates
tile_start_coords = static_cast<const T*>(bounding_coords[med]);
tile_end_coords = &(static_cast<const T*>(bounding_coords[med])[dim_num]);

// Calculate precedence
if (array_schema_->tile_cell_order_cmp(
        subarray_min_coords,
        tile_start_coords) < 0) {  // Subarray min precedes MBR
  max = med - 1;
} else if (
    array_schema_->tile_cell_order_cmp(
        subarray_min_coords,
        tile_end_coords) > 0) {  // Subarray min succeeds MBR
  min = med + 1;
} else {  // Subarray min in MBR
  break;
}
}

bool is_unary = utils::is_unary_subarray(subarray, dim_num);

// Determine the start position of the range
if (max < min)  // Subarray min precedes the tile at position min
tile_search_range_[0] = (is_unary) ? -1 : min;
else  // Subarray min included in a tile
tile_search_range_[0] = med;

if (is_unary) {  // Unary range
// The end position is the same as the start
tile_search_range_[1] = tile_search_range_[0];
} else {  // Need to find the end position
// --- Finding the end tile in search range

// Perform binary search
min = 0;
max = tile_num - 1;
while (min <= max) {
  med = min + ((max - min) / 2);

  // Get info for bounding coordinates
  tile_start_coords = static_cast<const T*>(bounding_coords[med]);
  tile_end_coords = &(static_cast<const T*>(bounding_coords[med])[dim_num]);

  // Calculate precedence
  if (array_schema_->tile_cell_order_cmp(
          subarray_max_coords,
          tile_start_coords) < 0) {  // Subarray max precedes MBR
    max = med - 1;
  } else if (
      array_schema_->tile_cell_order_cmp(
          subarray_max_coords,
          tile_end_coords) > 0) {  // Subarray max succeeds MBR
    min = med + 1;
  } else {  // Subarray max in MBR
    break;
  }
}

// Determine the start position of the range
if (max < min)  // Subarray max succeeds the tile at position max
  tile_search_range_[1] = max;
else  // Subarray max included in a tile
  tile_search_range_[1] = med;
}

// No overlap
if (tile_search_range_[1] < tile_search_range_[0]) {
tile_search_range_[0] = -1;
tile_search_range_[1] = -1;
}

// Clean up
delete[] subarray_min_coords;
delete[] subarray_max_coords;
}

Status ReadState::decompress_tile(
int attribute_id,
unsigned char* tile_compressed,
size_t tile_compressed_size,
unsigned char* tile,
size_t tile_size) {
// For easy reference
Compressor compression = array_schema_->compression(attribute_id);

// Special case for search coordinate tiles
if (attribute_id == attribute_num_ + 1)
attribute_id = attribute_num_;

switch (compression) {
case Compressor::GZIP:
  return decompress_tile_gzip(
      attribute_id, tile_compressed, tile_compressed_size, tile, tile_size);
case Compressor::ZSTD:
  return decompress_tile_zstd(
      attribute_id, tile_compressed, tile_compressed_size, tile, tile_size);
case Compressor::LZ4:
  return decompress_tile_lz4(
      attribute_id, tile_compressed, tile_compressed_size, tile, tile_size);
case Compressor::BLOSC:
#undef BLOSC_LZ4
case Compressor::BLOSC_LZ4:
#undef BLOSC_LZ4HC
case Compressor::BLOSC_LZ4HC:
#undef BLOSC_SNAPPY
case Compressor::BLOSC_SNAPPY:
#undef BLOSC_ZLIB
case Compressor::BLOSC_ZLIB:
#undef BLOSC_ZSTD
case Compressor::BLOSC_ZSTD:
  return decompress_tile_blosc(
      attribute_id, tile_compressed, tile_compressed_size, tile, tile_size);
case Compressor::RLE:
  return decompress_tile_rle(
      attribute_id, tile_compressed, tile_compressed_size, tile, tile_size);
case Compressor::BZIP2:
  return decompress_tile_bzip2(
      attribute_id, tile_compressed, tile_compressed_size, tile, tile_size);
case Compressor::NO_COMPRESSION:
  return Status::Ok();
}
}

Status ReadState::decompress_tile_gzip(
int attribute_id,
unsigned char* tile_compressed,
size_t tile_compressed_size,
unsigned char* tile,
size_t tile_size) {
// For easy reference
const ArraySchema* array_schema = fragment_->array()->array_schema();
int dim_num = array_schema->dim_num();
int attribute_num = array_schema->attribute_num();
bool coords = (attribute_id == attribute_num);
size_t coords_size = array_schema->coords_size();

// Decompress tile
size_t out_size = 0;
RETURN_NOT_OK(GZip::decompress(
  array_schema->type_size(attribute_id),
  tile_compressed,
  tile_compressed_size,
  tile,
  tile_size,
  &out_size));

// Zip coordinates
if (coords)
utils::zip_coordinates(tile, tile_size, dim_num, coords_size);

return Status::Ok();
}

Status ReadState::decompress_tile_zstd(
int attribute_id,
unsigned char* tile_compressed,
size_t tile_compressed_size,
unsigned char* tile,
size_t tile_size) {
// For easy reference
const ArraySchema* array_schema = fragment_->array()->array_schema();
int dim_num = array_schema->dim_num();
int attribute_num = array_schema->attribute_num();
bool coords = (attribute_id == attribute_num);
size_t coords_size = array_schema->coords_size();

// Decompress tile
size_t out_size = 0;
RETURN_NOT_OK(ZStd::decompress(
  array_schema->type_size(attribute_id),
  tile_compressed,
  tile_compressed_size,
  tile,
  tile_size,
  &out_size));

// Zip coordinates
if (coords)
utils::zip_coordinates(tile, tile_size, dim_num, coords_size);

// Success
return Status::Ok();
}

Status ReadState::decompress_tile_lz4(
int attribute_id,
unsigned char* tile_compressed,
size_t tile_compressed_size,
unsigned char* tile,
size_t tile_size) {
// For easy reference
const ArraySchema* array_schema = fragment_->array()->array_schema();
int dim_num = array_schema->dim_num();
int attribute_num = array_schema->attribute_num();
bool coords = (attribute_id == attribute_num);
size_t coords_size = array_schema->coords_size();

size_t out_size = 0;
RETURN_NOT_OK(LZ4::decompress(
  array_schema->type_size(attribute_id),
  tile_compressed,
  tile_compressed_size,
  tile,
  tile_size,
  &out_size));

// Zip coordinates
if (coords)
utils::zip_coordinates(tile, tile_size, dim_num, coords_size);

// Success
return Status::Ok();
}

Status ReadState::decompress_tile_blosc(
int attribute_id,
unsigned char* tile_compressed,
size_t tile_compressed_size,
unsigned char* tile,
size_t tile_size) {
// For easy reference
const ArraySchema* array_schema = fragment_->array()->array_schema();
int dim_num = array_schema->dim_num();
int attribute_num = array_schema->attribute_num();
bool coords = (attribute_id == attribute_num);
size_t coords_size = array_schema->coords_size();

size_t out_size = 0;
RETURN_NOT_OK(Blosc::decompress(
  array_schema->type_size(attribute_id),
  tile_compressed,
  tile_compressed_size,
  tile,
  tile_size,
  &out_size));

// Zip coordinates
if (coords)
utils::zip_coordinates(tile, tile_size, dim_num, coords_size);

// Success
return Status::Ok();
}

Status ReadState::decompress_tile_rle(
int attribute_id,
unsigned char* tile_compressed,
size_t tile_compressed_size,
unsigned char* tile,
size_t tile_size) {
// For easy reference
const ArraySchema* array_schema = fragment_->array()->array_schema();
int dim_num = array_schema->dim_num();
Layout order = array_schema->cell_order();
bool is_coords = (attribute_id == attribute_num_);
size_t value_size = (array_schema->var_size(attribute_id) || is_coords) ?
                      array_schema->type_size(attribute_id) :
                      array_schema->cell_size(attribute_id);

// Decompress tile
Status st;
if (!is_coords) {
size_t out_size;
st = RLE::decompress(
    value_size,
    tile_compressed_,
    tile_compressed_size,
    tile,
    tile_size,
    &out_size);
} else {
if (order == Layout::ROW_MAJOR) {
  st = utils::RLE_decompress_coords_row(
      (unsigned char*)tile_compressed_,
      tile_compressed_size,
      tile,
      tile_size,
      value_size,
      dim_num);
} else if (order == Layout::COL_MAJOR) {
  st = utils::RLE_decompress_coords_col(
      (unsigned char*)tile_compressed_,
      tile_compressed_size,
      tile,
      tile_size,
      value_size,
      dim_num);
} else {  // Error
  assert(0);
  return LOG_STATUS(Status::Error(
      "Failed decompressing with RLE; unsupported cell order"));
}
}
return st;
}

Status ReadState::decompress_tile_bzip2(
int attribute_id,
unsigned char* tile_compressed,
size_t tile_compressed_size,
unsigned char* tile,
size_t tile_size) {
// For easy reference
const ArraySchema* array_schema = fragment_->array()->array_schema();
int dim_num = array_schema->dim_num();
int attribute_num = array_schema->attribute_num();
bool coords = (attribute_id == attribute_num);
size_t coords_size = array_schema->coords_size();

// Decompress tile
size_t out_size = 0;
RETURN_NOT_OK(BZip::decompress(
  array_schema->type_size(attribute_id),
  tile_compressed,
  tile_compressed_size,
  tile,
  tile_size,
  &out_size));

// Zip coordinates
if (coords)
utils::zip_coordinates(tile, tile_size, dim_num, coords_size);

return Status::Ok();
}

template <class T>
Status ReadState::get_cell_pos_after(const T* coords, int64_t* pos) {
// For easy reference
int64_t cell_num = book_keeping_->cell_num(fetched_tile_[attribute_num_ + 1]);

// Perform binary search to find the position of coords in the tile
int64_t min = 0;
int64_t max = cell_num - 1;
int64_t med;
int cmp;
const void* coords_t;
while (min <= max) {
med = min + ((max - min) / 2);

// Update search range
RETURN_NOT_OK(GET_COORDS_PTR_FROM_SEARCH_TILE(med, coords_t));

// Compute order
cmp = array_schema_->tile_cell_order_cmp<T>(
    coords, static_cast<const T*>(coords_t));
if (cmp < 0)
  max = med - 1;
else if (cmp > 0)
  min = med + 1;
else
  break;
}

// Return
if (max < min)
*pos = min;  // After
else
*pos = med + 1;  // After (med is at)
return Status::Ok();
}

template <class T>
Status ReadState::get_cell_pos_at_or_after(const T* coords, int64_t* pos) {
// For easy reference
int64_t cell_num = book_keeping_->cell_num(fetched_tile_[attribute_num_ + 1]);

// Perform binary search to find the position of coords in the tile
int64_t min = 0;
int64_t max = cell_num - 1;
int64_t med;
int cmp;
const void* coords_t;
while (min <= max) {
med = min + ((max - min) / 2);

// Update search range
RETURN_NOT_OK(GET_COORDS_PTR_FROM_SEARCH_TILE(med, coords_t))

// Compute order
cmp = array_schema_->tile_cell_order_cmp<T>(
    coords, static_cast<const T*>(coords_t));

if (cmp < 0)
  max = med - 1;
else if (cmp > 0)
  min = med + 1;
else
  break;
}

// Return
if (max < min)
*pos = min;  // After
else
*pos = med;  // At
return Status::Ok();
}

template <class T>
Status ReadState::get_cell_pos_at_or_before(const T* coords, int64_t* end_pos) {
// For easy reference
int64_t cell_num = book_keeping_->cell_num(fetched_tile_[attribute_num_ + 1]);

// Perform binary search to find the position of coords in the tile
int64_t min = 0;
int64_t max = cell_num - 1;
int64_t med;
int cmp;
const void* coords_t;
while (min <= max) {
med = min + ((max - min) / 2);

// Update search range
RETURN_NOT_OK(GET_COORDS_PTR_FROM_SEARCH_TILE(med, coords_t));

// Compute order
cmp = array_schema_->tile_cell_order_cmp<T>(
    coords, static_cast<const T*>(coords_t));
if (cmp < 0)
  max = med - 1;
else if (cmp > 0)
  min = med + 1;
else
  break;
}

// Return
if (max < min)
*end_pos = max;  // Before
else
*end_pos = med;  // At
return Status::Ok();
}

Status ReadState::GET_COORDS_PTR_FROM_SEARCH_TILE(
int64_t i, const void*& coords) {
// For easy reference
char* tile = static_cast<char*>(tiles_[attribute_num_ + 1]);

// The tile is in main memory
if (tile != nullptr) {
coords = tile + i * coords_size_;
return Status::Ok();
}

// We need to read from the disk
std::string filename = fragment_->fragment_name() + "/" +
                     Configurator::coords() + Configurator::file_suffix();
Status st;
IOMethod read_method = array_->config()->read_method();
#ifdef HAVE_MPI
MPI_Comm* mpi_comm = array_->config()->mpi_comm();
#endif

if (read_method == IOMethod::READ) {
st = filesystem::read_from_file(
    filename,
    tiles_file_offsets_[attribute_num_ + 1] + i * coords_size_,
    tmp_coords_,
    coords_size_);
} else if (read_method == IOMethod::MPI) {
#ifdef HAVE_MPI
st = filesystem::mpi_io_read_from_file(
    mpi_comm,
    filename,
    tiles_file_offsets_[attribute_num_ + 1] + i * coords_size_,
    tmp_coords_,
    coords_size_);
#else
// Error: MPI not supported
return LOG_STATUS(Status::Error(
    "Cannot get coordinates from search tile; MPI not supported"));
#endif
}

// Get coordinates pointer
coords = tmp_coords_;

return st;
}

Status ReadState::GET_CELL_PTR_FROM_OFFSET_TILE(
int attribute_id, int64_t i, const size_t*& offset) {
// For easy reference
char* tile = static_cast<char*>(tiles_[attribute_id]);

// The tile is in main memory
if (tile != nullptr) {
offset = (const size_t*)(tile + i * sizeof(size_t));
return Status::Ok();
}

// We need to read from the disk
std::string filename = fragment_->fragment_name() + "/" +
                     array_schema_->attribute(attribute_id) +
                     Configurator::file_suffix();
Status st;
IOMethod read_method = array_->config()->read_method();
#ifdef HAVE_MPI
MPI_Comm* mpi_comm = array_->config()->mpi_comm();
#endif

if (read_method == IOMethod::READ) {
st = filesystem::read_from_file(
    filename,
    tiles_file_offsets_[attribute_id] + i * sizeof(size_t),
    &tmp_offset_,
    sizeof(size_t));
} else if (read_method == IOMethod::MPI) {
#ifdef HAVE_MPI
st = filesystem::mpi_io_read_from_file(
    mpi_comm,
    filename,
    tiles_file_offsets_[attribute_id] + i * sizeof(size_t),
    &tmp_offset_,
    sizeof(size_t));
#else
// Error: MPI not supported
return LOG_STATUS(Status::Error(
    "Cannot get cell pointer from offset tile; MPI not supported"));
#endif
}

// Get coordinates pointer
offset = &tmp_offset_;
return st;
}

bool ReadState::is_empty_attribute(int attribute_id) const {
// Special case for search coordinate tiles
if (attribute_id == attribute_num_ + 1)
attribute_id = attribute_num_;
return is_empty_attribute_[attribute_id];
}

Status ReadState::map_tile_from_file_cmp(
int attribute_id, off_t offset, size_t tile_size) {
// To handle the special case of the search tile
// The real attribute id corresponds to an actual attribute or coordinates
int attribute_id_real =
  (attribute_id == attribute_num_ + 1) ? attribute_num_ : attribute_id;

// Unmap
if (map_addr_compressed_ != nullptr) {
if (munmap(map_addr_compressed_, map_addr_compressed_length_)) {
  return LOG_STATUS(Status::MMapError(
      "Cannot read tile from file with map; Memory unmap error"));
}
}

// Prepare attribute file name
std::string filename = fragment_->fragment_name() + "/" +
                     array_schema_->attribute(attribute_id_real) +
                     Configurator::file_suffix();

// Calculate offset considering the page size
size_t page_size = sysconf(_SC_PAGE_SIZE);
off_t start_offset = (offset / page_size) * page_size;
size_t extra_offset = offset - start_offset;
size_t new_length = tile_size + extra_offset;

// Open file
int fd = open(filename.c_str(), O_RDONLY);
if (fd == -1) {
munmap(map_addr_compressed_, map_addr_compressed_length_);
map_addr_compressed_ = nullptr;
map_addr_compressed_length_ = 0;
tile_compressed_ = nullptr;
return LOG_STATUS(
    Status::MMapError("Cannot read tile from file; File opening error"));
}

// Map
map_addr_compressed_ = mmap(
  map_addr_compressed_,
  new_length,
  PROT_READ,
  MAP_SHARED,
  fd,
  start_offset);
if (map_addr_compressed_ == MAP_FAILED) {
map_addr_compressed_ = nullptr;
map_addr_compressed_length_ = 0;
tile_compressed_ = nullptr;
return LOG_STATUS(
    Status::MMapError("Cannot read tile from file; Memory map error"));
}
map_addr_compressed_length_ = new_length;

// Set properly the compressed tile pointer
tile_compressed_ = static_cast<char*>(map_addr_compressed_) + extra_offset;

// Close file
if (close(fd)) {
munmap(map_addr_compressed_, map_addr_compressed_length_);
map_addr_compressed_ = nullptr;
map_addr_compressed_length_ = 0;
tile_compressed_ = nullptr;
return LOG_STATUS(
    Status::OSError("Cannot read tile from file; File closing error"));
}

return Status::Ok();
}

Status ReadState::map_tile_from_file_var_cmp(
int attribute_id, off_t offset, size_t tile_size) {
// Unmap
if (map_addr_compressed_ != nullptr) {
if (munmap(map_addr_compressed_, map_addr_compressed_length_)) {
  return LOG_STATUS(Status::MMapError(
      "Cannot read tile from file with map; Memory unmap error"));
}
}

// Prepare attribute file name
std::string filename = fragment_->fragment_name() + "/" +
                     array_schema_->attribute(attribute_id) + "_var" +
                     Configurator::file_suffix();

// Calculate offset considering the page size
size_t page_size = sysconf(_SC_PAGE_SIZE);
off_t start_offset = (offset / page_size) * page_size;
size_t extra_offset = offset - start_offset;
size_t new_length = tile_size + extra_offset;

// Open file
int fd = open(filename.c_str(), O_RDONLY);
if (fd == -1) {
munmap(map_addr_compressed_, map_addr_compressed_length_);
map_addr_compressed_ = nullptr;
map_addr_compressed_length_ = 0;
tile_compressed_ = nullptr;
return LOG_STATUS(
    Status::OSError("Cannot read tile from file; File opening error"));
}

// Map
// new_length could be 0 for variable length fields, mmap will fail
// if new_length == 0
if (new_length > 0u) {
map_addr_compressed_ = mmap(
    map_addr_compressed_,
    new_length,
    PROT_READ,
    MAP_SHARED,
    fd,
    start_offset);
if (map_addr_compressed_ == MAP_FAILED) {
  map_addr_compressed_ = nullptr;
  map_addr_compressed_length_ = 0;
  tile_compressed_ = nullptr;
  return LOG_STATUS(
      Status::MMapError("Cannot read tile from file; Memory map error"));
}
} else {
map_addr_var_[attribute_id] = nullptr;
}
map_addr_compressed_length_ = new_length;

// Set properly the compressed tile pointer
tile_compressed_ = static_cast<char*>(map_addr_compressed_) + extra_offset;

// Close file
if (close(fd)) {
munmap(map_addr_compressed_, map_addr_compressed_length_);
map_addr_compressed_ = nullptr;
map_addr_compressed_length_ = 0;
tile_compressed_ = nullptr;
return LOG_STATUS(
    Status::OSError("Cannot read tile from file; File closing error"));
}
return Status::Ok();
}

Status ReadState::map_tile_from_file_cmp_none(
int attribute_id, off_t offset, size_t tile_size) {
// To handle the special case of the search tile
// The real attribute id corresponds to an actual attribute or coordinates
int attribute_id_real =
  (attribute_id == attribute_num_ + 1) ? attribute_num_ : attribute_id;

// Unmap
if (map_addr_[attribute_id] != nullptr) {
if (munmap(map_addr_[attribute_id], map_addr_lengths_[attribute_id])) {
  return LOG_STATUS(Status::MMapError(
      "Cannot read tile from file with map; Memory unmap error"));
}
}

// Prepare attribute file name
std::string filename = fragment_->fragment_name() + "/" +
                     array_schema_->attribute(attribute_id_real) +
                     Configurator::file_suffix();

// Calculate offset considering the page size
size_t page_size = sysconf(_SC_PAGE_SIZE);
off_t start_offset = (offset / page_size) * page_size;
size_t extra_offset = offset - start_offset;
size_t new_length = tile_size + extra_offset;

// Open file
int fd = open(filename.c_str(), O_RDONLY);
if (fd == -1) {
map_addr_[attribute_id] = nullptr;
map_addr_lengths_[attribute_id] = 0;
tiles_[attribute_id] = nullptr;
tiles_sizes_[attribute_id] = 0;
return LOG_STATUS(
    Status::OSError("Cannot read tile from file; File opening error"));
}

// Map
bool var_size = array_schema_->var_size(attribute_id_real);
int prot = var_size ? (PROT_READ | PROT_WRITE) : PROT_READ;
int flags = var_size ? MAP_PRIVATE : MAP_SHARED;
map_addr_[attribute_id] =
  mmap(map_addr_[attribute_id], new_length, prot, flags, fd, start_offset);
if (map_addr_[attribute_id] == MAP_FAILED) {
map_addr_[attribute_id] = nullptr;
map_addr_lengths_[attribute_id] = 0;
tiles_[attribute_id] = nullptr;
tiles_sizes_[attribute_id] = 0;
return LOG_STATUS(
    Status::MMapError("Cannot read tile from file; Memory map error"));
}
map_addr_lengths_[attribute_id] = new_length;

// Set properly the tile pointer
tiles_[attribute_id] =
  static_cast<char*>(map_addr_[attribute_id]) + extra_offset;

// Close file
if (close(fd)) {
munmap(map_addr_[attribute_id], map_addr_lengths_[attribute_id]);
map_addr_[attribute_id] = nullptr;
map_addr_lengths_[attribute_id] = 0;
tiles_[attribute_id] = nullptr;
tiles_sizes_[attribute_id] = 0;
return LOG_STATUS(
    Status::OSError("Cannot read tile from file; File closing error"));
}
return Status::Ok();
}

Status ReadState::map_tile_from_file_var_cmp_none(
int attribute_id, off_t offset, size_t tile_size) {
// Unmap
if (map_addr_var_[attribute_id] != nullptr) {
if (munmap(
        map_addr_var_[attribute_id], map_addr_var_lengths_[attribute_id])) {
  return LOG_STATUS(Status::MMapError(
      "Cannot read tile from file with map; Memory unmap error"));
}
}

// Prepare attribute file name
std::string filename = fragment_->fragment_name() + "/" +
                     array_schema_->attribute(attribute_id) + "_var" +
                     Configurator::file_suffix();

// Calculate offset considering the page size
size_t page_size = sysconf(_SC_PAGE_SIZE);
off_t start_offset = (offset / page_size) * page_size;
size_t extra_offset = offset - start_offset;
size_t new_length = tile_size + extra_offset;

// Open file
int fd = open(filename.c_str(), O_RDONLY);
if (fd == -1) {
map_addr_var_[attribute_id] = nullptr;
map_addr_var_lengths_[attribute_id] = 0;
tiles_var_[attribute_id] = nullptr;
tiles_var_sizes_[attribute_id] = 0;
return LOG_STATUS(
    Status::OSError("Cannot read tile from file; File opening error"));
}

// Map
// new_length could be 0 for variable length fields, mmap will fail
// if new_length == 0
if (new_length > 0u) {
map_addr_var_[attribute_id] = mmap(
    map_addr_var_[attribute_id],
    new_length,
    PROT_READ,
    MAP_SHARED,
    fd,
    start_offset);
if (map_addr_var_[attribute_id] == MAP_FAILED) {
  map_addr_var_[attribute_id] = nullptr;
  map_addr_var_lengths_[attribute_id] = 0;
  tiles_var_[attribute_id] = nullptr;
  tiles_var_sizes_[attribute_id] = 0;
  return LOG_STATUS(
      Status::MMapError("Cannot read tile from file; Memory map error"));
}
} else {
map_addr_var_[attribute_id] = nullptr;
}
map_addr_var_lengths_[attribute_id] = new_length;

// Set properly the tile pointer
tiles_var_[attribute_id] =
  static_cast<char*>(map_addr_var_[attribute_id]) + extra_offset;
tiles_var_sizes_[attribute_id] = tile_size;

// Close file
if (close(fd)) {
munmap(map_addr_var_[attribute_id], map_addr_var_lengths_[attribute_id]);
map_addr_var_[attribute_id] = nullptr;
map_addr_var_lengths_[attribute_id] = 0;
tiles_var_[attribute_id] = nullptr;
tiles_var_sizes_[attribute_id] = 0;
return LOG_STATUS(
    Status::OSError("Cannot read tile from file; File closing error"));
}
return Status::Ok();
}

#ifdef HAVE_MPI
Status ReadState::mpi_io_read_tile_from_file_cmp(
int attribute_id, off_t offset, size_t tile_size) {
// For easy reference
const MPI_Comm* mpi_comm = array_->config()->mpi_comm();

// To handle the special case of the search tile
// The real attribute id corresponds to an actual attribute or coordinates
int attribute_id_real =
  (attribute_id == attribute_num_ + 1) ? attribute_num_ : attribute_id;

// Potentially allocate compressed tile buffer
if (tile_compressed_ == NULL) {
size_t full_tile_size = fragment_->tile_size(attribute_id_real);
size_t tile_max_size =
    full_tile_size + 6 + 5 * (ceil(full_tile_size / 16834.0));
tile_compressed_ = malloc(tile_max_size);
tile_compressed_allocated_size_ = tile_max_size;
}
// Prepare attribute file name
std::string filename = fragment_->fragment_name() + "/" +
                     array_schema_->attribute(attribute_id_real) +
                     Configurator::file_suffix();
// Read from file
RETURN_NOT_OK(filesystem::mpi_io_read_from_file(
  mpi_comm, filename, offset, tile_compressed_, tile_size));
return Status::Ok();
}

Status ReadState::mpi_io_read_tile_from_file_var_cmp(
int attribute_id, off_t offset, size_t tile_size) {
// For easy reference
const MPI_Comm* mpi_comm = array_->config()->mpi_comm();

// Potentially allocate compressed tile buffer
if (tile_compressed_ == NULL) {
tile_compressed_ = malloc(tile_size);
tile_compressed_allocated_size_ = tile_size;
}

// Potentially expand compressed tile buffer
if (tile_compressed_allocated_size_ < tile_size) {
tile_compressed_ = realloc(tile_compressed_, tile_size);
tile_compressed_allocated_size_ = tile_size;
}

// Prepare attribute file name
std::string filename = fragment_->fragment_name() + "/" +
                     array_schema_->attribute(attribute_id) + "_var" +
                     Configurator::file_suffix();
// Read from file
RETURN_NOT_OK(filesystem::mpi_io_read_from_file(
  mpi_comm, filename, offset, tile_compressed_, tile_size));
return Status::Ok();
}
#endif

Status ReadState::prepare_tile_for_reading(int attribute_id, int64_t tile_i) {
// For easy reference
Compressor compression = array_schema_->compression(attribute_id);

// Invoke the proper function based on the compression type
if (compression == Compressor::NO_COMPRESSION)
return prepare_tile_for_reading_cmp_none(attribute_id, tile_i);
else  // All compressions
return prepare_tile_for_reading_cmp(attribute_id, tile_i);
}

Status ReadState::prepare_tile_for_reading_var(
int attribute_id, int64_t tile_i) {
// For easy reference
Compressor compression = array_schema_->compression(attribute_id);

// Invoke the proper function based on the compression type
if (compression == Compressor::NO_COMPRESSION)
return prepare_tile_for_reading_var_cmp_none(attribute_id, tile_i);
else  // All compressions
return prepare_tile_for_reading_var_cmp(attribute_id, tile_i);
}

Status ReadState::prepare_tile_for_reading_cmp(
int attribute_id, int64_t tile_i) {
// Return if the tile has already been fetched
if (tile_i == fetched_tile_[attribute_id])
return Status::Ok();

// To handle the special case of the search tile
// The real attribute id corresponds to an actual attribute or coordinates
int attribute_id_real =
  (attribute_id == attribute_num_ + 1) ? attribute_num_ : attribute_id;

// For easy reference
size_t cell_size = array_schema_->cell_size(attribute_id_real);
size_t full_tile_size = fragment_->tile_size(attribute_id_real);
int64_t cell_num = book_keeping_->cell_num(tile_i);
size_t tile_size = cell_num * cell_size;
const std::vector<std::vector<off_t>>& tile_offsets =
  book_keeping_->tile_offsets();
int64_t tile_num = book_keeping_->tile_num();

// Allocate space for the tile if needed
if (tiles_[attribute_id] == nullptr)
tiles_[attribute_id] = malloc(full_tile_size);

// Prepare attribute file name
std::string filename = fragment_->fragment_name() + "/" +
                     array_schema_->attribute(attribute_id_real) +
                     Configurator::file_suffix();

// Find file offset where the tile begins
off_t file_offset = tile_offsets[attribute_id_real][tile_i];
off_t file_size = 0;
RETURN_NOT_OK(filesystem::file_size(filename, &file_size));
size_t tile_compressed_size =
  (tile_i == tile_num - 1) ?
      file_size - tile_offsets[attribute_id_real][tile_i] :
      tile_offsets[attribute_id_real][tile_i + 1] -
          tile_offsets[attribute_id_real][tile_i];

// Read tile from file
Status st;
IOMethod read_method = array_->config()->read_method();
if (read_method == IOMethod::READ) {
st = read_tile_from_file_cmp(
    attribute_id, file_offset, tile_compressed_size);
} else if (read_method == IOMethod::MMAP) {
st =
    map_tile_from_file_cmp(attribute_id, file_offset, tile_compressed_size);
} else if (read_method == IOMethod::MPI) {
#ifdef HAVE_MPI
st = mpi_io_read_tile_from_file_cmp(
    attribute_id, file_offset, tile_compressed_size);
#else
// Error: MPI not supported
return LOG_STATUS(Status::Error(
    "Cannot prepare tile for reading (gzip); MPI not supported"));
#endif
}

// Error
if (!st.ok())
return st;

// Decompress tile
RETURN_NOT_OK(decompress_tile(
  attribute_id,
  static_cast<unsigned char*>(tile_compressed_),
  tile_compressed_size,
  static_cast<unsigned char*>(tiles_[attribute_id]),
  full_tile_size));

// Set the tile size
tiles_sizes_[attribute_id] = tile_size;

// Set tile offset
tiles_offsets_[attribute_id] = 0;

// Mark as fetched
fetched_tile_[attribute_id] = tile_i;

// Success
return st;
}

Status ReadState::prepare_tile_for_reading_cmp_none(
int attribute_id, int64_t tile_i) {
// Return if the tile has already been fetched
if (tile_i == fetched_tile_[attribute_id])
return Status::Ok();

// To handle the special case of the search tile
// The real attribute id corresponds to an actual attribute or coordinates
int attribute_id_real =
  (attribute_id == attribute_num_ + 1) ? attribute_num_ : attribute_id;

// For easy reference
size_t cell_size = array_schema_->cell_size(attribute_id_real);
size_t full_tile_size = fragment_->tile_size(attribute_id_real);
int64_t cell_num = book_keeping_->cell_num(tile_i);
size_t tile_size = cell_num * cell_size;

// Find file offset where the tile begins
off_t file_offset = tile_i * full_tile_size;

// Read tile from file
IOMethod read_method = array_->config()->read_method();
if (read_method == IOMethod::READ || read_method == IOMethod::MPI)
set_tile_file_offset(attribute_id, file_offset);
else if (read_method == IOMethod::MMAP)
RETURN_NOT_OK(
    map_tile_from_file_cmp_none(attribute_id, file_offset, tile_size));

// Set the tile size
tiles_sizes_[attribute_id] = tile_size;

// Set tile offset
tiles_offsets_[attribute_id] = 0;

// Mark as fetched
fetched_tile_[attribute_id] = tile_i;

return Status::Ok();
}

Status ReadState::prepare_tile_for_reading_var_cmp(
int attribute_id, int64_t tile_i) {
// Return if the tile has already been fetched
if (tile_i == fetched_tile_[attribute_id])
return Status::Ok();

// Sanity check
assert(
  attribute_id < attribute_num_ && array_schema_->var_size(attribute_id));

// For easy reference
size_t cell_size = Configurator::cell_var_offset_size();
size_t full_tile_size = fragment_->tile_size(attribute_id);
int64_t cell_num = book_keeping_->cell_num(tile_i);
size_t tile_size = cell_num * cell_size;
const std::vector<std::vector<off_t>>& tile_offsets =
  book_keeping_->tile_offsets();
const std::vector<std::vector<off_t>>& tile_var_offsets =
  book_keeping_->tile_var_offsets();
int64_t tile_num = book_keeping_->tile_num();

// ========== Get tile with variable cell offsets ========== //

// Prepare attribute file name
std::string filename = fragment_->fragment_name() + "/" +
                     array_schema_->attribute(attribute_id) +
                     Configurator::file_suffix();

// Find file offset where the tile begins
off_t file_offset = tile_offsets[attribute_id][tile_i];
off_t file_size = 0;
RETURN_NOT_OK(filesystem::file_size(filename, &file_size));
size_t tile_compressed_size =
  (tile_i == tile_num - 1) ?
      file_size - tile_offsets[attribute_id][tile_i] :
      tile_offsets[attribute_id][tile_i + 1] -
          tile_offsets[attribute_id][tile_i];

// Allocate space for the tile if needed
if (tiles_[attribute_id] == nullptr)
tiles_[attribute_id] = malloc(full_tile_size);

// Read tile from file
IOMethod read_method = array_->config()->read_method();
if (read_method == IOMethod::READ) {
RETURN_NOT_OK(read_tile_from_file_cmp(
    attribute_id, file_offset, tile_compressed_size))
} else if (read_method == IOMethod::MMAP) {
RETURN_NOT_OK(map_tile_from_file_cmp(
    attribute_id, file_offset, tile_compressed_size));
} else if (read_method == IOMethod::MPI) {
#ifdef HAVE_MPI
RETURN_NOT_OK(mpi_io_read_tile_from_file_cmp(
    attribute_id, file_offset, tile_compressed_size));
#else
// Error: MPI not supported
return LOG_STATUS(Status::Error(
    "Cannot prepare variable tile for reading (gzip); MPI not supported"));
#endif
}
// Decompress tile
RETURN_NOT_OK(decompress_tile(
  attribute_id,
  static_cast<unsigned char*>(tile_compressed_),
  tile_compressed_size,
  static_cast<unsigned char*>(tiles_[attribute_id]),
  tile_size));

// Set the tile size
tiles_sizes_[attribute_id] = tile_size;

// Update tile offset
tiles_offsets_[attribute_id] = 0;

// ========== Get variable tile ========== //

// Prepare variable attribute file name
filename = fragment_->fragment_name() + "/" +
         array_schema_->attribute(attribute_id) + "_var" +
         Configurator::file_suffix();

// Calculate offset and compressed tile size
file_offset = tile_var_offsets[attribute_id][tile_i];
file_size = 0;
RETURN_NOT_OK(filesystem::file_size(filename, &file_size));
tile_compressed_size =
  (tile_i == tile_num - 1) ?
      file_size - tile_var_offsets[attribute_id][tile_i] :
      tile_var_offsets[attribute_id][tile_i + 1] -
          tile_var_offsets[attribute_id][tile_i];

// Get size of decompressed tile
size_t tile_var_size = book_keeping_->tile_var_sizes()[attribute_id][tile_i];

// Non-empty tile, decompress
Status st;
if (tile_var_size > 0u) {
// Potentially allocate space for buffer
if (tiles_var_[attribute_id] == nullptr) {
  tiles_var_[attribute_id] = malloc(tile_var_size);
  tiles_var_allocated_size_[attribute_id] = tile_var_size;
}

// Potentially expand buffer
if (tile_var_size > tiles_var_allocated_size_[attribute_id]) {
  tiles_var_[attribute_id] =
      realloc(tiles_var_[attribute_id], tile_var_size);
  tiles_var_allocated_size_[attribute_id] = tile_var_size;
}

// Read tile from file
IOMethod read_method = array_->config()->read_method();
if (read_method == IOMethod::READ) {
  RETURN_NOT_OK(read_tile_from_file_var_cmp(
      attribute_id, file_offset, tile_compressed_size));
} else if (read_method == IOMethod::MMAP) {
  RETURN_NOT_OK(map_tile_from_file_var_cmp(
      attribute_id, file_offset, tile_compressed_size));
} else if (read_method == IOMethod::MPI) {
#ifdef HAVE_MPI
  RETURN_NOT_OK(mpi_io_read_tile_from_file_var_cmp(
      attribute_id, file_offset, tile_compressed_size));
#else
  // Error: MPI not supported
  return LOG_STATUS(
      Status::Error("Cannot prepare variable tile for reading (gzip); MPI "
                    "not supported"));
#endif
}
// Decompress tile
RETURN_NOT_OK(decompress_tile(
    attribute_id,
    static_cast<unsigned char*>(tile_compressed_),
    tile_compressed_size,
    static_cast<unsigned char*>(tiles_var_[attribute_id]),
    tile_var_size));
}

// Set the variable tile size
tiles_var_sizes_[attribute_id] = tile_var_size;

// Set the variable tile offset
tiles_var_offsets_[attribute_id] = 0;

// Shift variable cell offsets
shift_var_offsets(attribute_id);

// Mark as fetched
fetched_tile_[attribute_id] = tile_i;

// Success
return Status::Ok();
}

Status ReadState::prepare_tile_for_reading_var_cmp_none(
int attribute_id, int64_t tile_i) {
// Return if the tile has already been fetched
if (tile_i == fetched_tile_[attribute_id])
return Status::Ok();

// Sanity check
assert(
  attribute_id < attribute_num_ && array_schema_->var_size(attribute_id));

// For easy reference
size_t full_tile_size = fragment_->tile_size(attribute_id);
int64_t cell_num = book_keeping_->cell_num(tile_i);
size_t tile_size = cell_num * Configurator::cell_var_offset_size();
int64_t tile_num = book_keeping_->tile_num();
off_t file_offset = tile_i * full_tile_size;

// Read tile from file
IOMethod read_method = array_->config()->read_method();
if (read_method == IOMethod::READ || read_method == IOMethod::MPI)
set_tile_file_offset(attribute_id, file_offset);
else if (read_method == IOMethod::MMAP)
RETURN_NOT_OK(
    map_tile_from_file_cmp_none(attribute_id, file_offset, tile_size));

// Set tile size
tiles_sizes_[attribute_id] = tile_size;

// Calculate the start and end offsets for the variable-sized tile,
// as well as the variable tile size
const size_t* tile_s;
RETURN_NOT_OK(GET_CELL_PTR_FROM_OFFSET_TILE(attribute_id, 0, tile_s));
off_t start_tile_var_offset = *tile_s;
off_t end_tile_var_offset = 0;
size_t tile_var_size;
std::string filename = fragment_->fragment_name() + "/" +
                     array_schema_->attribute(attribute_id) +
                     Configurator::file_suffix();

if (tile_i != tile_num - 1) {  // Not the last tile
if (read_method == IOMethod::READ || read_method == IOMethod::MMAP) {
  RETURN_NOT_OK(filesystem::read_from_file(
      filename,
      file_offset + full_tile_size,
      &end_tile_var_offset,
      Configurator::cell_var_offset_size()));
} else if (read_method == IOMethod::MPI) {
#ifdef HAVE_MPI
  RETURN_NOT_OK(filesystem::mpi_io_read_from_file(
      array_->config()->mpi_comm(),
      filename,
      file_offset + full_tile_size,
      &end_tile_var_offset,
      Configurator::cell_var_offset_size()));
#else
  return LOG_STATUS(Status::Error(
      "Cannot prepare variable tile for reading; MPI not supported"));
#endif
}
tile_var_size = end_tile_var_offset - tile_s[0];
} else {  // Last tile
// Prepare variable attribute file name
std::string filename = fragment_->fragment_name() + "/" +
                       array_schema_->attribute(attribute_id) + "_var" +
                       Configurator::file_suffix();
off_t file_size = 0;
RETURN_NOT_OK(filesystem::file_size(filename, &file_size));
tile_var_size = file_size - tile_s[0];
}

// Read tile from file
if (read_method == IOMethod::READ || read_method == IOMethod::MPI)
set_tile_var_file_offset(attribute_id, start_tile_var_offset);
else if (read_method == IOMethod::MMAP)
RETURN_NOT_OK(map_tile_from_file_var_cmp_none(
    attribute_id, start_tile_var_offset, tile_var_size));

// Set offsets
tiles_offsets_[attribute_id] = 0;
tiles_var_offsets_[attribute_id] = 0;

// Set variable tile size
tiles_var_sizes_[attribute_id] = tile_var_size;

// Shift starting offsets of variable-sized cells
shift_var_offsets(attribute_id);

// Mark as fetched
fetched_tile_[attribute_id] = tile_i;

return Status::Ok();
}  // namespace tiledb

Status ReadState::READ_FROM_TILE(
int attribute_id, void* buffer, size_t tile_offset, size_t bytes_to_copy) {
// For easy reference
char* tile = static_cast<char*>(tiles_[attribute_id]);

// The tile is in main memory
if (tile != nullptr) {
memcpy(buffer, tile + tile_offset, bytes_to_copy);
return Status::Ok();
}

// We need to read from the disk
std::string filename = fragment_->fragment_name() + "/" +
                     array_schema_->attribute(attribute_id) +
                     Configurator::file_suffix();
Status st;
IOMethod read_method = array_->config()->read_method();

#ifdef HAVE_MPI
MPI_Comm* mpi_comm = array_->config()->mpi_comm();
#endif

if (read_method == IOMethod::READ) {
st = filesystem::read_from_file(
    filename,
    tiles_file_offsets_[attribute_id] + tile_offset,
    buffer,
    bytes_to_copy);
} else if (read_method == IOMethod::MPI) {
#ifdef HAVE_MPI
st = filesystem::mpi_io_read_from_file(
    mpi_comm,
    filename,
    tiles_file_offsets_[attribute_id] + tile_offset,
    buffer,
    bytes_to_copy);
#else
// Error: MPI not supported
return LOG_STATUS(
    Status::Error("Cannot read from tile; MPI not supported"));
#endif
}
return st;
}

Status ReadState::READ_FROM_TILE_VAR(
int attribute_id, void* buffer, size_t tile_offset, size_t bytes_to_copy) {
// For easy reference
char* tile = static_cast<char*>(tiles_var_[attribute_id]);

// The tile is in main memory
if (tile != nullptr) {
memcpy(buffer, tile + tile_offset, bytes_to_copy);
return Status::Ok();
}

// We need to read from the disk
std::string filename = fragment_->fragment_name() + "/" +
                     array_schema_->attribute(attribute_id) + "_var" +
                     Configurator::file_suffix();
Status st;
IOMethod read_method = array_->config()->read_method();
#ifdef HAVE_MPI
MPI_Comm* mpi_comm = array_->config()->mpi_comm();
#endif

if (read_method == IOMethod::READ) {
st = filesystem::read_from_file(
    filename,
    tiles_var_file_offsets_[attribute_id] + tile_offset,
    buffer,
    bytes_to_copy);
} else if (read_method == IOMethod::MPI) {
#ifdef HAVE_MPI
st = filesystem::mpi_io_read_from_file(
    mpi_comm,
    filename,
    tiles_var_file_offsets_[attribute_id] + tile_offset,
    buffer,
    bytes_to_copy);
#else
// Error: MPI not supported
return LOG_STATUS(
    Status::Error("Cannot read from variable tile; MPI not supported"));
#endif
}
return st;
}

Status ReadState::read_tile_from_file_cmp(
int attribute_id, off_t offset, size_t tile_size) {
// To handle the special case of the search tile
// The real attribute id corresponds to an actual attribute or coordinates
int attribute_id_real =
  (attribute_id == attribute_num_ + 1) ? attribute_num_ : attribute_id;

// Potentially allocate compressed tile buffer
if (tile_compressed_ == nullptr) {
tile_compressed_ = malloc(tile_size);
tile_compressed_allocated_size_ = tile_size;
}

// Potentially expand compressed tile buffer
if (tile_compressed_allocated_size_ < tile_size) {
tile_compressed_ = realloc(tile_compressed_, tile_size);
tile_compressed_allocated_size_ = tile_size;
}

// Prepare attribute file name
std::string filename = fragment_->fragment_name() + "/" +
                     array_schema_->attribute(attribute_id_real) +
                     Configurator::file_suffix();

// Read from file
return filesystem::read_from_file(
  filename, offset, tile_compressed_, tile_size);
}

Status ReadState::read_tile_from_file_var_cmp(
int attribute_id, off_t offset, size_t tile_size) {
// Potentially allocate compressed tile buffer
if (tile_compressed_ == nullptr) {
tile_compressed_ = malloc(tile_size);
tile_compressed_allocated_size_ = tile_size;
}

// Potentially expand compressed tile buffer
if (tile_compressed_allocated_size_ < tile_size) {
tile_compressed_ = realloc(tile_compressed_, tile_size);
tile_compressed_allocated_size_ = tile_size;
}

// Prepare attribute file name
std::string filename = fragment_->fragment_name() + "/" +
                     array_schema_->attribute(attribute_id) + "_var" +
                     Configurator::file_suffix();

// Read from file
return filesystem::read_from_file(
  filename, offset, tile_compressed_, tile_size);
}

void ReadState::set_tile_file_offset(int attribute_id, off_t offset) {
// Set file offset
tiles_file_offsets_[attribute_id] = offset;
}

void ReadState::set_tile_var_file_offset(int attribute_id, off_t offset) {
// Set file offset
tiles_var_file_offsets_[attribute_id] = offset;
}

void ReadState::shift_var_offsets(int attribute_id) {
// For easy reference
int64_t cell_num =
  tiles_sizes_[attribute_id] / Configurator::cell_var_offset_size();
size_t* tile_s = static_cast<size_t*>(tiles_[attribute_id]);
size_t first_offset = tile_s[0];

// Shift offsets
for (int64_t i = 0; i < cell_num; ++i)
tile_s[i] -= first_offset;
}

void ReadState::shift_var_offsets(
void* buffer, int64_t offset_num, size_t new_start_offset) {
// For easy reference
size_t* buffer_s = static_cast<size_t*>(buffer);
size_t start_offset = buffer_s[0];

// Shift offsets
for (int64_t i = 0; i < offset_num; ++i)
buffer_s[i] = buffer_s[i] - start_offset + new_start_offset;
}

// Explicit template instantiations

template Status ReadState::get_coords_after<int>(
const int* coords, int* coords_after, bool& coords_retrieved);
template Status ReadState::get_coords_after<int64_t>(
const int64_t* coords, int64_t* coords_after, bool& coords_retrieved);
template Status ReadState::get_coords_after<float>(
const float* coords, float* coords_after, bool& coords_retrieved);
template Status ReadState::get_coords_after<double>(
const double* coords, double* coords_after, bool& coords_retrieved);
template Status ReadState::get_coords_after<int8_t>(
const int8_t* coords, int8_t* coords_after, bool& coords_retrieved);
template Status ReadState::get_coords_after<uint8_t>(
const uint8_t* coords, uint8_t* coords_after, bool& coords_retrieved);
template Status ReadState::get_coords_after<int16_t>(
const int16_t* coords, int16_t* coords_after, bool& coords_retrieved);
template Status ReadState::get_coords_after<uint16_t>(
const uint16_t* coords, uint16_t* coords_after, bool& coords_retrieved);
template Status ReadState::get_coords_after<uint32_t>(
const uint32_t* coords, uint32_t* coords_after, bool& coords_retrieved);
template Status ReadState::get_coords_after<uint64_t>(
const uint64_t* coords, uint64_t* coords_after, bool& coords_retrieved);

template Status ReadState::get_enclosing_coords<int>(
int tile_i,
const int* target_coords,
const int* start_coords,
const int* end_coords,
int* left_coords,
int* right_coords,
bool& left_retrieved,
bool& right_retrieved,
bool& target_exists);
template Status ReadState::get_enclosing_coords<int64_t>(
int tile_i,
const int64_t* target_coords,
const int64_t* start_coords,
const int64_t* end_coords,
int64_t* left_coords,
int64_t* right_coords,
bool& left_retrieved,
bool& right_retrieved,
bool& target_exists);
template Status ReadState::get_enclosing_coords<float>(
int tile_i,
const float* target_coords,
const float* start_coords,
const float* end_coords,
float* left_coords,
float* right_coords,
bool& left_retrieved,
bool& right_retrieved,
bool& target_exists);
template Status ReadState::get_enclosing_coords<double>(
int tile_i,
const double* target_coords,
const double* start_coords,
const double* end_coords,
double* left_coords,
double* right_coords,
bool& left_retrieved,
bool& right_retrieved,
bool& target_exists);
template Status ReadState::get_enclosing_coords<int8_t>(
int tile_i,
const int8_t* target_coords,
const int8_t* start_coords,
const int8_t* end_coords,
int8_t* left_coords,
int8_t* right_coords,
bool& left_retrieved,
bool& right_retrieved,
bool& target_exists);
template Status ReadState::get_enclosing_coords<uint8_t>(
int tile_i,
const uint8_t* target_coords,
const uint8_t* start_coords,
const uint8_t* end_coords,
uint8_t* left_coords,
uint8_t* right_coords,
bool& left_retrieved,
bool& right_retrieved,
bool& target_exists);
template Status ReadState::get_enclosing_coords<int16_t>(
int tile_i,
const int16_t* target_coords,
const int16_t* start_coords,
const int16_t* end_coords,
int16_t* left_coords,
int16_t* right_coords,
bool& left_retrieved,
bool& right_retrieved,
bool& target_exists);
template Status ReadState::get_enclosing_coords<uint16_t>(
int tile_i,
const uint16_t* target_coords,
const uint16_t* start_coords,
const uint16_t* end_coords,
uint16_t* left_coords,
uint16_t* right_coords,
bool& left_retrieved,
bool& right_retrieved,
bool& target_exists);
template Status ReadState::get_enclosing_coords<uint32_t>(
int tile_i,
const uint32_t* target_coords,
const uint32_t* start_coords,
const uint32_t* end_coords,
uint32_t* left_coords,
uint32_t* right_coords,
bool& left_retrieved,
bool& right_retrieved,
bool& target_exists);
template Status ReadState::get_enclosing_coords<uint64_t>(
int tile_i,
const uint64_t* target_coords,
const uint64_t* start_coords,
const uint64_t* end_coords,
uint64_t* left_coords,
uint64_t* right_coords,
bool& left_retrieved,
bool& right_retrieved,
bool& target_exists);

template Status ReadState::get_fragment_cell_pos_range_sparse<int>(
const FragmentInfo& fragment_info,
const int* cell_range,
FragmentCellPosRange& fragment_cell_pos_range);
template Status ReadState::get_fragment_cell_pos_range_sparse<int64_t>(
const FragmentInfo& fragment_info,
const int64_t* cell_range,
FragmentCellPosRange& fragment_cell_pos_range);
template Status ReadState::get_fragment_cell_pos_range_sparse<float>(
const FragmentInfo& fragment_info,
const float* cell_range,
FragmentCellPosRange& fragment_cell_pos_range);
template Status ReadState::get_fragment_cell_pos_range_sparse<double>(
const FragmentInfo& fragment_info,
const double* cell_range,
FragmentCellPosRange& fragment_cell_pos_range);
template Status ReadState::get_fragment_cell_pos_range_sparse<int8_t>(
const FragmentInfo& fragment_info,
const int8_t* cell_range,
FragmentCellPosRange& fragment_cell_pos_range);
template Status ReadState::get_fragment_cell_pos_range_sparse<uint8_t>(
const FragmentInfo& fragment_info,
const uint8_t* cell_range,
FragmentCellPosRange& fragment_cell_pos_range);
template Status ReadState::get_fragment_cell_pos_range_sparse<int16_t>(
const FragmentInfo& fragment_info,
const int16_t* cell_range,
FragmentCellPosRange& fragment_cell_pos_range);
template Status ReadState::get_fragment_cell_pos_range_sparse<uint16_t>(
const FragmentInfo& fragment_info,
const uint16_t* cell_range,
FragmentCellPosRange& fragment_cell_pos_range);
template Status ReadState::get_fragment_cell_pos_range_sparse<uint32_t>(
const FragmentInfo& fragment_info,
const uint32_t* cell_range,
FragmentCellPosRange& fragment_cell_pos_range);
template Status ReadState::get_fragment_cell_pos_range_sparse<uint64_t>(
const FragmentInfo& fragment_info,
const uint64_t* cell_range,
FragmentCellPosRange& fragment_cell_pos_range);

template Status ReadState::get_fragment_cell_ranges_sparse<int>(
int fragment_i,
const int* start_coords,
const int* end_coords,
FragmentCellRanges& fragment_cell_ranges);
template Status ReadState::get_fragment_cell_ranges_sparse<int64_t>(
int fragment_i,
const int64_t* start_coords,
const int64_t* end_coords,
FragmentCellRanges& fragment_cell_ranges);
template Status ReadState::get_fragment_cell_ranges_sparse<float>(
int fragment_i,
const float* start_coords,
const float* end_coords,
FragmentCellRanges& fragment_cell_ranges);
template Status ReadState::get_fragment_cell_ranges_sparse<double>(
int fragment_i,
const double* start_coords,
const double* end_coords,
FragmentCellRanges& fragment_cell_ranges);
template Status ReadState::get_fragment_cell_ranges_sparse<int8_t>(
int fragment_i,
const int8_t* start_coords,
const int8_t* end_coords,
FragmentCellRanges& fragment_cell_ranges);
template Status ReadState::get_fragment_cell_ranges_sparse<uint8_t>(
int fragment_i,
const uint8_t* start_coords,
const uint8_t* end_coords,
FragmentCellRanges& fragment_cell_ranges);
template Status ReadState::get_fragment_cell_ranges_sparse<int16_t>(
int fragment_i,
const int16_t* start_coords,
const int16_t* end_coords,
FragmentCellRanges& fragment_cell_ranges);
template Status ReadState::get_fragment_cell_ranges_sparse<uint16_t>(
int fragment_i,
const uint16_t* start_coords,
const uint16_t* end_coords,
FragmentCellRanges& fragment_cell_ranges);
template Status ReadState::get_fragment_cell_ranges_sparse<uint32_t>(
int fragment_i,
const uint32_t* start_coords,
const uint32_t* end_coords,
FragmentCellRanges& fragment_cell_ranges);
template Status ReadState::get_fragment_cell_ranges_sparse<uint64_t>(
int fragment_i,
const uint64_t* start_coords,
const uint64_t* end_coords,
FragmentCellRanges& fragment_cell_ranges);

template Status ReadState::get_fragment_cell_ranges_sparse<int>(
int fragment_i, FragmentCellRanges& fragment_cell_ranges);
template Status ReadState::get_fragment_cell_ranges_sparse<int64_t>(
int fragment_i, FragmentCellRanges& fragment_cell_ranges);
template Status ReadState::get_fragment_cell_ranges_sparse<int8_t>(
int fragment_i, FragmentCellRanges& fragment_cell_ranges);
template Status ReadState::get_fragment_cell_ranges_sparse<uint8_t>(
int fragment_i, FragmentCellRanges& fragment_cell_ranges);
template Status ReadState::get_fragment_cell_ranges_sparse<int16_t>(
int fragment_i, FragmentCellRanges& fragment_cell_ranges);
template Status ReadState::get_fragment_cell_ranges_sparse<uint16_t>(
int fragment_i, FragmentCellRanges& fragment_cell_ranges);
template Status ReadState::get_fragment_cell_ranges_sparse<uint32_t>(
int fragment_i, FragmentCellRanges& fragment_cell_ranges);
template Status ReadState::get_fragment_cell_ranges_sparse<uint64_t>(
int fragment_i, FragmentCellRanges& fragment_cell_ranges);

template Status ReadState::get_fragment_cell_ranges_dense<int>(
int fragment_i, FragmentCellRanges& fragment_cell_ranges);
template Status ReadState::get_fragment_cell_ranges_dense<int64_t>(
int fragment_i, FragmentCellRanges& fragment_cell_ranges);
template Status ReadState::get_fragment_cell_ranges_dense<int8_t>(
int fragment_i, FragmentCellRanges& fragment_cell_ranges);
template Status ReadState::get_fragment_cell_ranges_dense<uint8_t>(
int fragment_i, FragmentCellRanges& fragment_cell_ranges);
template Status ReadState::get_fragment_cell_ranges_dense<int16_t>(
int fragment_i, FragmentCellRanges& fragment_cell_ranges);
template Status ReadState::get_fragment_cell_ranges_dense<uint16_t>(
int fragment_i, FragmentCellRanges& fragment_cell_ranges);
template Status ReadState::get_fragment_cell_ranges_dense<uint32_t>(
int fragment_i, FragmentCellRanges& fragment_cell_ranges);
template Status ReadState::get_fragment_cell_ranges_dense<uint64_t>(
int fragment_i, FragmentCellRanges& fragment_cell_ranges);

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

 */

}  // namespace tiledb
