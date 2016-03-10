/**
 * @file   fragment.cc
 * @author Stavros Papadopoulos <stavrosp@csail.mit.edu>
 *
 * @section LICENSE
 *
 * The MIT License
 * 
 * @copyright Copyright (c) 2015 Stavros Papadopoulos <stavrosp@csail.mit.edu>
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
 * This file implements the Fragment class.
 */

#include "fragment.h"
#include "utils.h"
#include <cassert>
#include <cstdio>
#include <cstring>
#include <iostream>

/* ****************************** */
/*             MACROS             */
/* ****************************** */

#if VERBOSE == 1
#  define PRINT_ERROR(x) std::cerr << "[TileDB] Error: " << x << ".\n" 
#  define PRINT_WARNING(x) std::cerr << "[TileDB] Warning: " \
                                     << x << ".\n"
#elif VERBOSE == 2
#  define PRINT_ERROR(x) std::cerr << "[TileDB::Fragment] Error: " \
                                   << x << ".\n" 
#  define PRINT_WARNING(x) std::cerr << "[TileDB::Fragment] Warning: " \
                                     << x << ".\n"
#else
#  define PRINT_ERROR(x) do { } while(0) 
#  define PRINT_WARNING(x) do { } while(0) 
#endif

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

Fragment::Fragment(const Array* array)
    : array_(array) {
  read_state_ = NULL;
  write_state_ = NULL;
  book_keeping_ = NULL;
}

Fragment::~Fragment() {
  if(write_state_ != NULL)
    delete write_state_;

  if(read_state_ != NULL)
    delete read_state_;

  if(book_keeping_ != NULL)
    delete book_keeping_;
}

/* ****************************** */
/*            ACCESSORS           */
/* ****************************** */

void* Fragment::mbr(int64_t pos) const {
  return book_keeping_->mbrs()[pos];
}

bool Fragment::overlaps() const {
  if(read_state_->overlap() == ReadState::NONE)
    return false;
  else
    return true;
}

bool Fragment::full_domain() const {
  return full_domain_;
}

bool Fragment::overflow(int attribute_id) const {
  return read_state_->overflow(attribute_id);
}

template<class T>
bool Fragment::max_overlap(const T* max_overlap_range) const {
  return read_state_->max_overlap<T>(max_overlap_range);
}

const Array* Fragment::array() const {
  return array_;
}

int64_t Fragment::cell_num_per_tile() const {
  return (dense_) ? array_->array_schema()->cell_num_per_tile() : 
                    array_->array_schema()->capacity(); 
}

bool Fragment::dense() const {
  return dense_;
}

const std::string& Fragment::fragment_name() const {
  return fragment_name_;
}

template<class T>
void Fragment::compute_fragment_cell_ranges(
    const FragmentInfo& fragment_info,
    FragmentCellRanges& fragment_cell_ranges) const {
  read_state_->compute_fragment_cell_ranges<T>(
      fragment_info, 
      fragment_cell_ranges);
}

const void* Fragment::get_global_tile_coords() const {
  return read_state_->get_global_tile_coords();
}

int Fragment::read(void** buffers, size_t* buffer_sizes) {
  // Sanity check
  if(array_->range() == NULL) {
    PRINT_ERROR("Cannot read from fragment; Invalid range");
    return TILEDB_BK_ERR;
  }

  // Forward the read command to the read state
  int rc = read_state_->read(buffers, buffer_sizes);

  if(rc == TILEDB_RS_OK)
    return TILEDB_FG_OK;
  else
    return TILEDB_FG_ERR;
}

ReadState* Fragment::read_state() const {
  return read_state_;
}

size_t Fragment::tile_size(int attribute_id) const {
  // For easy reference
  const ArraySchema* array_schema = array_->array_schema();
  bool var_size = array_schema->var_size(attribute_id);

  int64_t cell_num_per_tile = (dense_) ? 
              array_schema->cell_num_per_tile() : 
              array_schema->capacity(); 
 
  return (var_size) ? 
             cell_num_per_tile * TILEDB_CELL_VAR_OFFSET_SIZE :
             cell_num_per_tile * array_schema->cell_size(attribute_id);
}

template<class T>
int Fragment::copy_cell_range(
    int attribute_id,
    int tile_i,
    void* buffer,  
    size_t buffer_size,
    size_t& buffer_offset,
    const CellPosRange& cell_pos_range) {
  if(read_state_->copy_cell_range<T>(
         attribute_id, 
         tile_i,
         buffer,
         buffer_size,
         buffer_offset,
         cell_pos_range) != TILEDB_RS_OK)
    return TILEDB_FG_ERR;
  else
    return TILEDB_FG_OK;
}

template<class T>
int Fragment::copy_cell_range_var(
    int attribute_id,
    int tile_i,
    void* buffer,  
    size_t buffer_size,
    size_t& buffer_offset,
    void* buffer_var,  
    size_t buffer_var_size,
    size_t& buffer_var_offset,
    const CellPosRange& cell_pos_range) {
  if(read_state_->copy_cell_range_var<T>(
         attribute_id, 
         tile_i,
         buffer,
         buffer_size,
         buffer_offset,
         buffer_var,
         buffer_var_size,
         buffer_var_offset,
         cell_pos_range) != TILEDB_RS_OK)
    return TILEDB_FG_ERR;
  else
    return TILEDB_FG_OK;
}

/* ****************************** */
/*            MUTATORS            */
/* ****************************** */

// TODO: make it return int (errors)
template<class T>
bool Fragment::coords_exist(
    int64_t tile_i,
    const T* coords) {
  return read_state_->coords_exist<T>(tile_i, coords);
}

int64_t Fragment::overlapping_tiles_num() const {
  return read_state_->overlapping_tiles_num();
}

template<class T>
void Fragment::tile_done(int attribute_id) {
  read_state_->tile_done<T>(attribute_id);
}

void Fragment::reset_overflow() {
  read_state_->reset_overflow();
}

template<class T>
int Fragment::get_first_two_coords(
    int tile_i,
    T* start_coords,
    T* first_coords,
    T* second_coords) {
  return read_state_->get_first_two_coords<T>(
      tile_i,
      start_coords, 
      first_coords, 
      second_coords);
}

template<class T>
int Fragment::get_first_coords_after(
    int tile_i,
    T* start_coords_before,
    T* first_coords) {
  return read_state_->get_first_coords_after<T>(
      tile_i,
      start_coords_before, 
      first_coords);
}

void Fragment::get_next_overlapping_tile_mult() {
  read_state_->get_next_overlapping_tile_mult();
}

template<class T>
int Fragment::get_cell_pos_ranges_sparse(
    const FragmentInfo& fragment_info,
    const T* tile_domain,
    const T* cell_range,
    FragmentCellPosRanges& fragment_cell_pos_ranges) {
  if(read_state_->get_cell_pos_ranges_sparse(
             fragment_info,
             tile_domain,
             cell_range,
             fragment_cell_pos_ranges) != TILEDB_RS_OK)
    return TILEDB_FG_ERR;
  else
    return TILEDB_FG_OK;
}

template<class T>
void Fragment::get_next_overlapping_tile_sparse() {
  read_state_->get_next_overlapping_tile_sparse<T>();
}

void Fragment::get_bounding_coords(void* bounding_coords) const {
  read_state_->get_bounding_coords(bounding_coords);
}

int Fragment::init(const std::string& fragment_name, const void* range) {
  // Set fragment name
  fragment_name_ = fragment_name;

  // Check if the array is dense or not
  if(array_->mode() == TILEDB_WRITE || 
     array_->mode() == TILEDB_WRITE_UNSORTED) {
    dense_ = true;
    // Check the attributes given upon initialization
    const std::vector<int>& attribute_ids = array_->attribute_ids();
    int id_num = attribute_ids.size();
    int attribute_num = array_->array_schema()->attribute_num();
    for(int i=0; i<id_num; ++i) {
      if(attribute_ids[i] == attribute_num) {
        dense_ = false;
        break;
      }
    }
  } else { // The array mode is TILEDB_READ or TILEDB_READ_REVERSE 
    // The coordinates file should not exist
    dense_ = !is_file(
                fragment_name_ + "/" + TILEDB_COORDS_NAME + TILEDB_FILE_SUFFIX);
  }

  // For easy referece
  int mode = array_->mode();

  // Initialize book-keeping and write state
  book_keeping_ = new BookKeeping(this);
  if(mode == TILEDB_WRITE || mode == TILEDB_WRITE_UNSORTED) {
    read_state_ = NULL;
    if(book_keeping_->init(range) != TILEDB_BK_OK) {
      delete book_keeping_;
      book_keeping_ = NULL;
      write_state_ = NULL;
      return TILEDB_FG_ERR;
    }
    write_state_ = new WriteState(this, book_keeping_);
  } else if(mode == TILEDB_READ || mode == TILEDB_READ_REVERSE) {
    write_state_ = NULL;
    if(book_keeping_->load() != TILEDB_BK_OK) {
      delete book_keeping_;
      book_keeping_ = NULL;
      return TILEDB_FG_ERR;
    }
    read_state_ = new ReadState(this, book_keeping_);
    full_domain_ = !memcmp(
                       book_keeping_->non_empty_domain(), 
                       array_->array_schema()->domain(), 
                       2*array_->array_schema()->coords_size());
  }

  // Success
  return TILEDB_FG_OK;
}

int Fragment::write(const void** buffers, const size_t* buffer_sizes) {
  // Forward the write command to the write state
  int rc = write_state_->write(buffers, buffer_sizes);

  if(rc == TILEDB_WS_OK)
    return TILEDB_FG_OK;
  else
    return TILEDB_FG_ERR;
}

/* ****************************** */
/*               MISC             */
/* ****************************** */

void Fragment::reinit_read_state() {
  if(read_state_ != NULL)
    delete read_state_;
  read_state_ = new ReadState(this, book_keeping_);
}

int Fragment::finalize() {
  // The fragment was opened for writing
  if(write_state_ != NULL) {
    assert(book_keeping_ != NULL);  
    int rc_ws = write_state_->finalize();
    int rc_bk = book_keeping_->finalize();
    int rc_rn = rename_fragment();
    int rc_cf = create_fragment_file(fragment_name_);
      return TILEDB_WS_ERR;
    if(rc_ws != TILEDB_WS_OK || rc_bk != TILEDB_BK_OK || 
       rc_rn != TILEDB_FG_OK || rc_cf != TILEDB_UT_OK)
      return TILEDB_FG_ERR;
    else 
      return TILEDB_FG_OK;
  } else { // The fragment was opened for reading
    // Nothing to be done
    return TILEDB_FG_OK;
  } 
}

/* ****************************** */
/*         PRIVATE METHODS        */
/* ****************************** */

int Fragment::rename_fragment() {
  // Do nothing in READ mode
  if(array_->mode() == TILEDB_READ || array_->mode() == TILEDB_READ_REVERSE)
    return TILEDB_FG_OK;

  std::string parent_dir = ::parent_dir(fragment_name_);
  std::string new_fragment_name = parent_dir + "/" +
                                  fragment_name_.substr(parent_dir.size() + 2);

  if(rename(fragment_name_.c_str(), new_fragment_name.c_str())) {
    PRINT_ERROR(std::string("Cannot rename fragment directory; ") +
                strerror(errno));
    return TILEDB_FG_ERR;
  } 

  fragment_name_ = new_fragment_name;

  return TILEDB_FG_OK;
}

// Explicit template instantiations
template void Fragment::compute_fragment_cell_ranges<int>(
    const FragmentInfo& fragment_info,
    FragmentCellRanges& fragment_cell_ranges) const;
template void Fragment::compute_fragment_cell_ranges<int64_t>(
    const FragmentInfo& fragment_info,
    FragmentCellRanges& fragment_cell_ranges) const;
template void Fragment::compute_fragment_cell_ranges<float>(
    const FragmentInfo& fragment_info,
    FragmentCellRanges& fragment_cell_ranges) const;
template void Fragment::compute_fragment_cell_ranges<double>(
    const FragmentInfo& fragment_info,
    FragmentCellRanges& fragment_cell_ranges) const;

template bool Fragment::max_overlap<int>(
    const int* max_overlap_range) const;
template bool Fragment::max_overlap<int64_t>(
    const int64_t* max_overlap_range) const;
template bool Fragment::max_overlap<float>(
    const float* max_overlap_range) const;
template bool Fragment::max_overlap<double>(
    const double* max_overlap_range) const;

template int Fragment::get_first_two_coords<int>(
    int tile_i,
    int* start_coords,
    int* first_coords,
    int* second_coords);
template int Fragment::get_first_two_coords<int64_t>(
    int tile_i,
    int64_t* start_coords,
    int64_t* first_coords,
    int64_t* second_coords);
template int Fragment::get_first_two_coords<float>(
    int tile_i,
    float* start_coords,
    float* first_coords,
    float* second_coords);
template int Fragment::get_first_two_coords<double>(
    int tile_i,
    double* start_coords,
    double* first_coords,
    double* second_coords);

template int Fragment::get_cell_pos_ranges_sparse<int>(
    const FragmentInfo& fragment_info,
    const int* tile_domain,
    const int* cell_range,
    FragmentCellPosRanges& fragment_cell_pos_ranges);
template int Fragment::get_cell_pos_ranges_sparse<int64_t>(
    const FragmentInfo& fragment_info,
    const int64_t* tile_domain,
    const int64_t* cell_range,
    FragmentCellPosRanges& fragment_cell_pos_ranges);
template int Fragment::get_cell_pos_ranges_sparse<float>(
    const FragmentInfo& fragment_info,
    const float* tile_domain,
    const float* cell_range,
    FragmentCellPosRanges& fragment_cell_pos_ranges);
template int Fragment::get_cell_pos_ranges_sparse<double>(
    const FragmentInfo& fragment_info,
    const double* tile_domain,
    const double* cell_range,
    FragmentCellPosRanges& fragment_cell_pos_ranges);

template int Fragment::copy_cell_range<int>(
    int attribute_id,
    int tile_i,
    void* buffer,  
    size_t buffer_size,
    size_t& buffer_offset,
    const CellPosRange& cell_pos_range);
template int Fragment::copy_cell_range<int64_t>(
    int attribute_id,
    int tile_i,
    void* buffer,  
    size_t buffer_size,
    size_t& buffer_offset,
    const CellPosRange& cell_pos_range);
template int Fragment::copy_cell_range<float>(
    int attribute_id,
    int tile_i,
    void* buffer,  
    size_t buffer_size,
    size_t& buffer_offset,
    const CellPosRange& cell_pos_range);
template int Fragment::copy_cell_range<double>(
    int attribute_id,
    int tile_i,
    void* buffer,  
    size_t buffer_size,
    size_t& buffer_offset,
    const CellPosRange& cell_pos_range);

template bool Fragment::coords_exist<int>(
    int64_t tile_i,
    const int* coords);
template bool Fragment::coords_exist<int64_t>(
    int64_t tile_i,
    const int64_t* coords);
template bool Fragment::coords_exist<float>(
    int64_t tile_i,
    const float* coords);
template bool Fragment::coords_exist<double>(
    int64_t tile_i,
    const double* coords);

template int Fragment::copy_cell_range_var<int>(
    int attribute_id,
    int tile_i,
    void* buffer,  
    size_t buffer_size,
    size_t& buffer_offset,
    void* buffer_var,  
    size_t buffer_var_size,
    size_t& buffer_var_offset,
    const CellPosRange& cell_pos_range);
template int Fragment::copy_cell_range_var<int64_t>(
    int attribute_id,
    int tile_i,
    void* buffer,  
    size_t buffer_size,
    size_t& buffer_offset,
    void* buffer_var,  
    size_t buffer_var_size,
    size_t& buffer_var_offset,
    const CellPosRange& cell_pos_range);
template int Fragment::copy_cell_range_var<float>(
    int attribute_id,
    int tile_i,
    void* buffer,  
    size_t buffer_size,
    size_t& buffer_offset,
    void* buffer_var,  
    size_t buffer_var_size,
    size_t& buffer_var_offset,
    const CellPosRange& cell_pos_range);
template int Fragment::copy_cell_range_var<double>(
    int attribute_id,
    int tile_i,
    void* buffer,  
    size_t buffer_size,
    size_t& buffer_offset,
    void* buffer_var,  
    size_t buffer_var_size,
    size_t& buffer_var_offset,
    const CellPosRange& cell_pos_range);

template void Fragment::get_next_overlapping_tile_sparse<int>();
template void Fragment::get_next_overlapping_tile_sparse<int64_t>();
template void Fragment::get_next_overlapping_tile_sparse<float>();
template void Fragment::get_next_overlapping_tile_sparse<double>();

template void Fragment::tile_done<int>(int attribute_id);
template void Fragment::tile_done<int64_t>(int attribute_id);
template void Fragment::tile_done<float>(int attribute_id);
template void Fragment::tile_done<double>(int attribute_id);

template int Fragment::get_first_coords_after<int>(
    int tile_i,
    int* start_coords_before,
    int* first_coords);
template int Fragment::get_first_coords_after<int64_t>(
    int tile_i,
    int64_t* start_coords_before,
    int64_t* first_coords);
template int Fragment::get_first_coords_after<float>(
    int tile_i,
    float* start_coords_before,
    float* first_coords);
template int Fragment::get_first_coords_after<double>(
    int tile_i,
    double* start_coords_before,
    double* first_coords);

