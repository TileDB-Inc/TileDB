/**
 * @file   book_keeping.cc
 *
 * @section LICENSE
 *
 * The MIT License
 * 
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
 * This file implements the BookKeeping class.
 */

#include "book_keeping.h"
#include "utils.h"
#include <cassert>
#include <cstring>
#include <fcntl.h>
#include <iostream>
#include <sys/stat.h>
#include <unistd.h>




/* ****************************** */
/*             MACROS             */
/* ****************************** */

#if VERBOSE == 1
#  define PRINT_ERROR(x) std::cerr << "[TileDB] Error: " << x << ".\n" 
#  define PRINT_WARNING(x) std::cerr << "[TileDB] Warning: " \
                                     << x << ".\n"
#elif VERBOSE == 2
#  define PRINT_ERROR(x) std::cerr << "[TileDB::BookKeeping] Error: " \
                                   << x << ".\n" 
#  define PRINT_WARNING(x) std::cerr << "[TileDB::BookKeeping] Warning: " \
                                     << x << ".\n"
#else
#  define PRINT_ERROR(x) do { } while(0) 
#  define PRINT_WARNING(x) do { } while(0) 
#endif




/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

BookKeeping::BookKeeping(
    const ArraySchema* array_schema,
    bool dense,
    const std::string& fragment_name,
    int mode)
    : array_schema_(array_schema), 
      dense_(dense),
      fragment_name_(fragment_name),
      mode_(mode) {
  domain_ = NULL;
  non_empty_domain_ = NULL;
}

BookKeeping::~BookKeeping() {
  if(domain_ != NULL)
    free(domain_);

  if(non_empty_domain_ != NULL)
    free(non_empty_domain_);

  int64_t mbr_num = mbrs_.size(); 
  for(int64_t i=0; i<mbr_num; ++i)
    if(mbrs_[i] != NULL)
      free(mbrs_[i]);

  int64_t bounding_coords_num = bounding_coords_.size();
  for(int64_t i=0; i<bounding_coords_num; ++i)
    if(bounding_coords_[i] != NULL)
      free(bounding_coords_[i]);
}




/* ****************************** */
/*             ACCESSORS          */
/* ****************************** */

const std::vector<void*>& BookKeeping::bounding_coords() const {
  return bounding_coords_;
}

int64_t BookKeeping::cell_num(int64_t tile_pos) const {
  if(dense_) {
    return array_schema_->cell_num_per_tile(); 
  } else {
    int64_t tile_num = this->tile_num();
    if(tile_pos != tile_num-1)
      return array_schema_->capacity();
    else
      return last_tile_cell_num();
  }
}

bool BookKeeping::dense() const {
  return dense_;
}

const void* BookKeeping::domain() const {
  return domain_;
}

int64_t BookKeeping::last_tile_cell_num() const {
  return last_tile_cell_num_;
}

const std::vector<void*>& BookKeeping::mbrs() const {
  return mbrs_;
}

const void* BookKeeping::non_empty_domain() const {
  return non_empty_domain_;
}

int64_t BookKeeping::tile_num() const {
  if(dense_) {
    return array_schema_->tile_num(domain_);
  } else { 
    return mbrs_.size();
  }
}

const std::vector<std::vector<off_t> >& BookKeeping::tile_offsets() const {
  return tile_offsets_;
}

const std::vector<std::vector<off_t> >& BookKeeping::tile_var_offsets() const {
  return tile_var_offsets_;
}

const std::vector<std::vector<size_t> >& BookKeeping::tile_var_sizes() const {
  return tile_var_sizes_;
}




/* ****************************** */
/*             MUTATORS           */
/* ****************************** */

void BookKeeping::append_bounding_coords(const void* bounding_coords) {
  // For easy reference
  size_t bounding_coords_size = 2*array_schema_->coords_size();

  // Copy and append MBR
  void* new_bounding_coords = malloc(bounding_coords_size);
  memcpy(new_bounding_coords, bounding_coords, bounding_coords_size);
  bounding_coords_.push_back(new_bounding_coords);
}

void BookKeeping::append_mbr(const void* mbr) {
  // For easy reference
  size_t mbr_size = 2*array_schema_->coords_size();

  // Copy and append MBR
  void* new_mbr = malloc(mbr_size);
  memcpy(new_mbr, mbr, mbr_size);
  mbrs_.push_back(new_mbr);
}

void BookKeeping::append_tile_offset(
    int attribute_id,
    size_t step) {
  tile_offsets_[attribute_id].push_back(next_tile_offsets_[attribute_id]);
  size_t new_offset = tile_offsets_[attribute_id].back() + step;
  next_tile_offsets_[attribute_id] = new_offset;  
}

void BookKeeping::append_tile_var_offset(
    int attribute_id,
    size_t step) {
  tile_var_offsets_[attribute_id].push_back(
      next_tile_var_offsets_[attribute_id]);
  size_t new_offset = tile_var_offsets_[attribute_id].back() + step;
  next_tile_var_offsets_[attribute_id] = new_offset;  
}

void BookKeeping::append_tile_var_size(
    int attribute_id,
    size_t size) {
  tile_var_sizes_[attribute_id].push_back(size);
}

/* FORMAT:
 * non_empty_domain_size(size_t) non_empty_domain(void*)  
 * mbr_num(int64_t)
 * mbr_#1(void*) mbr_#2(void*) ... 
 * bounding_coords_num(int64_t)
 * bounding_coords_#1(void*) bounding_coords_#2(void*) ...
 * tile_offsets_attr#0_num(int64_t)
 * tile_offsets_attr#0_#1 (off_t) tile_offsets_attr#0_#2 (off_t) ...
 * ...
 * tile_offsets_attr#<attribute_num>_num(int64_t)
 * tile_offsets_attr#<attribute_num>_#1(off_t) 
 *     tile_offsets_attr#<attribute_num>_#2 (off_t) ...
 * tile_var_offsets_attr#0_num(int64_t)
 * tile_var_offsets_attr#0_#1 (off_t) tile_var_offsets_attr#0_#2 (off_t) ...
 * ...
 * tile_var_offsets_attr#<attribute_num-1>_num(int64_t)
 * tile_var_offsets_attr#<attribute_num-1>_#1 (off_t) 
 *     tile_var_offsets_attr#<attribute_num-1>_#2 (off_t) ...
 * tile_var_sizes_attr#0_num(int64_t)
 * tile_var_sizes_attr#0_#1(size_t) tile_sizes_attr#0_#2 (size_t) ...
 * ...
 * tile_var_sizes_attr#<attribute_num-1>_num(int64_t)
 * tile_var_sizes__attr#<attribute_num-1>_#1(size_t) 
 *     tile_var_sizes_attr#<attribute_num-1>_#2 (size_t) ...
 * last_tile_cell_num(int64_t)
 */
int BookKeeping::finalize() {
  // Nothing to do in READ mode
  if(mode_ == TILEDB_ARRAY_READ)
    return TILEDB_BK_OK;

  // Do nothing if the fragment directory does not exist (fragment empty) 
  if(!is_dir(fragment_name_))
    return TILEDB_BK_OK;

  // Prepare file name 
  std::string filename = fragment_name_ + "/" +
                         TILEDB_BOOK_KEEPING_FILENAME + 
                         TILEDB_FILE_SUFFIX + TILEDB_GZIP_SUFFIX;

  // Open book-keeping file
  gzFile fd = gzopen(filename.c_str(), "wb");
  if(fd == NULL) {
    PRINT_ERROR("Cannot finalize book-keeping; Cannot open file");
    return TILEDB_BK_ERR;
  }
  
  // Write non-empty domain
  if(flush_non_empty_domain(fd) != TILEDB_BK_OK)
    return TILEDB_BK_ERR;

  // Write MBRs
  if(flush_mbrs(fd) != TILEDB_BK_OK)
    return TILEDB_BK_ERR;

  // Write bounding coordinates
  if(flush_bounding_coords(fd) != TILEDB_BK_OK)
    return TILEDB_BK_ERR;

  // Write tile offsets
  if(flush_tile_offsets(fd) != TILEDB_BK_OK)
    return TILEDB_BK_ERR;

  // Write variable tile offsets
  if(flush_tile_var_offsets(fd) != TILEDB_BK_OK)
    return TILEDB_BK_ERR;

  // Write variable tile sizes
  if(flush_tile_var_sizes(fd) != TILEDB_BK_OK)
    return TILEDB_BK_ERR;

  // Write cell number of the last tile
  if(flush_last_tile_cell_num(fd) != TILEDB_BK_OK)
    return TILEDB_BK_ERR;

  // Close file
  if(gzclose(fd) != Z_OK) {
    PRINT_ERROR("Cannot finalize book-keeping; Cannot close file");
    return TILEDB_BK_ERR;
  }

  // Success
  return TILEDB_BK_OK;  
}

int BookKeeping::init(const void* non_empty_domain) {
  // For easy reference
  int attribute_num = array_schema_->attribute_num();

  // Sanity check
  assert(non_empty_domain_ == NULL);
  assert(domain_ == NULL);

  // Set non-empty domain
  size_t domain_size = 2*array_schema_->coords_size();
  non_empty_domain_ = malloc(domain_size);
  if(non_empty_domain == NULL) 
    memcpy(non_empty_domain_, array_schema_->domain(), domain_size);
  else
    memcpy(non_empty_domain_, non_empty_domain, domain_size);
  
  // Set expanded domain
  domain_ = malloc(domain_size);
  memcpy(domain_, non_empty_domain_, domain_size);
  array_schema_->expand_domain(domain_);

  // Set last tile cell number
  last_tile_cell_num_ = 0;

  // Initialize tile offsets
  tile_offsets_.resize(attribute_num+1);
  next_tile_offsets_.resize(attribute_num+1);
  for(int i=0; i<attribute_num+1; ++i)
    next_tile_offsets_[i] = 0;

  // Initialize variable tile offsets
  tile_var_offsets_.resize(attribute_num);
  next_tile_var_offsets_.resize(attribute_num);
  for(int i=0; i<attribute_num; ++i)
    next_tile_var_offsets_[i] = 0;

  // Initialize variable tile sizes
  tile_var_sizes_.resize(attribute_num);

  // Success
  return TILEDB_BK_OK;
}

/* FORMAT:
 * non_empty_domain_size(size_t) non_empty_domain(void*)  
 * mbr_num(int64_t)
 * mbr_#1(void*) mbr_#2(void*) ... 
 * bounding_coords_num(int64_t)
 * bounding_coords_#1(void*) bounding_coords_#2(void*) ...
 * tile_offsets_attr#0_num(int64_t)
 * tile_offsets_attr#0_#1 (off_t) tile_offsets_attr#0_#2 (off_t) ...
 * ...
 * tile_offsets_attr#<attribute_num>_num(int64_t)
 * tile_offsets_attr#<attribute_num>_#1(off_t) 
 *     tile_offsets_attr#<attribute_num>_#2 (off_t) ...
 * tile_var_offsets_attr#0_num(int64_t)
 * tile_var_offsets_attr#0_#1 (off_t) tile_var_offsets_attr#0_#2 (off_t) ...
 * ...
 * tile_var_offsets_attr#<attribute_num-1>_num(int64_t)
 * tile_var_offsets_attr#<attribute_num-1>_#1 (off_t) 
 *     tile_var_offsets_attr#<attribute_num-1>_#2 (off_t) ...
 * tile_var_sizes_attr#0_num(int64_t)
 * tile_var_sizes_attr#0_#1(size_t) tile_sizes_attr#0_#2 (size_t) ...
 * ...
 * tile_var_sizes_attr#<attribute_num-1>_num(int64_t)
 * tile_var_sizes__attr#<attribute_num-1>_#1(size_t) 
 *     tile_var_sizes_attr#<attribute_num-1>_#2 (size_t) ...
 * last_tile_cell_num(int64_t)
 */
int BookKeeping::load() {
  // Prepare file name
  std::string filename = fragment_name_ + "/" +
                         TILEDB_BOOK_KEEPING_FILENAME + 
                         TILEDB_FILE_SUFFIX + TILEDB_GZIP_SUFFIX;

  // Open book-keeping file
  gzFile fd = gzopen(filename.c_str(), "rb");
  if(fd == NULL) {
    PRINT_ERROR("Cannot load book-keeping; Cannot open file");
    return TILEDB_BK_ERR;
  }

  // Load non-empty domain
  if(load_non_empty_domain(fd) != TILEDB_BK_OK)
    return TILEDB_BK_ERR;

  // Load MBRs
  if(load_mbrs(fd) != TILEDB_BK_OK)
    return TILEDB_BK_ERR;

  // Load bounding coordinates
  if(load_bounding_coords(fd) != TILEDB_BK_OK)
    return TILEDB_BK_ERR;

  // Load tile offsets
  if(load_tile_offsets(fd) != TILEDB_BK_OK)
    return TILEDB_BK_ERR;

  // Load variable tile offsets
  if(load_tile_var_offsets(fd) != TILEDB_BK_OK)
    return TILEDB_BK_ERR;

  // Load variable tile sizes
  if(load_tile_var_sizes(fd) != TILEDB_BK_OK)
    return TILEDB_BK_ERR;

  // Load cell number of last tile
  if(load_last_tile_cell_num(fd) != TILEDB_BK_OK)
    return TILEDB_BK_ERR;

  // Close file
  if(gzclose(fd) != Z_OK) {
    PRINT_ERROR("Cannot load book-keeping; Cannot close file");
    return TILEDB_BK_ERR;
  }

  // Success
  return TILEDB_BK_OK;
}

void BookKeeping::set_last_tile_cell_num(int64_t cell_num) {
  last_tile_cell_num_ = cell_num;
}




/* ****************************** */
/*        PRIVATE METHODS         */
/* ****************************** */

/* FORMAT:
 * bounding_coords_num(int64_t)
 * bounding_coords_#1(void*) bounding_coords_#2(void*) ...
 */
int BookKeeping::flush_bounding_coords(gzFile fd) const {
  // For easy reference
  size_t bounding_coords_size = 2*array_schema_->coords_size();
  int64_t bounding_coords_num = bounding_coords_.size();

  // Write number of bounding coordinates
  if(gzwrite(fd, &bounding_coords_num, sizeof(int64_t)) != sizeof(int64_t)) {
    PRINT_ERROR("Cannot finalize book-keeping; Writing number of bounding "
                "coordinates failed");
    return TILEDB_BK_ERR;
  }

  // Write bounding coordinates
  for(int64_t i=0; i<bounding_coords_num; ++i) {
    if(gzwrite(fd, bounding_coords_[i], bounding_coords_size) != 
       int(bounding_coords_size)) {
      PRINT_ERROR("Cannot finalize book-keeping; Writing bounding coordinates "
                  "failed");
      return TILEDB_BK_ERR;
    }
  } 

  // Success
  return TILEDB_BK_OK;
}

/* FORMAT:
 * last_tile_cell_num(int64_t) 
 */
int BookKeeping::flush_last_tile_cell_num(gzFile fd) const {
  // For easy reference
  int64_t cell_num_per_tile = 
      dense_ ? array_schema_->cell_num_per_tile() :
               array_schema_->capacity();

  // Handle the case of zero
  int64_t last_tile_cell_num = 
      (last_tile_cell_num_ == 0) ? cell_num_per_tile : last_tile_cell_num_;

  if(gzwrite(fd, &last_tile_cell_num, sizeof(int64_t)) != sizeof(int64_t)) {
    PRINT_ERROR("Cannot finalize book-keeping; Writing last tile cell "
                "number failed");
    return TILEDB_BK_ERR;
  }

  // Success
  return TILEDB_BK_OK;
}

/* FORMAT:
 * mbr_num(int64_t)
 * mbr_#1(void*) mbr_#2(void*) ... 
 */
int BookKeeping::flush_mbrs(gzFile fd) const {
  // For easy reference
  size_t mbr_size = 2*array_schema_->coords_size();
  int64_t mbr_num = mbrs_.size();

  // Write number of MBRs
  if(gzwrite(fd, &mbr_num, sizeof(int64_t)) != sizeof(int64_t)) {
    PRINT_ERROR("Cannot finalize book-keeping; Writing number of MBRs failed");
    return TILEDB_BK_ERR;
  }

  // Write MBRs
  for(int64_t i=0; i<mbr_num; ++i) {
    if(gzwrite(fd, mbrs_[i], mbr_size) != int(mbr_size)) {
      PRINT_ERROR("Cannot finalize book-keeping; Writing MBR failed");
      return TILEDB_BK_ERR;
    }
  } 

  // Success
  return TILEDB_BK_OK;
}

/* FORMAT:
 * non_empty_domain_size(size_t) non_empty_domain(void*)  
 */
int BookKeeping::flush_non_empty_domain(gzFile fd) const {
  size_t domain_size = (non_empty_domain_ == NULL) 
                           ? 0 
                           : array_schema_->coords_size() * 2;

  // Write non-empty domain size
  if(gzwrite(fd, &domain_size, sizeof(size_t)) != sizeof(size_t)) {
    PRINT_ERROR("Cannot finalize book-keeping; Writing domain size failed");
    return TILEDB_BK_ERR;
  }

  // Write non-empty domain
  if(non_empty_domain_ != NULL) {
    if(gzwrite(fd, non_empty_domain_, domain_size) != int(domain_size)) {
      PRINT_ERROR("Cannot finalize book-keeping; Writing domain failed");
      return TILEDB_BK_ERR;
    }
  }

  // Success
  return TILEDB_BK_OK;
}

/* FORMAT:
 * tile_offsets_attr#0_num(int64_t)
 * tile_offsets_attr#0_#1 (off_t) tile_offsets_attr#0_#2 (off_t) ...
 * ...
 * tile_offsets_attr#<attribute_num>_num(int64_t)
 * tile_offsets_attr#<attribute_num>_#1 (off_t)
 * tile_offsets_attr#<attribute_num>_#2 (off_t) ...
 */
int BookKeeping::flush_tile_offsets(gzFile fd) const {
  // For easy reference
  int attribute_num = array_schema_->attribute_num();
  int64_t tile_offsets_num;

  // Write tile offsets for each attribute
  for(int i=0; i<attribute_num+1; ++i) {
    // Write number of tile offsets
    tile_offsets_num = tile_offsets_[i].size(); 
    if(gzwrite(fd, &tile_offsets_num, sizeof(int64_t)) != sizeof(int64_t)) {
      PRINT_ERROR("Cannot finalize book-keeping; Writing number of "
                  "tile offsets failed");
      return TILEDB_BK_ERR;
    }

    if(tile_offsets_num == 0)
      continue;

    // Write tile offsets
    if(gzwrite(fd, &tile_offsets_[i][0], tile_offsets_num * sizeof(off_t)) !=
       int(tile_offsets_num * sizeof(off_t))) {
      PRINT_ERROR("Cannot finalize book-keeping; Writing tile offsets failed");
      return TILEDB_BK_ERR;
    }
  }

  // Success
  return TILEDB_BK_OK;
}

/* FORMAT:
 * tile_var_offsets_attr#0_num(int64_t)
 * tile_var_offsets_attr#0_#1 (off_t) tile_var_offsets_attr#0_#2 (off_t) ...
 * ...
 * tile_var_offsets_attr#<attribute_num-1>_num(int64_t)
 * tile_var_offsets_attr#<attribute_num-1>_#1 (off_t)
 *     tile_var_offsets_attr#<attribute_num-1>_#2 (off_t) ...
 */
int BookKeeping::flush_tile_var_offsets(gzFile fd) const {
  // For easy reference
  int attribute_num = array_schema_->attribute_num();
  int64_t tile_var_offsets_num;

  // Write tile offsets for each attribute
  for(int i=0; i<attribute_num; ++i) {
    // Write number of offsets
    tile_var_offsets_num = tile_var_offsets_[i].size(); 
    if(gzwrite(fd, &tile_var_offsets_num, sizeof(int64_t)) != sizeof(int64_t)) {
      PRINT_ERROR("Cannot finalize book-keeping; Writing number of "
                  "variable tile offsets failed");
      return TILEDB_BK_ERR;
    }

    if(tile_var_offsets_num == 0)
      continue;

    // Write tile offsets
    if(gzwrite(
           fd,  
           &tile_var_offsets_[i][0], 
           tile_var_offsets_num * sizeof(off_t)) !=
       int(tile_var_offsets_num * sizeof(off_t))) {
      PRINT_ERROR("Cannot finalize book-keeping; Writing variable tile "
                  "offsets failed");
      return TILEDB_BK_ERR;
    }
  }

  // Success
  return TILEDB_BK_OK;
}
 
/* FORMAT:
 * tile_var_sizes_attr#0_num(int64_t)
 * tile_var_sizes_attr#0_#1 (size_t) tile_sizes_attr#0_#2 (size_t) ...
 * ...
 * tile_var_sizes_attr#<attribute_num-1>_num(int64_t)
 * tile_var_sizes__attr#<attribute_num-1>_#1(size_t) 
 *     tile_var_sizes_attr#<attribute_num-1>_#2 (size_t) ...
 */
int BookKeeping::flush_tile_var_sizes(gzFile fd) const {
  // For easy reference
  int attribute_num = array_schema_->attribute_num();
  int64_t tile_var_sizes_num;

  // Write tile sizes for each attribute
  for(int i=0; i<attribute_num; ++i) {
    // Write number of sizes
    tile_var_sizes_num = tile_var_sizes_[i].size(); 
    if(gzwrite(fd, &tile_var_sizes_num, sizeof(int64_t)) != sizeof(int64_t)) {
      PRINT_ERROR("Cannot finalize book-keeping; Writing number of "
                  "variable tile sizes failed");
      return TILEDB_BK_ERR;
    }

    if(tile_var_sizes_num == 0)
      continue;

    // Write tile sizes
    if(gzwrite(
           fd,  
           &tile_var_sizes_[i][0], 
           tile_var_sizes_num * sizeof(size_t)) !=
       int(tile_var_sizes_num * sizeof(size_t))) {
      PRINT_ERROR("Cannot finalize book-keeping; Writing variable tile "
                  "sizes failed");
      return TILEDB_BK_ERR;
    }
  }

  // Success
  return TILEDB_BK_OK;
}

/* FORMAT:
 * bounding_coords_num (int64_t)
 * bounding_coords_#1 (void*) bounding_coords_#2 (void*) ...
 */
int BookKeeping::load_bounding_coords(gzFile fd) {
  // For easy reference
  size_t bounding_coords_size = 2*array_schema_->coords_size();

  // Get number of bounding coordinates
  int64_t bounding_coords_num;
  if(gzread(fd, &bounding_coords_num, sizeof(int64_t)) != sizeof(int64_t)) {
    PRINT_ERROR("Cannot load book-keeping; Reading number of "
                "bounding coordinates failed");
    return TILEDB_BK_ERR;
  }

  // Get bounding coordinates
  void* bounding_coords;
  bounding_coords_.resize(bounding_coords_num);
  for(int64_t i=0; i<bounding_coords_num; ++i) {
    bounding_coords = malloc(bounding_coords_size);
    if(gzread(fd, bounding_coords, bounding_coords_size) != 
       int(bounding_coords_size)) {
      PRINT_ERROR("Cannot load book-keeping; Reading bounding coordinates "
                  "failed");
      free(bounding_coords);
      return TILEDB_BK_ERR;
    }
    bounding_coords_[i] = bounding_coords;
  }

  // Success
  return TILEDB_BK_OK;
}

/* FORMAT:
 * last_tile_cell_num (int64_t)  
 */
int BookKeeping::load_last_tile_cell_num(gzFile fd) {
  // Get last tile cell number
  if(gzread(fd, &last_tile_cell_num_, sizeof(int64_t)) != sizeof(int64_t)) {
    PRINT_ERROR("Cannot load book-keeping; Reading last tile cell "
                "number failed");
    return TILEDB_BK_ERR;
  }

  // Success
  return TILEDB_BK_OK;
}

/* FORMAT:
 * mbr_num (int64_t)
 * mbr_#1 (void*) mbr_#2 (void*) ... mbr_#<mbr_num> (void*)
 */
int BookKeeping::load_mbrs(gzFile fd) {
  // For easy reference
  size_t mbr_size = 2*array_schema_->coords_size();

  // Get number of MBRs
  int64_t mbr_num;
  if(gzread(fd, &mbr_num, sizeof(int64_t)) != sizeof(int64_t)) {
    PRINT_ERROR("Cannot load book-keeping; Reading number of MBRs failed");
    return TILEDB_BK_ERR;
  }

  // Get MBRs
  void* mbr;
  mbrs_.resize(mbr_num);
  for(int64_t i=0; i<mbr_num; ++i) {
    mbr = malloc(mbr_size);
    if(gzread(fd, mbr, mbr_size) != int(mbr_size)) {
      PRINT_ERROR("Cannot load book-keeping; Reading MBR failed");
      free(mbr);
      return TILEDB_BK_ERR;
    }
    mbrs_[i] = mbr;
  }

  // Success
  return TILEDB_BK_OK;
}

/* FORMAT:
 * non_empty_domain_size (size_t) non_empty_domain (void*)  
 */
int BookKeeping::load_non_empty_domain(gzFile fd) {
  // Get domain size
  size_t domain_size;
  if(gzread(fd, &domain_size, sizeof(size_t)) != sizeof(size_t)) {
    PRINT_ERROR("Cannot load book-keeping; Reading domain size failed");
    return TILEDB_BK_ERR;
  }

  // Get non-empty domain
  if(domain_size == 0) {
    non_empty_domain_ = NULL;
  } else {
    non_empty_domain_ = malloc(domain_size);
    if(gzread(fd, non_empty_domain_, domain_size) != int(domain_size)) {
      PRINT_ERROR("Cannot load book-keeping; Reading domain failed");
      return TILEDB_BK_ERR;
    }
  }

  // Get expanded domain
  if(non_empty_domain_ == NULL) {
    domain_ = NULL;
  } else { 
    domain_ = malloc(domain_size);
    memcpy(domain_, non_empty_domain_, domain_size);
    array_schema_->expand_domain(domain_);
  }

  // Success
  return TILEDB_BK_OK;
}

/* FORMAT:
 * tile_offsets_attr#0_num (int64_t)
 * tile_offsets_attr#0_#1 (off_t) tile_offsets_attr#0_#2 (off_t) ...
 * ...
 * tile_offsets_attr#<attribute_num>_num (int64_t)
 * tile_offsets_attr#<attribute_num>_#1 (off_t) 
 * tile_offsets_attr#<attribute_num>_#2 (off_t) ...
 */
int BookKeeping::load_tile_offsets(gzFile fd) {
  // For easy reference
  int attribute_num = array_schema_->attribute_num();
  int64_t tile_offsets_num;

  // Allocate tile offsets
  tile_offsets_.resize(attribute_num+1);

  // For all attributes, get the tile offsets
  for(int i=0; i<attribute_num+1; ++i) {
    // Get number of tile offsets
    if(gzread(fd, &tile_offsets_num, sizeof(int64_t)) != sizeof(int64_t)) {
      PRINT_ERROR("Cannot load book-keeping; Reading number of tile "
                  "offsets failed");
      return TILEDB_BK_ERR;
    }
 
    if(tile_offsets_num == 0)
      continue;

    // Get tile offsets
    tile_offsets_[i].resize(tile_offsets_num);
    if(gzread(fd, &tile_offsets_[i][0], tile_offsets_num * sizeof(off_t)) != 
       int(tile_offsets_num * sizeof(off_t))) {
      PRINT_ERROR("Cannot load book-keeping; Reading tile offsets failed");
      return TILEDB_BK_ERR;
    }
  }

  // Success
  return TILEDB_BK_OK;
}

/* FORMAT:
 * tile_var_offsets_attr#0_num (int64_t)
 * tile_var_offsets_attr#0_#1 (off_t) tile_var_offsets_attr#0_#2 (off_t) ...
 * ...
 * tile_var_offsets_attr#<attribute_num-1>_num(int64_t)
 * tile_var_offsets_attr#<attribute_num-1>_#1 (off_t)
 *     tile_ver_offsets_attr#<attribute_num-1>_#2 (off_t) ...
 */
int BookKeeping::load_tile_var_offsets(gzFile fd) {
  // For easy reference
  int attribute_num = array_schema_->attribute_num();
  int64_t tile_var_offsets_num;

  // Allocate tile offsets
  tile_var_offsets_.resize(attribute_num);

  // For all attributes, get the variable tile offsets
  for(int i=0; i<attribute_num; ++i) {
    // Get number of tile offsets
    if(gzread(fd, &tile_var_offsets_num, sizeof(int64_t)) != sizeof(int64_t)) {
      PRINT_ERROR("Cannot load book-keeping; Reading number of variable tile "
                  "offsets failed");
      return TILEDB_BK_ERR;
    }
 
    if(tile_var_offsets_num == 0)
      continue;

    // Get variable tile offsets
    tile_var_offsets_[i].resize(tile_var_offsets_num);
    if(gzread(
           fd, 
           &tile_var_offsets_[i][0], 
           tile_var_offsets_num * sizeof(off_t)) != 
       int(tile_var_offsets_num * sizeof(off_t))) {
      PRINT_ERROR("Cannot load book-keeping; Reading variable tile "
                  "offsets failed");
      return TILEDB_BK_ERR;
    }
  }

  // Success
  return TILEDB_BK_OK;
}

/* FORMAT:
 * tile_var_sizes_attr#0_num (int64_t)
 * tile_var_sizes_attr#0_#1 (size_t) tile_sizes_attr#0_#2 (size_t) ...
 * ...
 * tile_var_sizes_attr#<attribute_num-1>_num( int64_t)
 * tile_var_sizes__attr#<attribute_num-1>_#1 (size_t) 
 *     tile_var_sizes_attr#<attribute_num-1>_#2 (size_t) ...
 */
int BookKeeping::load_tile_var_sizes(gzFile fd) {
  // For easy reference
  int attribute_num = array_schema_->attribute_num();
  int64_t tile_var_sizes_num;

  // Allocate tile sizes
  tile_var_sizes_.resize(attribute_num);

  // For all attributes, get the variable tile sizes
  for(int i=0; i<attribute_num; ++i) {
    // Get number of tile sizes
    if(gzread(fd, &tile_var_sizes_num, sizeof(int64_t)) != sizeof(int64_t)) {
      PRINT_ERROR("Cannot load book-keeping; Reading number of variable tile "
                  "sizes failed");
      return TILEDB_BK_ERR;
    }
 
    if(tile_var_sizes_num == 0)
      continue;

    // Get variable tile sizes
    tile_var_sizes_[i].resize(tile_var_sizes_num);
    if(gzread(
           fd, 
           &tile_var_sizes_[i][0], 
           tile_var_sizes_num * sizeof(size_t)) != 
       int(tile_var_sizes_num * sizeof(size_t))) {
      PRINT_ERROR("Cannot load book-keeping; Reading variable tile "
                  "sizes failed");
      return TILEDB_BK_ERR;
    }
  }

  // Success
  return TILEDB_BK_OK;
}
