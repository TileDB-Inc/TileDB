/**
 * @file   book_keeping.cc
 * @author Stavros Papadopoulos <stavrosp@csail.mit.edu>
 *
 * @section LICENSE
 *
 * The MIT License
 * 
 * @copyright Copyright (c) 2014 Stavros Papadopoulos <stavrosp@csail.mit.edu>
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

BookKeeping::BookKeeping(const Fragment* fragment)
    : fragment_(fragment) {
  range_ = NULL;
}

BookKeeping::~BookKeeping() {
  if(range_ != NULL)
    free(range_);

  for(int i=0; i<mbrs_.size(); ++i)
    free(mbrs_[i]);

  for(int i=0; i<bounding_coords_.size(); ++i)
    free(bounding_coords_[i]);
}

/* ****************************** */
/*             ACCESSORS          */
/* ****************************** */

const std::vector<void*>& BookKeeping::bounding_coords() const {
  return bounding_coords_;
}

const std::vector<void*>& BookKeeping::mbrs() const {
  return mbrs_;
}

const void* BookKeeping::range() const {
  return range_;
}

int64_t BookKeeping::tile_num() const {
  return mbrs_.size();
}

/* ****************************** */
/*             MUTATORS           */
/* ****************************** */

void BookKeeping::append_bounding_coords(const void* bounding_coords) {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  size_t bounding_coords_size = 2*array_schema->coords_size();

  // Copy and append MBR
  void* new_bounding_coords = malloc(bounding_coords_size);
  memcpy(new_bounding_coords, bounding_coords, bounding_coords_size);
  bounding_coords_.push_back(new_bounding_coords);
}

void BookKeeping::append_mbr(const void* mbr) {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  size_t mbr_size = 2*array_schema->coords_size();

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

int BookKeeping::init(const void* range) {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  int attribute_num = array_schema->attribute_num();

  // Set range
  if(range == NULL) {
    range_ = NULL;
  } else {
    size_t range_size = 2*array_schema->coords_size();
    range_ = malloc(range_size);
    memcpy(range_, range, range_size);
  }

  // Initialize tile offsets
  tile_offsets_.resize(attribute_num+1);
  next_tile_offsets_.resize(attribute_num+1);
  for(int i=0; i<attribute_num+1; ++i)
    next_tile_offsets_[i] = 0;

  // Success
  return TILEDB_BK_OK;
}

/* FORMAT:
 * range_size(size_t) range(void*)  
 * mbr_num(int64_t)
 * mbr_#1(void*) mbr_#2(void*) ... 
 * bounding_coords_num(int64_t)
 * bounding_coords_#1(void*) bounding_coords_#2(void*) ...
 */
int BookKeeping::load() {
  // Prepare file name
  std::string filename = fragment_->fragment_name() + "/" +
                         TILEDB_BOOK_KEEPING_FILENAME + 
                         TILEDB_FILE_SUFFIX + TILEDB_GZIP_SUFFIX;

  // Open book-keeping file
  gzFile fd = gzopen(filename.c_str(), "rb");
  if(fd == NULL) {
    PRINT_ERROR("Cannot load book-keeping; Cannot open file");
    return TILEDB_BK_ERR;
  }

  // Load range
  if(load_range(fd) != TILEDB_BK_OK)
    return TILEDB_BK_ERR;

  // Load MBRs
  if(load_mbrs(fd) != TILEDB_BK_OK)
    return TILEDB_BK_ERR;

  // Load bounding coordinates
  if(load_bounding_coords(fd) != TILEDB_BK_OK)
    return TILEDB_BK_ERR;

  // Close file
  if(gzclose(fd)) {
    PRINT_ERROR("Cannot load book-keeping; Cannot close file");
    return TILEDB_BK_ERR;
  }

  // Success
  return TILEDB_BK_OK;
}

/* ****************************** */
/*               MISC             */
/* ****************************** */

/* FORMAT:
 * range_size(size_t) range(void*)  
 * mbr_num(int64_t)
 * mbr_#1(void*) mbr_#2(void*) ... 
 * bounding_coords_num(int64_t)
 * bounding_coords_#1(void*) bounding_coords_#2(void*) ...
 */
int BookKeeping::finalize() {
  // Nothing to do in READ mode
  int mode = fragment_->array()->mode();
  if(mode == TILEDB_READ || mode == TILEDB_READ_REVERSE)
    return TILEDB_BK_OK;

  // Do nothing if the fragment directory does not exist (fragment empty) 
  std::string fragment_name = fragment_->fragment_name();
  if(!is_dir(fragment_name))
    return TILEDB_BK_OK;

  // Prepare file name 
  std::string filename = fragment_name + "/" +
                         TILEDB_BOOK_KEEPING_FILENAME + 
                         TILEDB_FILE_SUFFIX + TILEDB_GZIP_SUFFIX;

  // Open book-keeping file
  gzFile fd = gzopen(filename.c_str(), "wb");
  if(fd == NULL) {
    PRINT_ERROR("Cannot finalize book-keeping; Cannot open file");
    return TILEDB_BK_ERR;
  }
  
  // Write range
  if(flush_range(fd) != TILEDB_BK_OK)
    return TILEDB_BK_ERR;

  // Write MBRs
  if(flush_mbrs(fd) != TILEDB_BK_OK)
    return TILEDB_BK_ERR;

  // Write bounding coordinates
  if(flush_bounding_coords(fd) != TILEDB_BK_OK)
    return TILEDB_BK_ERR;

  // Close file
  if(gzclose(fd)) {
    PRINT_ERROR("Cannot finalize book-keeping; Cannot close file");
    return TILEDB_BK_ERR;
  }

  // Success
  return TILEDB_BK_OK;  
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
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  size_t bounding_coords_size = 2*array_schema->coords_size();
  int64_t bounding_coords_num = bounding_coords_.size();

  if(gzwrite(fd, &bounding_coords_num, sizeof(int64_t)) != sizeof(int64_t)) {
    PRINT_ERROR("Cannot finalize book-keeping; Writing number of bounding "
                "coordinates failed");
    return TILEDB_BK_ERR;
  }

  for(int i=0; i<bounding_coords_num; ++i) {
    if(gzwrite(fd, bounding_coords_[i], bounding_coords_size) != 
       bounding_coords_size) {
      PRINT_ERROR("Cannot finalize book-keeping; Writing bounding coordinates "
                  "failed");
      return TILEDB_BK_ERR;
    }
  } 

  return TILEDB_BK_OK;
}

/* FORMAT:
 * mbr_num(int64_t)
 * mbr_#1(void*) mbr_#2(void*) ... 
 */
int BookKeeping::flush_mbrs(gzFile fd) const {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  size_t mbr_size = 2*array_schema->coords_size();
  int64_t mbr_num = mbrs_.size();

  if(gzwrite(fd, &mbr_num, sizeof(int64_t)) != sizeof(int64_t)) {
    PRINT_ERROR("Cannot finalize book-keeping; Writing number of MBRs failed");
    return TILEDB_BK_ERR;
  }

  for(int i=0; i<mbr_num; ++i) {
    if(gzwrite(fd, mbrs_[i], mbr_size) != mbr_size) {
      PRINT_ERROR("Cannot finalize book-keeping; Writing MBR failed");
      return TILEDB_BK_ERR;
    }
  } 

  return TILEDB_BK_OK;
}

/* FORMAT:
 * range_size(size_t) range(void*)  
 */
int BookKeeping::flush_range(gzFile fd) const {
  size_t range_size = (range_ == NULL) ? 0 : 
      fragment_->array()->array_schema()->coords_size() * 2;

  if(gzwrite(fd, &range_size, sizeof(size_t)) != sizeof(size_t)) {
    PRINT_ERROR("Cannot finalize book-keeping; Writing range size failed");
    return TILEDB_BK_ERR;
  }

  if(range_ != NULL) {
    if(gzwrite(fd, range_, range_size) != range_size) {
      PRINT_ERROR("Cannot finalize book-keeping; Writing range failed");
      return TILEDB_BK_ERR;
    }
  }

  // Success
  return TILEDB_BK_OK;
}

int BookKeeping::flush_tile_offsets(gzFile fd) const {
  // If the array is dense, do nothing
  // TODO: Probably this will be true also for sparse arrays with irregular
  // tiles
  // TODO: If there is compression though, this should change!
  return TILEDB_BK_OK;

  // Get number of offsets, number of tiles, and size of offsets
  int tile_offsets_num = tile_offsets_.size();
  if(tile_offsets_.back().size() == 0) // No coordinates tiles
    --tile_offsets_num;
  int64_t tile_num = tile_offsets_[0].size();
  assert(tile_num > 0);
  size_t tile_offsets_size = tile_offsets_num * tile_num * sizeof(size_t); 

  // Write number of offsets
  if(gzwrite(fd, &tile_offsets_num, sizeof(int)) != sizeof(int)) {
    PRINT_ERROR("Cannot finalize book-keeping; Writing number of "
                "tile offsets failed");
    return TILEDB_BK_ERR;
  }

  // Write tile_num
  if(gzwrite(fd, &tile_num, sizeof(int64_t)) != sizeof(int64_t)) {
    PRINT_ERROR("Cannot finalize book-keeping; Writing number of tiles failed");
    return TILEDB_BK_ERR;
  }

  // Write tile offsets
  for(int i=0; i<tile_offsets_num; ++i) {
    if(gzwrite(fd, &tile_offsets_[i][0], tile_num * sizeof(size_t)) !=
       tile_num * sizeof(size_t)) {
      PRINT_ERROR("Cannot finalize book-keeping; Writing tile offsets failed");
      return TILEDB_BK_ERR;
    }
  }

  // Success
  return TILEDB_BK_OK;
}

/* FORMAT:
 * bounding_coords_num(int64_t)
 * bounding_coords_#1(void*) bounding_coords_#2(void*) ...
 */
int BookKeeping::load_bounding_coords(gzFile fd) {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  size_t bounding_coords_size = 2*array_schema->coords_size();

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
       bounding_coords_size) {
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
 * mbr_num(int64_t)
 * mbr_#1(void*) mbr_#2(void*) ... mbr_#<mbr_num>(void*)
 */
int BookKeeping::load_mbrs(gzFile fd) {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  size_t mbr_size = 2*array_schema->coords_size();

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
    if(gzread(fd, mbr, mbr_size) != mbr_size) {
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
 * range_size(size_t) range(void*)  
 */
int BookKeeping::load_range(gzFile fd) {
  // Get range size
  size_t range_size;
  if(gzread(fd, &range_size, sizeof(size_t)) != sizeof(size_t)) {
    PRINT_ERROR("Cannot load book-keeping; Reading range size failed");
    return TILEDB_BK_ERR;
  }

  // Get range
  if(range_size == 0) {
    range_ = NULL;
  } else {
    range_ = malloc(range_size);
    if(gzread(fd, range_, range_size) != range_size) {
      PRINT_ERROR("Cannot load book-keeping; Reading range failed");
      return TILEDB_BK_ERR;
    }
  }

  // Success
  return TILEDB_BK_OK;
}

int BookKeeping::load_tile_offsets(gzFile fd) {
  // If the array is dense, do nothing
  // TODO: Probably this will be true also for sparse arrays with irregular
  // tiles
  return TILEDB_BK_OK;

  // TODO

  // Success
  return TILEDB_BK_OK;
}
