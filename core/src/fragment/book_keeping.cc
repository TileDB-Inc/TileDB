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
#include <iostream>

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

BookKeeping::BookKeeping(const Fragment* fragment, const void* range)
    : fragment_(fragment) {
  // For easy reference
  const ArraySchema* array_schema = fragment->array()->array_schema();
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
}

BookKeeping::~BookKeeping() {
  if(range_ != NULL)
    free(range_);
}

/* ****************************** */
/*             ACCESSORS          */
/* ****************************** */

const void* BookKeeping::range() const {
  return range_;
}

/* ****************************** */
/*             MUTATORS           */
/* ****************************** */

void BookKeeping::append_tile_offset(
    int attribute_id,
    size_t step) {
  tile_offsets_[attribute_id].push_back(next_tile_offsets_[attribute_id]);
  size_t new_offset = tile_offsets_[attribute_id].back() + step;
  next_tile_offsets_[attribute_id] = new_offset;  
}

/* ****************************** */
/*               MISC             */
/* ****************************** */

int BookKeeping::finalize() {
  // Do nothing if the fragment directory does not exist (fragment empty) 
  std::string fragment_name = fragment_->fragment_name();
  if(!is_dir(fragment_name))
    return TILEDB_BK_OK;

  // Serialize book-keeping
  void* buffer;
  size_t buffer_size; 
  if(serialize(buffer, buffer_size) == TILEDB_BK_ERR)
    return TILEDB_BK_ERR;

  // Prepare file name 
  std::string filename = fragment_name + "/" +
      TILEDB_BOOK_KEEPING_FILENAME + TILEDB_FILE_SUFFIX + TILEDB_GZIP_SUFFIX;
 
  // Write buffer to file
  if(write_to_file_cmp_gzip(filename.c_str(), buffer, buffer_size) != 
     TILEDB_UT_OK) {
    free(buffer);
    return TILEDB_BK_ERR;
  }

  // Clean up
  free(buffer);

  // Success
  return TILEDB_BK_OK;  
}

/* ****************************** */
/*        PRIVATE METHODS         */
/* ****************************** */

/* FORMAT:
 * range_size(size_t) range(void*)  
 * tile_offsets_num(int)
 * tile_num(int64_t)
 * tile_offsets_attr_#1_tile_#1(size_t) ... 
 * tile_offsets_attr_#1_tile_#tile_num(size_t)
 * ...
 * tile_offsets_attr_#attribute_num_tile_#1(size_t) ... 
 * tile_offsets_attr_#attribute_num_tile_#tile_num(size_t)
 */
int BookKeeping::serialize(void*& buffer, size_t& buffer_size) const {
  size_t buffer_allocated_size = TILEDB_BK_BUFFER_SIZE;
  buffer = malloc(buffer_allocated_size); 
  buffer_size = 0;

  // Serialize range
  if(serialize_range(buffer, buffer_allocated_size, buffer_size) != 
     TILEDB_BK_OK)
    return TILEDB_BK_ERR;

  // Serialize tile offsets
  if(serialize_tile_offsets(buffer, buffer_allocated_size, buffer_size) != 
     TILEDB_BK_OK)
    return TILEDB_BK_ERR;

  // Success
  return TILEDB_BK_OK;
}

int BookKeeping::serialize_range(
    void*& buffer, 
    size_t& buffer_allocated_size,
    size_t& buffer_size) const {
  // Sanity check
  assert(buffer != NULL);
 
  // For easy reference
  size_t range_size = (range_ == NULL) ? 0 : 
      fragment_->array()->array_schema()->coords_size() * 2;

  // Expand buffer if necessary
  while(buffer_size + range_size > buffer_allocated_size) {
    if(expand_buffer(buffer, buffer_allocated_size) != TILEDB_UT_OK)
      return TILEDB_BK_ERR;
  }

  // Copy range
  memcpy(static_cast<char*>(buffer) + buffer_size, &range_size, sizeof(size_t));
  buffer_size += sizeof(size_t);
  if(range_ != NULL) {
    memcpy(static_cast<char*>(buffer) + buffer_size, range_, range_size);
    buffer_size += range_size;
  }

  // Success
  return TILEDB_BK_OK;
}

int BookKeeping::serialize_tile_offsets(
    void*& buffer, 
    size_t& buffer_allocated_size,
    size_t& buffer_size) const {
  // Sanity check
  assert(buffer != NULL);

  // Get number of offsets, number of tiles, and size of offsets
  int tile_offsets_num = tile_offsets_.size();
  if(tile_offsets_.back().size() == 0) // No coordinates tiles
    --tile_offsets_num;
  int64_t tile_num = tile_offsets_[0].size();
  assert(tile_num > 0);
  size_t tile_offsets_size = tile_offsets_num * tile_num * sizeof(size_t); 

   // Expand buffer if necessary
  while(buffer_size + sizeof(int) + sizeof(int64_t) + tile_offsets_size > 
        buffer_allocated_size) {
    if(expand_buffer(buffer, buffer_allocated_size) != TILEDB_UT_OK)
      return TILEDB_BK_ERR;
  }  

  // Copy number of offsets
  memcpy(
      static_cast<char*>(buffer) + buffer_size, 
      &tile_offsets_num, 
      sizeof(int));
  buffer_size += sizeof(int);

  // Copy tile_num
  memcpy(static_cast<char*>(buffer) + buffer_size, &tile_num, sizeof(int64_t));
  buffer_size += sizeof(int64_t);

  // Copy tile offsets
  for(int i=0; i<tile_offsets_num; ++i) {
    memcpy(
        static_cast<char*>(buffer) + buffer_size,
        &tile_offsets_[i][0], 
        tile_num * sizeof(size_t));
    buffer_size += tile_num * sizeof(size_t);
  }

  // Success
  return TILEDB_BK_OK;
}
