/**
 * @file   read_state.cc
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
 * This file implements the ReadState class.
 */

#include "read_state.h"
#include <assert.h>
#include <fcntl.h>
#include <iostream>
#include <sys/stat.h>
#include <unistd.h>

/******************************************************
************** CONSTRUCTORS & DESTRUCTORS *************
******************************************************/

ReadState::ReadState(
    const ArraySchema* array_schema, 
    const BookKeeping* book_keeping,
    const std::string* fragment_name,
    const std::string* workspace,
    size_t segment_size) 
    : array_schema_(array_schema), 
      book_keeping_(book_keeping),
      fragment_name_(fragment_name),
      workspace_(workspace),
      segment_size_(segment_size) {
  // For easy reference
  int attribute_num = array_schema_->attribute_num();

  // Initialize segments
  segments_.resize(attribute_num+1);
  for(int i=0; i<=attribute_num; ++i)
    segments_[i] = malloc(segment_size_);

  // Initialize tiles
  tiles_.resize(attribute_num+1);

  // Initialize pos_ranges
  pos_ranges_.resize(attribute_num+1);
}

ReadState::~ReadState() {
  // Clean-up tiles
  for(int i=0; i<tiles_.size(); ++i)
    delete_tiles(i);

  // Clean-up segments
  for(int i=0; i<segments_.size(); ++i)
    free(segments_[i]);
}

/******************************************************
******************** TILE FUNCTIONS *******************
******************************************************/

const Tile* ReadState::get_tile_by_pos(int attribute_id, int64_t pos) {
  // For easy reference
  const int64_t& pos_lower = pos_ranges_[attribute_id].first;
  const int64_t& pos_upper = pos_ranges_[attribute_id].second;

  // Fetch from the disk if the tile is not in main memory
  if(tiles_[attribute_id].size() == 0 ||
     (pos < pos_lower || pos > pos_upper)) 
    // The following updates pos_lower and pos_upper
    load_tiles_from_disk(attribute_id, pos);

  assert(pos >= pos_lower && pos <= pos_upper);
  assert(pos - pos_lower <= tiles_[attribute_id].size());

  return tiles_[attribute_id][pos-pos_lower];
}

const Tile* ReadState::rget_tile_by_pos(int attribute_id, int64_t pos) {
  // For easy reference
  const int64_t& pos_lower = pos_ranges_[attribute_id].first;
  const int64_t& pos_upper = pos_ranges_[attribute_id].second;

  // Fetch from the disk if the tile is not in main memory
  if(tiles_[attribute_id].size() == 0 ||
     (pos < pos_lower || pos > pos_upper))  {
    // Find the starting position, so that the pos tile is 
    // the last tile in the set of tiles loaded from the disk
    int64_t start_pos = pos; 
    size_t segment_size = 0;
    size_t tile_size;
    while(start_pos >= 0) {
      tile_size = book_keeping_->tile_size(attribute_id, start_pos);
      if(segment_size + tile_size > segment_size_) {
        ++start_pos;
        break;
      }
      segment_size += tile_size;
      --start_pos;
    }
    if(start_pos < 0)
      start_pos = 0;

    // The following updates pos_lower and pos_upper
    load_tiles_from_disk(attribute_id, start_pos);
  }

  assert(pos >= pos_lower && pos <= pos_upper);
  assert(pos - pos_lower <= tiles_[attribute_id].size());

  return tiles_[attribute_id][pos-pos_lower];
}

/******************************************************
******************* PRIVATE METHODS *******************
******************************************************/

void ReadState::delete_tiles(int attribute_id) {
  for(int i=0; i<tiles_[attribute_id].size(); ++i)
    delete tiles_[attribute_id][i];
}

std::pair<size_t, int64_t> ReadState::load_payloads_into_segment(
    int attribute_id, int64_t start_pos) {
  // For easy reference
  const std::string& array_name = array_schema_->array_name();
  const std::string& attribute_name = 
      array_schema_->attribute_name(attribute_id);
  int64_t tile_num = book_keeping_->tile_num();

  // Open file
  std::string filename = *workspace_ + "/" + array_name + "/" +
                         *fragment_name_ + "/" +
                         attribute_name + 
                         TILE_DATA_FILE_SUFFIX;
  int fd = open(filename.c_str(), O_RDONLY);
  assert(fd != -1);

  // Initilizations
  struct stat st;
  fstat(fd, &st);
  size_t segment_utilization = 0;
  size_t offset_diff;
  int64_t tiles_in_segment = 0;
  int64_t pos = start_pos;

  // Calculate buffer size (largest size smaller than the segment_size_)
  while(pos < tile_num) {
    if(pos == tile_num-1)
      offset_diff = st.st_size - 
                    book_keeping_->offset(attribute_id, pos);
    else
      offset_diff = book_keeping_->offset(attribute_id, pos+1) - 
                    book_keeping_->offset(attribute_id, pos);
    
    if(segment_utilization + offset_diff > segment_size_)
      break;

    segment_utilization += offset_diff;
    pos++;
    tiles_in_segment++;
  }

  assert(segment_utilization != 0);
  assert(book_keeping_->offset(attribute_id, start_pos) + segment_utilization <=
         st.st_size);
  assert(segment_utilization <= segment_size_);

  // Read payloads into buffer
  lseek(fd, book_keeping_->offset(attribute_id, start_pos), SEEK_SET);
  read(fd, segments_[attribute_id], segment_utilization); 
  close(fd);

  return std::pair<size_t, int64_t>(segment_utilization, tiles_in_segment);
}

void ReadState::load_tiles_from_disk(int attribute_id, int64_t start_pos) { 
  char* buffer;
  // Load the tile payloads from the disk into a segment, starting
  // from the tile at position start_pos
  std::pair<size_t, int64_t> ret = 
      load_payloads_into_segment(attribute_id, start_pos); 
  size_t segment_utilization = ret.first;
  int64_t tiles_in_segment = ret.second;

  // Delete previous tiles from main memory
  delete_tiles(attribute_id);
  
  // Create the tiles from the payloads in the segment
  load_tiles_from_segment(attribute_id, start_pos, 
                          segment_utilization, tiles_in_segment);         
 
  // Update pos range in main memory
  pos_ranges_[attribute_id].first = start_pos;
  pos_ranges_[attribute_id].second = start_pos + tiles_in_segment - 1;
}

void ReadState::load_tiles_from_segment(
    int attribute_id, int64_t start_pos, 
    size_t segment_utilization, int64_t tiles_in_segment) {
  // For easy reference
  TileList& tiles = tiles_[attribute_id];
  char* segment = static_cast<char*>(segments_[attribute_id]);
  int attribute_num = array_schema_->attribute_num();
  int dim_num = (attribute_id != attribute_num) ? 0 : array_schema_->dim_num();
  const std::type_info* cell_type = array_schema_->type(attribute_id);
  int val_num  = (dim_num != 0) ? 1 : array_schema_->val_num(attribute_id);
  int64_t tile_num = book_keeping_->tile_num();

  // Initializations
  size_t segment_offset = 0, payload_size;
  int64_t tile_id;
  int64_t pos = start_pos;
  tiles.resize(tiles_in_segment);
  void* payload;
  
  // Load tiles
  for(int64_t i=0; i<tiles_in_segment; ++i, ++pos) {
    assert(pos < tile_num);
    tile_id = book_keeping_->tile_id(pos);

    if(pos == tile_num-1)
      payload_size = segment_utilization - segment_offset;
    else
      payload_size = book_keeping_->offset(attribute_id, pos+1) - 
                     book_keeping_->offset(attribute_id, pos);

    payload = segment + segment_offset;
    
    Tile* tile = new Tile(tile_id, dim_num, cell_type, val_num);
    tile->set_payload(payload, payload_size);
    if(dim_num != 0) // Coordinate type
      tile->set_mbr(book_keeping_->mbr(pos));
    
    tiles[i] = tile;
    segment_offset += payload_size;
  }
}

