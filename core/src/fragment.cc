/**
 * @file   fragment.cc
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
 * This file implements the Fragment class.
 */

#include "fragment.h"
#include "utils.h"
#include <algorithm>
#include <assert.h>
#include <fcntl.h>
#include <math.h>
#include <sys/stat.h>
#include <sstream>

/******************************************************
************* CONSTRUCTORS & DESTRUCTORS **************
******************************************************/

Fragment::Fragment(
    const std::string& workspace, size_t segment_size,
    size_t write_state_max_size,
    const ArraySchema* array_schema, const std::string& fragment_name)
    : workspace_(workspace), array_schema_(array_schema),
      fragment_name_(fragment_name), segment_size_(segment_size) {
  book_keeping_ = NULL;
  read_state_ = NULL;
  write_state_ = NULL;

  temp_dirname_ = workspace_ + "/" + TEMP + "/" + 
                  array_schema->array_name() + "_" + fragment_name + "/";
  std::string fragment_dirname = workspace_ + 
                                 array_schema->array_name() + "/" + 
                                 fragment_name;

  if(is_dir(fragment_dirname)) {
    load_book_keeping();
    init_read_state();
  } else { 
    // Create directories 
    create_directory(fragment_dirname);
    create_directory(temp_dirname_);

    // Initialize write state and book-keeping structures
    write_state_ = new WriteState(array_schema, write_state_max_size);
    init_book_keeping();
  }
}

Fragment::~Fragment() {
  clear_read_state();
  flush_write_state();
  commit_book_keeping();
  clear_book_keeping();
  delete_directory(temp_dirname_);
}

/******************************************************
********************** ACCESSORS **********************
******************************************************/

const ArraySchema* Fragment::array_schema() const {
  return array_schema_;
}

const std::string& Fragment::fragment_name() const {
  return fragment_name_;
}

int64_t Fragment::tile_num() const {
  return book_keeping_->tile_num();
}

size_t Fragment::tile_size(
    int attribute_id, int64_t pos) const {
  assert(tile_num() > 0);
  assert(pos >= 0 && pos < tile_num());

  if(pos == tile_num() - 1) {
    const std::string& array_name = array_schema_->array_name();
    const std::string& attribute_name = 
        array_schema_->attribute_name(attribute_id);
    std::string filename = workspace_ + "/" + array_name + "/" +
                           fragment_name_ + "/" +
                           attribute_name + TILE_DATA_FILE_SUFFIX;
    return file_size(filename) - book_keeping_->offset(attribute_id, pos); 
  } else {
    return book_keeping_->offset(attribute_id, pos+1) - 
           book_keeping_->offset(attribute_id, pos); 
  } 
}

/******************************************************
******************** CELL FUNCTIONS *******************
******************************************************/

template<class T>
void Fragment::write_cell(const void* cell, size_t cell_size) const {
  write_state_->write_cell<T>(cell, cell_size);
}

template<class T>
void Fragment::write_cell_sorted(const void* cell) {
  // For easy reference
  int attribute_num = array_schema_->attribute_num();
  size_t coords_size = array_schema_->cell_size(attribute_num);
  const char* c_cell = static_cast<const char*>(cell);
  size_t cell_offset, attr_size;
  std::vector<size_t> attr_sizes;

  // Flush tile info to book-keeping if a new tile must be created
  if(write_state_->cell_num_ == array_schema_->capacity())
    flush_tile_info_to_book_keeping();

  // Append coordinates to segment  
  append_coordinates_to_segment(c_cell);
  cell_offset = coords_size; 
  if(array_schema_->cell_size() == VAR_SIZE)
    cell_offset += sizeof(size_t);

  // Append attribute values to the respective segments
  for(int i=0; i<attribute_num; ++i) {
    append_attribute_to_segment(c_cell + cell_offset, i, attr_size);
    cell_offset += attr_size;
    attr_sizes.push_back(attr_size);
  }
  attr_sizes.push_back(coords_size);
    
  // Update the info of the currently populated tile
  int64_t tile_id = (write_state_->cell_num_) ? write_state_->tile_id_ 
                                              : write_state_->tile_id_ + 1;
  update_tile_info<T>(static_cast<const T*>(cell), tile_id, attr_sizes);
}

template<class T>
void Fragment::write_cell_sorted_with_id(const void* cell) {
  // For easy reference
  int attribute_num = array_schema_->attribute_num();
  size_t coords_size = array_schema_->cell_size(attribute_num);
  bool regular = array_schema_->has_regular_tiles();
  int64_t id = *(static_cast<const int64_t*>(cell));
  const void* coords = static_cast<const char*>(cell) + sizeof(int64_t);
  const char* c_cell = static_cast<const char*>(coords);
  size_t cell_offset, attr_size;
  std::vector<size_t> attr_sizes;

  // Flush tile info to book-keeping if a new tile must be created
  if((regular && id != write_state_->tile_id_) || 
     (!regular && (write_state_->cell_num_ == array_schema_->capacity())))
    flush_tile_info_to_book_keeping();

  // Append coordinates to segment  
  append_coordinates_to_segment(c_cell);
  cell_offset = coords_size; 
  if(array_schema_->cell_size() == VAR_SIZE)
    cell_offset += sizeof(size_t);

  // Append attribute values to the respective segments
  for(int i=0; i<attribute_num; ++i) {
    append_attribute_to_segment(c_cell + cell_offset, i, attr_size);
    cell_offset += attr_size;
    attr_sizes.push_back(attr_size);
  }
  attr_sizes.push_back(coords_size);

    
  // Update the info of the currently populated tile
  update_tile_info<T>(static_cast<const T*>(coords), id, attr_sizes);
}

template<class T>
void Fragment::write_cell_sorted_with_2_ids(const void* cell) {
  // For easy reference
  int attribute_num = array_schema_->attribute_num();
  size_t coords_size = array_schema_->cell_size(attribute_num);
  int64_t id = *(static_cast<const int64_t*>(cell));
  const void* coords = static_cast<const char*>(cell) + 2*sizeof(int64_t);
  const char* c_cell = static_cast<const char*>(coords);
  size_t cell_offset, attr_size;
  std::vector<size_t> attr_sizes;

  // Flush tile info to book-keeping if a new tile must be created
  if(id != write_state_->tile_id_)    
    flush_tile_info_to_book_keeping();

  // Append coordinates to segment  
  append_coordinates_to_segment(c_cell);
  cell_offset = coords_size; 
  if(array_schema_->cell_size() == VAR_SIZE)
    cell_offset += sizeof(size_t);

  // Append attribute values to the respective segments
  for(int i=0; i<attribute_num; ++i) {
    append_attribute_to_segment(c_cell + cell_offset, i, attr_size);
    cell_offset += attr_size;
    attr_sizes.push_back(attr_size);
  }
  attr_sizes.push_back(coords_size);

  // Update the info of the currently populated tile
  update_tile_info<T>(static_cast<const T*>(coords), id, attr_sizes);
}

/******************************************************
******************** TILE FUNCTIONS *******************
******************************************************/

FragmentConstTileIterator Fragment::begin(int attribute_id) {
  // Check attribute id
  assert(attribute_id <= array_schema_->attribute_num());

  if(book_keeping_->tile_num() > 0) 
    return FragmentConstTileIterator(this, attribute_id, 0);
  else
    return FragmentConstTileIterator();

}

const Tile* Fragment::get_tile_by_pos(int attribute_id, int64_t pos) {
  // For easy reference
  const int64_t& pos_lower = read_state_->pos_ranges_[attribute_id].first;
  const int64_t& pos_upper = read_state_->pos_ranges_[attribute_id].second;

  // Fetch from the disk if the tile is not in main memory
  if(read_state_->tiles_[attribute_id].size() == 0 ||
     (pos < pos_lower || pos > pos_upper)) 
    // The following updates pos_lower and pos_upper
    load_tiles_from_disk(attribute_id, pos);

  assert(pos >= pos_lower && pos <= pos_upper);
  assert(pos - pos_lower <= read_state_->tiles_[attribute_id].size());

  return read_state_->tiles_[attribute_id][pos-pos_lower];
}

FragmentConstReverseTileIterator Fragment::rbegin(int attribute_id) {
  // Check attribute id
  assert(attribute_id <= array_schema_->attribute_num());

  if(book_keeping_->tile_num() > 0) 
    return FragmentConstReverseTileIterator(this, attribute_id, 
                                            book_keeping_->tile_num()-1);
  else
    return FragmentConstReverseTileIterator();
}

const Tile* Fragment::rget_tile_by_pos(int attribute_id, int64_t pos) {

  // For easy reference
  const int64_t& pos_lower = read_state_->pos_ranges_[attribute_id].first;
  const int64_t& pos_upper = read_state_->pos_ranges_[attribute_id].second;

  // Fetch from the disk if the tile is not in main memory
  if(read_state_->tiles_[attribute_id].size() == 0 ||
     (pos < pos_lower || pos > pos_upper))  {
    // Find the starting position, so that the pos tile is 
    // the last tile in the set of tiles loaded from the disk
    int64_t start_pos = pos; 
    size_t segment_size = tile_size(attribute_id, start_pos);
    while(start_pos > 0 && segment_size < segment_size_) {
      --start_pos;
      segment_size += tile_size(attribute_id, start_pos);
    }

    // The following updates pos_lower and pos_upper
    load_tiles_from_disk(attribute_id, start_pos);
  }

  assert(pos >= pos_lower && pos <= pos_upper);
  assert(pos - pos_lower <= read_state_->tiles_[attribute_id].size());

  return read_state_->tiles_[attribute_id][pos-pos_lower];
}

/******************************************************
******************* PRIVATE METHODS *******************
******************************************************/

// ------------- READ STATE FUNCTIONS ------------- //

void Fragment::clear_read_state() {
  // Do nothing if the read state is NULL
  if(read_state_ == NULL)
    return;

  // Clean-up tiles
  for(int i=0; i<read_state_->tiles_.size(); ++i)
    delete_tiles(i);
  read_state_->tiles_.clear();

  // Clean-up segments
  for(int i=0; i<read_state_->segments_.size(); ++i)
    free(read_state_->segments_[i]);
  read_state_->segments_.clear();

  // Clean-up pos_ranges_
  read_state_->pos_ranges_.clear();

  delete read_state_;
  read_state_ = NULL;
}

void Fragment::delete_tiles(int attribute_id) {
  for(int i=0; i<read_state_->tiles_[attribute_id].size(); ++i)
    delete read_state_->tiles_[attribute_id][i];
}

void Fragment::init_read_state() {
  assert(read_state_ == NULL);

  // For easy reference
  int attribute_num = array_schema_->attribute_num();

  read_state_ = new ReadState();

  // Initialize segments
  read_state_->segments_.resize(attribute_num+1);
  for(int i=0; i<=attribute_num; ++i)
    read_state_->segments_[i] = malloc(segment_size_);

  // Initialize tiles
  read_state_->tiles_.resize(attribute_num+1);

  // Initialize pos_ranges
  read_state_->pos_ranges_.resize(attribute_num+1);
}

void Fragment::load_sorted_bin(const std::string& dirname) {
  // Merge sorted files
  bool merged = merge_sorted_runs(dirname);
  
  // Make tiles
  if(merged)
    make_tiles(temp_dirname_);
  else 
    make_tiles(dirname);
}

void Fragment::load_tiles_from_disk(int attribute_id, int64_t start_pos) { 
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
  read_state_->pos_ranges_[attribute_id].first = start_pos;
  read_state_->pos_ranges_[attribute_id].second = 
      start_pos + tiles_in_segment - 1;
}

void Fragment::load_tiles_from_segment(
    int attribute_id, int64_t start_pos, 
    size_t segment_utilization, int64_t tiles_in_segment) {
  // For easy reference
  const BookKeeping::OffsetList& offsets = book_keeping_.offsets_[attribute_id];
  const BookKeeping::TileIds& tile_ids = book_keeping_.tile_ids_;
  const BookKeeping::MBRs& mbrs = book_keeping_.mbrs_;
  const BookKeeping::BoundingCoordinates& bounding_coordinates = 
      book_keeping_.bounding_coordinates_;
  ReadState::TileList& tiles = read_state_->tiles_[attribute_id];
  int attribute_num = array_schema_->attribute_num();
  int dim_num = (attribute_id != attribute_num) ? 0 : array_schema_->dim_num();
  char* segment = static_cast<char*>(read_state_->segments_[attribute_id]);
  const std::type_info* cell_type = array_schema_->type(attribute_id);
  int val_num  = (dim_num != 0) ? 1 : array_schema_->val_num(attribute_id);

  // Initializations
  size_t segment_offset = 0, payload_size;
  int64_t tile_id;
  int64_t pos = start_pos;
  tiles.resize(tiles_in_segment);
  void* payload;
  
  // Load tiles
  for(int64_t i=0; i<tiles_in_segment; ++i, ++pos) {
    assert(pos < tile_ids.size());
    tile_id = tile_ids[pos];

    if(pos == offsets.size()-1)
      payload_size = segment_utilization - segment_offset;
    else
      payload_size = offsets[pos+1] - offsets[pos];

    payload = segment + segment_offset;
    
    Tile* tile = new Tile(tile_id, dim_num, cell_type, val_num);
    tile->set_payload(payload, payload_size);
    if(dim_num != 0) // Coordinate type
      tile->set_mbr(mbrs[pos]);
    
    tiles[i] = tile;
    segment_offset += payload_size;
  }
}

inline
std::pair<size_t, int64_t> Fragment::load_payloads_into_segment(
    int attribute_id, int64_t start_pos) {
  // For easy reference
  const std::string& array_name = array_schema_->array_name();
  const std::string& attribute_name = 
      array_schema_->attribute_name(attribute_id);
  const BookKeeping::OffsetList& offsets = book_keeping_.offsets_[attribute_id];
  int64_t tile_num = book_keeping_.tile_ids_.size();

  // Open file
  std::string filename = workspace_ + "/" + array_name + "/" +
                         fragment_name_ + "/" +
                         attribute_name + 
                         TILE_DATA_FILE_SUFFIX;
  int fd = open(filename.c_str(), O_RDONLY);
  assert(fd != -1);

  // Initilizations
  struct stat st;
  fstat(fd, &st);
  size_t segment_utilization = 0;
  int64_t tiles_in_segment = 0;
  int64_t pos = start_pos;

  // Calculate buffer size (largest size smaller than the segment_size_)
  while(pos < tile_num && segment_utilization < segment_size_) {
    if(pos == tile_num-1)
      segment_utilization += st.st_size - offsets[pos];
    else
      segment_utilization += offsets[pos+1] - offsets[pos];
    pos++;
    tiles_in_segment++;
  }

  assert(segment_utilization != 0);
  assert(offsets[start_pos] + segment_utilization <= st.st_size);

  // Read payloads into buffer
  lseek(fd, offsets[start_pos], SEEK_SET);
  read(fd, read_state_->segments_[attribute_id], segment_utilization); 
  close(fd);

  return std::pair<size_t, int64_t>(segment_utilization, tiles_in_segment);
}

// ------------- WRITE STATE FUNCTIONS ------------- //

void Fragment::append_attribute_to_segment(
    const char* attr, int attribute_id, size_t& attr_size) {
  // For easy reference
  bool var_size = (array_schema_->cell_size(attribute_id) == VAR_SIZE);

  if(!var_size) {
    attr_size = array_schema_->cell_size(attribute_id); 
  } else {
    int val_num;
    memcpy(&val_num, attr, sizeof(int));
    attr_size = val_num * array_schema_->type_size(attribute_id) + sizeof(int);
  }  

  // Check if the segment is full
  if(write_state_->segment_utilization_[attribute_id] + attr_size > 
     segment_size_)
    flush_segment(attribute_id);

  // Append cell to the segment
  memcpy(static_cast<char*>(write_state_->segments_[attribute_id]) + 
         write_state_->segment_utilization_[attribute_id], attr, attr_size); 
  write_state_->segment_utilization_[attribute_id] += attr_size;
}

void Fragment::append_coordinates_to_segment(
    const char* cell) {
  // For easy reference
  int attribute_num = array_schema_->attribute_num();
  bool var_size = (array_schema_->cell_size() == VAR_SIZE);
  size_t coords_size = array_schema_->cell_size(attribute_num);

  // Check if the segment is full
  if(write_state_->segment_utilization_[attribute_num] + coords_size > 
     segment_size_)
    flush_segment(attribute_num);

  // Append cell to the segment
  memcpy(static_cast<char*>(write_state_->segments_[attribute_num]) + 
         write_state_->segment_utilization_[attribute_num], cell, coords_size); 
  write_state_->segment_utilization_[attribute_num] += coords_size;
}


void Fragment::clear_write_state() {
  // Do nothing if the write state is NULL
  if(write_state_ == NULL)
    return;

  // For easy reference
  int attribute_num = array_schema_->attribute_num();
  int64_t cells_num = write_state_->cells_.size();
  int64_t cells_with_id_num = write_state_->cells_with_id_.size();
  int64_t cells_with_2_ids_num = write_state_->cells_with_2_ids_.size();

  if(cells_num > 0) {
    for(int64_t i=0; i<cells_num; ++i)
      free(write_state_->cells_[i].cell_);
  }
  if(cells_with_id_num > 0) {
    for(int64_t i=0; i<cells_with_id_num; ++i)
      free(write_state_->cells_with_id_[i].cell_);
  }
  if(cells_with_2_ids_num > 0) {
    for(int64_t i=0; i<cells_with_2_ids_num; ++i)
      free(write_state_->cells_with_2_ids_[i].cell_);
  }

  // Clear segments
  for(int i=0; i<=attribute_num; ++i) 
    free(write_state_->segments_[i]); 

  // Clear MBR and bounding coordinates
  if(write_state_->mbr_ != NULL) 
    free(write_state_->mbr_);
  if(write_state_->bounding_coordinates_.first != NULL) 
    free(write_state_->bounding_coordinates_.first);
  if(write_state_->bounding_coordinates_.second != NULL) 
    free(write_state_->bounding_coordinates_.second);

  delete write_state_;
  write_state_ = NULL;
}

void Fragment::finalize_last_run() {
  if(write_state_->cells_.size() > 0) {
    sort_run();
    flush_sorted_run();
  } else if(write_state_->cells_with_id_.size() > 0) {
    sort_run_with_id();
    flush_sorted_run_with_id();
  } else if(write_state_->cells_with_2_ids_.size() > 0) {
    sort_run_with_2_ids();
    flush_sorted_run_with_2_ids();
  }
}

void Fragment::flush_segment(int attribute_id) {
  // Exit if the segment has no useful data
  if(write_state_->segment_utilization_[attribute_id] == 0)
    return;

  // Open file
  std::string filename = workspace_ + "/" + 
                         array_schema_->array_name() + "/" + 
                         fragment_name_ + "/" + 
                         array_schema_->attribute_name(attribute_id) +
                         TILE_DATA_FILE_SUFFIX;		
  int fd = open(filename.c_str(), O_WRONLY | O_APPEND | O_CREAT | O_SYNC,  
                S_IRWXU);
  assert(fd != -1);

  // Retrieve the current file offset (equal to the file size)
  struct stat st;
  fstat(fd, &st);
  int64_t offset = st.st_size;

  // Append the segment to the file
  write(fd, write_state_->segments_[attribute_id], 
         write_state_->segment_utilization_[attribute_id]);
 
  // Update the write state 
  write_state_->segment_utilization_[attribute_id] = 0;

  // Clean up 
  close(fd);
}

void Fragment::flush_segments() {
  // For easy reference
  int attribute_num = array_schema_->attribute_num();

  // Store the info of the lastly populated tile
  flush_tile_info_to_book_keeping();

  // Flush the segments
  for(int i=0; i<=attribute_num; ++i) {
    flush_segment(i);
    free(write_state_->segments_[i]); 
    write_state_->segments_[i] = NULL;
  }
}

void Fragment::flush_sorted_run() {
  // For easy reference
  size_t cell_size = array_schema_->cell_size();
  bool var_size = (cell_size == VAR_SIZE);
  int attribute_num = array_schema_->attribute_num();
  size_t coords_size = array_schema_->cell_size(attribute_num);

  // Prepare file
  std::stringstream filename;
  filename << temp_dirname_ << write_state_->runs_num_;
  remove(filename.str().c_str());
  int file = open(filename.str().c_str(), O_WRONLY | O_CREAT | O_SYNC, S_IRWXU);
  assert(file != -1);

  // Write the cells into the file 
  char* segment = new char[segment_size_];
  size_t offset = 0; 
  int64_t cell_num = write_state_->cells_.size();
  char* cell;

  for(int64_t i=0; i<cell_num; ++i) {
    // For easy reference
    cell = static_cast<char*>(write_state_->cells_[i].cell_);

    // If variable-sized, calculate size
    if(var_size) 
      memcpy(&cell_size, cell + coords_size, sizeof(size_t));

    // Flush the segment to the file
    if(offset + cell_size > segment_size_) { 
      write(file, segment, offset);
      offset = 0;
    }

    // Write cell to segment
    memcpy(segment + offset, cell, cell_size);
    offset += cell_size;
  }

  if(offset != 0) {
    write(file, segment, offset);
    offset = 0;
  }

  // Clean up
  close(file);
  delete [] segment;

  // Update write state
  for(int64_t i=0; i<cell_num; ++i)
    free(write_state_->cells_[i].cell_);
  write_state_->cells_.clear();
  write_state_->run_size_ = 0;
  ++write_state_->runs_num_;
}

void Fragment::flush_sorted_run_with_id() {
  // For easy reference
  size_t cell_size = array_schema_->cell_size();
  bool var_size = (cell_size == VAR_SIZE);
  int attribute_num = array_schema_->attribute_num();
  size_t coords_size = array_schema_->cell_size(attribute_num);

  // Prepare file
  std::stringstream filename;
  filename << temp_dirname_ << write_state_->runs_num_;
  remove(filename.str().c_str());
  int file = open(filename.str().c_str(), O_WRONLY | O_CREAT | O_SYNC, S_IRWXU);
  assert(file != -1);

  // Write the cells into the file 
  char* buffer = new char[segment_size_];
  size_t buffer_offset = 0; 
  int64_t cell_num = write_state_->cells_with_id_.size();
  char* cell;

  for(int64_t i=0; i<cell_num; ++i) {
    // For easy reference
    cell = static_cast<char*>(write_state_->cells_with_id_[i].cell_);

    // If variable-sized, calculate size
    if(var_size) 
      memcpy(&cell_size, cell + coords_size, sizeof(size_t));

    // Flush the segment to the file
    if(buffer_offset + sizeof(int64_t) + cell_size > segment_size_) { 
      write(file, buffer, buffer_offset);
      buffer_offset = 0;
    }

    // Write id to segment
    memcpy(buffer + buffer_offset, &write_state_->cells_with_id_[i].id_, 
           sizeof(int64_t));
    buffer_offset += sizeof(int64_t);
    // Write cell to segment
    memcpy(buffer + buffer_offset, cell, cell_size);
    buffer_offset += cell_size;
  }

  if(buffer_offset != 0) {
    write(file, buffer, buffer_offset);
    buffer_offset = 0;
  }

  // Clean up
  delete [] buffer;
  close(file);

  // Update write state
  for(int64_t i=0; i<cell_num; ++i)
    free(write_state_->cells_with_id_[i].cell_);
  write_state_->cells_with_id_.clear();
  write_state_->run_size_ = 0;
  ++write_state_->runs_num_;
}

void Fragment::flush_sorted_run_with_2_ids() {
  // For easy reference
  size_t cell_size = array_schema_->cell_size();
  bool var_size = (cell_size == VAR_SIZE);
  int attribute_num = array_schema_->attribute_num();
  size_t coords_size = array_schema_->cell_size(attribute_num);

  // Prepare file
  std::stringstream filename;
  filename << temp_dirname_ << write_state_->runs_num_;
  remove(filename.str().c_str());
  int file = open(filename.str().c_str(), O_WRONLY | O_CREAT | O_SYNC, S_IRWXU);
  assert(file != -1);

  // Write the cells into the file 
  char* buffer = new char[segment_size_];
  size_t buffer_offset = 0; 
  int64_t cell_num = write_state_->cells_with_2_ids_.size();
  char* cell;

  for(int64_t i=0; i<cell_num; ++i) {
    // For easy reference
    cell = static_cast<char*>(write_state_->cells_with_2_ids_[i].cell_);

    // If variable-sized, calculate size
    if(var_size) 
      memcpy(&cell_size, cell + coords_size, sizeof(size_t));

    // Flush the segment to the file
    if(buffer_offset + 2*sizeof(int64_t) + cell_size > segment_size_) { 
      write(file, buffer, buffer_offset);
      buffer_offset = 0;
    }

    // Write tile id to segment
    memcpy(buffer + buffer_offset, 
           &write_state_->cells_with_2_ids_[i].tile_id_, 
           sizeof(int64_t));
    buffer_offset += sizeof(int64_t);
    // Write cell id to segment
    memcpy(buffer + buffer_offset, 
           &write_state_->cells_with_2_ids_[i].cell_id_, 
           sizeof(int64_t));
    buffer_offset += sizeof(int64_t);
    // Write cell to segment
    memcpy(buffer + buffer_offset, cell, cell_size);
    buffer_offset += cell_size;
  }

  if(buffer_offset != 0) {
    write(file, buffer, buffer_offset);
    buffer_offset = 0;
  }

  // Clean up
  delete [] buffer;
  close(file);

  // Update write state
  for(int64_t i=0; i<cell_num; ++i)
    free(write_state_->cells_with_2_ids_[i].cell_);
  write_state_->cells_with_2_ids_.clear();
  write_state_->run_size_ = 0;
  ++write_state_->runs_num_;
}

void Fragment::flush_tile_info_to_book_keeping() {
  // Exit if there are no cells in the current tile
  if(write_state_->cell_num_ == 0)
    return;

  // For easy reference
  int attribute_num = array_schema_->attribute_num();
  size_t coords_size = array_schema_->cell_size(attribute_num);

  // Flush info
  for(int i=0; i<=attribute_num; ++i) 
    book_keeping_.offsets_[i].push_back(write_state_->file_offsets_[i]);

  book_keeping_.bounding_coordinates_.push_back(
      write_state_->bounding_coordinates_);
  book_keeping_.mbrs_.push_back(write_state_->mbr_);
  book_keeping_.tile_ids_.push_back(write_state_->tile_id_);
  write_state_->cell_num_ = 0;

  // Nullify MBR and bounding coordinates
  write_state_->mbr_ = NULL;
  write_state_->bounding_coordinates_.first = NULL;
  write_state_->bounding_coordinates_.second = NULL;
}

void Fragment::flush_write_state() {
  // Do nothing if the write state is NULL
  if(write_state_ == NULL)
    return;

  // Make tiles, after finalizing the last run and merging the runs
  finalize_last_run();

  // Merge runs
  merge_sorted_runs(temp_dirname_);
  make_tiles(temp_dirname_);
  flush_segments();
 
  delete write_state_;
  write_state_ = NULL;
}

template<class T>
void* Fragment::get_next_cell(
    SortedRun** runs, int runs_num, size_t& cell_size) const {
  assert(runs_num > 0);
  // For easy reference
  int attribute_num = array_schema_->attribute_num();
  size_t coords_size = array_schema_->cell_size(attribute_num);

  // Get the first non-NULL cell
  void *next_cell, *cell;
  int next_run;
  int r = 0;
  do {
    next_cell = runs[r]->current_cell(); 
    ++r;
  } while((next_cell == NULL) && (r < runs_num)); 

  next_run = r-1;

  // Get the next cell in the global cell order
  for(int i=r; i<runs_num; ++i) {
    cell = runs[i]->current_cell();
    if(cell != NULL && 
       array_schema_->precedes(static_cast<const T*>(cell), 
                               static_cast<const T*>(next_cell))) { 
      next_cell = cell;
      next_run = i;
    }
  }

  if(next_cell != NULL) {
    if(runs[next_run]->var_size()) {
      memcpy(&cell_size, static_cast<char*>(next_cell) + coords_size,
             sizeof(size_t));
    }

    runs[next_run]->advance_cell(cell_size);
  }

  return next_cell;
}

template<class T>
void* Fragment::get_next_cell_with_id(
    SortedRun** runs, int runs_num, size_t& cell_size) const {
  assert(runs_num > 0);
  // For easy reference
  int attribute_num = array_schema_->attribute_num();
  size_t coords_size = array_schema_->cell_size(attribute_num);

  // Get the first non-NULL cell
  void *next_cell, *cell;
  int next_run;
  int r = 0;
  do {
    next_cell = runs[r]->current_cell(); 
    ++r;
  } while((next_cell == NULL) && (r < runs_num)); 

  next_run = r-1;
  int64_t* next_cell_id = static_cast<int64_t*>(next_cell);
  void* next_cell_coords = next_cell_id + 1; 
  int64_t* cell_id;
  void* cell_coords;

  // Get the next cell in the global cell order
  for(int i=r; i<runs_num; ++i) {
    cell = runs[i]->current_cell();
    if(cell == NULL)
      continue;

    cell_id = static_cast<int64_t*>(cell);
    cell_coords = cell_id + 1;

    if(*cell_id < *next_cell_id || 
       (*cell_id == *next_cell_id && 
         array_schema_->precedes(static_cast<T*>(cell_coords), 
                                 static_cast<T*>(next_cell_coords)))) { 
      next_cell = cell;
      next_cell_id = static_cast<int64_t*>(next_cell);
      next_cell_coords = next_cell_id + 1; 
      next_run = i;
    }
  }

  if(next_cell != NULL) {
    if(runs[next_run]->var_size()) {
      memcpy(&cell_size, 
             static_cast<char*>(next_cell) + sizeof(int64_t) + coords_size, 
             sizeof(size_t));
    }

    runs[next_run]->advance_cell(cell_size + sizeof(int64_t));
  }

  return next_cell;
}


template<class T>
void* Fragment::get_next_cell_with_2_ids(
    SortedRun** runs, int runs_num, size_t& cell_size) const {
  assert(runs_num > 0);
  // For easy reference
  int attribute_num = array_schema_->attribute_num();
  size_t coords_size = array_schema_->cell_size(attribute_num);

  // Get the first non-NULL cell
  void *next_cell, *cell;
  int next_run;
  int r = 0;
  do {
    next_cell = runs[r]->current_cell(); 
    ++r;
  } while((next_cell == NULL) && (r < runs_num)); 

  next_run = r-1;
  int64_t* next_cell_tile_id = static_cast<int64_t*>(next_cell);
  int64_t* next_cell_cell_id = next_cell_tile_id + 1;
  void* next_cell_coords = next_cell_cell_id + 1; 
  int64_t* cell_tile_id;
  int64_t* cell_cell_id;
  void* cell_coords; 

  // Get the next cell in the global cell order
  for(int i=r; i<runs_num; ++i) {
    cell = runs[i]->current_cell();
    if(cell == NULL)
      continue;

    cell_tile_id = static_cast<int64_t*>(cell);
    cell_cell_id = cell_tile_id + 1;
    cell_coords = cell_cell_id + 1;

    if(*cell_tile_id < *next_cell_tile_id       ||
       (*cell_tile_id == *next_cell_tile_id && 
        *cell_cell_id < *next_cell_cell_id)     ||
       (*cell_tile_id == *next_cell_tile_id && 
        *cell_cell_id == *next_cell_cell_id &&
         array_schema_->precedes(static_cast<T*>(cell_coords), 
                                 static_cast<T*>(next_cell_coords)))) { 
      next_cell = cell;
      next_cell_tile_id = static_cast<int64_t*>(next_cell);
      next_cell_cell_id = next_cell_tile_id;
      next_cell_coords = next_cell_cell_id + 1; 
      next_run = i;
    }
  }

  if(next_cell != NULL) {
    if(runs[next_run]->var_size()) {
      memcpy(&cell_size, 
             static_cast<char*>(next_cell) + 2*sizeof(int64_t) + coords_size, 
             sizeof(size_t));
    }

    runs[next_run]->advance_cell(cell_size + 2*sizeof(int64_t));
  }

  return next_cell;
}

void Fragment::make_tiles(const std::string& dirname) {
  // For easy reference
  int attribute_num = array_schema_->attribute_num();
  const std::type_info* coords_type = array_schema_->type(attribute_num);
  ArraySchema::TileOrder tile_order = array_schema_->tile_order();
  ArraySchema::CellOrder cell_order = array_schema_->cell_order();
  bool regular_tiles = array_schema_->has_regular_tiles();

  if(!regular_tiles && (cell_order == ArraySchema::CO_ROW_MAJOR ||
                        cell_order == ArraySchema::CO_COLUMN_MAJOR)) {
    // Cell
    if(*coords_type == typeid(int)) 
      make_tiles<int>(dirname); 
    else if(*coords_type == typeid(int64_t)) 
      make_tiles<int64_t>(dirname); 
    else if(*coords_type == typeid(float)) 
      make_tiles<float>(dirname); 
    else if(*coords_type == typeid(double)) 
      make_tiles<double>(dirname); 
  } else if((regular_tiles && (cell_order == ArraySchema::CO_ROW_MAJOR ||
                               cell_order == ArraySchema::CO_COLUMN_MAJOR)) ||
            (!regular_tiles && cell_order == ArraySchema::CO_HILBERT)) {
    // CellWithId
    if(*coords_type == typeid(int)) 
      make_tiles_with_id<int>(dirname); 
    else if(*coords_type == typeid(int64_t)) 
      make_tiles_with_id<int64_t>(dirname); 
    else if(*coords_type == typeid(float)) 
      make_tiles_with_id<float>(dirname); 
    else if(*coords_type == typeid(double)) 
      make_tiles_with_id<double>(dirname); 
  } else if(regular_tiles && cell_order == ArraySchema::CO_HILBERT) {
    // CellWith2Ids
    if(*coords_type == typeid(int)) 
      make_tiles_with_2_ids<int>(dirname); 
    else if(*coords_type == typeid(int64_t)) 
      make_tiles_with_2_ids<int64_t>(dirname); 
    else if(*coords_type == typeid(float)) 
      make_tiles_with_2_ids<float>(dirname); 
    else if(*coords_type == typeid(double)) 
      make_tiles_with_2_ids<double>(dirname); 
  }
}

// NOTE: This function applies only to irregular tiles
template<class T>
void Fragment::make_tiles(const std::string& dirname) {
  // For easy reference
  size_t cell_size = array_schema_->cell_size();
  std::vector<std::string> filenames = get_filenames(dirname);
  int runs_num = filenames.size();

  if(runs_num == 0)
    return;

  // Information about the runs to be merged 
  SortedRun** runs = new SortedRun*[runs_num];
  for(int i=0; i<runs_num; ++i) {
    runs[i] = new SortedRun(dirname + filenames[i], 
                            cell_size == VAR_SIZE, 
                            segment_size_);
  }

  // Loop over the cells
  void* cell;
  while((cell = get_next_cell<T>(runs, runs_num, cell_size)) != NULL) 
    write_cell_sorted<T>(cell);

  // Clean up
  for(int i=0; i<runs_num; ++i) 
    delete runs[i];
  delete [] runs;
}

// This function applies either to regular tiles with row- or column-major
// order, or irregular tiles with Hilbert order
template<class T>
void Fragment::make_tiles_with_id(const std::string& dirname) {
  // For easy reference
  size_t cell_size = array_schema_->cell_size();
  std::vector<std::string> filenames = get_filenames(dirname);
  int runs_num = filenames.size();

  if(runs_num == 0)
    return;

  // Information about the runs to be merged 
  SortedRun** runs = new SortedRun*[runs_num];
  for(int i=0; i<runs_num; ++i) {
    runs[i] = new SortedRun(dirname + filenames[i], 
                            cell_size == VAR_SIZE, 
                            segment_size_);
  }

  // Loop over the cells
  void* cell;
  if(array_schema_->has_regular_tiles()) {
    while((cell = get_next_cell_with_id<T>(runs, runs_num, cell_size)) != NULL) 
      write_cell_sorted_with_id<T>(cell);
  } else { // Irregular + Hilbert cell order --> Skip the Hilber id
    while((cell = get_next_cell_with_id<T>(runs, runs_num, cell_size)) != NULL) 
      write_cell_sorted<T>(static_cast<char*>(cell) + sizeof(int64_t));
  }

  // Clean up
  for(int i=0; i<runs_num; ++i) 
    delete runs[i];
  delete [] runs;
}

// NOTE: This function applies only to regular tiles
template<class T>
void Fragment::make_tiles_with_2_ids(const std::string& dirname) {
  // For easy reference
  size_t cell_size = array_schema_->cell_size();
  std::vector<std::string> filenames = get_filenames(dirname);
  int runs_num = filenames.size();

  if(runs_num == 0)
    return;

  // Information about the runs to be merged 
  SortedRun** runs = new SortedRun*[runs_num];
  for(int i=0; i<runs_num; ++i) {
    runs[i] = new SortedRun(dirname + filenames[i], 
                            cell_size == VAR_SIZE, 
                            segment_size_);
  }

  // Loop over the cells
  void* cell;
  while((cell = get_next_cell_with_2_ids<T>(runs, runs_num, cell_size)) != NULL)
    write_cell_sorted_with_2_ids<T>(cell);

  // Clean up
  for(int i=0; i<runs_num; ++i) 
    delete runs[i];
  delete [] runs;
}

bool Fragment::merge_sorted_runs(const std::string& dirname) {
  // For easy reference
  int attribute_num = array_schema_->attribute_num();
  const std::type_info* coords_type = array_schema_->type(attribute_num);
  ArraySchema::TileOrder tile_order = array_schema_->tile_order();
  ArraySchema::CellOrder cell_order = array_schema_->cell_order();
  bool regular_tiles = array_schema_->has_regular_tiles();

  if(!regular_tiles && (cell_order == ArraySchema::CO_ROW_MAJOR ||
                        cell_order == ArraySchema::CO_COLUMN_MAJOR)) {
    // Cell
    if(*coords_type == typeid(int)) 
      return merge_sorted_runs<int>(dirname); 
    else if(*coords_type == typeid(int64_t)) 
      return merge_sorted_runs<int64_t>(dirname); 
    else if(*coords_type == typeid(float)) 
      return merge_sorted_runs<float>(dirname); 
    else if(*coords_type == typeid(double)) 
      return merge_sorted_runs<double>(dirname); 
  } else if((regular_tiles && (cell_order == ArraySchema::CO_ROW_MAJOR ||
                               cell_order == ArraySchema::CO_COLUMN_MAJOR)) ||
            (!regular_tiles && cell_order == ArraySchema::CO_HILBERT)) {
    // CellWithId
    if(*coords_type == typeid(int)) 
      return merge_sorted_runs_with_id<int>(dirname); 
    else if(*coords_type == typeid(int64_t)) 
      return merge_sorted_runs_with_id<int64_t>(dirname); 
    else if(*coords_type == typeid(float)) 
      return merge_sorted_runs_with_id<float>(dirname); 
    else if(*coords_type == typeid(double)) 
      return merge_sorted_runs_with_id<double>(dirname); 
  } else if(regular_tiles && cell_order == ArraySchema::CO_HILBERT) {
    // CellWith2Ids
    if(*coords_type == typeid(int)) 
      return merge_sorted_runs_with_2_ids<int>(dirname); 
    else if(*coords_type == typeid(int64_t)) 
      return merge_sorted_runs_with_2_ids<int64_t>(dirname); 
    else if(*coords_type == typeid(float)) 
      return merge_sorted_runs_with_2_ids<float>(dirname); 
    else if(*coords_type == typeid(double)) 
      return merge_sorted_runs_with_2_ids<double>(dirname); 
  }
}

template<class T>
bool Fragment::merge_sorted_runs(const std::string& dirname) {
  int runs_per_merge = double(write_state_max_size_)/segment_size_-1;
  int merges, first_run, last_run;
  std::vector<std::string> filenames = get_filenames(dirname);
  int initial_runs_num = filenames.size();
  int new_run = initial_runs_num;

  // No merge
  if(initial_runs_num <= runs_per_merge)
    return false;

  // Merge hierarchically
  bool first_merge = true;
  while(filenames.size() > runs_per_merge) {
    merges = ceil(double(filenames.size()) / runs_per_merge);

    for(int i=0; i<merges; ++i) {
      first_run = i*runs_per_merge;
      last_run = std::min((i+1)*runs_per_merge-1, int(filenames.size()-1));
      merge_sorted_runs<T>((first_merge) ? dirname : temp_dirname_, 
                           filenames, first_run, last_run, new_run);
      ++new_run;
    }

    filenames = get_filenames(temp_dirname_);
    first_merge = false;
  }

  return true;
}

template<class T>
void Fragment::merge_sorted_runs(
    const std::string& dirname, const std::vector<std::string>& filenames,
    int first_run, int last_run, int new_run) {
  // For easy reference
  int attribute_num = array_schema_->attribute_num();
  size_t cell_size = array_schema_->cell_size();
  struct stat st;

  // Information about the runs to be merged 
  int runs_num = last_run-first_run+1;
  SortedRun** runs = new SortedRun*[runs_num];
  for(int i=0; i<runs_num; ++i) {
    runs[i] = new SortedRun(dirname + filenames[first_run + i], 
                            cell_size == VAR_SIZE, 
                            segment_size_);
  }

  // Information about the output merged run
  void* cell;
  char* segment = new char[segment_size_];
  size_t offset = 0;
  std::stringstream new_filename;
  new_filename << temp_dirname_ << new_run;
  int new_run_fd = open(new_filename.str().c_str(), 
                        O_WRONLY | O_CREAT | O_TRUNC | O_SYNC, S_IRWXU);
  assert(new_run_fd != -1);
 
  // Loop over the cells
  while((cell = get_next_cell<T>(runs, runs_num, cell_size)) != NULL) {
    // If the output segment is full, flush it into the output file
    if(offset + cell_size > segment_size_) {
      write(new_run_fd, segment, offset);
      offset = 0;
    }

    // Write cell to segment 
    memcpy(segment + offset, cell, cell_size);
    offset += cell_size;
  }

  // Flush the remaining data of the output segment into the output file
  if(offset > 0) 
    write(new_run_fd, segment, offset);

  // Delete the files of the merged runs
  if(dirname == temp_dirname_)
    for(int i=0; i<runs_num; ++i) 
      remove(runs[i]->filename_.c_str());

  // Clean up
  delete [] segment;
  for(int i=0; i<runs_num; ++i)
    delete runs[i];
  delete [] runs;
  close(new_run_fd);
}

template<class T>
bool Fragment::merge_sorted_runs_with_id(const std::string& dirname) {
  int runs_per_merge = double(write_state_max_size_)/segment_size_-1;
  int merges, first_run, last_run;
  std::vector<std::string> filenames = get_filenames(dirname);
  int initial_runs_num = filenames.size();
  int new_run = initial_runs_num;

  // No merge
  if(initial_runs_num <= runs_per_merge)
    return false;

  // Merge hierarchically
  bool first_merge = true;
  while(filenames.size() > runs_per_merge) {
    merges = ceil(double(filenames.size()) / runs_per_merge);

    for(int i=0; i<merges; ++i) {
      first_run = i*runs_per_merge;
      last_run = std::min((i+1)*runs_per_merge-1, int(filenames.size()-1));
      merge_sorted_runs_with_id<T>((first_merge) ? dirname : temp_dirname_, 
                                   filenames, first_run, last_run, new_run);
      ++new_run;
    }

    filenames = get_filenames(temp_dirname_);
    first_merge = false;
  }

  return true;
}

template<class T>
void Fragment::merge_sorted_runs_with_id(
    const std::string& dirname, const std::vector<std::string>& filenames,
    int first_run, int last_run, int new_run) {
  // For easy reference
  int attribute_num = array_schema_->attribute_num();
  size_t cell_size = array_schema_->cell_size();
  struct stat st;

  // Information about the runs to be merged 
  int runs_num = last_run-first_run+1;
  SortedRun** runs = new SortedRun*[runs_num];
  for(int i=0; i<runs_num; ++i) {
    std::stringstream filename;
    filename << dirname << (first_run+i);
    runs[i] = new SortedRun(dirname + filenames[first_run + i], 
                            cell_size == VAR_SIZE, 
                            segment_size_);
  }

  // Information about the output merged run
  void* cell;
  char* segment = new char[segment_size_];
  size_t offset = 0;
  std::stringstream new_filename;
  new_filename << temp_dirname_ << new_run;
  int new_run_fd = open(new_filename.str().c_str(), 
                        O_WRONLY | O_CREAT | O_TRUNC | O_SYNC, S_IRWXU);
  assert(new_run_fd != -1);
 
  // Loop over the cells
  while((cell = get_next_cell_with_id<T>(runs, runs_num, cell_size)) != NULL) {
    // If the output segment is full, flush it into the output file
    if(offset + cell_size + sizeof(int64_t) > segment_size_) {
      write(new_run_fd, segment, offset);
      offset = 0;
    }

    // Write cell (along with its id) to segment 
    memcpy(segment + offset, cell, cell_size + sizeof(int64_t));
    offset += cell_size;
  }

  // Flush the remaining data of the output segment into the output file
  if(offset > 0) 
    write(new_run_fd, segment, offset);

  // Delete the files of the merged runs
  if(dirname == temp_dirname_)
    for(int i=0; i<runs_num; ++i) 
      remove(runs[i]->filename_.c_str());

  // Clean up
  delete [] segment;
  for(int i=0; i<runs_num; ++i)
    delete runs[i];
  delete [] runs;
  close(new_run_fd);
}

template<class T>
bool Fragment::merge_sorted_runs_with_2_ids(
  const std::string& dirname) {
  int runs_per_merge = double(write_state_max_size_)/segment_size_-1;
  int merges, first_run, last_run;
  std::vector<std::string> filenames = get_filenames(dirname);
  int initial_runs_num = filenames.size();
  int new_run = initial_runs_num;

  // No merge
  if(initial_runs_num <= runs_per_merge)
    return false;

  // Merge hierarchically
  bool first_merge = true;
  while(filenames.size() > runs_per_merge) {
    merges = ceil(double(filenames.size()) / runs_per_merge);

    for(int i=0; i<merges; ++i) {
      first_run = i*runs_per_merge;
      last_run = std::min((i+1)*runs_per_merge-1, int(filenames.size()-1));
      merge_sorted_runs_with_2_ids<T>((first_merge) ? dirname : temp_dirname_, 
                                      filenames, first_run, last_run, new_run);
      ++new_run;
    }

    filenames = get_filenames(temp_dirname_);
    first_merge = false;
  }

  return true;
}

template<class T>
void Fragment::merge_sorted_runs_with_2_ids(
    const std::string& dirname, const std::vector<std::string>& filenames,
    int first_run, int last_run, int new_run) {
  // For easy reference
  int attribute_num = array_schema_->attribute_num();
  size_t cell_size = array_schema_->cell_size();
  struct stat st;

  // Information about the runs to be merged 
  int runs_num = last_run-first_run+1;
  SortedRun** runs = new SortedRun*[runs_num];
  for(int i=0; i<runs_num; ++i) {
    std::stringstream filename;
    filename << dirname << (first_run+i);
    runs[i] = new SortedRun(dirname + filenames[first_run + i], 
                            cell_size == VAR_SIZE, 
                            segment_size_);
  }

  // Information about the output merged run
  void* cell;
  char* segment = new char[segment_size_];
  size_t offset = 0;
  std::stringstream new_filename;
  new_filename << temp_dirname_ << new_run;
  int new_run_fd = open(new_filename.str().c_str(), 
                        O_WRONLY | O_CREAT | O_TRUNC | O_SYNC, S_IRWXU);
  assert(new_run_fd != -1);

  // Loop over the cells
  while((cell = get_next_cell_with_2_ids<T>(runs, runs_num, cell_size)) != 
        NULL) {
    // If the output segment is full, flush it into the output file
    if(offset + cell_size + 2*sizeof(int64_t) > segment_size_) {
      write(new_run_fd, segment, offset);
      offset = 0;
    }

    // Write cell (along with its 2 ids) to segment 
    memcpy(segment + offset, cell, cell_size + 2*sizeof(int64_t));
    offset += cell_size;
  }

  // Flush the remaining data of the output segment into the output file
  if(offset > 0) 
    write(new_run_fd, segment, offset);

  // Delete the files of the merged runs
  if(dirname == temp_dirname_)
    for(int i=0; i<runs_num; ++i) 
      remove(runs[i]->filename_.c_str());

  // Clean up
  delete [] segment;
  for(int i=0; i<runs_num; ++i)
    delete runs[i];
  delete [] runs;
  close(new_run_fd);
}

void Fragment::sort_run() {
  // For easy reference
  int dim_num = array_schema_->dim_num();
  int attribute_num = array_schema_->attribute_num();
  const std::type_info* coords_type = array_schema_->type(attribute_num);
  ArraySchema::CellOrder cell_order = array_schema_->cell_order();

  // Sort the cells
  if(cell_order == ArraySchema::CO_ROW_MAJOR) {
    if(*coords_type == typeid(int)) {
      std::sort(write_state_->cells_.begin(), write_state_->cells_.end(), 
                WriteState::SmallerRow<int>(dim_num));
    } else if(*coords_type == typeid(int64_t)) {
      std::sort(write_state_->cells_.begin(), write_state_->cells_.end(), 
                WriteState::SmallerRow<int64_t>(dim_num));
    } else if(*coords_type == typeid(float)) {
      std::sort(write_state_->cells_.begin(), write_state_->cells_.end(), 
                WriteState::SmallerRow<float>(dim_num));
    } else if(*coords_type == typeid(double)) {
      std::sort(write_state_->cells_.begin(), write_state_->cells_.end(), 
                WriteState::SmallerRow<double>(dim_num));
    }
  } else if(cell_order == ArraySchema::CO_COLUMN_MAJOR) {
    if(*coords_type == typeid(int)) {
      std::sort(write_state_->cells_.begin(), write_state_->cells_.end(), 
                WriteState::SmallerCol<int>(dim_num));
    } else if(*coords_type == typeid(int64_t)) {
      std::sort(write_state_->cells_.begin(), write_state_->cells_.end(), 
                WriteState::SmallerCol<int64_t>(dim_num));
    } else if(*coords_type == typeid(float)) {
      std::sort(write_state_->cells_.begin(), write_state_->cells_.end(), 
                WriteState::SmallerCol<float>(dim_num));
    } else if(*coords_type == typeid(double)) {
      std::sort(write_state_->cells_.begin(), write_state_->cells_.end(), 
                WriteState::SmallerCol<double>(dim_num));
    }
  }
}

void Fragment::sort_run_with_id() {
  // For easy reference
  int attribute_num = array_schema_->attribute_num();
  int dim_num = array_schema_->dim_num();
  const std::type_info* coords_type = array_schema_->type(attribute_num);
  ArraySchema::TileOrder tile_order = array_schema_->tile_order();
  ArraySchema::CellOrder cell_order = array_schema_->cell_order();

  if(tile_order == ArraySchema::TO_NONE || // Irregular + Hilbert co
     cell_order == ArraySchema::CO_ROW_MAJOR) { // Regular + row co
    if(*coords_type == typeid(int)) {
      std::sort(write_state_->cells_with_id_.begin(), 
                write_state_->cells_with_id_.end(), 
                WriteState::SmallerRowWithId<int>(dim_num));
    } else if(*coords_type == typeid(int64_t)) {
      std::sort(write_state_->cells_with_id_.begin(), 
                write_state_->cells_with_id_.end(), 
                WriteState::SmallerRowWithId<int64_t>(dim_num));
    } else if(*coords_type == typeid(float)) {
      std::sort(write_state_->cells_with_id_.begin(), 
                write_state_->cells_with_id_.end(), 
                WriteState::SmallerRowWithId<float>(dim_num));
    } else if(*coords_type == typeid(double)) {
      std::sort(write_state_->cells_with_id_.begin(), 
                write_state_->cells_with_id_.end(), 
                WriteState::SmallerRowWithId<double>(dim_num));
    }
  } else if(cell_order == ArraySchema::CO_COLUMN_MAJOR) { // Rregular + col co
    if(*coords_type == typeid(int)) {
      std::sort(write_state_->cells_with_id_.begin(), 
                write_state_->cells_with_id_.end(), 
                WriteState::SmallerColWithId<int>(dim_num));
    } else if(*coords_type == typeid(int64_t)) {
      std::sort(write_state_->cells_with_id_.begin(), 
                write_state_->cells_with_id_.end(), 
                WriteState::SmallerColWithId<int64_t>(dim_num));
    } else if(*coords_type == typeid(float)) {
      std::sort(write_state_->cells_with_id_.begin(), 
                write_state_->cells_with_id_.end(), 
                WriteState::SmallerColWithId<float>(dim_num));
    } else if(*coords_type == typeid(double)) {
      std::sort(write_state_->cells_with_id_.begin(), 
                write_state_->cells_with_id_.end(), 
                WriteState::SmallerColWithId<double>(dim_num));
    }
  }
}

void Fragment::sort_run_with_2_ids() {
  // For easy reference
  int attribute_num = array_schema_->attribute_num();
  int dim_num = array_schema_->dim_num();
  const std::type_info* coords_type = array_schema_->type(attribute_num);

  if(*coords_type == typeid(int)) {
    std::sort(write_state_->cells_with_2_ids_.begin(), 
              write_state_->cells_with_2_ids_.end(), 
              WriteState::SmallerWith2Ids<int>(dim_num));
  } else if(*coords_type == typeid(int64_t)) {
    std::sort(write_state_->cells_with_2_ids_.begin(), 
              write_state_->cells_with_2_ids_.end(), 
              WriteState::SmallerWith2Ids<int64_t>(dim_num));
  } else if(*coords_type == typeid(float)) {
    std::sort(write_state_->cells_with_2_ids_.begin(), 
              write_state_->cells_with_2_ids_.end(), 
              WriteState::SmallerWith2Ids<float>(dim_num));
  } else if(*coords_type == typeid(double)) {
    std::sort(write_state_->cells_with_2_ids_.begin(), 
              write_state_->cells_with_2_ids_.end(), 
              WriteState::SmallerWith2Ids<double>(dim_num));
  }
}

template<class T>
void Fragment::update_tile_info(
    const T* coords, int64_t tile_id, 
    const std::vector<size_t>& attr_sizes) {
  // For easy reference
  int attribute_num = array_schema_->attribute_num();
  int dim_num = array_schema_->dim_num();
  size_t coords_size = array_schema_->cell_size(attribute_num);

  // Update MBR and (potentially) the first bounding coordinate
  if(write_state_->cell_num_ == 0) {
    // Allocate space for MBR and bounding coordinates
    write_state_->mbr_ = malloc(2*array_schema_->cell_size(attribute_num));
    write_state_->bounding_coordinates_.first = 
        malloc(array_schema_->cell_size(attribute_num));
    write_state_->bounding_coordinates_.second = 
        malloc(array_schema_->cell_size(attribute_num));

    // Init MBR first bounding coordinate
    init_mbr(coords, static_cast<T*>(write_state_->mbr_), dim_num);
    memcpy(write_state_->bounding_coordinates_.first, coords, coords_size);
  } else {
    expand_mbr(coords, static_cast<T*>(write_state_->mbr_), dim_num);
  }

  // Update the second bounding coordinate, tile id, and cell number
  memcpy(write_state_->bounding_coordinates_.second, coords, coords_size);
  write_state_->tile_id_ = tile_id;
  ++write_state_->cell_num_;

  // Update file offsets
  for(int i=0; i<=attribute_num; ++i) {
    write_state_->file_offsets_[i] += attr_sizes[i];
  }
}

// ------------- BOOK-KEEPING FUNCTIONS ------------- //

void Fragment::clear_book_keeping() {
  // For easy reference
  int64_t tile_num = book_keeping_.tile_ids_.size();
 
  for(int64_t i=0; i<tile_num; ++i) {
    if(book_keeping_.bounding_coordinates_[i].first != NULL) {
      free(book_keeping_.bounding_coordinates_[i].first);
      book_keeping_.bounding_coordinates_[i].first = NULL;
    }

    if(book_keeping_.bounding_coordinates_[i].second != NULL) {
      free(book_keeping_.bounding_coordinates_[i].second);
      book_keeping_.bounding_coordinates_[i].second = NULL;
    }

    if(book_keeping_.mbrs_[i] != NULL) {
      free(book_keeping_.mbrs_[i]);
      book_keeping_.mbrs_[i] = NULL;
    }
  }

  book_keeping_.bounding_coordinates_.clear();
  book_keeping_.mbrs_.clear();
  book_keeping_.offsets_.clear(); 
  book_keeping_.tile_ids_.clear(); 
}

void Fragment::commit_book_keeping() {
  // Do nothing if the book keeping files exist
  std::string filename = workspace_ + "/" + array_schema_->array_name() + "/" + 
                         fragment_name_ + "/" +
                         BOUNDING_COORDINATES_FILENAME + 
                         BOOK_KEEPING_FILE_SUFFIX;

  if(is_file(filename)) 
    return;

  commit_bounding_coordinates();
  commit_mbrs();
  commit_offsets();
  commit_tile_ids();
}

// FILE FORMAT:
// tile#1_lower_dim#1(T) tile#1_lower_dim#2(T) ... 
// tile#1_upper_dim#1(T) tile#1_upper_dim#2(T) ... 
// tile#2_lower_dim#1(T) tile#2_lower_dim#2(T) ... 
// tile#2_upper_dim#1(T) tile#2_upper_dim#2(T) ...
// ... 
// NOTE: T is the type of the dimensions of this array
void Fragment::commit_bounding_coordinates() {
  // For easy reference
  int attribute_num = array_schema_->attribute_num();
  int64_t tile_num = book_keeping_.tile_ids_.size();
  size_t cell_size = array_schema_->cell_size(attribute_num);

  // Prepare filename
  std::string filename = workspace_ + "/" + array_schema_->array_name() + "/" + 
                         fragment_name_ + "/" +
                         BOUNDING_COORDINATES_FILENAME + 
                         BOOK_KEEPING_FILE_SUFFIX;

  // Open file
  int fd = open(filename.c_str(), O_WRONLY | O_CREAT | O_SYNC, S_IRWXU);
  assert(fd != -1);

  if(tile_num != 0) { // If the array is not empty
    // Prepare the buffer
    size_t buffer_size = 2 * tile_num * cell_size;
    char* buffer = new char[buffer_size];
 
    // Populate buffer
    size_t offset = 0;
    for(int64_t i=0; i<tile_num; ++i) {
      // Lower bounding coordinates
      memcpy(buffer+offset, 
             book_keeping_.bounding_coordinates_[i].first, cell_size);
      offset += cell_size;
      // Upper bounding coordinates
      memcpy(buffer+offset, 
             book_keeping_.bounding_coordinates_[i].second, cell_size);
      offset += cell_size;
    }

    // Write buffer and clean up
    write(fd, buffer, buffer_size);
    delete [] buffer;
  }  

  close(fd);
}

// FILE FORMAT:
// MBR#1_dim#1_low(T) MBR#1_dim#1_high(T) ...
// MBR#1_dim#2_low(T) MBR#1_dim#2_high(T) ...
// ...
// MBR#2_dim#1_low(T) MBR#2_dim#1_high(T) ...
// ...
// NOTE: T is the type of the dimensions of this array
void Fragment::commit_mbrs() {
  // For easy reference
  int attribute_num = array_schema_->attribute_num();
  int64_t tile_num = book_keeping_.tile_ids_.size();
  size_t cell_size = array_schema_->cell_size(attribute_num);

  // prepare file name
  std::string filename = workspace_ + "/" + array_schema_->array_name() + "/" + 
                         fragment_name_ + "/" +
                         MBRS_FILENAME + BOOK_KEEPING_FILE_SUFFIX;

  // Open file
  int fd = open(filename.c_str(), O_WRONLY | O_CREAT | O_SYNC, S_IRWXU);
  assert(fd != -1);

  if(tile_num != 0) { // If the array is not empty
    // Prepare the buffer
    size_t buffer_size = tile_num * 2 * cell_size;
    char* buffer = new char[buffer_size];
 
    // Populate buffer
    size_t offset = 0;
    for(int64_t i=0; i<tile_num; ++i) {
      memcpy(buffer+offset, book_keeping_.mbrs_[i], 2 * cell_size);
      offset += 2 * cell_size;
    }

    // Write buffer and clean up
    write(fd, buffer, buffer_size);
    delete [] buffer;
  }

  close(fd);
}

// FILE FORMAT:
// tile#1_of_attribute#1_offset(int64_t)
// tile#2_of_attribute#1_offset(int64_t)
// ...
// tile#1_of_attribute#2_offset(int64_t)
// tile#2_of_attribute#2_offset(int64_t)
// ...
// NOTE: Do not forget the extra coordinate attribute
void Fragment::commit_offsets() {
  // For easy reference
  int64_t tile_num = book_keeping_.tile_ids_.size();

  // Prepare file name
  std::string filename = workspace_ + "/" + array_schema_->array_name() + "/" + 
                         fragment_name_ + "/" +
                         OFFSETS_FILENAME + BOOK_KEEPING_FILE_SUFFIX;

  // Open file
  int fd = open(filename.c_str(), O_WRONLY | O_CREAT | O_SYNC, S_IRWXU);
  assert(fd != -1);

  if(tile_num != 0) { // If the array is not empty
    // Prepare the buffer
    int attribute_num = array_schema_->attribute_num();
    size_t buffer_size = (attribute_num+1) * tile_num * sizeof(int64_t);
    char* buffer = new char[buffer_size];
 
    // Populate buffer
    int64_t offset = 0;
    for(int i=0; i<=attribute_num; ++i) {
      memcpy(buffer+offset, 
             &book_keeping_.offsets_[i][0], tile_num * sizeof(int64_t));
      offset += tile_num * sizeof(int64_t);
    }

    // Write buffer and clean up
    write(fd, buffer, buffer_size);
    delete [] buffer;
  }
  
  close(fd);
}

// FILE FORMAT:
// tile_num(int64_t)
//   tile_id#1(int64_t) tile_id#2(int64_t)  ...
void Fragment::commit_tile_ids() {
  // For easy reference
  int64_t tile_num = book_keeping_.tile_ids_.size();

  // Prepare file name
  std::string filename = workspace_ + "/" + array_schema_->array_name() + "/" +
                         fragment_name_ + "/" +
                         TILE_IDS_FILENAME + BOOK_KEEPING_FILE_SUFFIX;

  // Open file
  int fd = open(filename.c_str(), O_WRONLY | O_CREAT | O_SYNC, S_IRWXU);
  assert(fd != -1);
 
  if(tile_num != 0) { // If the array is not empty
    // Prepare the buffer
    size_t buffer_size = (tile_num+1) * sizeof(int64_t);
    char* buffer = new char[buffer_size];
 
    // Populate buffer
    memcpy(buffer, &tile_num, sizeof(int64_t));
    memcpy(buffer+sizeof(int64_t), 
           &book_keeping_.tile_ids_[0], tile_num * sizeof(int64_t));

    // Write buffer and clean up
    write(fd, buffer, buffer_size);  
    delete [] buffer;
  }

  close(fd);
}

void Fragment::init_book_keeping() {
  int attribute_num = array_schema_->attribute_num();

  book_keeping_.offsets_.resize(attribute_num+1);
  for(int i=0; i<=attribute_num; ++i)
    book_keeping_.offsets_[i].push_back(0);
}

void Fragment::load_book_keeping() {
  load_tile_ids();
  load_bounding_coordinates();
  load_mbrs();
  load_offsets();
}

// FILE FORMAT:
// tile#1_lower_dim#1(T) tile#1_lower_dim#2(T) ... 
// tile#1_upper_dim#1(T) tile#1_upper_dim#2(T) ... 
// tile#2_lower_dim#1(T) tile#2_lower_dim#2(T) ... 
// tile#2_upper_dim#1(T) tile#2_upper_dim#2(T) ...
// ... 
// NOTE: T is the type of the dimensions of this array
void Fragment::load_bounding_coordinates() {
  // For easy reference
  int attribute_num = array_schema_->attribute_num();
  int64_t tile_num = book_keeping_.tile_ids_.size();
  assert(tile_num != 0);
  size_t cell_size = array_schema_->cell_size(attribute_num);
 
  // Open file
  std::string filename = workspace_ + "/" + array_schema_->array_name() + "/" +
                         fragment_name_ + "/" +
                         BOUNDING_COORDINATES_FILENAME + 
                         BOOK_KEEPING_FILE_SUFFIX;
  int fd = open(filename.c_str(), O_RDONLY);
  assert(fd != -1);

  // Initializations
  struct stat st;
  fstat(fd, &st);
  size_t buffer_size = st.st_size;
  assert(buffer_size == tile_num * 2 * cell_size);
  char* buffer = new char[buffer_size];
  read(fd, buffer, buffer_size);
  int64_t offset = 0;
  book_keeping_.bounding_coordinates_.resize(tile_num);
  void* coord;

  // Load bounding coordinates
  for(int64_t i=0; i<tile_num; ++i) {
    coord = malloc(cell_size);
    memcpy(coord, buffer + offset, cell_size);
    book_keeping_.bounding_coordinates_[i].first = coord; 
    offset += cell_size;
    coord = malloc(cell_size);
    memcpy(coord, buffer + offset, cell_size);
    book_keeping_.bounding_coordinates_[i].second = coord; 
    offset += cell_size;
  }

  // Clean up
  delete [] buffer;
  close(fd);
}

// FILE FORMAT:
// MBR#1_dim#1_low(T) MBR#1_dim#1_high(T) ...
// MBR#1_dim#2_low(T) MBR#1_dim#2_high(T) ...
// ...
// MBR#2_dim#1_low(T) MBR#2_dim#1_high(T) ...
// ...
// NOTE: T is the type of the dimensions of this array
void Fragment::load_mbrs() {
  // For easy reference
  int attribute_num = array_schema_->attribute_num();
  size_t cell_size = array_schema_->cell_size(attribute_num);
  int64_t tile_num = book_keeping_.tile_ids_.size();
  assert(tile_num != 0);
 
  // Open file
  std::string filename = workspace_ + "/" + array_schema_->array_name() + "/" +
                         fragment_name_ + "/" +
                         MBRS_FILENAME + BOOK_KEEPING_FILE_SUFFIX;
  int fd = open(filename.c_str(), O_RDONLY);
  assert(fd != -1);

  // Initializations
  struct stat st;
  fstat(fd, &st);
  size_t buffer_size = st.st_size;
  assert(buffer_size == tile_num * 2 * cell_size);
  char* buffer = new char[buffer_size];
  read(fd, buffer, buffer_size);
  size_t offset = 0;
  book_keeping_.mbrs_.resize(tile_num);
  void* mbr;

  // Load MBRs
  for(int64_t i=0; i<tile_num; ++i) {
    mbr = malloc(2 * cell_size);
    memcpy(mbr, buffer + offset, 2 * cell_size);
    book_keeping_.mbrs_[i] = mbr;
    offset += 2 * cell_size;
  }

  // Clean up
  delete [] buffer;
  close(fd);
}

// FILE FORMAT:
// tile#1_of_attribute#1_offset(int64_t)
// tile#2_of_attribute#1_offset(int64_t)
// ...
// tile#1_of_attribute#2_offset(int64_t)
// tile#2_of_attribute#2_offset(int64_t)
// ...
// NOTE: Do not forget the extra coordinate attribute
void Fragment::load_offsets() {
  // For easy reference
  int attribute_num = array_schema_->attribute_num();
  int64_t tile_num = book_keeping_.tile_ids_.size();
  assert(tile_num != 0);
 
  // Open file
  std::string filename = workspace_ + "/" + array_schema_->array_name() + "/" +
                         fragment_name_ + "/" +
                         OFFSETS_FILENAME + BOOK_KEEPING_FILE_SUFFIX;
  int fd = open(filename.c_str(), O_RDONLY);
  assert(fd != -1);

  // Initializations
  struct stat st;
  fstat(fd, &st);
  size_t buffer_size = st.st_size;
  assert(buffer_size == (attribute_num+1)*tile_num*sizeof(int64_t));
  char* buffer = new char[buffer_size];
  read(fd, buffer, buffer_size);
  int64_t offset = 0;

  // Load offsets
  book_keeping_.offsets_.resize(attribute_num+1);
  for(int i=0; i<=attribute_num; ++i) {
    book_keeping_.offsets_[i].resize(tile_num);
    memcpy(&book_keeping_.offsets_[i][0], buffer + offset, 
           tile_num*sizeof(int64_t));
    offset += tile_num * sizeof(int64_t);
  }

  // Clean up
  delete [] buffer;
  close(fd);
}

// FILE FORMAT:
// tile_num(int64_t)
//   tile_id#1(int64_t) tile_id#2(int64_t)  ...
void Fragment::load_tile_ids() {
  // Open file
  std::string filename = workspace_ + "/" + array_schema_->array_name() + "/" +
                         fragment_name_ + "/" +
                         TILE_IDS_FILENAME + BOOK_KEEPING_FILE_SUFFIX;
  int fd = open(filename.c_str(), O_RDONLY);
  assert(fd != -1);

  // Initializations
  struct stat st;
  fstat(fd, &st);
  size_t buffer_size = st.st_size;

  if(buffer_size == 0) // Empty array
    return;

  assert(buffer_size > sizeof(int64_t));
  char* buffer = new char[buffer_size];
  read(fd, buffer, buffer_size);
  int64_t tile_num;
  memcpy(&tile_num, buffer, sizeof(int64_t));
  assert(buffer_size == (tile_num+1)*sizeof(int64_t));
  book_keeping_.tile_ids_.resize(tile_num);

  // Load tile_ids
  memcpy(&book_keeping_.tile_ids_[0], 
         buffer+sizeof(int64_t), tile_num*sizeof(int64_t));

  // Clean up
  delete [] buffer;
  close(fd);
}

