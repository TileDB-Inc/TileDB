/**
 * @file   write_state.cc
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
 * This file implements the WriteState class.
 */

#include "write_state.h"

/******************************************************
************* CONSTRUCTORS & DESTRUCTORS **************
******************************************************/

WriteState::WriteState(
    const ArraySchema* array_schema, 
    size_t write_state_max_size) 
    : array_schema_(array_schema),
      write_state_max_size_(write_state_max_size) {
  // For easy reference
  int attribute_num = array_schema_->attribute_num();

  tile_id_ = INVALID_TILE_ID;
  cell_num_ = 0;
  run_offset_ = 0;
  run_size_ = 0;
  runs_num_ = 0;

  mbr_ = NULL;
  bounding_coordinates_.first = NULL; 
  bounding_coordinates_.second = NULL;

  segments_.resize(attribute_num+1);
  segment_utilization_.resize(attribute_num+1);
  file_offsets_.resize(attribute_num+1);
  for(int i=0; i<= attribute_num; ++i) {
    segments_[i] = malloc(segment_size_); 
    segment_utilization_[i] = 0;
    file_offsets_[i] = 0;
  } 
}









template<class T>
void WriteState::write_cell(
    const void* input_cell, size_t cell_size, 
    const ArraySchema* array_schema) {
  // For easy reference
  int dim_num = array_schema->dim_num();

  // Copy the input cell
  void* cell = malloc(cell_size);
  memcpy(cell, input_cell, cell_size);

  // Write each logical cell to the array
  if(array_schema->has_irregular_tiles()) { // Irregular tiles
    if(array_schema->cell_order() == ArraySchema::CO_ROW_MAJOR ||
       array_schema->cell_order() == ArraySchema::CO_COLUMN_MAJOR) {
      Cell new_cell;
      new_cell.cell_ = cell;
      write_cell(new_cell, cell_size); 
    } else { // array_schema->cell_order() == ArraySchema::CO_HILBERT
      CellWithId new_cell;
      new_cell.cell_ = cell;
      new_cell.id_ = 
          array_schema->cell_id_hilbert<T>(static_cast<const T*>(cell));
      write_cell(new_cell, cell_size); 
    }
  } else { // Regular tiles
    if(array_schema->tile_order() == ArraySchema::TO_ROW_MAJOR) { 
      if(array_schema->cell_order() == ArraySchema::CO_ROW_MAJOR ||
         array_schema->cell_order() == ArraySchema::CO_COLUMN_MAJOR) {
        CellWithId new_cell;
        new_cell.cell_ = cell;
        new_cell.id_ = array_schema->tile_id_row_major(cell);
        write_cell(new_cell, cell_size); 
      } else { // array_schema->cell_order() == ArraySchema::CO_HILBERT) {
        CellWith2Ids new_cell;
        new_cell.cell_ = cell;
        new_cell.tile_id_ = array_schema->tile_id_row_major(cell);
        new_cell.cell_id_ = 
          array_schema->cell_id_hilbert<T>(static_cast<const T*>(cell));
        write_cell(new_cell, cell_size); 
      }
    } else if(array_schema->tile_order() == ArraySchema::TO_COLUMN_MAJOR) { 
      if(array_schema->cell_order() == ArraySchema::CO_ROW_MAJOR ||
         array_schema->cell_order() == ArraySchema::CO_COLUMN_MAJOR) {
        CellWithId new_cell;
        new_cell.cell_ = cell;
        new_cell.id_ = array_schema->tile_id_column_major(cell);
        write_cell(new_cell, cell_size); 
      } else { // array_schema->cell_order() == ArraySchema::CO_HILBERT) {
        CellWith2Ids new_cell;
        new_cell.cell_ = cell;
        new_cell.tile_id_ = array_schema->tile_id_column_major(cell);
        new_cell.cell_id_ = 
          array_schema->cell_id_hilbert<T>(static_cast<const T*>(cell));
        write_cell(new_cell, cell_size); 
      }
    } else if(array_schema->tile_order() == ArraySchema::TO_HILBERT) { 
      if(array_schema->cell_order() == ArraySchema::CO_ROW_MAJOR ||
         array_schema->cell_order() == ArraySchema::CO_COLUMN_MAJOR) {
        CellWithId new_cell;
        new_cell.cell_ = cell;
        new_cell.id_ = array_schema->tile_id_hilbert(cell);
        write_cell(new_cell, cell_size); 
      } else { // array_schema->cell_order() == ArraySchema::CO_HILBERT) {
        CellWith2Ids new_cell;
        new_cell.cell_ = cell;
        new_cell.tile_id_ = array_schema->tile_id_hilbert(cell);
        new_cell.cell_id_ = 
          array_schema->cell_id_hilbert<T>(static_cast<const T*>(cell));
        write_cell(new_cell, cell_size); 
      }
    } 
  }
}

void WriteState::write_cell(const Cell& cell, size_t cell_size) {

  size_t size_cost = sizeof(Cell) + cell_size;

  if(run_size_ + size_cost > write_state_max_size_) {
    sort_run();
    flush_sorted_run();
  }

  cells_.push_back(cell);
  run_size_ += size_cost;
}

void WriteState::write_cell(const CellWithId& cell, size_t cell_size) {
  size_t size_cost = sizeof(CellWithId) + cell_size;

  if(run_size_ + size_cost > write_state_max_size_) {
    sort_run_with_id();
    flush_sorted_run_with_id();
  }

  cells_with_id_.push_back(cell);
  run_size_ += size_cost;
}

void WriteState::write_cell(const CellWith2Ids& cell, size_t cell_size) {
  size_t size_cost = sizeof(CellWith2Ids) + cell_size;

  if(run_size_ + size_cost > write_state_max_size_) {
    sort_run_with_2_ids();
    flush_sorted_run_with_2_ids();
  }

  cells_with_2_ids_.push_back(cell);
  run_size_ += size_cost;
}

