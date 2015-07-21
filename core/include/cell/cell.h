/**
 * @file   cell.h
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
 * This file defines class Cell.
 */

#ifndef CELL_H
#define CELL_H

#include "array_schema.h"
#include "cell_const_attr_iterator.h"
#include "csv_line.h"
#include "type_converter.h"
#include <assert.h>
#include <map>
#include <string.h>

class CellConstAttrIterator;

/**
 * Objects of this class store a logical cell in binary form, and allow the easy
 * retrieval of its coordinates and attribute values.
 */
class Cell {
 public:
  template<typename T> struct Precedes;
  template<typename T> struct Succeeds;

  // CONSTRUCTORS AND DESTRUCTORS
  /**  
   * Constructor. The number of ids indicates how many id values precede the
   * cell actual payload.
   */
  Cell(const ArraySchema* array_schema, 
       int id_num = 0,
       bool random_access = false);
  /** 
   * Constructor. It takes as input the schema of the array the cell belongs to.
   * Also the number of ids indicates how many id values precede the cell actual
   * payload.
   */
  Cell(const void* cell,
       const ArraySchema* array_schema,
       int id_num = 0,
       bool random_access = false);
  /** 
   * Constructor. It takes as input the schema of the array the cell belongs to,
   * and a list of ids of the attributes it contains (with the coordinates
   * always being the last of them). Note that the list of ids must be ordered
   * in the same manner as the attribute values appear in the cell, which is
   * the same as that specified by the array schema (although they may be any
   * subset of them). The only important difference is that, inside the cell
   * payload (i.e., Cell::cell_), the coordinates always appear first (although
   * their corresponding attribute id appears always last in the input attribute
   * ids vector). The last arguments indicates whether the attributes of the
   * cell will be accessed in a random manner (as opposed to be simply iterated
   * on via an attribute iterator. 
   */
  Cell(const ArraySchema* array_schema,
       const std::vector<int>& attribute_ids,
       int id_num = 0,
       bool random_access = false);
  /** Empty destructor. */
  ~Cell();

  // ACCESSORS
  /** The array schema. */
  const ArraySchema* array_schema() const;
  /** Returns the i-th attribute id in the cell. */
  int attribute_id(int i) const;
  /** The number of attributes in the cell. */
  int attribute_num() const;
  /** Returns a begin constant attribute iterator. */
  CellConstAttrIterator begin() const;
  /** Returns the cell payload. */
  const void* cell() const;
  /** Returns the cell size. */
  ssize_t cell_size() const;
  /** 
   * Returns a CSV line with the values of the coordinates and attributes,
   * whose ids are specified in the input vectors. The template is on the
   * coordinates type. Note that the attribute ids should NOT contain
   * the coordinates id (i.e., m+1 if there are m attributes). This is
   * because this function must make a clear distinction between 
   * coordinates and attributes, and always print the coordinates first.
   */ 
  template<class T>
  CSVLine csv_line(
      const std::vector<int>& dim_ids,
      const std::vector<int>& attribute_ids) const;
  /** Returns the i-th id of the cell (if it exists). */
  int64_t id(int i) const;
  /** Returns the size of all ids. */
  size_t ids_size() const;
  /** Returns the number of values stored for the input attribute. */
  int val_num(int attribute_id) const; 
  /** Returns true if the entire cell is of variable size. */
  bool var_size() const;
  /** Returns true if the values for the input attribute are variable-sized. */
  bool var_size(int attribute_id) const;

  // MUTATORS
  void set_cell(const void* cell);

  // OPERATORS
  /** 
   * Returns the beginning of the attribute values for the given id. 
   * Example (supposing cell is of type Cell): 
   *
   * const int* v = cell[2];
   *
   * This will cause the returned TypeConverter to properly assign the
   * start of the values for the third attribute in variable v.
   */
  TypeConverter operator [](int attribute_id) const;

 private:
  // PRIVATE ATTRIBUTES
  /** 
   * The schema of the array the cell belongs to. Necessary for the sound
   * retrieval of the cell coordinates and attribute values.
   */
  const ArraySchema* array_schema_;
  /** The attribute ids that the cell payload contains values for. */
  std::vector<int> attribute_ids_;
  /** Stores the offset of values in the cell payload for an attribute. */
  std::map<int, size_t> attribute_offsets_;
  /** The payload of the cell. */
  const void* cell_;
  /** The cell size (could be VAR_SIZE). */
  ssize_t cell_size_;
  /** Number of ids that prece the actual cell payload. */
  int id_num_;
  /** True if operator [] is to be used for random access of cell elements. */
  bool random_access_;
  /** Stores the number of values for an attribute. */
  std::map<int, int> val_num_;
  /** True if the size of the cell is variable. */
  bool var_size_;

  // PRIVATE METHODS
  /** Appends an attribute value to the input CSV line. */
  template<class T>
  void append_attribute(int attribute_id, CSVLine& csv_line) const;
  /** 
   * Appends the string stored in the attribute with the input id to the input
   * CSV line.
   */
  void append_string(int attribute_id, CSVLine& csv_line) const;
  /** Finds the attribute offsets in the cell payload. */
  void init_attribute_offsets();
  /** 
   * Finds the number of values per attribute for the case of
   * variable-size cells.
   */
  void init_val_num();
};

/** Wrapper of comparison function for sorting cells. */
template<typename T>
struct Cell::Precedes {
  /** Constructor. */
  Precedes() { }

  /** Comparison operator. */
  bool operator () (const Cell* a, 
                    const Cell* b) {
    assert(a->id_num_ == b->id_num_);

    size_t offset = 0;

    // Check tile id
    if(a->id_num_ == 2) {
      int64_t a_tile_id, b_tile_id;
      memcpy(&a_tile_id, a->cell_, sizeof(int64_t));
      memcpy(&b_tile_id, b->cell_, sizeof(int64_t));

      if(a_tile_id < b_tile_id)
        return true;
      if(a_tile_id > b_tile_id)
        return false;

      offset += sizeof(int64_t);
    }

    // Check cell id
    if(a->id_num_ > 0) {
      int64_t a_cell_id, b_cell_id;
      memcpy(&a_cell_id, 
             static_cast<const char*>(a->cell_) + offset, 
             sizeof(int64_t));
      memcpy(&b_cell_id, 
             static_cast<const char*>(b->cell_) + offset, 
             sizeof(int64_t));

      if(a_cell_id < b_cell_id)
        return true;
      if(a_cell_id > b_cell_id)
        return false;

      offset += sizeof(int64_t);
    }

    // a.tile_id_ == b.tile_id_ && 
    // a.cell_id_ == b.cell_id_ 
    const void* temp_a = static_cast<const char*>(a->cell_) + offset;
    const void* temp_b = static_cast<const char*>(b->cell_) + offset;
    const T* coords_a = static_cast<const T*>(temp_a);
    const T* coords_b = static_cast<const T*>(temp_b);

    return a->array_schema_->precedes(coords_a, coords_b);
  }
};

/** Wrapper of comparison function for sorting cells. */
template<typename T>
struct Cell::Succeeds {
  /** Constructor. */
  Succeeds() { }

  /** Comparison operator. */
  bool operator () (const std::pair<const Cell*, int>& a, 
                    const std::pair<const Cell*, int>& b) {
    assert(a.first->id_num_ == b.first->id_num_);

    size_t offset = 0;

    // Check tile id
    if(a.first->id_num_ == 2) {
      int64_t a_tile_id, b_tile_id;
      memcpy(&a_tile_id, a.first->cell_, sizeof(int64_t));
      memcpy(&b_tile_id, b.first->cell_, sizeof(int64_t));

      if(a_tile_id > b_tile_id)
        return true;
      if(a_tile_id < b_tile_id)
        return false;

      offset += sizeof(int64_t);
    }

    // Check cell id
    if(a.first->id_num_ > 0) {
      int64_t a_cell_id, b_cell_id;
      memcpy(&a_cell_id, 
             static_cast<const char*>(a.first->cell_) + offset, 
             sizeof(int64_t));
      memcpy(&b_cell_id, 
             static_cast<const char*>(b.first->cell_) + offset, 
             sizeof(int64_t));

      if(a_cell_id > b_cell_id)
        return true;
      if(a_cell_id < b_cell_id)
        return false;

      offset += sizeof(int64_t);
    }

    // a.tile_id_ == b.tile_id_ && 
    // a.cell_id_ == b.cell_id_ 
    const void* temp_a = static_cast<const char*>(a.first->cell_) + offset;
    const void* temp_b = static_cast<const char*>(b.first->cell_) + offset;
    const T* coords_a = static_cast<const T*>(temp_a);
    const T* coords_b = static_cast<const T*>(temp_b);

    return a.first->array_schema_->succeeds(coords_a, coords_b);
  }
};

#endif
