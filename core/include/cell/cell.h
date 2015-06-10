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
#include <map>

class CellConstAttrIterator;

/**
 * Objects of this class store a logical cell in binary form, and allow the easy
 * retrieval of its coordinates and attribute values.
 */
class Cell {
 public:
  // CONSTRUCTORS AND DESTRUCTORS
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
       bool random_access = false);
  /** Empty destructor. */
  ~Cell() {}

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
  /** True to allow efficient accessing of cell attributes in random order. */
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

#endif
