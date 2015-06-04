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
#include "csv_file.h"
#include <map>

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
  /** Returns the cell payload. */
  const void* cell() const { return cell_; }
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
  /** Returns true if the values for the input attribute are variable-sized. */
  bool var_size(int attribute_id) const;

  // MUTATORS
  void set_cell(const void* cell);

  // TYPE CONVERTER
  /** 
   * Objects of this class help in converting void* values to a certain type. 
   * They are used in conjunction with Cell::operator[].
   */
  class TypeConverter {
   public:
    // CONSTRUCTORS
    /** Constructor. */
    TypeConverter(const void* value);

    // CONVERSION OPERATORS
    /** Conversion operator. */
    template<class T>
    operator const T*(); 

   private:
    // PRIVATE ATTRIBUTES
    /** The stored value. */
    const void* value_;
  };

  // ITERATORS
  /** Constant iterator over the attribute values of the cell. */
  class const_attr_iterator {
   public:
    // CONSTRUCTORS & DESTRUCTORS
    /** 
     * Constructor. The inputs are a cell and a position. The latter is a
     * position in the attribute_ids_ list of the input cell, i.e., it
     * indicates which attribute the iterator starts on. 
     */
    const_attr_iterator(const Cell* cell, int pos);
    /** Destructor. */
    ~const_attr_iterator() {}

    // ACCESSORS
    /** Returns the id of the attribute the iterator is currently on. */
    int attribute_id() const { return cell_->attribute_ids_[pos_]; }
    /** True if the iterator has reached the end of the attributes. */
    bool end() const { return end_; }
    /** Returns the offset in the cell payload the iterator is currently on. */
    size_t offset() const { return offset_; }

    // OPERATORS
    /** Advances the iterator to the next attribute. */
    void operator++();
    /** Returns the attribute value(s). */
    TypeConverter operator*() const;

   private:
    // PRIVATE ATTRIBUTES
    /** The cell the iterator iterates over. */
    const Cell* cell_;
    /** True if the iterator has reached the end of the attributes. */
    bool end_;
    /** The offset in the cell payload the iterator is currently on. */
    size_t offset_;
    /** The position of the iterator on the attributes of the cell. */
    int pos_;
  };
  /** Returns a begin constant attribute iterator. */
  const_attr_iterator begin() const;

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
