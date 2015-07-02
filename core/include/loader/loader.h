/**
 * @file   loader.h
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
 * This file defines class Loader.
 */

#ifndef LOADER_H
#define LOADER_H

#include "array_schema.h"
#include "csv_line.h"
#include "storage_manager.h"
#include "tiledb_error.h"

/** The Loader creates an array from raw data. */
class Loader {
 public:
  // CONSTRUCTORS AND DESTRUCTORS
  /** 
   * Simple constructor. The storage manager is the module the loader 
   * interefaces with.
   */
  Loader(StorageManager* storage_manager, const std::string& workspace);
  /** Empty destructor. */
  ~Loader();

  // ACCESSORS
  /**
   * Returns the error code. It is 0 upon success.
   */
  int err() const;

  // LOADING FUNCTIONS
  /** Loads a binary file collection into an array. */
  int load_bin(
      const std::string& array_name, 
      const std::string& path,
      bool sorted) const;

  /** Loads a CSV file collection into an array. */
  int load_csv(
      const std::string& array_name, 
      const std::string& path,
      bool sorted) const;

  /** Loads a binary file collection into an array (creating a new fragment). */
  int update_bin(
      const std::string& array_name, 
      const std::string& path,
      bool sorted) const;

  /** Loads a CSV file collection into an array (creating a new fragment). */
  int update_csv(
      const std::string& array_name, 
      const std::string& path,
      bool sorted) const;

  /** Loads a binary file into an array. */
  int load_bin(
      const std::string& filename, const std::string& array_name) const;
  /** Loads a CSV file into an array. */
  int load_csv(
      const std::string& array_name, 
      const std::string& filename) const;
  /** Loads a sorted BIN file into an array. */
  int load_sorted_bin(
      const std::string& filename, const std::string& array_name) const;
  /** 
   * Updates an existing array with a new CSV file. 
   * This is similar to Loader::load_csv, with the differnce being
   * that Loader::load_csv opens the array in write mode (i.e., it
   * destroys the previous contents of the array), whereas 
   * Loader::update_csv incorporates the updates of the input CSV
   * file into a potentially existing array.
   */
  int update_csv(
      const std::string& filename, const std::string& array_name, 
      std::string& err_msg) const;

 private:
  // PRIVATE ATTRIBUTES
  /** The StorageManager object the loader interfaces with. */
  StorageManager* storage_manager_;
  /** Error code. Equal to 0 on success. */
  int err_;
  /** Max memory size of the write state when loading data. */
  size_t write_state_max_size_;
  /** Directory that stores the loader's data. */
  std::string workspace_;
 
  // PRIVATE METHODS
  /** 
   * Retrieves an attribute value from the CSV line and puts it into the
   * input cell at the input offset. val_num is the number of values in the
   * attribute cell. If val_num is equal to VAR_SIZE, then this attribute
   * receives a variable number of values per cell. The function may expand the
   * cell buffer, if it fills up (updating the cell_size). It also updates the
   * offset in the cell buffer. 
   */
  template<class T>
  bool append_attribute(
      CSVLine& csv_line, int val_num, void* cell, size_t& offset) const;
  /** 
   * Retrieves a set of coordinates from the CSV line and puts them into the
   * input cell.
   */
  template<class T>
  bool append_coordinates(CSVLine& csv_line, void* cell, int dim_num) const;
  /** 
   * Applicable only to variable-sized attribute cells. It calculates
   * the size needed to create a cell from a CSV line. Returns -1
   * on error.
   */
  ssize_t calculate_cell_size(CSVLine& csv_line, 
                             const ArraySchema* array_schema) const;
  /** 
   * Treats the input CSV line as a logical cell, retrieves from it
   * the coordinates and attribute values, and places them into
   * the input cell in binary form (first the coordinates, then the
   * attribute values in the order of their appearence in the input
   * array schema). 
   */
  template<class T>
  bool csv_line_to_cell(const ArraySchema* array_schema, 
                        CSVLine& csv_line, void* cell, 
                        ssize_t cell_size) const;

  /** Loads a binary file collection into an array. */
  template<class T>
  int load_bin(
      int ad, 
      const std::string& path,
      bool sorted) const;

  /** Loads a CSV file collection into an array. */
  template<class T>
  int load_csv(
      int ad, 
      const std::string& path,
      bool sorted) const;

  /** Loads a CSV file into a new fragment for the input array descriptor. */
  int load_bin(const std::string& filename, int ad) const;
  /** Loads a CSV file into a new fragment for the input array descriptor. */
  template<class T>
  int load_bin(const std::string& filename, int ad) const;
  /** Loads a CSV file into a new fragment for the input array descriptor. */
  int load_csv(int ad, const std::string& filename) const;
  /** Loads a CSV file into a new fragment for the input array descriptor. */
  template<class T>
  int load_csv(int ad, const std::string& filename) const;
  /** 
   * Loads a sorted BIN file into a new fragment for the input array descriptor.
   */
  int load_sorted_bin(const std::string& filename, int ad) const;
  /** 
   * Loads a sorted BIN file into a new fragment for the input array descriptor.
   */
  template<class T>
  int load_sorted_bin(const std::string& filename, int ad) const;
  /** Simply sets the workspace. */
  void set_workspace(const std::string& path);
};

#endif
