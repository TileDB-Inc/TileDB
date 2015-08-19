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

  /** Simply sets the workspace. */
  void set_workspace(const std::string& path);
};

#endif
