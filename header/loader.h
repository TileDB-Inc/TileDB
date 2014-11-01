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
 * This file defines class Loader. It also defines LoaderException, which is 
 * thrown by Loader.
 */

#ifndef LOADER_H
#define LOADER_H

#include "array_schema.h"
#include "csv_file.h"
#include "storage_manager.h"

/** The buffer size to be used by Linux sort, invoked by the Loader. */
#define LD_SORT_BUFFER_SIZE 2 // 2GB
/** The maximum size of a physical tile in the case of irregular tiles. */
#define LD_MAX_TILE_SIZE 1000000 // 1MB 

/**
 * The Loader is the module that creates the array layout from raw data 
 * presented in a CSV format. It can load data into arrays with both 
 * regular and irregular tiles, supporting various cell and tile orders.
 */
class Loader {
 public:
  /** 
   * The cell order dictates the way the cells are stored within the tiles
   * and across tiles (i.e., practically it also determines their order
   * in the files that store the tiles). For irregular tiles, it is the
   * order in which the cells are sorted prior to forming the tiles. For
   * regular tiles, it is the order in which the tiles are stored on the
   * disk.
   */
  enum Order {ROW_MAJOR, COLUMN_MAJOR, HILBERT};

  // CONSTRUCTORS AND DESTRUCTORS
  /** 
   * Takes as input the StorageManager object the Loader will be interfacing
   * with, as well as the tile size assigned to Loader::tile_size_.
   */
  Loader(const std::string& workspace, 
         StorageManager& storage_manager, 
         uint64_t tile_size = LD_MAX_TILE_SIZE);
  ~Loader() {}

  // MUTATORS
  /** Simple setter for Loader::tile_size_. */
  void set_tile_size(uint64_t tile_size) { tile_size_ = tile_size; }

  // LOADING FUNCTIONS
  /**
   * Loads a CSV file into an array.
   * \param filename The name of the input CSV file.
   * \param array_schema The schema of the array the CSV file is loaded into.
   * \param order The order that will determine the tile formation (in the case 
   * of irregular tiles) and storage (for both regular and irregular tiles).
   */
  void load(const std::string& filename, 
            const ArraySchema& array_schema, 
            Order order) const; 

 private:
  // PRIVATE ATTRIBUTES
  /** The StorageManager object the Loader will be interfacing with. */
  StorageManager& storage_manager_;
  /** The maximum size of a physical tile in the case of irregular tiles. */
  uint64_t tile_size_;
  /** Is a folder in the disk where the loader creates all its data. */
  std::string workspace_;
 
  // PRIVATE METHODS
  /**
   * Retrieves the next attribute value from the CSV line and appends it
   * to the tile.
   */
  template<class T>
  void append_attribute_value(CSVLine* csv_line, Tile* tile) const;
  /** 
   * Treats the CSV line as a logical cell encompassing coordinates and
   * attribute values, and appends the coordinates to a coordinate tile
   * and the attribute values to the respective attribute tiles. 
   */
  void append_cell(const ArraySchema& array_schema, 
                    CSVLine* csv_line, Tile** tiles) const;
  /**
   * Retrieves the next set of coordinates from the CSV line and appends it
   * to the tile.
   */ 
  template<class T>
  void append_coordinates(unsigned int dim_num, 
                               CSVLine* csv_line, Tile* tile) const;
  /** Necessary checks when invoking the load command. */
  void check_on_load(const std::string& filename) const;
  /** Creates the workspace folder. */
  void create_workspace() const;
  /** 
   * Creates an array of new tile pointers with the input tile id, 
   * and based on the input array schema. 
   */
  void init_tiles(const ArraySchema& array_schema, 
                  uint64_t tile_id, Tile** tiles) const;
  /**
   * Preprocesses the CSV file to prepare it for loading (applies only to
   * regular tiles with any order and irregular tiles with Hilbert order. 
   */
  void inject_ids_to_csv_file(const std::string& filename, 
                              const std::string& injected_filename,
                              const ArraySchema& array_schema,
                              Order order) const;
  /** Creates the (irregular) tiles and sends them to the storage manager. */
  void make_tiles_irregular(const std::string& filename,
                            const ArraySchema& array_schema,
                            Order order) const;
  /** Creates the (regular) tiles and sends them to the storage manager. */
  void make_tiles_regular(const std::string& filename,
                          const ArraySchema& array_schema) const;
  /** Simply sets the workspace. */
  void set_workspace(const std::string& workspace);
  /** 
   * Sorts the csv file depending on the type of tiles and order. 
   */
  void sort_csv_file(const std::string& to_be_sorted_filename,
                     const std::string& sorted_filename,
                     const ArraySchema& array_schema,
                     Order order) const;
  /** Sends the tiles to the storage manager. */
  void store_tiles(const ArraySchema& array_schema, Tile** tiles) const;
};


/** This exception is thrown by Loader. */
class LoaderException {
 public:
  // CONSTRUCTORS & DESTRUCTORS
  /** Takes as input the exception message. */
  LoaderException(const std::string& msg) 
      : msg_(msg), array_name_("") {}
  /** 
   * Takes as input the exception message and the name of the array 
   * where the exception occured. 
   */
  LoaderException(const std::string& msg, const std::string& array_name) 
      : msg_(msg), array_name_(array_name) {}
  ~LoaderException() {}

  // ACCESSORS
  /** Returns the exception message. */
  const std::string& what() const { return msg_; }
  /** Returns the array name where the exception occured. */
  const std::string& where() const { return array_name_; }

 private:
  /** The exception message. */
  std::string msg_;
  /** The array name where the exception occured. */
  std::string array_name_;
};

#endif
