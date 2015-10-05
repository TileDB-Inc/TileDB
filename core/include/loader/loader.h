/**
 * @file   loader.h
 * @author Stavros Papadopoulos <stavrosp@csail.mit.edu>
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2015 Stavros Papadopoulos <stavrosp@csail.mit.edu>
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
#include "special_values.h"
#include "storage_manager.h"

/** 
 * The Loader creates TileDB data from raw data, interfacing with the 
 * StorageManager. 
 *
 * For better understanding of this class, some useful information is 
 * summarized below:
 *
 * - **Workspace** <br>
 *   This is the main place where the arrays persist on the disk. It is
 *   implemented as a directory in the underlying file system. 
 * - **Group** <br>
 *   Groups enable hierarchical organization of the arrays. They are 
 *   implemented as sub-directories inside the workspace directory. Even
 *   the workspace directory is regarded as a group (i.e., the root
 *   group of all groups in the workspace). Note that a group path inserted
 *   by the user is translated with respect to the workspace, i.e., all
 *   home ("~/"), current ("./") and root ("/") refer to the workspace.
 *   For instance, if the user gives "W1" as a workspace, and "~/G1" as
 *   a group, then the directory in which the array directory will be
 *   stored is "W1/G1".  
 * - **Canonicalized absolute workspace/group paths** <br>
 *   Most of the functions of this class take as arguments a workspace and
 *   a group path. These paths may be given in relative format (e.g., "W1") and 
 *   potentially including strings like "../". The canonicalized
 *   absolute format of a path is an absolute path that does not contain 
 *   "../" or mulitplicities of slashes. Moreover, the canonicalized absolute
 *   format of the group is the *full* path of the group in the disk. For
 *   instance, suppose the current working directory is "/stavros/TileDB",
 *   and the user provided "W1" as the workspace, and "~/G1/G2/../" as the 
 *   group. The canonicalized absolute path of the workspace is 
 *   "/stavros/TileDB/W1" and that of the group is 
 *   "/stavros/TileDB/W1/G2". Most functions take an extra argument called
 *   *real_path* or *real_paths*, which indicates whether the input workspace
 *   and group path(s) are already in canonicalized absolute (i.e., real) 
 *   format, so that the function avoids redundant conversions. Finally, note 
 *   that an empty ("") workspace refers to the current working directory, 
 *   whereas an empty group refers to the default workspace group.
 * - <b>%Array</b> <br>
 *   A TileDB array. All the data of the array are stored in a directory 
 *   named after the array, which is placed in a certain group inside
 *   a workspace. See class Array for more information.
 * - <b>%Array descriptor</b> <br> 
 *   When an array is opened, an array descriptor is returned. This 
 *   descriptor is used in all subsequent operations with this array.
 * - <b>%Array schema</b> <br>
 *   An array consists of *dimensions* and *attributes*. The dimensions have a
 *   specific domain that orients the *coordinates* of the array cells. The
 *   attributes and coordinates have potentially different data types. Each
 *   array specifies a *global cell order*. This determines the order in which
 *   the cells are stored on the disk. See ArraySchema for more information.
 */
class Loader {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** 
   * Simple constructor. The storage manager is the module the loader 
   * interefaces with.
   */
  Loader(StorageManager* storage_manager);

  /**
   * Checks if the constructor of the object was executed successfully.
   * Always check this function after creating a Loader object.
   *
   * @return *True* for successfull creation and *false* otherwise. 
   */
  bool created_successfully() const;

  /** 
   * Finalizes a Loader object. Always execute this function before deleting
   * a Loader object (otherwise a warning will be printed in the destructor,
   * if compiled in VERBOSE mode). 
   *
   * @return **0** for success and <b>-1</b> for error.
   */
  int finalize();

  /** Deestructor. */
  ~Loader();

  /* ********************************* */
  /*          LOADING FUNCTIONS        */
  /* ********************************* */

  /** 
   * Loads a collection of CSV or binary files into an array. For more details
   * on array load, check tiledb_array_load().
   *
   * @param workspace The workspace where the array is defined.
   * @param group The group inside the workspace where the array is defined.
   * @param array_name The name of the array.
   * @param path The path to a CSV/binary file or to a directory of CSV/binary 
   * files. If it is a file, then this single file will be loaded. If it is a 
   * directory, **all** the files in the directory will be loaded.  
   * @param format It can be one of the following: 
   * - **csv** (CSV format)
   * - **sorted.csv** (sorted CSV format)
   * - **csv.gz** (GZIP-compressed CSV format)
   * - **sorted.csv.gz** (sorted GZIP-compressed CSV format)
   * - **bin** (binary format)
   * - **sorted.bin** (sorted binary format)
   * - **bin.gz** (GZIP-compressed binary format)
   * - **sorted.bin.gz** (sorted GZIP-compressed binary format)
   * @param delimiter This is meaningful only for CSV format. It stands for the 
   * delimiter which separates the values in a CSV line in the CSV file. If not 
   * given, the default is CSV_DELIMITER. The delimiter is ignored in the case
   * of loading binary data.    
   * @param update If *false* (default), then the array will be cleared if it
   * exists, and then the input files will be loaded. If *true*, then if the 
   * array already contains some data, these data are not lost. In this case the
   * input files correspond to **updates**. See tiledb_array_update() for more
   * details on array updates. 
   * @param real_paths *True* if all the workspace, group and file paths are in
   * canonicalized absolute from, and *false* otherwise. 
   * @return **0** for success and <b>-1</b> for error.
   * @note All the files in a directory path must be of the **same** format.
   */
  int array_load(
      const std::string& workspace, 
      const std::string& group, 
      const std::string& array_name, 
      const std::string& path,
      const std::string& format,
      char delimiter = CSV_DELIMITER,
      bool update = false,
      bool real_paths = false) const;

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** *True* if the constructor succeeded, or *false* otherwise. */
  bool created_successfully_;
  /** *True* if the object was finalized, or *false* otherwise. */
  bool finalized_;
  /** The storage manager object the loader interfaces with. */
  StorageManager* storage_manager_;
 
  /* ********************************* */
  /*          PRIVATE METHODS          */
  /* ********************************* */

  /** 
   * Loads a binary file collection into an array. 
   *
   * @tparam T The array coordinates type.
   * @param ad The array descriptor.
   * @param path The path to a CSV/binary file or to a directory of CSV/binary 
   * files. If it is a file, then this single file will be loaded. If it is a 
   * directory, **all** the files in the directory will be loaded.  
   * @param sorted *True* if the cells in each file are sorted on the global
   * cell order of the array, and *false* otherwise.
   * @param compression The type of compression of the file(s) to be loaded.
   * @return **0** for success and <b>-1</b> for error.
   */
  template<class T>
  int array_load_bin(
      int ad, 
      const std::string& path,
      bool sorted,
      CompressionType compression) const;

  /** 
   * Loads a CSV file collection into an array. 
   *
   * @tparam T This is the array coordinates type.
   * @param ad The array descriptor.
   * @param path The path to a CSV/binary file or to a directory of CSV/binary 
   * files. If it is a file, then this single file will be loaded. If it is a 
   * directory, **all** the files in the directory will be loaded.  
   * @param sorted *True* if the cells in each file are sorted on the global
   * cell order of the array, and *false* otherwise.
   * @param compression The type of compression of the file(s) to be loaded.
   * @param delimiter This is meaningful only for CSV format. It stands for the 
   * delimiter which separates the values in a CSV line in the CSV file. If not 
   * given, the default is CSV_DELIMITER.
   * @return **0** for success and <b>-1</b> for error.
   */
  template<class T>
  int array_load_csv(
      int ad, 
      const std::string& path,
      bool sorted,
      CompressionType compression,
      char delimiter = CSV_DELIMITER) const;
};

#endif
