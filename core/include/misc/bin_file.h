/**
 * @file   bin_file.h
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
 * This file defines class BINFile, which implements a simple binary file. */

#ifndef BIN_FILE_H
#define BIN_FILE_H

#include "array_schema.h"
#include "cell.h"
#include <string>

/** 
 * The segment size determines the amount of data that can be exchanged
 * between the BIN file (in hard disk) and the main memory in one I/O operation.
 * Unless otherwise defined, this default size is used. 
 */
#define BIN_SEGMENT_SIZE 10000000 // ~10 MB

/** Initial size for a variable cell. */
#define BIN_INITIAL_VAR_CELL_SIZE 4000

/**
 * This class implements a simple BIN file with basic operations such as
 * getting reading and writing of binary data. 
 */
class BINFile {
 public:
  // CONSTRUCTORS & DESTRUCTORS
  /** Constructor. */
  BINFile();
  /** Constructor. */
  BINFile(const ArraySchema* array_schema, int id_num = 0);
  /** Constructor. */
  BINFile(const std::string& filename, const char* mode);
  /** Destructor. */
  ~BINFile();
 
  // MUTATORS
  /** Closes the BIN file. Returns 0 on success and -1 on failure. */
  int close();
  /** Opens the BIN file in the input mode (see BINFile::mode_). */
  int open(const std::string& filename, const char* mode, 
           size_t segment_size = BIN_SEGMENT_SIZE);
 
  // READ/WRITE
  /** 
   * Reads binary data from the file and into destination, whose amount is
   * specified by the input size. Returns the number of bytes read.
   */
  ssize_t read(void* destination, size_t size);
  /** 
   * Writes data from the source into the file, whose amount is specified
   * by the input size.
   */
  ssize_t write(const void* source, size_t data);

  // OPERATORS
  /** Retrieves the next cell from the file.  */
  bool operator>>(Cell& cell);
  /** Writes a cell to the file.  */
  bool operator<<(const Cell& cell);

 private:
  // PRIVATE ATTRIBUTES
  /** Important for variable cells. */
  size_t allocated_cell_size_;
  /** The array schema. */
  const ArraySchema* array_schema_;
  /** 
   * The buffer will temporarily store the data, before they are written to
   * the file on the disk (in WRITE/APPEND mode) or when segments are read
   * from the disk (in READ mode).
   */
  char* buffer_;
  /** 
    * The position AFTER the last useful byte in buffer.
    */
  size_t buffer_end_;
  /** 
    * A pointer to the current position (for reading or writing) in the buffer.
    */
  size_t buffer_offset_;
  /** Part of the read state: stores the current cell to be read/written. */
  void* cell_;
  /** Part of the read state: stores the cell size. */
  ssize_t cell_size_;
  /** 
   * Part of the read state: stores the current coordinates to be 
   * read/written. 
   */
  void* coords_;
  /** The coordinates size. */
  size_t coords_size_;
  /** 
    * A pointer to the current position in the file where the NEXT read will 
    * take place (used only by BINFile::read in READ mode).
    */
  int64_t file_offset_;
    /** The name of the BIN file. */
  std::string filename_;
  /** Number of ids in each cell stored in the file. */
  int id_num_;
  /** Stores the ids of a cell. */
  void* ids_;
  /** 
   * The mode of the BIN file. There are three modes available:
   *
   * "r": READ mode
   *
   * "w": WRITE mode
   *
   * "a": APPEND mode
   *
   */
  char mode_[2];
  /** 
    * Determines the amount of data exchanged in an I/O operation between the 
    * disk and the main memory.
    */	
  size_t segment_size_;
  /** True if the cells are variable-sized. */
  bool var_size_;

  // PRIVATE METHODS
  /** 
   * Writes the content of the buffer to the end of the file on the disk. 
   * Returns the number of bytes flushed into the file.
   */
  ssize_t flush_buffer();
  /** Initialization. */
  void init();
  /** 
   * Reads a segment of binary data with size BINFile::segment_size_. 
   * Returns the number of bytes read into the segment.
   */
  ssize_t read_segment();
};

#endif
