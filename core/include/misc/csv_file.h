/**
 * @file   csv_file.h
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
 * This file defines class CSVFile. A CSV (comma-separated
 * values) file consists of a set of (text) CSV lines. Each such line is 
 * comprised of text segments (values) separated by a comma character (',').
 */

#ifndef CSV_FILE_H
#define CSV_FILE_H

#include "array_schema.h"
#include "cell.h"
#include "csv_line.h"
#include "special_values.h"
#include <string>
#include <vector>
#include <inttypes.h>
#include <limits>

/** 
 * The segment size determines the amount of data that can be exchanged
 * between the CSV file (in hard disk) and the main memory in one I/O operation.
 * Unless otherwise defined, this default size is used. 
 */
#define CSV_SEGMENT_SIZE 10000000 // 10 MB

/** Initial size for a variable cell. */
#define CSV_INITIAL_VAR_CELL_SIZE 4000

/**
 * This class implements a simple CSV file with basic operations such as
 * getting a line from the file, or appending a line to it. It makes
 * sure that the I/Os are performed such that about CSVFile::segment_size_
 * bytes are exchanged between the main memory and the disk, so that
 * the disk seek time becomes insignificant when amortized over the 
 * data segment transfer.
 */
class CSVFile {
 public:
  // CONSTRUCTORS & DESTRUCTORS
  /** Constructor. */
  CSVFile();
  /** Constructor. */
  CSVFile(const ArraySchema* array_schema);
  /** Constructor. */
  CSVFile(const std::string& filename, const char* mode);
  /** Destructor. */
  ~CSVFile();

  // ACCESSORS
  /** Returns the number of bytes read from the file. */
  ssize_t bytes_read() const;
  /** Returns the size of the file in bytes. */
  ssize_t size() const;
 
  // MUTATORS
  /** Closes the CSV file. */
  void close();
  /** Opens the CSV file in the input mode (see CSVFile::mode_). */
  bool open(const std::string& filename, const char* mode, 
            size_t segment_size = CSV_SEGMENT_SIZE);
 
  // OPERATORS
  /** 
   * Appends a CSV line to the end of the CSV file. The CSV file is treated
   * as an output stream.
   */
  void operator<<(const CSVLine& line);
  /** 
   * Retrieves the next CSV line from the CSV file. The CSV file is treated
   * as an input stream. If a line starts with '#', it is ignored as a comment
   * line.
   */
  bool operator>>(CSVLine& line);
  /** Retrieves the next cell from the collection. */
  bool operator>>(Cell& cell);

 private:
  // PRIVATE ATTRIBUTES
  /** 
   * This is important when the cells retrieved from the CSV file are of
   * variable size.
   */
  size_t allocated_cell_size_;
  /** An array schema. */
  const ArraySchema* array_schema_; 
  /** 
   * The buffer will temporarily store the lines, before they are written to
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
  /** Stores the cell format of the lastly read CSV line. */
  void* cell_;
  /** 
    * A pointer to the current position in the file where the NEXT read will 
    * take place (used only by CSVFile::operator>> in READ mode).
    */
  int64_t file_offset_;
    /** The name of the CSV file. */
  std::string filename_;
  /** 
   * The mode of the CSV file. There are three modes available:
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

  // PRIVATE METHODS
  /** Writes the content of the buffer to the end of the file on the disk. */
  void flush_buffer();
  /** 
   * Reads a set of lines from the file, whose aggregate size is at most
   * CSVFile::segment_size_. Returns true if it could retrieve new lines
   * from the file, and false otherwise.
   */
  bool read_segment();
};

#endif
