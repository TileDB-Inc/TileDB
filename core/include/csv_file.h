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
 * This file defines classes CSVLine and CSVFile. A CSV (comma-separated
 * values) file consists of a set of (text) CSV lines. Each such line is 
 * comprised of text segments (values) separated by a comma character (',').
 */

#ifndef CSV_FILE_H
#define CSV_FILE_H

#include <string>
#include <vector>
#include <inttypes.h>
#include <limits>

/** The maximum digits of a number appended to a CSV line. */
#define CSV_MAX_DIGITS 50
/** 
 * The segment size determines the amount of data that can be exchanged
 * between the CSV file (in hard disk) and the main memory in one I/O operation.
 * Unless otherwise defined, this default size is used. 
 */
#define CSV_SEGMENT_SIZE 10000000 // 10 MB
/** Deleted char. */
#define CSV_DEL_CHAR 127
/** Deleted int. */
#define CSV_DEL_INT std::numeric_limits<int>::min()
/** Deleted int64_t. */
#define CSV_DEL_INT64_T std::numeric_limits<int64_t>::min()
/** Deleted float. */
#define CSV_DEL_FLOAT std::numeric_limits<float>::min()
/** Deleted double. */
#define CSV_DEL_DOUBLE std::numeric_limits<double>::min()
/** Missing char. */
#define CSV_NULL_CHAR '\0'
/** Missing int. */
#define CSV_NULL_INT std::numeric_limits<int>::max()
/** Missing int64_t. */
#define CSV_NULL_INT64_T std::numeric_limits<int64_t>::max()
/** Missing uint64_t. */
#define CSV_NULL_UINT64_T std::numeric_limits<uint64_t>::max()
/** Missing float. */
#define CSV_NULL_FLOAT std::numeric_limits<float>::max()
/** Missing double. */
#define CSV_NULL_DOUBLE std::numeric_limits<double>::max()
/** The symbol indicating a deleted value. */
#define CSV_DEL_VALUE "$"
/** The symbol indicating a missing (NULL) value. */
#define CSV_NULL_VALUE "*"

/** 
 * This class implements a CSV line, which is comprised of text segments
 * (values) separated by a comma character (','). A CSV line is the atomic
 * unit of storage in a CSVFile object. Note that a line that starts with
 * '#' is a comment line. A CSV_NULL_VALUE indicates a mising (NULL) value,
 * whereas a CSV_DEL_VALUE indicates a deletion. 
 */
class CSVLine {
 public:
  // CONSTRUCTORS & DESTRUCTORS
  /** Simple constructor. */
  CSVLine() { pos_ = 0; }
  /** 
   * A simple constructor that takes as input a CSV line as a string, and 
   * tokenizes it into values inserted into CSVLine::values_ (see also
   * CSVLine::tokenize).  
   */
  explicit CSVLine(const std::string& line);
  /** Empty destructor. */
  ~CSVLine() {}
 
  // ACCESSORS
  /** 
    * Returns the CSV line as a string of comma-separate values. To do so, it 
    * puts together the elements of CSVLine::values_, separating them with the 
    * comma (',') character.
    */
  std::string str() const;

  // MUTATORS
  /** 
   * Clears the CSV line (i.e., clears CSVLine::values_ 
   * and resets CSVLine::pos_). 
   */
  void clear();

  // MISC
  /** Returns true if the input represents a deleted value. */
  template<class T>
  static bool is_del(T v);
  /** Returns true if the input represents a NULL value. */
  template<class T>
  static bool is_null(T v);

  // OPERATORS
  /** Appends a string value to the CSV line, which is properly tokenized. */
  void operator<<(const std::string& value);
  /** Appends the input CSV line to the CSV line object. */
  void operator<<(const CSVLine& value);
  /** 
   * Appends a value to the CSV line. If the value is a string, it tokenizes 
   * it (see CSVLine::tokenize). The line is treated as an output stream.
   */
  template<class T> 
  void operator<<(T value);
  /** 
   * Appends a vector of values to the CSV line. The line is treated as an 
   * output stream.
   */
  template<class T> 
  void operator<<(const std::vector<T>& values);
  /** 
   * Retrieves the next value from the CSV line. The line is treated as an input
   * stream. 
   */
  template<class T> 
  bool operator>>(T& value);
  /** 
   * Clears CSVLine::values_, tokenizes the input string, and inserts the new
   * values into CSVLine::values_. 
   */
  void operator=(const std::string& value);
  /** 
   * Clears CSVLine::values_ and copies into it the contents of the input
   * CSV line. 
   */
  void operator=(const CSVLine& value);
  /** 
   * Clears CSVLine::values_ and inserts the new value in CSVLine::values_. 
   * If the value is a string, it tokenizes it (see CSVLine::tokenize). 
   */
  template<class T> 
  void operator=(T value);
  /** 
   * Clears CSVLine::values_ and inserts the new values in CSVLine::values_. 
   */
  template<class T> 
  void operator=(const std::vector<T>& values);

 private:
  // PRIVATE ATTRIBUTES
  /** 
   * The current position (index) in CSVLine::values_ for reading, when 
   * using CSVLine::operator>>.
   */
  int pos_;
  /** 
   * Internally, the line is modeled as a vector of values (the ','
   * characters are not explicitly stored). 
   */
  std::vector<std::string> values_;

  // PRIVATE METHODS
  /** 
   * Tokenizes a line into values that are inserted into CSVLine::values_,
   * using ',' as the delimiter.  
   */
  void tokenize(std::string line);
};

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
  /** Destructor. */
  ~CSVFile();
 
  // METHODS
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

 private:
  // PRIVATE ATTRIBUTES
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
   * "r": Read mode
   *
   * "w": Write mode
   *
   * "a": Append mode
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
