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
 * It also implements CSVFileException thrown by CSVFile.
 */

#ifndef CSVFILE_H
#define CSVFILE_H

#include <string>
#include <vector>
#include <inttypes.h>

/** 
 * The segment size determines the data that can be exchanged
 * between the CSV file (in hard disk) and the main memory. Unless otherwise 
 * defined, this default size is used. 
 */
#define CSV_SEGMENT_SIZE 10000000

/** 
 * This class implements a CSV line, which is comprised of text segments
 * (values) separated by a comma character (','). A CSV line is the atomic
 * unit of storage in a CSVFile object. 
 */
class CSVLine {
 public:
  // CONSTRUCTORS & DESTRUCTORS
  CSVLine();
  /** 
   * A simple constructor that takes as input a CSV line as a string, and 
   * tokenizes it into values inserted into CSVLine::values_ (see also
   * CSVLine::tokenize).  
   */
  explicit CSVLine(const std::string& line);
  ~CSVLine();
 
  // ACCESSORS
  /** 
    * Returns the CSV line as a string of comma-separate values. To do so, it 
    * puts together the elements of CSVLine::values_, separating them with the 
    * comma (',') character.
    */
  std::string str() const;
  /** Returns a const begin iterator on CSVLine::values_. */
  std::vector<std::string>::const_iterator begin() const 
      { return values_.begin(); }
  /** Returns a begin iterator on CSVLine::values_. */
  std::vector<std::string>::iterator begin()  
      { return values_.begin(); }
  /** Returns a const end iterator on CSVLine::values_. */
  std::vector<std::string>::const_iterator end() const 
      { return values_.end(); }
  /** Returns a end iterator on CSVLine::values_. */
  std::vector<std::string>::iterator end()  
      { return values_.end(); }

  // MUTATORS
  /** 
   * Clears the CSV line (i.e., clears CSVLine::values_ 
   * and resets CSVLine::pos_). 
   */
  void clear();

  // OPERATORS 
  /** 
   * Appends a value to the CSV line. If the value is a string, it tokenizes 
   * it (see CSVLine::tokenize).
   */
  template<class T> 
  void operator<<(T value);
  /** 
   * Retrieves the next value from the CSV line. The line is treated as an input 
   * stream. 
   */
  template<class T> 
  bool operator>>(T& value);
  /** 
   * Clears CSVLine::values_ and inserts the new value in CSVLine::values_. 
   * If the value is a string, it tokenizes it (see CSVLine::tokenize). 
   */
  template<class T> 
  void operator=(T value);

 private:
  // PRIVATE ATTRIBUTES
  /** 
   * The current position (index) in CSVLine::values_ for reading, when 
   * using CSVLine::operator>>.
   */
  unsigned int pos_;
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
 *
 * It throws a CSVFileException.
 */
class CSVFile {
 public:
  /** 
    * A CSV file can be opened in READ mode (for reading lines)
    * or WRITE/APPEND mode (for appending lines to the end of the file).
    * In both WRITE/APPEND modes, for as long as the object is alive,
    * new lines are always appended at the end. The difference in WRITE
    * is that, upon initialization, if the file existed it will be deleted.
    */ 
  enum Mode {READ, WRITE, APPEND};

  // CONSTRUCTORS
  /** 
   * Simple constructor.
   * \param filename The name of the CSV file.
   * \param mode The mode of the CSV file (see CSVFile::Mode).
   * \param segment_size The segment size determines the amount of data 
   * exchanged in an I/O operation between the disk and the main memory.
   */
  CSVFile(const std::string& filename, Mode mode, 
          uint64_t segment_size = CSV_SEGMENT_SIZE);
  ~CSVFile();
  
  // OPERATORS
  /** Appends a CSV line to the end of the CSV file. */
  void operator<<(const CSVLine& line);
  /** 
   * Retrieves the next CSV line from the CSV file. The CSV file is treated
   * as an input stream.
   */
  bool operator>>(CSVLine& line);

 private:
  // PRIVATE ATTRIBUTES
  /** 
   * The buffer will temporarily store the lines, before they are written to
   * the file on the disk.
   */
  char* buffer_;
  /** 
    * The position AFTER the last useful byte in buffer.
    */
  uint64_t buffer_end_;
  /** 
    * A pointer to the current position (for reading or writing) in the buffer.
    */
  uint64_t buffer_offset_;
  /** 
    * A pointer to the current position in the file where the NEXT read will 
    * take place (used only by CSVFile::operator>> in READ MODE).
    */
  uint64_t file_offset_;
    /** The name of the CSV file (full path) */
  std::string filename_;
  /** The mode of the CSV file (see CSVFile::Mode). */
  Mode mode_;	
  /** 
    * Determines the amount of data exchanged in an I/O operation between the 
    * disk and the main memory.
    */	
  uint64_t segment_size_;

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

/** This exception is thrown by CSVFile. */
class CSVFileException {
 public:
  // CONSTRUCTORS
  /** 
    * Takes as input the exception message and the name of the CSV file 
    * where the exception occured. 
    */
  CSVFileException(const std::string& msg, const std::string& filename) 
      : msg_(msg), filename_(filename) {}
  ~CSVFileException() {}

  // ACCESSORS
  /** Returns the exception message. */
  const std::string& what() const { return msg_; }
  /** Returns the name of the CSV file where the exception occured. */
  const std::string& where() const { return filename_; }

 private:
  /** The exception message. */
  std::string msg_;
  /** The name of the CSV file where the exception occured. */
  std::string filename_;
};

#endif
