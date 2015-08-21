/**
 * @file   csv_line.h
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
 * This file defines class CSVLine. Each CSV line is comprised of text
 * segments (values) separated by a special character (',' by default).
 */

#ifndef CSV_LINE_H
#define CSV_LINE_H

#include <string>
#include <vector>

// DEFINITIONS
/** This character starts a comment CSV line (treated as an integral string). */
#define CSV_COMMENT '#'
/** 
 * Determines a default initial number of values for a CSV line, which is used
 * in initial memory allocation when the CSV line is created in READ mode
 * (see CSVLine::Mode).
 */
#define CSV_INITIAL_VAL_NUM 40
/** Initial byte allocation for a CSV line in WRITE mode. */
#define CSV_INITIAL_LINE_SIZE 1000
/** The maximum digits of a number appended to a CSV line. */
#define CSV_MAX_DIGITS 50
/** 
 * The separator in the CSV line. 
 * 
 * **Note:** Currently, this class does not handle the case where the CSV line 
 * contains the separator as an actual character in a string value, even when 
 * inserted with escape character '\'.
 */
#define CSV_SEPARATOR ','

/** 
 * This class implements a CSV line, which is comprised of text segments
 * (values) separated by a special character (',' by default). 
 * A CSV line is the atomic unit of storage in a CSVFile object. Note that a 
 * line that starts with CSV_COMMENT. A NULL_VALUE indicates a mising
 * (NULL) value, whereas a DEL_VALUE indicates a deletion.
 *
 * A CSV line has practically two modes of operation: READ and WRITE. In 
 * either case, it functions as an input or output stream respectively.
 * Specifically, the CSVLine object maintains the position of the next
 * value to be retrieved, or the offset where the next value will be
 * written.
 */
class CSVLine {
 public:
  // TYPE DEFINITIONS
  /**
   * A CSVLine line can be created in *read*, *write* or *none* (i.e., 
   * unspecified) mode. If in *read* mode, writing functions cannot be used,
   * and vice-versa.
   */
  enum Mode {READ, WRITE, NONE};

  // CONSTRUCTORS & DESTRUCTORS
  /** Simple constructor. The object is created in NONE mode. */
  CSVLine();
  /** 
   * A simple constructor that takes as input a CSV line as a string. The 
   * CSVLine object is created in READ mode. 
   *
   * **Note:** The input line is going to be modified and then stored. 
   * Specifically, all the CSV_SEPARATOR characters in the input will be 
   * substituted by '\0'. This is important for performance purposes, in order 
   * to avoid extra copying costs of the input by the constructor. 
   */
  explicit CSVLine(char* line);
  /** Destructor. De-allocates only the space allocated in WRITE mode. */
  ~CSVLine();
 
  // ACCESSORS
  /** Returns the stored CSV line as a string. */
  const char* c_str() const; 
  /** Returns the size of the line string. */
  size_t strlen() const; 
  /** Returns the number of values in the CSV line. */
  int val_num() const;
  /** Returns the CSV line values as a vector of strings. */
  std::vector<std::string> values_str_vec() const;

  // MUTATORS
  /** Clears the CSV line. */
  void clear();
  /** Resets the position to the next value (to be read) to zero. */
  void reset();

  // OPERATORS
  /** Returns the currently pointed value as a string. */
  const char* operator*() const;
  /** Increments the position to the next value to be read. */
  void operator++();
  /** Increments the position to the next value to be read by step. */
  void operator+=(int step);
  /** Appends a string value to the CSV line. */
  void operator<<(const std::string& value);
  /** Appends a string value to the CSV line. */
  void operator<<(const char* value);
  /** Appends the input CSV line to the CSV line object. */
  void operator<<(const CSVLine& value);
  /** Appends a value to the CSV line.  */
  template<class T> 
  void operator<<(T value);
  /** Retrieves the next value from the CSV line. */
  template<class T> 
  bool operator>>(T& value);
  /** Substitutes the current CSV line with the input line. */
  void operator=(char* line);

 private:
  // PRIVATE ATTRIBUTES
  /** The CSV line string. */
  void* line_;
  /** Allocated size of the line string in bytes (for WRITE mode only).*/
  size_t line_allocated_size_;
  /** 
   * Actual size of the line string in bytes (including last '\0' character). 
   */
  size_t line_size_;
  /** Mode of the CSV line */
  Mode mode_;
  /** 
   * The offsets in CSVLine::line_ pointing to the beginning of each CSV value.
   * Used only in READ mode.
   */
  std::vector<size_t> offsets_;
  /** Allocated space for the CSVLine::offsets_ vector. */
  size_t offsets_allocated_size_;
  /** 
   * The current position in the CSV line (pointing to a value). It is used when
   * reading from the CSV line with operator>>().
   */
  int pos_;
  /** Number of values in the CSV line. */
  int val_num_;

  // PRIVATE METHODS
  /** Appends the input value to the CSV line, which is of 'size' bytes. */
  void append(const char* value, size_t size);
  /** 
   * Tokenizes the CSV line string. This practically means that CSV_SEPARATOR
   * characters are replaced with '\0', and the CSVLine::offsets_ vector
   * is populated.
   */
  void tokenize();
};

#endif
