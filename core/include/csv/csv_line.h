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
 * segments (values) separated by a comma character (',').
 */

#ifndef CSV_LINE_H
#define CSV_LINE_H

#include <string>
#include <vector>

/** The maximum digits of a number appended to a CSV line. */
#define CSV_MAX_DIGITS 50

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
  CSVLine();
  /** 
   * A simple constructor that takes as input a CSV line as a string, and 
   * tokenizes it into values inserted into CSVLine::values_ (see also
   * CSVLine::tokenize).  
   */
  explicit CSVLine(const std::string& line);
  /** Empty destructor. */
  ~CSVLine();
 
  // ACCESSORS
  /** 
    * Returns the CSV line as a string of comma-separate values. To do so, it 
    * puts together the elements of CSVLine::values_, separating them with the 
    * comma (',') character.
    */
  std::string str() const;
  /** Returns the list of values of the CSV line. */
  const std::vector<std::string>& values() const;

  // MUTATORS
  /** 
   * Clears the CSV line (i.e., clears CSVLine::values_ 
   * and resets CSVLine::pos_). 
   */
  void clear();
  /** Resets CSVLine::pos_ to zero. */
  void reset();

  // OPERATORS
  /** Simply returns the currently pointed value as a string. */
  const std::string& operator*() const;
  /** Simply increments CSVLine::pos. */
  void operator++();
  /** Simply increments CSVLine::pos by step. */
  void operator+=(int step);
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

#endif
