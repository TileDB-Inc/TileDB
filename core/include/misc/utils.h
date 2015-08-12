/**
 * @file   utils.h
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
 * This file contains useful (global) functions.
 */

#ifndef UTILS_H
#define UTILS_H

#include "array_schema.h"
#include "storage_manager.h"

/** Returns true if string `value` ends with string `ending`. */
bool ends_with(const std::string& value, const std::string& ending);

/** Replaces '~' in the input path with the corresponding absolute path. */
std::string absolute_path(const std::string& path);

/** Converts the values of a into the data type of b and stores them in b. */
template<class T>
void convert(const double* a, T* b, int size);

/** Creates a directory. Returns 0 on success and an error code on error.  */
int create_directory(const std::string& dirname);

/** 
 * Deletes a directory (along with its files). 
 * Note: It does not work recursively for nested directories.
 */
void delete_directory(const std::string& dirname);

/** Returns true if there are duplicates in the input vector. */
template<class T>
bool duplicates(const std::vector<T>& v);

/** True if the directory is empty or not existent. */
bool empty_directory(const std::string& dirname); 

/** Expands the input MBR with the input coordinates. */
void expand_mbr(const ArraySchema* array_schema, 
                const void* coords, void* mbr);

/** Expands the input MBR with the input coordinates. */
template<class T>
void expand_mbr(const T* coords, T* mbr, int dim_num);

/** Doubles the size of the buffer. The original size is given as input. */
void expand_buffer(void*& buffer, size_t size);

/** Returns the size of the input file. */
size_t file_size(const std::string& filename);

/** Returns a string storing the current date in format YYYY-MM-DD HH:MM:SS. */
std::string get_date();

/** Returns a list with the names of all files in the input directory. */
std::vector<std::string> get_filenames(const std::string& dirname);

/** Initializes the input MBR with the input coordinates. */
void init_mbr(const ArraySchema* array_schema, 
              const void* coords, void*& mbr);

/** Expands the input MBR with the input coordinates. */
template<class T>
void init_mbr(const T* coords, T* mbr, int dim_num);

/** True if the point lies inside the range. */
template<class T>
bool inside_range(const T* point, const T* range, int dim_num);

/** Returns true if the input vectors have common elements. */
template<class T>
bool intersect(const std::vector<T>& v1, const std::vector<T>& v2);

/** True if the input is a del value (i.e., represents deletion). */
template<class T>
bool is_del(T v);

/** Returns true if the input is an existing directory. */ 
bool is_dir(const std::string& dirname);

/** Returns true if the input is an existing file. */ 
bool is_file(const std::string& filename);

/** Returns true if the input string is an integer number. */
bool is_integer(const char* s);

/** Returns true if the input string is a non-negative integer number. */
bool is_non_negative_integer(const char* s);

/** Returns true if the input string is a positive (>0) integer number. */
bool is_positive_integer(const char* s);

/** True if the input is a null value. */
template<class T>
bool is_null(T v);

/** 
 * Returns true if the input string is a positive real number.
 * NOTE: scientific notation is currently not supported. 
 */
bool is_real(const char* s);

/** 
 * True if the input is a valid name, i.e., it contains only alphanumerics
 * and potentially '_'.
 */
bool is_valid_name(const char* s);

/** Appends the input message to the error log file. */
 void log_error(const std::string& err_msg);

/** Returns true if there are no duplicates in the input vector. */
template<class T>
bool no_duplicates(const std::vector<T>& v);

/** 
 * Checks the overlap between two ranges of dimensionality dim_num. 
 * Returns a pair where the first boolean indicates whether there is
 * an overlap or not, whereas the second indicates if the overlap
 * is full or not (in case the first is true).
 */
template<class T>
std::pair<bool, bool> overlap(const T* r1, const T* r2, int dim_num);

/** Returns true if the input path is an existing directory. */ 
bool path_exists(const std::string& path);

/** 
 * Returns a new vector that is deduplicated version of the input. The 
 * deduplication starts from the end of the input (i.e., in reverse).
 */
template<class T>
std::vector<T> rdedup(const std::vector<T>& v); 

/** 
 * Returns a new vector that is the sorted, deduplicated version of the input.
 */
template<class T>
std::vector<T> sort_dedup(const std::vector<T>& v); 

#endif
