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

#ifndef __UTILS_H__
#define __UTILS_H__

/* ********************************* */
/*             CONSTANTS            */
/* ********************************* */

// Return codes
#define TILEDB_UT_OK     0
#define TILEDB_UT_ERR   -1

#include <string>
#include <vector>

/**  
 * Deduplicates adjacent '/' characters in the input.
 *
 * @param value The string to be deduped.
 * @return void. 
 */
void adjacent_slashes_dedup(std::string& value);

/**
 * Checks if both inputs represent the '/' character. This is an auxiliary
 * function to adjacent_slashes_dedup().
 *
 * @param a The first input.
 * @param b The second input.
 * @return *True* if *a* == *b* == '/', and *false* otherwise. 
 */
bool both_slashes(char a, char b);


/**
 * Creates a new directory.
 *
 * @param dir The name of the directory to be created.
 * @return TILEDB_UT_OK upon success, and TILEDB_UT_ERR upon failure. 
 */
int create_dir(const std::string& dir);

/** 
 * Returns the directory where the program is executed. 
 *
 * @return The directory where the program is executed.
 */
std::string current_dir();

/** 
 * GZIPs the input buffer and stores the result in the output buffer, returning
 * the size of compressed data. 
 *
 * @param in The input buffer.
 * @param in_size The size of the input buffer.
 * @param out The output buffer.
 * @param avail_out_size The available size in the output buffer.
 * @return The size of compressed data.
 */
ssize_t gzip(
    unsigned char* in, 
    size_t in_size, 
    unsigned char* out, 
    size_t out_size);

/** Returns true if there are duplicates in the input vector. */
template<class T>
bool has_duplicates(const std::vector<T>& v);

/** Returns true if the input vectors have common elements. */
template<class T>
bool intersect(const std::vector<T>& v1, const std::vector<T>& v2);

/** 
 * Checks if the input is an existing directory. 
 *
 * @param dir The directory to be checked.
 * @return *True* if *dir* is an existing directory, and *false* otherwise.
 */ 
bool is_dir(const std::string& dir);

/** 
 * Checks if the input is an existing file. 
 *
 * @param file The file to be checked.
 * @return *True* if *file* is an existing file, and *false* otherwise.
 */ 
bool is_file(const std::string& file);

/** Returns true if the input string is a positive (>0) integer number. */
bool is_positive_integer(const char* s);

/** 
 * Returns the parent directory of the input directory. 
 *
 * @param dir The input directory.
 * @return The parent directory of the input directory.
 */
std::string parent_dir(const std::string& dir);

/**
 * It takes as input an **absolute** path, and returns it in its conicalized
 * form, after appropriately replacing "./" and "../" in the path.
 *
 * @param path The input path passed by reference, which will be modified
 *     by the function to hold the canonicalized absolute path. Note that the
 *     path must be absolute, otherwise the function fails. In case of error
 *     (e.g., if "../" are not properly used in *path*, or if *path* is not
 *     absolute), the function sets the empty string (i.e., "") to *path*.
 * @return void
 */
void purge_dots_from_path(std::string& path);

/**
 * Returns the absolute canonicalized directory path of the input directory.
 *
 * @param The input directory to be canonicalized.
 * @return The absolute canonicalized directory path of the input directory.
 */
std::string real_dir(const std::string& dir);

/** 
 * Checks if a string starts with a certain prefix.
 *
 * @param value The base string.
 * @param prefix The prefix string to be tested.
 * @return *True* if *value* starts with the *prefix* and *false* otherwise. 
 */
bool starts_with(const std::string& value, const std::string& prefix);


/** 
 * Write the input buffer to a file.
 * 
 * @param filename The name of the file.
 * @param buffer The input buffer.
 * @param buffer_size The size of the input buffer.
 * @return 0 on success, and -1 on error.
 */
int write_to_file(
    const char* filename,
    const void* buffer, 
    size_t buffer_size);

#endif
