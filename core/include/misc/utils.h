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

#include <cstdlib>
#include <set>
#include <string>
#include <vector>
#include <typeinfo>

std::string parent_folder(const std::string& name);

std::string get_file_format(const std::string& file);

/** 
 * Returns true if the name is valid (in POSIX format) and false
 * false otherwise.
 */
bool name_is_valid(const char* name);

/**
 * Checks if the directory specified by the input path contains only binary
 * files, telling only by the file names.  
 * @param path The input directory path.
 * @return *True* if the input directory contains only files with suffix
 * ".bin" or ".bin.gz". In any other case (also if the directory contains
 * other sub-directories), the function returns *false*.
 */
bool is_bin_collection(const std::string& path);

/**
 * Checks if the directory specified by the input path contains only CSV
 * files, telling only by the file names.  
 * @param path The input directory path.
 * @return *True* if the input directory contains only files with suffix
 * ".csv" or ".csv.gz". In any other case (also if the directory contains
 * other sub-directories), the function returns *false*.
 */
bool is_csv_collection(const std::string& path);


/** Returns true if string `value` ends with string `ending`. */
bool ends_with(const std::string& value, const std::string& ending);

/** 
 * Checks if a string starts with a certain prefix.
 * @param value The base string.
 * @param prefix The prefix string to be tested.
 * @return *True* if *value* starts with the *prefix* and *false* otherwise. 
 */
bool starts_with(const std::string& value, const std::string& prefix);

/**
 * It takes as input an **absolute** path, and returns it in its conicalized
 * form, after appropriately replacing "./" and "../" in the path.
 * @param path The input path passed by reference, which will be modified
 * by the function to hold the canonicalized absolute path. Note that the path 
 * must be absolute, otherwise the function fails. In case of error (e.g., if 
 * "../" are not properly used in *path*, or if *path* is not absolute),
 * the function sets the empty string (i.e., "") to *path*.
 */
void path_purge_dots(std::string& path);

/**
 * Checks if both inputs represent the '/' character. This is an auxiliary
 * function to adjacent_slashes_dedup().
 * @param a The first input.
 * @param b The second input.
 * @return *True* if *a* = *b* = '/', and *false* otherwise. 
 */
bool both_slashes(char a, char b);

/**  
 * Deduplicates adjacent '/' characters in the input.
 * @param value The string to be deduped.
 * @return void. 
 */
void adjacent_slashes_dedup(std::string& value);

/** 
 * Returns the directory where the program is executed. 
 * @param dirname It will store the current directory name after the execution
 * of the function.
 * @return A code:
 * - **0**: Success
 * - <b>-1</b>: Failure. If VERBOSE is defined, then an error message will be
 *   output in stderr.
 */
std::string current_dirname();

/** 
 * Returns the canonicalized absolute path, after appropriately replacing
 * "~", "./", "../", removing multiplicities of '/', etc. It is also extended to
 * take as arguments arbitrary current, home and root directories with respect
 * to which it will replace "~", "./", etc. If no such directories are
 * provided, default ones will be used, which are the actual current,
 * home and root directories of the underlying file system.
 *
 * **Examples** 
 * - If no arguments are given except for *path*, and if the *current*
 *   working directory is "/stavros/TileDB", and the *home* directory is 
 *   "/stavros", then the default value for *current* is "/stavros/TileDB", for
 *   *home* it is "/stavros", and for *root* it is "/". If *path* is equal to
 *   "~", then it will be converted to "/stavros", if it is equal to 
 *   "./W1" it will be converted to "stavros/TileDB/W1", and if it is equal
 *   to "../../" it will be converted to "/".
 * - Suppose now that the function takes parameters 
 *   *home* = *root* = *current* = "/stavros/TileDB/W1". Then, if the *path* is
 *   equal to "~/G1", it will be converted to "/stavros/TileDB/W1/G1",
 *   if it is equal to "/G2", it will be converted to "/stavros/TileDB/W1/G2",
 *   and if it is equal to "./G3", it will be converted to 
 *   "/stavros/TileDB/W1/G3".
 *
 * @param path The input path to be converted. If it is the empty string (""),
 * the current directory will be returned.
 * @param current The current directory (**must** be canonicalized absolute).
 * @param home The home directory (**must** be canonicalized absolute).
 * @param root The root directory (**must** be canonicalized absolute).
 * @return The canonicalized absolute path of the input path. In case of an
 * error (e.g., if "../" not used properly), it returns an empty string ("").
 */
std::string real_path(
    const std::string& path,
    const std::string& current = current_dirname(),
    const std::string& home = getenv("HOME"),
    const std::string& root = std::string("/"));

/** Converts the values of a into the data type of b and stores them in b. */
template<class T>
void convert(const double* a, T* b, int size);

/** 
 * TODO: mention that it sets errno
 * Creates a path of directories. If any directory in  the path does not exist,
 * the function creates it. If the direcrtory exists, the function
 * does nothing (but still succeeds). If any directory cannot be
 * created, the function fails, and all the directories it managed to create
 * are deleted.
 * @param path The path of directories to be created.
 * @return A code:
 * - **0**\n
 *   Success
 * - <b>-1</b>\n
 *   Error
 * @note The path must be in canonicalized absolute form, otherwise the outcome
 * will be unpredictable.
 */
int path_create(const std::string& path);

/** 
 * Creates a directory. Note that the path where the directory will be
 * stored must exists, otherwise the function fails. If the directory
 * exists, the function does nothing and terminates sucessfully.
 * @param dirname The name of the directory to be created.
 * @param real_path *True* if *dirname* is in canonicalized absolute form,  
 * and *false* otherwise.
 * @return A return code:
 * @return A code:
 * - **0**\n
 *   Success
 * - <b>-1</b>\n
 *   Error
 */
int directory_create(const std::string& dirname, bool real_path = false);

/** 
 * Deletes a directory (along with its files). 
 * **Note:** It does not work recursively for nested directories. In fact, it
 * will fail if the directory contains sub directories.
 * @param dirname The directory to be deleted.
 * @param *True* if *dirname* is in canonicalized absolute form, and *false*
 * otherwise.
 * @return A code:
 * - **0**\n
 *   Success
 * - <b>-1</b>\n
 *   Error
 */
int directory_delete(const std::string& dirname, bool real_path = false);

/** Returns true if there are duplicates in the input vector. */
template<class T>
bool duplicates(const std::vector<T>& v);

/** True if the directory is empty or not existent. */
bool directory_is_empty(const std::string& dirname); 

/** Expands the input MBR with the input coordinates. */
void expand_mbr(const void* coords, void* mbr,
                const std::type_info* type, int dim_num);

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

/** 
 * GZIPs the input buffer and stores the result in the output buffer,
 * of maximum size avail_out. It also stores the compressed data size 
 * into out_size.
 */
void gzip(unsigned char* in, size_t in_size, 
          unsigned char* out, size_t avail_out, size_t& out_size);

/** 
 * Decompresses the GZIPed input buffer and stores the result in the output 
 * buffer, of maximum size avail_out. It also stores the decompressed data 
 * size into out_size.
 */
void gunzip(unsigned char* in, size_t in_size, 
            unsigned char* out, size_t avail_out, size_t& out_size);


/** Initializes the input MBR with the input coordinates. */
void init_mbr(const void* coords, void*& mbr,
              const std::type_info* type, int dim_num);

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

/** 
 * Checks if the input is an existing directory. 
 * @param path The path to be checked.
 * @param real_path *True* if the path is in canonicalized absolute form, and
 * *false* otherwise.
 * @return *True* if *path* is an existing directory, and *false* otherwise.
 */ 
bool is_dir(const std::string& path, bool real_path = false);

/** Returns true if the input is an existing file. */ 
bool is_file(const std::string& path, bool real_path = false);

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
