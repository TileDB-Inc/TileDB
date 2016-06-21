/**
 * @file   utils.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2016 MIT and Intel Corporation
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

#include <mpi.h>
#include <pthread.h>
#include <string>
#include <vector>


#ifdef OPENMP
  #include <omp.h>
#endif


/* ********************************* */
/*             CONSTANTS             */
/* ********************************* */

/**@{*/
/** Return code. */
#define TILEDB_UT_OK         0
#define TILEDB_UT_ERR       -1
/**@}*/




/* ********************************* */
/*             FUNCTIONS             */
/* ********************************* */

/**  
 * Deduplicates adjacent '/' characters in the input.
 *
 * @param value The string to be deduped.
 * @return void 
 */
void adjacent_slashes_dedup(std::string& value);

/**
 * Checks if both inputs represent the '/' character. This is an auxiliary
 * function to adjacent_slashes_dedup().
 */
bool both_slashes(char a, char b);

/** 
 * Checks if the input cell is inside the input subarray. 
 *
 * @template T The type of the cell and subarray.
 * @param cell The cell to be checked.
 * @param range The subarray to be checked, expresses as [low, high] pairs
 *     along each dimension.
 * @param dim_num The number of dimensions for the cell and subarray.
 * @return *true* if the input cell is inside the input range and
 *     *false* otherwise.
 */
template<class T>
bool cell_in_subarray(const T* cell, const T* subarray, int dim_num);

/** 
 * Returns the number of cells in the input subarray (considering that the
 * subarray is dense). 
 *
 * @template T The type of the subarray.
 * @param subarray The input subarray.
 * @param dim_num The number of dimensions of the subarray.
 * @return The number of cells in the input subarray.
 */
template<class T>
int64_t cell_num_in_subarray(const T* subarray, int dim_num);

/**
 * Compares the precedence of two coordinates based on the column-major order.
 *
 * @template T The type of the input coordinates.
 * @param coords_a The first coordinates.
 * @param coords_b The second coordinates.
 * @param dim_num The number of dimensions of the coordinates.
 * @return -1 if *coords_a* precedes *coords_b*, 0 if *coords_a* and
 *     *coords_b* are equal, and +1 if *coords_a* succeeds *coords_b*.
 */
template<class T>
int cmp_col_order(
    const T* coords_a, 
    const T* coords_b, 
    int dim_num); 

/**
 * Compares the precedence of two coordinates associated with ids,
 * first on their ids (the smaller preceeds the larger) and then based 
 * on the column-major order.
 *
 * @template T The type of the input coordinates.
 * @param id_a The id of the first coordinates.
 * @param coords_a The first coordinates.
 * @param id_b The id of the second coordinates.
 * @param coords_b The second coordinates.
 * @param dim_num The number of dimensions of the coordinates.
 * @return -1 if *coords_a* precedes *coords_b*, 0 if *coords_a* and
 *     *coords_b* are equal, and +1 if *coords_a* succeeds *coords_b*.
 */
template<class T>
int cmp_col_order(
    int64_t id_a,
    const T* coords_a, 
    int64_t id_b,
    const T* coords_b, 
    int dim_num);

/**
 * Compares the precedence of two coordinates based on the row-major order.
 *
 * @template T The type of the input coordinates.
 * @param coords_a The first coordinates.
 * @param coords_b The second coordinates.
 * @param dim_num The number of dimensions of the coordinates.
 * @return -1 if *coords_a* precedes *coords_b*, 0 if *coords_a* and
 *     *coords_b* are equal, and +1 if *coords_a* succeeds *coords_b*.
 */
template<class T>
int cmp_row_order(
    const T* coords_a, 
    const T* coords_b, 
    int dim_num); 

/**
 * Compares the precedence of two coordinates associated with ids,
 * first on their ids (the smaller preceeds the larger) and then based 
 * on the row-major order.
 *
 * @template T The type of the input coordinates.
 * @param id_a The id of the first coordinates.
 * @param coords_a The first coordinates.
 * @param id_b The id of the second coordinates.
 * @param coords_b The second coordinates.
 * @param dim_num The number of dimensions of the coordinates.
 * @return -1 if *coords_a* precedes *coords_b*, 0 if *coords_a* and
 *     *coords_b* are equal, and +1 if *coords_a* succeeds *coords_b*.
 */
template<class T>
int cmp_row_order(
    int64_t id_a, 
    const T* coords_a, 
    int64_t id_b, 
    const T* coords_b, 
    int dim_num); 

/**
 * Creates a new directory.
 *
 * @param dir The name of the directory to be created.
 * @return TILEDB_UT_OK for success, and TILEDB_UT_ERR for error. 
 */
int create_dir(const std::string& dir);

/**
 * Creates a special file to indicate that the input directory is a
 * TileDB fragment.
 *
 * @param dir The name of the fragment directory where the file is created.
 * @return TILEDB_UT_OK for success, and TILEDB_UT_ERR for error. 
 */
int create_fragment_file(const std::string& dir);

/** 
 * Returns the directory where the program is executed. 
 *
 * @return The directory where the program is executed. If the program cannot
 *     retrieve the current working directory, the empty string is returned.
 */
std::string current_dir();

/**
 * Deletes a directory. Note that the directory must not contain other
 * directories, but it should only contain files.
 *
 * @param dirname The name of the directory to be deleted.
 * @return TILEDB_UT_OK for success, and TILEDB_UT_ERR for error. 
 */
int delete_dir(const std::string& dirname);

/**
 * Checks if the input is a special TileDB empty value.
 *
 * @template T The type of the input value.
 * @param value The value to be checked.
 * @return *true* if the input value is a special TileDB empty value, and 
 *     *false* otherwise.
 */
template<class T>
bool empty_value(T value);

/** 
 * Doubles the size of the buffer.
 *
 * @param buffer The buffer to be expanded. 
 * @param buffer_allocated_size The original allocated size of the buffer.
 *     After the function call, this size doubles.
 * @param return TILEDB_UT_OK for success, and TILEDB_UT_ERR for error.
 */
int expand_buffer(void*& buffer, size_t& buffer_allocated_size);

/**
 * Expands the input MBR so that it encompasses the input coordinates.
 *
 * @template T The type of the MBR and coordinates.
 * @param mbr The input MBR to be expanded.
 * @param coords The input coordinates to expand the MBR.
 * @param dim_num The number of dimensions of the MBR and coordinates.
 * @return void
 */
template<class T>
void expand_mbr(T* mbr, const T* coords, int dim_num);

/** 
 * Returns the size of the input file.
 *
 * @param filename The name of the file whose size is to be retrieved.
 * @param return The file size on success, and TILEDB_UT_ERR for error.
 */
off_t file_size(const std::string& filename);

/** Returns the names of the directories inside the input directory. */
std::vector<std::string> get_dirs(const std::string& dir);

/** Returns the names of the fragments inside the input directory. */
std::vector<std::string> get_fragment_dirs(const std::string& dir);

/** 
 * GZIPs the input buffer and stores the result in the output buffer, returning
 * the size of compressed data. 
 *
 * @param in The input buffer.
 * @param in_size The size of the input buffer.
 * @param out The output buffer.
 * @param avail_out_size The available size in the output buffer.
 * @return The size of compressed data on success, and TILEDB_UT_ERR on error.
 */
ssize_t gzip(
    unsigned char* in, 
    size_t in_size, 
    unsigned char* out, 
    size_t out_size);

/** 
 * Decompresses the GZIPed input buffer and stores the result in the output 
 * buffer, of maximum size avail_out. 
 *
 * @param in The input buffer.
 * @param in_size The size of the input buffer.
 * @param out The output buffer.
 * @param avail_out_size The available size in the output buffer.
 * @param out_size The size of the decompressed data.
 * @return TILEDB_UT_OK on success and TILEDB_UT_ERR on error.
 */
int gunzip(
    unsigned char* in, 
    size_t in_size, 
    unsigned char* out, 
    size_t avail_out, 
    size_t& out_size);

/** 
 * Checks if there are duplicates in the input vector. 
 * 
 * @template T The type of the values in the input vector.
 * @param v The input vector.
 * @return *true* if the vector has duplicates, and *false* otherwise.
 */
template<class T>
bool has_duplicates(const std::vector<T>& v);

/**
 * Checks if the input coordinates lie inside the input subarray.
 *
 * @template T The coordinates and subarray type.
 * @param coords The input coordinates.
 * @param subarray The input subarray.
 * @param dim_num The number of dimensions of the subarray.
 * @return *true* if the coordinates lie in the subarray, and *false* otherwise.
 */
template<class T>
bool inside_subarray(const T* coords, const T* subarray, int dim_num);

/** 
 * Checks if the input vectors have common elements. 
 *
 * @template T The type of the elements of the input vectors.
 * @param v1 The first input vector.
 * @param v2 The second input vector.
 * @return *true* if the input vectors have common elements, and *false*
 *     otherwise.
 */
template<class T>
bool intersect(const std::vector<T>& v1, const std::vector<T>& v2);

/**
 * Checks if the input directory is an array.
 *
 * @param dir The directory to be checked.
 * @return *true* if the directory is an array, and *false* otherwise.
 */
bool is_array(const std::string& dir);

/** 
 * Checks if the input is an existing directory. 
 *
 * @param dir The directory to be checked.
 * @return *true* if *dir* is an existing directory, and *false* otherwise.
 */ 
bool is_dir(const std::string& dir);

/** 
 * Checks if the input is an existing file. 
 *
 * @param file The file to be checked.
 * @return tTrue* if *file* is an existing file, and *false* otherwise.
 */ 
bool is_file(const std::string& file);

/**
 * Checks if the input directory is a fragment.
 *
 * @param dir The directory to be checked.
 * @return *true* if the directory is a fragment, and *false* otherwise.
 */
bool is_fragment(const std::string& dir);

/**
 * Checks if the input directory is a group.
 *
 * @param dir The directory to be checked.
 * @return *true* if the directory is a group, and *false* otherwise.
 */
bool is_group(const std::string& dir);

/**
 * Checks if the input directory is a metadata object.
 *
 * @param dir The directory to be checked.
 * @return *true* if the directory is a metadata object, and *false* otherwise.
 */
bool is_metadata(const std::string& dir);

/** Returns *true* if the input string is a positive (>0) integer number. */
bool is_positive_integer(const char* s);

/** Returns *true* if the subarray contains a single element. */
template<class T>
bool is_unary_subarray(const T* subarray, int dim_num);

/**
 * Checks if the input directory is a workspace.
 *
 * @param dir The directory to be checked.
 * @return *true* if the directory is a workspace, and *false* otherwise.
 */
bool is_workspace(const std::string& dir);

/**
 * Reads data from a file into a buffer using MPI-IO.
 *
 * @param mpi_comm The MPI communicator.
 * @param filename The name of the file.
 * @param offset The offset in the file from which the read will start.
 * @param buffer The buffer into which the data will be written.
 * @param length The size of the data to be read from the file.
 * @return TILEDB_UT_OK on success and TILEDB_UT_ERR on error.
 */
int mpi_io_read_from_file(
    const MPI_Comm* mpi_comm,
    const std::string& filaname,
    off_t offset,
    void* buffer,
    size_t length);

/** 
 * Writes the input buffer to a file using MPI-IO.
 * 
 * @param mpi_comm The MPI communicator.
 * @param filename The name of the file.
 * @param buffer The input buffer.
 * @param buffer_size The size of the input buffer.
 * @return TILEDB_UT_OK on success, and TILEDB_UT_ERR on error.
 */
int mpi_io_write_to_file(
    const MPI_Comm* mpi_comm,
    const char* filename,
    const void* buffer, 
    size_t buffer_size);

#ifdef OPENMP
/**
 * Destroys an OpenMP mutex.
 *
 * @param mtx The mutex to be destroyed.
 * @return TILEDB_UT_OK for success, and TILEDB_UT_ERR for error.
 */
int mutex_destroy(omp_lock_t* mtx);

/**
 * Initializes an OpenMP mutex.
 *
 * @param mtx The mutex to be initialized.
 * @return TILEDB_UT_OK for success, and TILEDB_UT_ERR for error.
 */
int mutex_init(omp_lock_t* mtx);

/**
 * Locks an OpenMP mutex.
 *
 * @param mtx The mutex to be locked.
 * @return TILEDB_UT_OK for success, and TILEDB_UT_ERR for error.
 */
int mutex_lock(omp_lock_t* mtx);

/**
 * Unlocks an OpenMP mutex.
 *
 * @param mtx The mutex to be unlocked.
 * @return TILEDB_UT_OK for success, and TILEDB_UT_ERR for error.
 */
int mutex_unlock(omp_lock_t* mtx);
#endif

/**
 * Destroys a pthread mutex.
 *
 * @param mtx The mutex to be destroyed.
 * @return TILEDB_UT_OK for success, and TILEDB_UT_ERR for error.
 */
int mutex_destroy(pthread_mutex_t* mtx);

/**
 * Initializes a pthread mutex.
 *
 * @param mtx The mutex to be initialized.
 * @return TILEDB_UT_OK for success, and TILEDB_UT_ERR for error.
 */
int mutex_init(pthread_mutex_t* mtx);

/**
 * Locks a pthread mutex.
 *
 * @param mtx The mutex to be locked.
 * @return TILEDB_UT_OK for success, and TILEDB_UT_ERR for error.
 */
int mutex_lock(pthread_mutex_t* mtx);

/**
 * Unlocks a pthread mutex.
 *
 * @param mtx The mutex to be unlocked.
 * @return TILEDB_UT_OK for success, and TILEDB_UT_ERR for error.
 */
int mutex_unlock(pthread_mutex_t* mtx);

/** 
 * Returns the parent directory of the input directory. 
 *
 * @param dir The input directory.
 * @return The parent directory of the input directory.
 */
std::string parent_dir(const std::string& dir);

/**
 * It takes as input an **absolute** path, and returns it in its canonicalized
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
 * Reads data from a file into a buffer.
 *
 * @param filename The name of the file.
 * @param offset The offset in the file from which the read will start.
 * @param buffer The buffer into which the data will be written.
 * @param length The size of the data to be read from the file.
 * @return TILEDB_UT_OK on success and TILEDB_UT_ERR on error.
 */
int read_from_file(
    const std::string& filaname,
    off_t offset,
    void* buffer,
    size_t length);

/**
 * Reads data from a file into a buffer, using memory map (mmap).
 *
 * @param filename The name of the file
 * @param offset The offset in the file from which the read will start.
 * @param buffer The buffer into which the data will be written.
 * @param length The size of the data to be read from the file.
 * @return TILEDB_UT_OK on success and TILEDB_UT_ERR on error.
 */
int read_from_file_with_mmap(
    const std::string& filaname,
    off_t offset,
    void* buffer,
    size_t length);

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
 * @return *true* if *value* starts with the *prefix*, and *false* otherwise. 
 */
bool starts_with(const std::string& value, const std::string& prefix);

/** 
 * Writes the input buffer to a file.
 * 
 * @param filename The name of the file.
 * @param buffer The input buffer.
 * @param buffer_size The size of the input buffer.
 * @return TILEDB_UT_OK on success, and TILEDB_UT_ERR on error.
 */
int write_to_file(
    const char* filename,
    const void* buffer, 
    size_t buffer_size);

/** 
 * Write the input buffer to a file, compressed with GZIP.
 * 
 * @param filename The name of the file.
 * @param buffer The input buffer.
 * @param buffer_size The size of the input buffer.
 * @return TILEDB_UT_OK on success, and TILEDB_UT_ERR on error.
 */
int write_to_file_cmp_gzip(
    const char* filename,
    const void* buffer, 
    size_t buffer_size);

#endif
