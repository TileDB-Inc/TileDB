/**
 * @file   array.h
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
 * This file defines class Array. 
 */

#ifndef __ARRAY_H__
#define __ARRAY_H__

#include "aio_request.h"
#include "array_read_state.h"
#include "array_sorted_read_state.h"
#include "array_sorted_write_state.h"
#include "array_schema.h"
#include "book_keeping.h"
#include "config.h"
#include "constants.h"
#include "fragment.h"
#include <pthread.h>
#include <queue>




/* ********************************* */
/*             CONSTANTS             */
/* ********************************* */

/**@{*/
/** Return code. */
#define TILEDB_AR_OK          0
#define TILEDB_AR_ERR        -1
/**@}*/

/** Default error message. */
#define TILEDB_AR_ERRMSG std::string("[TileDB::Array] Error: ")




/* ********************************* */
/*          GLOBAL VARIABLES         */
/* ********************************* */

extern std::string tiledb_ar_errmsg;



class ArrayReadState;
class ArraySortedReadState;
class ArraySortedWriteState;
class Fragment;




/** Manages a TileDB array object. */
class Array {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */
  
  /** Constructor. */
  Array();

  /** Destructor. */
  ~Array();




  /* ********************************* */
  /*             ACCESSORS             */
  /* ********************************* */

  /** 
   * Enters an indefinite loop that handles all the AIO requests. This is
   * executed in the background by the AIO thread.
   *
   * @return void.
   */
  void aio_handle_requests();

  /**
   * Submits an asynchronous (AIO) read request and immediately returns control
   * to the caller. The request is queued up and executed in the background by
   * another thread. 
   *  
   * @param aio_request The AIO read request. 
   * @return TILEDB_AR_OK for success and TILEDB_AR_ERR for error.
   */
  int aio_read(AIO_Request* aio_request);

  /**
   * Submits an asynchronous (AIO) write request and immediately returns control
   * to the caller. The request is queued up and executed in the background by
   * another thread. 
   *  
   * @param aio_request The AIO write request. 
   * @return TILEDB_AR_OK for success and TILEDB_AR_ERR for error.
   */
  int aio_write(AIO_Request* aio_request);

  /** Returns the array schema. */
  const ArraySchema* array_schema() const;

  /** Returns the array clone. */
  Array* array_clone() const;

  /** Returns the ids of the attributes the array focuses on. */
  const std::vector<int>& attribute_ids() const;

  /** Returns the configuration parameters. */
  const Config* config() const;

  /** Returns the number of fragments in this array. */
  int fragment_num() const;

  /** Returns the fragment objects of this array. */
  std::vector<Fragment*> fragments() const;

  /** Returns the array mode. */
  int mode() const;

  /**
   * Checks if *at least one* attribute buffer has overflown during a read 
   * operation.
   *
   * @return *true* if at least one attribute buffer has overflown and *false* 
   * otherwise.
   */
  bool overflow() const;

  /**
   * Checks if an attribute buffer has overflown during a read operation.
   *
   * @param attribute_id The id of the attribute that is being checked.
   * @return *true* if the attribute buffer has overflown and *false* otherwise.
   */
  bool overflow(int attribute_id) const;

  /**
   * Performs a read operation in an array, which must be initialized in read 
   * mode. The function retrieves the result cells that lie inside
   * the subarray specified in init() or reset_subarray(). The results are
   * written in input buffers provided by the user, which are also allocated by
   * the user. Note that the results are written in the buffers in the same
   * order as that specified by the user in the init() function. 
   * 
   * @param buffers An array of buffers, one for each attribute. These must be
   *     provided in the same order as the attributes specified in
   *     init() or reset_attributes(). The case of variable-sized attributes is
   *     special. Instead of providing a single buffer for such an attribute,
   *     **two** must be provided: the second will hold the variable-sized cell
   *     values, whereas the first holds the start offsets of each cell in the
   *     second buffer.
   * @param buffer_sizes The sizes (in bytes) allocated by the user for the
   *     input buffers (there is a one-to-one correspondence). The function will
   *     attempt to write as many results as can fit in the buffers, and
   *     potentially alter the buffer size to indicate the size of the *useful*
   *     data written in the buffer. If a buffer cannot hold all results, the
   *     function will still succeed, writing as much data as it can and turning
   *     on an overflow flag which can be checked with function overflow(). The
   *     next invocation will resume for the point the previous one stopped,
   *     without inflicting a considerable performance penalty due to overflow.
   * @return TILEDB_AR_OK for success and TILEDB_AR_ERR for error.
   */
  int read(void** buffers, size_t* buffer_sizes); 

  /**
   * Performs a read operation in an array, which must be initialized in read 
   * mode. The function retrieves the result cells that lie inside
   * the subarray specified in init() or reset_subarray(). The results are
   * written in input buffers provided by the user, which are also allocated by
   * the user. Note that the results are written in the buffers in the same
   * order they appear on the disk, which leads to maximum performance. 
   * 
   * @param buffers An array of buffers, one for each attribute. These must be
   *     provided in the same order as the attributes specified in
   *     init() or reset_attributes(). The case of variable-sized attributes is
   *     special. Instead of providing a single buffer for such an attribute,
   *     **two** must be provided: the second will hold the variable-sized cell
   *     values, whereas the first holds the start offsets of each cell in the
   *     second buffer.
   * @param buffer_sizes The sizes (in bytes) allocated by the user for the
   *     input buffers (there is a one-to-one correspondence). The function will
   *     attempt to write as many results as can fit in the buffers, and
   *     potentially alter the buffer size to indicate the size of the *useful*
   *     data written in the buffer. If a buffer cannot hold all results, the
   *     function will still succeed, writing as much data as it can and turning
   *     on an overflow flag which can be checked with function overflow(). The
   *     next invocation will resume for the point the previous one stopped,
   *     without inflicting a considerable performance penalty due to overflow.
   * @return TILEDB_AR_OK for success and TILEDB_AR_ERR for error.
   */
  int read_default(void** buffers, size_t* buffer_sizes); 

  /** Returns true if the array is in read mode. */
  bool read_mode() const;

  /** Returns the subarray in which the array is constrained. */
  const void* subarray() const;

  /** Returns true if the array is in write mode. */
  bool write_mode() const;




  /* ********************************* */
  /*              MUTATORS             */
  /* ********************************* */

  /**
   * Consolidates all fragments into a new single one, on a per-attribute basis.
   * Returns the new fragment (which has to be finalized outside this function),
   * along with the names of the old (consolidated) fragments (which also have
   * to be deleted outside this function).
   *
   * @param new_fragment The new fragment to be returned.
   * @param old_fragment_names The names of the old fragments to be returned.
   * @return TILEDB_AR_OK for success and TILEDB_AR_ERR for error.
   */
  int consolidate(
      Fragment*& new_fragment, 
      std::vector<std::string>& old_fragment_names);

  /**
   * Consolidates all fragment into a new single one, focusing on a specific
   * attribute.
   *
   * @param new_fragment The new consolidated fragment object.
   * @param attribute_i The id of the target attribute.
   */
  int consolidate(
      Fragment* new_fragment,
      int attribute_id);

  /**
   * Finalizes the array, properly freeing up memory space.
   *
   * @return TILEDB_AR_OK on success, and TILEDB_AR_ERR on error.
   */
  int finalize();

  /**
   * Initializes a TileDB array object.
   *
   * @param array_schema The array schema.
   * @param fragment_names The names of the fragments of the array.
   * @param book_keeping The book-keeping structures of the fragments
   *     of the array.
   * @param mode The mode of the array. It must be one of the following:
   *    - TILEDB_ARRAY_WRITE 
   *    - TILEDB_ARRAY_WRITE_SORTED_COL 
   *    - TILEDB_ARRAY_WRITE_SORTED_ROW
   *    - TILEDB_ARRAY_WRITE_UNSORTED 
   *    - TILEDB_ARRAY_READ 
   *    - TILEDB_ARRAY_READ_SORTED_COL 
   *    - TILEDB_ARRAY_READ_SORTED_ROW
   * @param attributes A subset of the array attributes the read/write will be
   *     constrained on. A NULL value indicates **all** attributes (including
   *     the coordinates in the case of sparse arrays).
   * @param attribute_num The number of the input attributes. If *attributes* is
   *     NULL, then this should be set to 0.
   * @param subarray The subarray in which the array read/write will be
   *     constrained on. If it is NULL, then the subarray is set to the entire
   *     array domain. For the case of writes, this is meaningful only for
   *     dense arrays, and specifically dense writes.
   * @param config Configuration parameters.
   * @param array_clone An clone of this array object. Used specifically in 
   *     asynchronous IO (AIO) read/write operations.
   * @return TILEDB_AR_OK on success, and TILEDB_AR_ERR on error.
   */
  int init(
      const ArraySchema* array_schema, 
      const std::vector<std::string>& fragment_names,
      const std::vector<BookKeeping*>& book_keeping,
      int mode,
      const char** attributes,
      int attribute_num,
      const void* subarray,
      const Config* config,
      Array* array_clone = NULL);

  /**
   * Resets the attributes used upon initialization of the array. 
   *
   * @param attributes The new attributes to focus on. If it is NULL, then
   *     all the attributes are used (including the coordinates in the case of
   *     sparse arrays).
   * @param attribute_num The number of the attributes. If *attributes* is NULL,
   *     then this should be 0.
   * @return TILEDB_AR_OK on success, and TILEDB_AR_ERR on error.
   */
  int reset_attributes(const char** attributes, int attribute_num);

  /**
   * Resets the subarray used upon initialization of the array. This is useful
   * when the array is used for reading, and the user wishes to change the
   * query subarray without having to finalize and re-initialize the array
   * with a different subarray.
   *
   * @param subarray The new subarray. Note that the type of the values in
   *     *subarray* should match the coordinates type in the array schema.
   * @return TILEDB_AR_OK on success, and TILEDB_AR_ERR on error.
   */
  int reset_subarray(const void* subarray);

  /**
   * Same as reset_subarray(), with the difference that the 
   * ArraySortedReadState object of the array is not re-initialized.
   *
   * @param subarray The new subarray. Note that the type of the values in
   *     *subarray* should match the coordinates type in the array schema.
   * @return TILEDB_AR_OK on success, and TILEDB_AR_ERR on error.
   */
  int reset_subarray_soft(const void* subarray);

  /**
   * Syncs all currently written files in the input array. 
   *
   * @return TILEDB_AR_OK on success, and TILEDB_AR_ERR on error.
   */
  int sync();

  /**
   * Syncs the currently written files associated with the input attribute
   * in the input array. 
   *
   * @return TILEDB_AR_OK on success, and TILEDB_AR_ERR on error.
   */
  int sync_attribute(const std::string& attribute);

  /**
   * Performs a write operation in the array. The cell values are provided
   * in a set of buffers (one per attribute specified upon initialization).
   * Note that there must be a one-to-one correspondance between the cell
   * values across the attribute buffers.
   *
   * The array must be initialized in one of the following write modes,
   * each of which having a different behaviour:
   *    - TILEDB_ARRAY_WRITE: \n
   *      In this mode, the cell values are provided in the buffers respecting
   *      the cell order on the disk. It is practically an **append** operation,
   *      where the provided cell values are simply written at the end of
   *      their corresponding attribute files. This mode leads to the best
   *      performance. The user may invoke this function an arbitrary number
   *      of times, and all the writes will occur in the same fragment. 
   *      Moreover, the buffers need not be synchronized, i.e., some buffers
   *      may have more cells than others when the function is invoked.
   *    - TILEDB_ARRAY_WRITE_SORTED_COL: \n
   *      In this mode, the cell values are provided in the buffer in 
   *      column-major
   *      order with respect to the subarray used upon array initialization. 
   *      TileDB will properly re-organize the cells so that they follow the 
   *      array cell order for storage on the disk.
   *    - TILEDB_ARRAY_WRITE_SORTED_ROW: \n
   *      In this mode, the cell values are provided in the buffer in row-major
   *      order with respect to the subarray used upon array initialization. 
   *      TileDB will properly re-organize the cells so that they follow the 
   *      array cell order for storage on the disk.
   *    - TILEDB_ARRAY_WRITE_UNSORTED: \n
   *      This mode is applicable to sparse arrays, or when writing sparse
   *      updates to a dense array. One of the buffers holds the coordinates.
   *      The cells in this mode are given in an arbitrary, unsorted order
   *      (i.e., without respecting how the cells must be stored on the disk
   *      according to the array schema definition). Each invocation of this
   *      function internally sorts the cells and writes them to the disk on the
   *      proper order. In addition, each invocation creates a **new** fragment.
   *      Finally, the buffers in each invocation must be synced, i.e., they
   *      must have the same number of cell values across all attributes.
   * 
   * @param buffers An array of buffers, one for each attribute. These must be
   *     provided in the same order as the attributes specified in
   *     init() or reset_attributes(). The case of variable-sized attributes is
   *     special. Instead of providing a single buffer for such an attribute,
   *     **two** must be provided: the second holds the variable-sized cell
   *     values, whereas the first holds the start offsets of each cell in the
   *     second buffer.
   * @param buffer_sizes The sizes (in bytes) of the input buffers (there is
   *     a one-to-one correspondence).
   * @return TILEDB_AR_OK for success and TILEDB_AR_ERR for error.
   */
  int write(const void** buffers, const size_t* buffer_sizes);

  /**
   * Performs a write operation in the array. The cell values are provided
   * in a set of buffers (one per attribute specified upon initialization).
   * Note that there must be a one-to-one correspondance between the cell
   * values across the attribute buffers.
   *
   * The array must be initialized in moder TILEDB_ARRAY_WRITE or 
   * TILEDB_ARRAY_WRITE_UNSORTED. These modes are essentially the default modes.
   * Modes TILEDB_ARRAY_WRITE_SORTED_COL and TILEDB_ARRAY_WRITE_SORTED_ROW are
   * more complicated and, thus, handled by the ArraySortedWriteState class.
   * 
   * @param buffers An array of buffers, one for each attribute. These must be
   *     provided in the same order as the attributes specified in
   *     init() or reset_attributes(). The case of variable-sized attributes is
   *     special. Instead of providing a single buffer for such an attribute,
   *     **two** must be provided: the second holds the variable-sized cell
   *     values, whereas the first holds the start offsets of each cell in the
   *     second buffer.
   * @param buffer_sizes The sizes (in bytes) of the input buffers (there is
   *     a one-to-one correspondence).
   * @return TILEDB_AR_OK for success and TILEDB_AR_ERR for error.
   */
  int write_default(const void** buffers, const size_t* buffer_sizes); 

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** The AIO mutex condition. */
  pthread_cond_t aio_cond_;
  /** Stores the id of the last handled AIO request. */
  size_t aio_last_handled_request_;
  /** The AIO mutex. */
  pthread_mutex_t aio_mtx_;
  /** The queue that stores the pending AIO requests. */
  std::queue<AIO_Request*> aio_queue_;
  /** The thread that handles all the AIO reads and writes in the background. */
  pthread_t aio_thread_;
  /** Indicates whether the AIO thread was canceled or not. */
  bool aio_thread_canceled_;
  /** Indicates whether the AIO thread was created or not. */
  volatile bool aio_thread_created_;
  /** An array clone, used in AIO requests. */
  Array* array_clone_;
  /** The array schema. */
  const ArraySchema* array_schema_;
  /** The read state of the array. */
  ArrayReadState* array_read_state_;
  /** The sorted read state of the array. */
  ArraySortedReadState* array_sorted_read_state_;
  /** The sorted write state of the array. */
  ArraySortedWriteState* array_sorted_write_state_;
  /** 
   * The ids of the attributes the array is initialized with. Note that the
   * array may be initialized with a subset of attributes when writing or
   * reading.
   */
  std::vector<int> attribute_ids_;
  /** Configuration parameters. */
  const Config* config_;
  /** The array fragments. */
  std::vector<Fragment*> fragments_;
  /** 
   * The array mode. It must be one of the following:
   *    - TILEDB_ARRAY_WRITE 
   *    - TILEDB_ARRAY_WRITE_SORTED_COL
   *    - TILEDB_ARRAY_WRITE_SORTED_ROW
   *    - TILEDB_ARRAY_WRITE_UNSORTED 
   *    - TILEDB_ARRAY_READ 
   *    - TILEDB_ARRAY_READ_SORTED_COL
   *    - TILEDB_ARRAY_READ_SORTED_ROW
   */
  int mode_;
  /**
   * The subarray in which the array is constrained. Note that the type of the
   * range must be the same as the type of the array coordinates.
   */
  void* subarray_;




  /* ********************************* */
  /*           PRIVATE METHODS         */
  /* ********************************* */

  /** 
   * Handles an AIO request.
   *
   * @param aio_request The AIO request. The function will resolve whether it is
   *     a read or write request based on the array mode.
   * @return void.
   * 
   */
  void aio_handle_next_request(AIO_Request* aio_request);

  /**
   * Function called by the AIO thread. 
   *
   * @param context This is practically the Array object for which the function
   *     is called (typically *this* is passed to this argument by the caller).
   */
  static void *aio_handler(void* context);

  /**
   * Pusghes an AIO request into the AIO queue.
   *
   * @param aio_request The AIO request. 
   * @return TILEDB_AR_OK for success and TILEDB_AR_ERR for error.
   */ 
  int aio_push_request(AIO_Request* aio_request);

  /**
   * Creates the AIO thread.
   *
   * @return TILEDB_AR_OK for success and TILEDB_AR_ERR for error.
   */ 
  int aio_thread_create();

  /**
   * Destroys the AIO thread.
   *
   * @return TILEDB_AR_OK for success and TILEDB_AR_ERR for error.
   */ 
  int aio_thread_destroy();
  
  /** 
   * Returns a new fragment name, which is in the form: <br>
   * .__<thread_id>_<timestamp>
   *
   * Note that this is a temporary name, initiated by a new write process.
   * After the new fragmemt is finalized, the array will change its name
   * by removing the leading '.' character. 
   *
   * @return A new special fragment name on success, or "" (empty string) on
   *     error.
   */
  std::string new_fragment_name() const;

  /**
   * Opens the existing fragments.
   *
   * @param fragment_names The vector with the fragment names.
   * @param book_keeping The book-keeping of the array fragments.
   * @return TILEDB_AR_OK for success and TILEDB_AR_ERR for error.
   */
  int open_fragments(
      const std::vector<std::string>& fragment_names,
      const std::vector<BookKeeping*>& book_keeping);
};

#endif
