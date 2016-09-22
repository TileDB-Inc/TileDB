/**
 * @file   array_sorted_read_state.h
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
 * This file defines class ArraySortedReadState. 
 */

#ifndef __ARRAY_SORTED_READ_STATE_H__
#define __ARRAY_SORTED_READ_STATE_H__

#include "array.h"
#include <pthread.h>
#include <string>
#include <vector>


/* ********************************* */
/*             CONSTANTS             */
/* ********************************* */

/**@{*/
/** Return code. */
#define TILEDB_ASRS_OK                                0
#define TILEDB_ASRS_ERR                              -1
/**@}*/

/** Default error message. */
#define TILEDB_ASRS_ERRMSG std::string("[TileDB::ArraySortedReadState] Error: ")




/* ********************************* */
/*          GLOBAL VARIABLES         */
/* ********************************* */

extern std::string tiledb_asrs_errmsg;


class Array;

/** 
 * Stores the state necessary when reading cells from the array fragments,
 * sorted in a way different to the global cell order.
 */
class ArraySortedReadState {
 public:

  /* ********************************* */
  /*          TYPE DEFINITIONS         */
  /* ********************************* */

  /** Simple struct used for an AIO or copy request. */
  struct ASRS_Request {
    /** The id of the targeted tile slab. */
    int id_;
    /** The calling object. */
    ArraySortedReadState* asrs_;
  };

  /** Info about a tile slab. It is used by the copy_tile_slab() function. */
  struct TileSlabInfo {
    /** 
     * Number of cell slabs to be copied in as tile pass, 
     * per attribute per tile. 
     */
    int64_t** cell_slab_num_in_pass_;
    /** The multi-dimensional range of cell slabs per tile. */
    int64_t** cell_slab_range_;
    /** Cell slab size per attribute per tile. */
    size_t** cell_slab_size_;
    /** 
     * Bytes to jump after copying a cell slab, per attribute, per tile, 
     * per dimension.
     */
    size_t*** offsets_per_dim_;
    /** Number of tiles in the tile slab. */
    int64_t tile_num_;
    /** Offset of each starting result cell per attribute per tile. */
    size_t** tile_offsets_;
  }; 

  /** The state for a tile slab copy. */
  struct TileSlabState {
    /** The current tile per attribute. */
    int64_t* current_tile_;
    /** The cell slab in the current pass per attribute per tile. */
    int64_t** current_cell_slab_in_pass_;
    /** The offset of the next cell slab to be copied per attribute per tile. */
    size_t** current_offset_;
  };

 
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** 
   * Constructor.
   *
   * @param array The array this array sorted read state belongs to.
   */
  ArraySortedReadState(Array* array);

  /** Destructor. */
  ~ArraySortedReadState();




  /* ********************************* */
  /*             ACCESSORS             */
  /* ********************************* */

  /** Returns true if copying into the user buffers resulted in overflow. */
  bool overflow() const;

  /**
   * Same as Array::read(), but it sorts the cells in the buffers based on the
   * order the user specified in Array::init(). Note that this function will 
   * fail if there is not enough system memory to hold the cells of a 
   * 'tile slab' overlapping with the selected subarray.
   *
   * @param buffers An array of buffers, one for each attribute. These must be
   *     provided in the same order as the attributes specified in
   *     Array::init() or Array::reset_attributes(). The case of variable-sized 
   *     attributes is special. Instead of providing a single buffer for such 
   *     an attribute, **two** must be provided: the second will hold the 
   *     variable-sized cell values, whereas the first holds the start offsets 
   *     of each cell in the second buffer.
   * @param buffer_sizes The sizes (in bytes) allocated by the user for the
   *     input buffers (there is a one-to-one correspondence). The function will
   *     attempt to write as many results as can fit in the buffers, and
   *     potentially alter the buffer size to indicate the size of the *useful*
   *     data written in the buffer. If a buffer cannot hold all results, the
   *     function will still succeed, writing as much data as it can and turning
   *     on an overflow flag which can be checked with function overflow(). The
   *     next invocation will resume for the point the previous one stopped,
   *     without inflicting a considerable performance penalty due to overflow.
   * @return TILEDB_ASRS_OK for success and TILEDB_ASRS_ERR for error.
   */
  int read(void** buffers, size_t* buffer_sizes); 




  /* ********************************* */
  /*             MUTATORS              */
  /* ********************************* */


  /** 
   * Initialized the array sorted read state. 
   *
   * @return TILEDB_ASRS_OK for success and TILEDB_ASRS_ERR for error.
   */
  int init();


 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** The AIO mutex conditions (one for each buffer). */
  pthread_cond_t aio_cond_[2];

  /** The current id of the buffers the next AIO will occur into. */
  int aio_id_;

  /** The AIO mutex. */
  pthread_mutex_t aio_mtx_;
  
  /** The array this sorted read state belongs to. */
  Array* array_;

  /** The ids of the attributes the array was initialized with. */
  const std::vector<int>& attribute_ids_;

  /** Number of allocated buffers. */
  int buffer_num_;

  /** Allocated sizes for buffers_ (similar to those used in Array::read). */
  size_t* buffer_sizes_[2];

  /** Local buffers (similar to those used in Array::read). */
  void** buffers_[2];

  /** The copy mutex conditions (one for each buffer). */
  pthread_cond_t copy_cond_[2];

  /** The current id of the buffers the next copy will occur from. */
  int copy_id_;

  /** The copy mutex. */
  pthread_mutex_t copy_mtx_;

  /** The thread tha handles all the copying in the background. */
  pthread_t copy_thread_;

  /** True if the read is done. */
  bool done_;

  /** The overflow mutex condition. */
  pthread_cond_t overflow_cond_;

  /** The overflow mutex. */
  pthread_mutex_t overflow_mtx_;

  /** Overflow flag for each attribute. */
  std::vector<bool> overflow_;

  /** True if a copy must be resumed. */
  bool resume_copy_;

  /** True if an AIO must be resumed. */
  bool resume_aio_;

  /** The query subarray. */
  const void* subarray_;

  /** The tile slab to be read for the first and second buffers. */
  void* tile_slab_[2];

  /** The info for each of the two tile slabs under investigation. */
  TileSlabInfo tile_slab_info_[2];

  /** User buffer sizes. */
  size_t* user_buffer_sizes_;

  /** User buffers. */
  void** user_buffers_;

  /** Wait for copy flags, one for each local buffer. */
  bool wait_copy_[2];

  /** Wait for AIO flags, one for each local buffer. */
  bool wait_aio_[2];

  /* ********************************* */
  /*           PRIVATE METHODS         */
  /* ********************************* */

  /**
   * Called when an AIO completes.
   *
   * @param data A ASRS_Request object.
   * @return void 
   */
  static void *aio_done(void* data);

  /** Sets the flag of wait_aio_[id] to true. */
  void block_aio(int id);

  /** Sets the flag of wait_copy_[id] to true. */
  void block_copy(int id);

  /** Sets the flag of resume_copy_ to true. */
  void block_overflow();

  /** 
   * Calculates the number of buffers to be allocated, based on the number
   * of attributes initialized for the array.
   *
   * @return void
   */
  void calculate_buffer_num();

  /** 
   * Calculates the buffer sizes based on the subarray and the number of cells
   * in a (full) tile slab.
   *
   * @return void
   */
  void calculate_buffer_sizes();

  /** 
   * Calculates the info used in the copy_tile_slab() function for the case
   * of column-major order.
   *
   * @param T The domain type.
   * @param id The tile slab id.
   * @return void.
   */
  template<class T>
  void calculate_tile_slab_info_col(int id);

  /** 
   * Calculates the info used in the copy_tile_slab() function for the case
   * of row-major order.
   *
   * @param T The domain type.
   * @param id The tile slab id.
   * @return void.
   */
  template<class T>
  void calculate_tile_slab_info_row(int id);

  /**
   * Function called by the copy thread. 
   *
   * @param context This is practically the ArraySortedReadState object for 
   *     which the function is called (typically *this* is passed to this 
   *     argument by the caller).
   */
  static void *copy_handler(void* context);

  /** 
   * Copies a tile slab from the local buffers into the user buffers, 
   * properly re-organizing the cell order to fit the targeted order.  
   * 
   * @return void.
   */
  void copy_tile_slab();

  /** 
   * Creates the buffers based on the calculated buffer sizes.
   *
   * @return TILEDB_ASRS_OK for success and TILEDB_ASRS_ERR for error.
   */
  int create_buffers();

  /** Frees the tile slab info. */
  void free_tile_slab_info();

  /** Frees the tile slab state. */
  void free_tile_slab_state();

  /** Handles the copy requests. */
  void handle_copy_requests();

  /** Initializes the tile slab info. */
  void init_tile_slab_info();

  /** Initializes the tile slab state. */
  void init_tile_slab_state();

  /**  
   * Locks the AIO mutex. 
   * 
   * @return TILEDB_ASRS_OK for success and TILEDB_ASRS_ERR for error.
   */
  int lock_aio_mtx();

  /**  
   * Locks the copy mutex. 
   * 
   * @return TILEDB_ASRS_OK for success and TILEDB_ASRS_ERR for error.
   */
  int lock_copy_mtx();

  /**  
   * Locks the overflow mutex. 
   * 
   * @return TILEDB_ASRS_OK for success and TILEDB_ASRS_ERR for error.
   */
  int lock_overflow_mtx();

  /** 
   * Retrieves the next column tile slab to be processed.
   * 
   * @template T The domain type.
   * @return True if the next tile slab was retrieved, and false otherwise.
   */
  template<class T>
  bool next_tile_slab_col();

  /** 
   * Retrieves the next row tile slab to be processed.
   * 
   * @template T The domain type.
   * @return True if the next tile slab was retrieved, and false otherwise.
   */
  template<class T>
  bool next_tile_slab_row();

  /**
   * Same as Array::read(), but it sorts the cells in the buffers based on the
   * order the user specified in Array::init(). Note that this function will 
   * fail if there is not enough system memory to hold the cells of a 
   * 'tile slab' overlapping with the selected subarray.
   *
   * @template T The domain type.
   * @return TILEDB_ASRS_OK for success and TILEDB_ASRS_ERR for error.
   */
  template<class T>
  int read(); 

  /**
   * Same as read(), but the cells are placed in 'buffers' sorted in 
   * column-major order with respect to the selected subarray. 
   * Applicable only to dense arrays. 
   * 
   * @template T The domain type.
   * @return TILEDB_AR_OK for success and TILEDB_AR_ERR for error.
   */
  template<class T>
  int read_dense_sorted_col(); 

  /**
   * Same as read(), but the cells are placed in 'buffers' sorted in 
   * row-major order with respect to the selected subarray. 
   * Applicable only to dense arrays. 
   * 
   * @template T The domain type.
   * @return TILEDB_AR_OK for success and TILEDB_AR_ERR for error.
   */
  template<class T>
  int read_dense_sorted_row(); 

  /**
   * Same as read(), but the cells are placed in 'buffers' sorted in 
   * column-major order with respect to the selected subarray. 
   * Applicable only to sparse arrays. 
   * 
   * @template T The domain type.
   * @return TILEDB_AR_OK for success and TILEDB_AR_ERR for error.
   */
  template<class T>
  int read_sparse_sorted_col(); 

  /**
   * Same as read(), but the cells are placed in 'buffers' sorted in 
   * row-major order with respect to the selected subarray. 
   * Applicable only to sparse arrays. 
   * 
   * @template T The domain type.
   * @return TILEDB_AR_OK for success and TILEDB_AR_ERR for error.
   */
  template<class T>
  int read_sparse_sorted_row(); 

  /** 
   * Reads the current tile slab into the input buffers.
   * 
   * @return TILEDB_ASRS_OK for success and TILEDB_ASRS_ERR for error.
   */
  int read_tile_slab();

  /** 
   * Signals an AIO condition. 
   * 
   * @param id The id of the AIO condition to be signaled.
   * @return TILEDB_ASRS_OK for success and TILEDB_ASRS_ERR for error.
   */
  int release_aio(int id);

  /** 
   * Signals a copy condition. 
   * 
   * @param id The id of the copy condition to be signaled.
   * @return TILEDB_ASRS_OK for success and TILEDB_ASRS_ERR for error.
   */
  int release_copy(int id);

  /** 
   * Signals the overflow condition. 
   * 
   * @return TILEDB_ASRS_OK for success and TILEDB_ASRS_ERR for error.
   */
  int release_overflow();

  /** Resets the oveflow flags to **false**. */
  void reset_overflow();
  
  /** Resets the state of the tile slab with the input id. */
  void reset_tile_slab_state(int id);

  /**  
   * Unlocks the AIO mutex. 
   * 
   * @return TILEDB_ASRS_OK for success and TILEDB_ASRS_ERR for error.
   */
  int unlock_aio_mtx();

  /**  
   * Unlocks the copy mutex. 
   * 
   * @return TILEDB_ASRS_OK for success and TILEDB_ASRS_ERR for error.
   */
  int unlock_copy_mtx();

  /**  
   * Unlocks the overflow mutex. 
   * 
   * @return TILEDB_ASRS_OK for success and TILEDB_ASRS_ERR for error.
   */
  int unlock_overflow_mtx();

  /**
   * Waits on a copy operation for the buffer with input id to finish.
   *
   * @param id The id of the buffer the copy operation must be completed.
   * @return TILEDB_ASRS_OK for success and TILEDB_ASRS_ERR for error.
   */
  int wait_copy(int id);

  /**
   * Waits on a AIO operation for the buffer with input id to finish.
   *
   * @param id The id of the buffer the AIO operation must be completed.
   * @return TILEDB_ASRS_OK for success and TILEDB_ASRS_ERR for error.
   */
  int wait_aio(int id);

  /**
   * Waits until there is no buffer overflow.
   *
   * @return TILEDB_ASRS_OK for success and TILEDB_ASRS_ERR for error.
   */
  int wait_overflow();
};

#endif
