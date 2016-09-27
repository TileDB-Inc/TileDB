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

  /** Used in advance_cell_slab(). */
  struct AdvanceCellSlabInfo {
    /** The id of the targeted attribute id in attribute_ids_. */
    int aid_;
    /** The calling object. */
    ArraySortedReadState* asrs_;
  };

  /** Stores state about the current read/copy request. */
  struct CopyState {
    /** Current offsets in user buffers. */
    size_t* buffer_offsets_;
    /** User buffer sizes. */
    size_t* buffer_sizes_;
    /** User buffers. */
    void** buffers_;
  };

  /** Simple struct used for an AIO or copy request. */
  struct ASRS_Request {
    /** The id of the targeted tile slab. */
    int id_;
    /** The calling object. */
    ArraySortedReadState* asrs_;
  };

  /** Info about a tile slab. It is used by the copy_tile_slab() function. */
  struct TileSlabInfo {
    /** The multi-dimensional range of cell slabs per tile. */
    int64_t** cell_slab_range_;
    /** Cell slab size per attribute per tile. */
    size_t** cell_slab_size_;
    /** first dimension to advance when advancing the cell slab coordinates. */
    int first_dim_to_advance_;
    /** 
     * Bytes to jump after copying a cell slab, per attribute, per tile, 
     * per dimension.
     */
    size_t*** offsets_per_dim_;
    /** Offset of first result cell slab per attribute per tile. */
    size_t** start_offsets_;
    /** The normalized tile domain of the tile slab. */
    void* tile_domain_;
    /** Number of tiles in the tile slab. */
    int64_t tile_num_;
    /** Tiles to jump when a tile is done. */
    int64_t tiles_to_advance_;
  }; 

  /** The state for a tile slab copy. */
  struct TileSlabState {
    /** The current tile per attribute. */
    int64_t* current_tile_;
    /** The current coordinates in the slab range per attribute per tile. */
    int64_t*** current_cell_slab_coords_;
    /** The offset of the next cell slab to be copied per attribute per tile. */
    size_t** current_offsets_;
    /** Keeps track of whether a tile slab copy for an attribute id done. */
    bool* copy_tile_slab_done_;
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

  /** Returns true if the current slab is finished being copied. */
  bool copy_tile_slab_done() const;

  /** True if read is done for all attributes. */
  bool done() const;

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

  /** Function for advancing a cell slab during a copy operation. */
  void *(*advance_cell_slab_) (void*);

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

  /**
   * The sizes of the attributes. For variable-length attributes, sizeof(size_t)
   * is stored.
   */
  std::vector<size_t> attribute_sizes_;

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

  /** The copy state. */
  CopyState copy_state_;

  /** The copy mutex. */
  pthread_mutex_t copy_mtx_;

  /** The thread tha handles all the copying in the background. */
  pthread_t copy_thread_;

  /** True if the copy thread is running. */
  bool copy_thread_running_;

  /** The overflow mutex condition. */
  pthread_cond_t overflow_cond_;

  /** The overflow mutex. */
  pthread_mutex_t overflow_mtx_;

  /** Overflow flag for each attribute. */
  std::vector<bool> overflow_;

  /** True if no more tile slabs to read. */
  bool read_tile_slabs_done_;

  /** True if a copy must be resumed. */
  bool resume_copy_;

  /** True if an AIO must be resumed. */
  bool resume_aio_;

  /** The query subarray. */
  const void* subarray_;

  /** The tile slab to be read for the first and second buffers. */
  void* tile_slab_[2];

  /** Normalized tile slab. */
  void* tile_slab_norm_[2];

  /** The info for each of the two tile slabs under investigation. */
  TileSlabInfo tile_slab_info_[2];

  /** The state for the current tile slab being copied. */
  TileSlabState tile_slab_state_;

  /** Wait for copy flags, one for each local buffer. */
  bool wait_copy_[2];

  /** Wait for AIO flags, one for each local buffer. */
  bool wait_aio_[2];

  /* ********************************* */
  /*           PRIVATE METHODS         */
  /* ********************************* */

  /** 
   * Advances a cell slab focusing on column-major order, and updates
   * the CopyState and TileSlabState. 
   * Used in copy_tile_slab(). 
   *
   * @param data Essentially a pointer to a AdvanceCellSlabInfo object.
   * @return void
   */
  static void *advance_cell_slab_col(void* data);

  /** 
   * Advances a cell slab focusing on row-major order, and updates
   * the CopyState and TileSlabState. 
   * Used in copy_tile_slab(). 
   *
   * @param data Essentially a pointer to a AdvanceCellSlabInfo object.
   * @return void
   */
  static void *advance_cell_slab_row(void* data);

  /** 
   * Advances a cell slab when the requested order is column-major. 
   * 
   * @param aid The id of the attribute in attribute_ids_ to focus on.
   * @return void
   */
  void advance_cell_slab_col(int aid);

  /** 
   * Advances a cell slab when the requested order is row-major. 
   * 
   * @param aid The id of the attribute in attribute_ids_ to focus on.
   * @return void
   */
  void advance_cell_slab_row(int aid);

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
   * of column-major order, when the array tile order is column-major and
   * the array cell order is column-major.
   *
   * @param T The domain type.
   * @param id The tile slab id.
   * @return void.
   */
  template<class T>
  void calculate_tile_slab_info_col_cc(int id);

  /** 
   * Calculates the info used in the copy_tile_slab() function for the case
   * of column-major order, when the array tile order is column-major and
   * the array cell order is row-major.
   *
   * @param T The domain type.
   * @param id The tile slab id.
   * @return void.
   */
  template<class T>
  void calculate_tile_slab_info_col_cr(int id);

  /** 
   * Calculates the info used in the copy_tile_slab() function for the case
   * of column-major order, when the array tile order is row-major and
   * the array cell order is column-major.
   *
   * @param T The domain type.
   * @param id The tile slab id.
   * @return void.
   */
  template<class T>
  void calculate_tile_slab_info_col_rc(int id);

  /** 
   * Calculates the info used in the copy_tile_slab() function for the case
   * of column-major order, when the array tile order is row-major and
   * the array cell order is row-major.
   *
   * @param T The domain type.
   * @param id The tile slab id.
   * @return void.
   */
  template<class T>
  void calculate_tile_slab_info_col_rr(int id);

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
   * Calculates the info used in the copy_tile_slab() function for the case
   * of row-major order, when the array tile order is column-major and
   * the array cell order is column-major.
   *
   * @param T The domain type.
   * @param id The tile slab id.
   * @return void.
   */
  template<class T>
  void calculate_tile_slab_info_row_cc(int id);

  /** 
   * Calculates the info used in the copy_tile_slab() function for the case
   * of row-major order, when the array tile order is column-major and
   * the array cell order is row-major.
   *
   * @param T The domain type.
   * @param id The tile slab id.
   * @return void.
   */
  template<class T>
  void calculate_tile_slab_info_row_cr(int id);

  /** 
   * Calculates the info used in the copy_tile_slab() function for the case
   * of row-major order, when the array tile order is row-major and
   * the array cell order is column-major.
   *
   * @param T The domain type.
   * @param id The tile slab id.
   * @return void.
   */
  template<class T>
  void calculate_tile_slab_info_row_rc(int id);

  /** 
   * Calculates the info used in the copy_tile_slab() function for the case
   * of row-major order, when the array tile order is row-major and
   * the array cell order is row-major.
   *
   * @param T The domain type.
   * @param id The tile slab id.
   * @return void.
   */
  template<class T>
  void calculate_tile_slab_info_row_rr(int id);

  /**
   * Kills the copy thread (if it is still running).
   *
   * @return TILEDB_ASRS_OK for success and TILEDB_ASRS_ERR for error.
   */
  int cancel_copy_thread();

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
   * Copies a tile slab from the local buffers into the user buffers, 
   * properly re-organizing the cell order to fit the targeted order,
   * focusing on a particular fixed-length attribute.
   * 
   * @param aid The index on attribute_ids_ to focus on.
   * @param bid The index on the copy state buffers to focus on.
   * @return void.
   */
  void copy_tile_slab(int aid, int bid);

  /** 
   * Copies a tile slab from the local buffers into the user buffers, 
   * properly re-organizing the cell order to fit the targeted order,
   * focusing on a particular variable-length attribute.
   * 
   * @param aid The index on attribute_ids_ to focus on.
   * @param bid The index on the copy state buffers to focus on.
   * @return void.
   */
  void copy_tile_slab_var(int aid, int bid);

  /** 
   * Creates the buffers based on the calculated buffer sizes.
   *
   * @return TILEDB_ASRS_OK for success and TILEDB_ASRS_ERR for error.
   */
  int create_buffers();

  /** Frees the copy state. */
  void free_copy_state();

  /** Frees the tile slab info. */
  void free_tile_slab_info();

  /** Frees the tile slab state. */
  void free_tile_slab_state();

  /** Handles the copy requests. */
  void handle_copy_requests();

  /** Initializes the copy state. */
  void init_copy_state();

  /** Initializes the tile slab info. */
  void init_tile_slab_info();

  /** 
   * Initializes the tile slab info for a particular tile slab, using the
   * input tile number.
   *
   * @template The domain type.
   * @param id The slab id.
   * @param tile_num The number of tiles overlapped by the tile slab.
   * @return void.
   */
  template<class T>
  void init_tile_slab_info(int id, int64_t tile_num);

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

  /** Resets the copy state using the input buffer info. */
  void reset_copy_state(void** buffers, size_t* buffer_sizes);

  /** Resets the oveflow flags to **false**. */
  void reset_overflow();
  
  /** Resets the tile slab state. */
  void reset_tile_slab_state();

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
