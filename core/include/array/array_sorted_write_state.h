/**
 * @file   array_sorted_write_state.h
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
 * This file defines class ArraySortedWriteState. 
 */

#ifndef __ARRAY_SORTED_WRITE_STATE_H__
#define __ARRAY_SORTED_WRITE_STATE_H__

#include "array.h"
#include <pthread.h>
#include <string>
#include <vector>


/* ********************************* */
/*             CONSTANTS             */
/* ********************************* */

/**@{*/
/** Return code. */
#define TILEDB_ASWS_OK                                0
#define TILEDB_ASWS_ERR                              -1
/**@}*/

/** Default error message. */
#define TILEDB_ASWS_ERRMSG \
    std::string("[TileDB::ArraySortedWriteState] Error: ")




/* ********************************* */
/*          GLOBAL VARIABLES         */
/* ********************************* */

extern std::string tiledb_asws_errmsg;


class Array;

/** 
 * It is responsibled for re-arranging the cells sorted in column- or row-major
 * order within the user subarray, such that they are sorted along the array
 * global cell order, and writes them into a new fragment.
 */
class ArraySortedWriteState {
 public:

  /* ********************************* */
  /*          TYPE DEFINITIONS         */
  /* ********************************* */

  /** Used in functors. */
  struct ASWS_Data {
    /** An id (typically an attribute id or a tile slab id. */
    int id_;
    /** Another id (typically a tile id). */
    int64_t id_2_;
    /** The calling object. */
    ArraySortedWriteState* asws_;
  };

  /** Stores local state about the current write/copy request. */
  struct CopyState {
    /** Local buffer offsets. */
    size_t* buffer_offsets_[2];
    /** Local buffer sizes. */
    size_t* buffer_sizes_[2];
    /** Local buffers. */
    void** buffers_[2];
  }; 

  /** Info about a tile slab. */
  struct TileSlabInfo {
    /** Used in calculations of cell ids, one vector per tile. */
    int64_t** cell_offset_per_dim_;
    /** Cell slab size per attribute per tile. */
    size_t** cell_slab_size_;
    /** Number of cells in a cell slab per tile. */
    int64_t* cell_slab_num_;
    /** 
     * The range overlap of the **normalized** tile slab with each 
     *  **normalized** tile range. 
     */
    void** range_overlap_;
    /** 
     * Start offsets of each tile in the user buffer, per attribute per tile.
     */
    size_t** start_offsets_;
    /** Number of tiles in the tile slab. */
    int64_t tile_num_;
    /** Used in calculations of tile ids. */
    int64_t* tile_offset_per_dim_;
  }; 

  /** The state for a tile slab copy. */
  struct TileSlabState {
    /** Keeps track of whether a tile slab copy for an attribute id done. */
    bool* copy_tile_slab_done_;
    /** Current coordinates in tile slab per attribute. */
    void** current_coords_;
    /** 
     * The offset in the local buffers of the next cell slab to be copied per 
     * attribute. Note that this applies only to fixed-sized attributes
     * because the offsets of the variable-sized attributes can be derived from
     * the buffers that hold the fixed-sized offsets.
     */
    size_t* current_offsets_;
    /** The current tile per attribute. */
    int64_t* current_tile_;
  };

 
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** 
   * Constructor.
   *
   * @param array The array this array sorted read state belongs to.
   */
  ArraySortedWriteState(Array* array);

  /** Destructor. */
  ~ArraySortedWriteState();




  /* ********************************* */
  /*             ACCESSORS             */
  /* ********************************* */

  /** Returns true if the current slab is finished being copied. */
  bool copy_tile_slab_done() const;

  /** True if write is done for all attributes. */
  bool done() const;




  /* ********************************* */
  /*             MUTATORS              */
  /* ********************************* */


  /** 
   * Initializes the array sorted write state. 
   *
   * @return TILEDB_ASWS_OK for success and TILEDB_ASWS_ERR for error.
   */
  int init();

  /**
   * Same as Array::write(), but it sorts the cells in the buffers based on the
   * array global cell order, prior to writing them to the disk. Note that this 
   * function will fail if there is not enough system memory to hold the cells 
   * of a 'tile slab' overlapping with the selected subarray.
   *
   * @param buffers The buffers that hold the input cells to be written. 
   * @param buffer_sizes The corresponding buffer sizes. 
   * @return TILEDB_ASWS_OK for success and TILEDB_ASWS_ERR for error.
   */
  int write(const void** buffers, const size_t* buffer_sizes); 




 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** Function for advancing a cell slab during a copy operation. */
  void *(*advance_cell_slab_) (void*);

  /** Counter for the AIO requests. */
  size_t aio_cnt_;

  /** The AIO mutex conditions (one for each buffer). */
  pthread_cond_t aio_cond_[2];

  /** Data for the AIO requests. */
  ASWS_Data aio_data_[2];

  /** The current id of the buffers the next AIO will occur into. */
  int aio_id_;

  /** The AIO mutex. */
  pthread_mutex_t aio_mtx_;
  
  /** AIO requests. */
  AIO_Request aio_request_[2];

  /** The status of the AIO requests.*/
  int aio_status_[2];

  /** The thread that handles all the AIO in the background. */
  pthread_t aio_thread_;

  /** True if the AIO thread is canceled. */
  volatile bool aio_thread_canceled_;

  /** True if the AIO thread is running. */
  volatile bool aio_thread_running_;

  /** The array this sorted read state belongs to. */
  Array* array_;

  /** The ids of the attributes the array was initialized with. */
  const std::vector<int> attribute_ids_;

  /**
   * The sizes of the attributes. For variable-length attributes, sizeof(size_t)
   * is stored.
   */
  std::vector<size_t> attribute_sizes_;

  /** Number of allocated buffers. */
  int buffer_num_;

  /** The user buffer offsets. */
  size_t* buffer_offsets_;

  /** The user buffer sizes. */
  const size_t* buffer_sizes_;

  /** The user buffers. */
  const void** buffers_;

  /** Function for calculating cell slab info during a copy operation. */
  void *(*calculate_cell_slab_info_) (void*);

  /** Function for calculating tile slab info during a copy operation. */
  void *(*calculate_tile_slab_info_) (void*);

  /** The coordinates size of the array. */
  size_t coords_size_;

  /** The copy mutex conditions (one for each buffer). */
  pthread_cond_t copy_cond_[2];

  /** The current id of the buffers the next copy will occur from. */
  int copy_id_;

  /** The copy state, one per tile slab. */
  CopyState copy_state_;

  /** The copy mutex. */
  pthread_mutex_t copy_mtx_;

  /** The number of dimensions in the array. */
  int dim_num_;

  /** The expanded subarray, such that it coincides with tile boundaries. */
  void* expanded_subarray_;

  /** The query subarray. */
  void* subarray_;

  /** Auxiliary variable used in calculate_tile_slab_info(). */
  void* tile_coords_;

  /** Auxiliary variable used in calculate_tile_slab_info(). */
  void* tile_domain_;

  /** The tile slab to be read for the first and second buffers. */
  void* tile_slab_[2];

  /** Indicates if the tile slab has been initialized. */
  bool tile_slab_init_[2];

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
   * @template T The domain type.
   * @param data Essentially a pointer to a ASWS_Data object.
   * @return void
   */
  template<class T>
  static void *advance_cell_slab_col_s(void* data);

  /** 
   * Advances a cell slab focusing on row-major order, and updates
   * the CopyState and TileSlabState. 
   * Used in copy_tile_slab(). 
   *
   * @template T The domain type.
   * @param data Essentially a pointer to a ASWS_Data object.
   * @return void
   */
  template<class T>
  static void *advance_cell_slab_row_s(void* data);

  /** 
   * Advances a cell slab when the requested order is column-major. 
   * 
   * @template T The domain type.
   * @param aid The id of the attribute in attribute_ids_ to focus on.
   * @return void
   */
  template<class T>
  void advance_cell_slab_col(int aid);

  /** 
   * Advances a cell slab when the requested order is row-major. 
   * 
   * @template T The domain type.
   * @param aid The id of the attribute in attribute_ids_ to focus on.
   * @return void
   */
  template<class T>
  void advance_cell_slab_row(int aid);

  /**
   * Called when an AIO completes.
   *
   * @param data A ASWS_Request object.
   * @return void 
   */
  static void *aio_done(void* data);

  /** Sets the flag of wait_aio_[id] to true. */
  void block_aio(int id);

  /** Sets the flag of wait_copy_[id] to true. */
  void block_copy(int id);

  /** 
   * Calculates the number of buffers to be allocated, based on the number
   * of attributes initialized for the array.
   *
   * @return void
   */
  void calculate_buffer_num();

  /** 
   * Calculates the buffer sizes based on the array type.
   *
   * @return void
   */
  void calculate_buffer_sizes();

  /** 
   * Calculates the info used in the copy_tile_slab() function, for the case
   * where the **user** cell order is column-major and the **array** cell 
   * order is column-major.
   *
   * @template T The domain type.
   * @param data Essentially a pointer to a ASWS_Data object.
   * @return void
   */
  template<class T>
  static void *calculate_cell_slab_info_col_col_s(void* data);

  /** 
   * Calculates the info used in the copy_tile_slab() function, for the case
   * where the **user** cell order is column-major and the **array** cell 
   * order is row-major.
   *
   * @template T The domain type.
   * @param data Essentially a pointer to a ASWS_Data object.
   * @return void
   */
  template<class T>
  static void *calculate_cell_slab_info_col_row_s(void* data);

  /** 
   * Calculates the info used in the copy_tile_slab() function, for the case
   * where the **user** cell order in row-major and the **array** cell 
   * order is column-major.
   *
   * @template T The domain type.
   * @param data Essentially a pointer to a AWRS_Data object.
   * @return void
   */
  template<class T>
  static void *calculate_cell_slab_info_row_col_s(void* data);

  /** 
   * Calculates the info used in the copy_tile_slab() function, for the case
   * where the **user** cell order is row-major and the **array** cell 
   * order is row-major.
   *
   * @template T The domain type.
   * @param data Essentially a pointer to a AWRS_Data object.
   * @return void
   */
  template<class T>
  static void *calculate_cell_slab_info_row_row_s(void* data);

  /** 
   * Calculates the info used in the copy_tile_slab() function, for the case
   * where the **user** cell order is column-major and the **array** cell 
   * order is column-major.
   *
   * @param T The domain type.
   * @param id The tile slab id.
   * @param tid The tile id.
   * @return void.
   */
  template<class T>
  void calculate_cell_slab_info_col_col(int id, int64_t tid);

  /** 
   * Calculates the info used in the copy_tile_slab() function, for the case
   * where the **user** cell order is column-major and the **array** cell 
   * order is row-major.
   *
   * @param T The domain type.
   * @param id The tile slab id.
   * @param tid The tile id.
   * @return void.
   */
  template<class T>
  void calculate_cell_slab_info_col_row(int id, int64_t tid);
 
  /** 
   * Calculates the info used in the copy_tile_slab() function, for the case
   * where the **user** cell order is row-major and the **array** cell 
   * order is row-major.
   *
   * @param T The domain type.
   * @param id The tile slab id.
   * @param tid The tile id.
   * @return void.
   */
  template<class T>
  void calculate_cell_slab_info_row_row(int id, int64_t tid);

  /** 
   * Calculates the info used in the copy_tile_slab() function, for the case
   * where the **user** cell order is row-major and the **array** cell 
   * order is column-major.
   *
   * @param T The domain type.
   * @param id The tile slab id.
   * @param tid The tile id.
   * @return void.
   */
  template<class T>
  void calculate_cell_slab_info_row_col(int id, int64_t tid);

  /** 
   * Calculates the info used in the copy_tile_slab() function, for the case
   * where the **array** cell order is row-major.
   *
   * @param T The domain type.
   * @param id The tile slab id.
   * @param tid The tile id.
   * @return void.
   */
  template<class T>
  void calculate_cell_slab_info_row(int id, int64_t tid);

  /** 
   * Calculates the **normalized** tile domain overlapped by the input tile 
   * slab. Note that this domain is the same for all tile slabs
   *
   * @param T The domain type.
   * @param id The tile slab id.
   * @return void.
   */ 
  template<class T>
  void calculate_tile_domain(int id);

  /** 
   * Calculates the info used in the copy_tile_slab() function.
   *
   * @param T The domain type.
   * @param id The tile slab id.
   * @return void.
   */
  template<class T>
  void calculate_tile_slab_info(int id);

  /** 
   * Calculates tile slab info for the case where the **array** tile order is
   * column-major
   *
   * @template T The domain type.
   * @param data Essentially a pointer to a ASWS_Data object.
   * @return void
   */
  template<class T>
  static void *calculate_tile_slab_info_col(void* data);

  /** 
   * Calculates the info used in the copy_tile_slab() function, for the case
   * where the **array** tile order is column-major.
   *
   * @param T The domain type.
   * @param id The tile slab id.
   * @return void.
   */
  template<class T>
  void calculate_tile_slab_info_col(int id);
 
  /** 
   * Calculates tile slab info for the case where the **array** tile order is
   * row-major
   *
   * @template T The domain type.
   * @param data Essentially a pointer to a ASWS_Data object.
   * @return void
   */
  template<class T>
  static void *calculate_tile_slab_info_row(void* data);

  /** 
   * Calculates the info used in the copy_tile_slab() function, for the case
   * where the **array** tile order is row-major.
   *
   * @param T The domain type.
   * @param id The tile slab id.
   * @return void.
   */
  template<class T>
  void calculate_tile_slab_info_row(int id);

  /**
   * Function called by the AIO thread. 
   *
   * @param context This is practically the ArraySortedWriteState object for 
   *     which the function is called (typically *this* is passed to this 
   *     argument by the caller).
   */
  static void *aio_handler(void* context);

  /** 
   * Copies a tile slab from the user buffers into the local buffers, 
   * properly re-organizing the cell order to follow the array global
   * cell order.  
   * 
   * @return void.
   */
  void copy_tile_slab();

  /** 
   * Copies a tile slab from the local buffers into the user buffers, 
   * properly re-organizing the cell order to fit the targeted order,
   * focusing on a particular fixed-length attribute.
   * 
   * @template T The attribute type.
   * @param aid The index on attribute_ids_ to focus on.
   * @param bid The index on the copy state buffers to focus on.
   * @return void.
   */
  template<class T>
  void copy_tile_slab(int aid, int bid);

  /** 
   * Copies a tile slab from the local buffers into the user buffers, 
   * properly re-organizing the cell order to fit the targeted order,
   * focusing on a particular variable-length attribute.
   * 
   * @template T The attribute type.
   * @param aid The index on attribute_ids_ to focus on.
   * @param bid The index on the copy state buffers to focus on.
   * @return void.
   */
  template<class T>
  void copy_tile_slab_var(int aid, int bid);

  /** 
   * Creates the copy state buffers.
   *
   * @return TILEDB_ASWS_OK for success and TILEDB_ASWS_ERR for error.
   */
  int create_copy_state_buffers();

  /** 
   * Creates the user buffers.
   *
   * @param buffers The user buffers that hold the input cells to be written. 
   * @param buffer_sizes The corresponding buffer sizes. 
   * @return void
   */
  void create_user_buffers(const void** buffers, const size_t* buffer_sizes);

  /**
   * Fills the **entire** buffer of the current copy tile slab with the input id
   * with empty values, based on the template type. Applicable only to
   * fixed-sized attributes.
   *
   * @template T The attribute type.
   * @param bid The buffer id corresponding to the targeted attribute.
   * @return void
   */
  template<class T>
  void fill_with_empty(int bid);

  /**
   * Fills the **a single** cell in a variable-sized buffer of the current copy 
   * tile slab with the input id with an empty value, based on the template t
   * ype. Applicable only to variable-sized attributes.
   *
   * @template T The attribute type.
   * @param bid The buffer id corresponding to the targeted attribute.
   * @return void
   */
  template<class T>
  void fill_with_empty_var(int bid);

  /** Frees the copy state. */
  void free_copy_state();

  /** Frees the tile slab info. */
  void free_tile_slab_info();

  /** Frees the tile slab state. */
  void free_tile_slab_state();

  /** 
   * Returns the cell id along the **array** order for the current coordinates
   * in the tile slab state for a particular attribute. 
   *
   * @template T The domain type.
   * @param aid The targeted attribute.
   * @return The cell id. 
   */
  template<class T>
  int64_t get_cell_id(int aid);

  /** 
   * Returns the tile id along the **array** order for the current coordinates
   * in the tile slab state for a particular attribute. 
   *
   * @template T The domain type.
   * @param aid The targeted attribute.
   * @return The tile id. 
   */
  template<class T>
  int64_t get_tile_id(int aid);

  /** 
   * Handles the AIO requests. 
   *
   * @template T The domain type.
   * @return void.
   */
  template<class T>
  void handle_aio_requests();

  /** Initializes the AIO requests. */
  void init_aio_requests();

  /** Initializes the copy state. */
  void init_copy_state();

  /** Initializes the tile slab info. */
  void init_tile_slab_info();

  /** 
   * Initializes the tile slab info for a particular tile slab, using the
   * input tile number.
   *
   * @template T The domain type.
   * @param id The slab id.
   * @return void.
   */
  template<class T>
  void init_tile_slab_info(int id);

  /** Initializes the tile slab state. */
  void init_tile_slab_state();

  /**  
   * Locks the AIO mutex. 
   * 
   * @return TILEDB_ASWS_OK for success and TILEDB_ASWS_ERR for error.
   */
  int lock_aio_mtx();

  /**  
   * Locks the copy mutex. 
   * 
   * @return TILEDB_ASWS_OK for success and TILEDB_ASWS_ERR for error.
   */
  int lock_copy_mtx();

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
   * Same as Array::write(), but it sorts the cells in the buffers based on the
   * global cell order prior to writing them on disk. Note that this function 
   * will fail if there is not enough system memory to hold the cells of a 
   * 'tile slab' overlapping with the selected subarray.
   *
   * @template T The domain type.
   * @return TILEDB_ASWS_OK for success and TILEDB_ASWS_ERR for error.
   */
  template<class T>
  int write(); 

  /**
   * Same as write(), but the cells are provided by the user sorted in 
   * column-major order with respect to the selected subarray. 
   * 
   * @template T The domain type.
   * @return TILEDB_ASWS_OK for success and TILEDB_ASWS_ERR for error.
   */
  template<class T>
  int write_sorted_col(); 

  /**
   * Same as write(), but the cells are provided by the user sorted in 
   * row-major order with respect to the selected subarray. 
   * 
   * @template T The domain type.
   * @return TILEDB_ASWS_OK for success and TILEDB_ASWS_ERR for error.
   */
  template<class T>
  int write_sorted_row(); 

  /** 
   * Signals an AIO condition. 
   * 
   * @param id The id of the AIO condition to be signaled.
   * @return TILEDB_ASWS_OK for success and TILEDB_ASWS_ERR for error.
   */
  int release_aio(int id);

  /** 
   * Signals a copy condition. 
   * 
   * @param id The id of the copy condition to be signaled.
   * @return TILEDB_ASWS_OK for success and TILEDB_ASWS_ERR for error.
   */
  int release_copy(int id);

  /** Resets the copy state for the current copy id. */
  void reset_copy_state();

  /** 
   * Resets the tile_coords_ auxiliary variable. 
   *
   * @template T The domain type.
   * @return void.
   */
  template<class T> 
  void reset_tile_coords();
  
  /** 
   * Resets the tile slab state. 
   *
   * @template T The domain type.
   * @return void.
   */
  template<class T> 
  void reset_tile_slab_state();

  /** 
   * Sends an AIO request. 
   *
   * @param aio_id The id of the tile slab the AIO request focuses on.
   * @return TILEDB_ASWS_OK for success and TILEDB_ASWS_ERR for error.
   */
  int send_aio_request(int aio_id);

  /**  
   * Unlocks the AIO mutex. 
   * 
   * @return TILEDB_ASWS_OK for success and TILEDB_ASWS_ERR for error.
   */
  int unlock_aio_mtx();

  /**  
   * Unlocks the copy mutex. 
   * 
   * @return TILEDB_ASWS_OK for success and TILEDB_ASWS_ERR for error.
   */
  int unlock_copy_mtx();

  /**
   * Calculates the new tile and local buffer offset for the new (already 
   * computed) current cell coordinates in the tile slab. 
   *
   * @param aid The attribute id to focus on.
   * @return void.
   */
  void update_current_tile_and_offset(int aid);

  /**
   * Calculates the new tile and local buffer offset for the new (already 
   * computed) current cell coordinates in the tile slab. 
   *
   * @template T The domain type
   * @param aid The attribute id to focus on.
   * @return void.
   */
  template<class T> 
  void update_current_tile_and_offset(int aid);

  /**
   * Waits on a copy operation for the buffer with input id to finish.
   *
   * @param id The id of the buffer the copy operation must be completed.
   * @return TILEDB_ASWS_OK for success and TILEDB_ASWS_ERR for error.
   */
  int wait_copy(int id);

  /**
   * Waits on a AIO operation for the buffer with input id to finish.
   *
   * @param id The id of the buffer the AIO operation must be completed.
   * @return TILEDB_ASWS_OK for success and TILEDB_ASWS_ERR for error.
   */
  int wait_aio(int id);
};

#endif
