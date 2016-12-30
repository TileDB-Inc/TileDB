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

/** Initial internal buffer size for the case of sparse arrays. */
#define TILEDB_ASRS_INIT_BUFFER_SIZE 10000000 // ~ 10MB




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

  /** Used in functors. */
  struct ASRS_Data {
    /** An id (typically an attribute id or a tile slab id. */
    int id_;
    /** Another id (typically a tile id). */
    int64_t id_2_;
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
     * Start offsets of each tile in the local buffer, per attribute per tile.
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
    /** 
     * Applicable only to the sparse case. It holds the current cell position
     * to be considered, per attribute.
     */
    int64_t* current_cell_pos_;
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
   * Returns true if copying into the user buffers resulted in overflow, for
   * the input attribute id. 
   */
  bool overflow(int attribute_id) const;

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
   * Initializes the array sorted read state. 
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

  /** AIO counter. */
  int aio_cnt_;

  /** The AIO mutex conditions (one for each buffer). */
  pthread_cond_t aio_cond_[2];

  /** Data for the AIO requests. */
  ASRS_Data aio_data_[2];

  /** The current id of the buffers the next AIO will occur into. */
  int aio_id_;

  /** The AIO mutex. */
  pthread_mutex_t aio_mtx_;
  
  /** Indicates overflow per tile slab per attribute upon an AIO operation. */
  bool* aio_overflow_[2];

  /** AIO requests. */
  AIO_Request aio_request_[2];

  /** The status of the AIO requests.*/
  int aio_status_[2];

  /** The array this sorted read state belongs to. */
  Array* array_;

  /** The ids of the attributes the array was initialized with. */
  std::vector<int> attribute_ids_;

  /**
   * The sizes of the attributes. For variable-length attributes, sizeof(size_t)
   * is stored.
   */
  std::vector<size_t> attribute_sizes_;

  /** Number of allocated buffers. */
  int buffer_num_;

  /** Allocated sizes for buffers_ (similar to those used in Array::read). */
  size_t* buffer_sizes_[2];

  /** Temporary buffer sizes used in AIO requests. */
  size_t* buffer_sizes_tmp_[2];

  /**
   * Backup of temporary buffer sizes used in AIO requests (used when there is
   * overflow).
   */
  size_t* buffer_sizes_tmp_bak_[2];

  /** Local buffers (similar to those used in Array::read). */
  void** buffers_[2];

  /** Function for calculating cell slab info during a copy operation. */
  void *(*calculate_cell_slab_info_) (void*);

  /** Function for calculating tile slab info during a copy operation. */
  void *(*calculate_tile_slab_info_) (void*);

  /** 
   * Used only in the sparse case. Holds the sorted positions of the cells
   * for the current tile slab to be copied. 
   */
  std::vector<int64_t> cell_pos_;

  /** 
   * Used only in the sparse case. It is the element index in attribute_ids_
   * that represents the coordinates attribute.
   */
  int coords_attr_i_;

  /** 
   * Used only in the sparse case. It is the element index in buffers_
   * that represents the coordinates attribute.
   */
  int coords_buf_i_;

  /** The coordinates size of the array. */
  size_t coords_size_;

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

  /** True if the copy thread is canceled. */
  volatile bool copy_thread_canceled_;

  /** True if the copy thread is running. */
  volatile bool copy_thread_running_;

  /** The number of dimensions in the array. */
  int dim_num_;

  /** 
   * Used only in the sparse case. It is true if the coordinates are not asked
   * by the user and, thus, TileDB had to append them as an extra attribute
   * to facilitate sorting the cell positions.
   *
   */
  bool extra_coords_;

  /** The overflow mutex condition. */
  pthread_cond_t overflow_cond_;

  /** The overflow mutex. */
  pthread_mutex_t overflow_mtx_;

  /** Overflow flag for each attribute. */
  bool* overflow_;

  /** 
   * Overflow flag for each attribute. It starts with *true* for all 
   * attributes, and becomes false once an attribute does not overflow any more.
   */
  bool* overflow_still_;

  /** True if no more tile slabs to read. */
  bool read_tile_slabs_done_;

  /** True if a copy must be resumed. */
  bool resume_copy_;

  /** True if an AIO must be resumed. */
  bool resume_aio_;

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
   * @param data Essentially a pointer to a ASRS_Data object.
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
   * @param data Essentially a pointer to a ASRS_Data object.
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
   * @param data A ASRS_Request object.
   * @return void 
   */
  static void *aio_done(void* data);

  /** True if some attribute overflowed for the input tile slab upon an AIO. */
  bool aio_overflow(int aio_id);

  /** Sets the flag of wait_aio_[id] to true. */
  void block_aio(int id);

  /** Sets the flag of wait_copy_[id] to true. */
  void block_copy(int id);

  /** Sets the flag of resume_copy_ to true. */
  void block_overflow();

  /** 
   * Calculate the attribute ids specified by the user upon array 
   * initialization. 
   */
  void calculate_attribute_ids();

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
   * Calculates the buffer sizes based on the subarray and the number of cells
   * in a (full) tile slab. Applicable to dense arrays.
   *
   * @return void
   */
  void calculate_buffer_sizes_dense();

  /** 
   * Calculates the buffer sizes based on configurable parameters. Applicable to
   * sparse arrays.
   *
   * @return void
   */
  void calculate_buffer_sizes_sparse();

  /** 
   * Calculates the info used in the copy_tile_slab() function, for the case
   * where the **user** cell order is column-major and the **array** cell 
   * order is column-major.
   *
   * @template T The domain type.
   * @param data Essentially a pointer to a ASRS_Data object.
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
   * @param data Essentially a pointer to a ASRS_Data object.
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
   * @param data Essentially a pointer to a ASRS_Data object.
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
   * @param data Essentially a pointer to a ASRS_Data object.
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
   * @param data Essentially a pointer to a ASRS_Data object.
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
   * @param data Essentially a pointer to a ASRS_Data object.
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
   * Applicable to dense arrays.
   * 
   * @return void.
   */
  void copy_tile_slab_dense();

  /** 
   * Copies a tile slab from the local buffers into the user buffers, 
   * properly re-organizing the cell order to fit the targeted order.  
   * Applicable to sparse arrays.
   * 
   * @return void.
   */
  void copy_tile_slab_sparse();

  /** 
   * Copies a tile slab from the local buffers into the user buffers, 
   * properly re-organizing the cell order to fit the targeted order,
   * focusing on a particular fixed-length attribute.
   * Applicable to dense arrays.
   * 
   * @param aid The index on attribute_ids_ to focus on.
   * @param bid The index on the copy state buffers to focus on.
   * @return void.
   */
  void copy_tile_slab_dense(int aid, int bid);

  /** 
   * Copies a tile slab from the local buffers into the user buffers, 
   * properly re-organizing the cell order to fit the targeted order,
   * focusing on a particular fixed-length attribute.
   * Applicable to sparse arrays.
   * 
   * @param aid The index on attribute_ids_ to focus on.
   * @param bid The index on the copy state buffers to focus on.
   * @return void.
   */
  void copy_tile_slab_sparse(int aid, int bid);

  /** 
   * Copies a tile slab from the local buffers into the user buffers, 
   * properly re-organizing the cell order to fit the targeted order,
   * focusing on a particular variable-length attribute.
   * Applicable to dense arrays.
   * 
   * @param aid The index on attribute_ids_ to focus on.
   * @param bid The index on the copy state buffers to focus on.
   * @return void.
   */
  void copy_tile_slab_dense_var(int aid, int bid);

  /** 
   * Copies a tile slab from the local buffers into the user buffers, 
   * properly re-organizing the cell order to fit the targeted order,
   * focusing on a particular variable-length attribute.
   * Applicable to sparse arrays.
   * 
   * @param aid The index on attribute_ids_ to focus on.
   * @param bid The index on the copy state buffers to focus on.
   * @return void.
   */
  void copy_tile_slab_sparse_var(int aid, int bid);

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
   * Handles the copy requests. Applicable to dense arrays. 
   *
   * @template T The domain type.
   * @return void.
   */
  template<class T>
  void handle_copy_requests_dense();

  /** 
   * Handles the copy requests. Applicable to sparse arrays. 
   *
   * @template T The domain type.
   * @return void.
   */
  template<class T>
  void handle_copy_requests_sparse();

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
   * Retrieves the next column tile slab to be processed. Applicable to dense
   * arrays.
   * 
   * @template T The domain type.
   * @return True if the next tile slab was retrieved, and false otherwise.
   */
  template<class T>
  bool next_tile_slab_dense_col();

  /** 
   * Retrieves the next row tile slab to be processed. Applicable to dense 
   * arrays.
   * 
   * @template T The domain type.
   * @return True if the next tile slab was retrieved, and false otherwise.
   */
  template<class T>
  bool next_tile_slab_dense_row();

  /** 
   * Retrieves the next column tile slab to be processed. Applicable to sparse
   * arrays.
   * 
   * @template T The domain type.
   * @return True if the next tile slab was retrieved, and false otherwise.
   */
  template<class T>
  bool next_tile_slab_sparse_col();

  /** 
   * Retrieves the next row tile slab to be processed. Applicable to sparse
   * arrays.
   * 
   * @template T The domain type.
   * @return True if the next tile slab was retrieved, and false otherwise.
   */
  template<class T>
  bool next_tile_slab_sparse_row();

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
   * @return TILEDB_ASRS_OK for success and TILEDB_ASRS_ERR for error.
   */
  template<class T>
  int read_dense_sorted_col(); 

  /**
   * Same as read(), but the cells are placed in 'buffers' sorted in 
   * row-major order with respect to the selected subarray. 
   * Applicable only to dense arrays. 
   * 
   * @template T The domain type.
   * @return TILEDB_ASRS_OK for success and TILEDB_ASRS_ERR for error.
   */
  template<class T>
  int read_dense_sorted_row(); 

  /**
   * Same as read(), but the cells are placed in 'buffers' sorted in 
   * column-major order with respect to the selected subarray. 
   * Applicable only to sparse arrays. 
   * 
   * @template T The domain type.
   * @return TILEDB_ASRS_OK for success and TILEDB_ASRS_ERR for error.
   */
  template<class T>
  int read_sparse_sorted_col(); 

  /**
   * Same as read(), but the cells are placed in 'buffers' sorted in 
   * row-major order with respect to the selected subarray. 
   * Applicable only to sparse arrays. 
   * 
   * @template T The domain type.
   * @return TILEDB_ASRS_OK for success and TILEDB_ASRS_ERR for error.
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

  /** Resets the AIO overflow flags for the input tile slab id. */
  void reset_aio_overflow(int aio_id);
  
  /** Resets the temporary buffer sizes for the input tile slab id. */
  void reset_buffer_sizes_tmp(int id);

  /** Resets the copy state using the input buffer info. */
  void reset_copy_state(void** buffers, size_t* buffer_sizes);

  /** Resets the oveflow flags to **false**. */
  void reset_overflow();

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
   * @return TILEDB_ASRS_OK for success and TILEDB_ASRS_ERR for error.
   */
  int send_aio_request(int aio_id);

  /** 
   * It sorts the positions of the cells based on the coordinates
   * of the current tile slab to be copied.
   */
  template<class T>
  void sort_cell_pos();

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
