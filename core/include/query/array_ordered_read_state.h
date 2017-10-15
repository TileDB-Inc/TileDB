/**
 * @file   array_ordered_read_state.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017 TileDB, Inc.
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
 * This file defines class ArrayOrderedReadState.
 */

#ifndef TILEDB_ARRAY_ORDERED_READ_STATE_H
#define TILEDB_ARRAY_ORDERED_READ_STATE_H

#include "query.h"
#include "status.h"

#include <condition_variable>
#include <mutex>
#include <string>
#include <vector>

namespace tiledb {

class Query;

/**
 * Stores the state necessary when reading cells from the array fragments,
 * sorted in a way different to the global cell order.
 */
class ArrayOrderedReadState {
 public:
  /* ********************************* */
  /*          TYPE DEFINITIONS         */
  /* ********************************* */

  /** Used in functors. */
  struct ASRS_Data {
    /** An id (typically an attribute id or a tile slab id. */
    unsigned int id_;
    /** Another id (typically a tile id). */
    uint64_t id_2_;
    /** The calling object. */
    ArrayOrderedReadState* asrs_;
  };

  /** Stores state about the current read/copy request. */
  struct CopyState {
    /** Current offsets in user buffers. */
    uint64_t* buffer_offsets_;
    /** User buffer sizes. */
    uint64_t* buffer_sizes_;
    /** User buffers. */
    void** buffers_;
  };

  /** Info about a tile slab. */
  struct TileSlabInfo {
    /** Used in calculations of cell ids, one vector per tile. */
    uint64_t** cell_offset_per_dim_;
    /** Cell slab size per attribute per tile. */
    uint64_t** cell_slab_size_;
    /** Number of cells in a cell slab per tile. */
    uint64_t* cell_slab_num_;
    /**
     * The range overlap of the **normalized** tile slab with each
     *  **normalized** tile range.
     */
    void** range_overlap_;
    /**
     * Start offsets of each tile in the local buffer, per attribute per tile.
     */
    uint64_t** start_offsets_;
    /** Number of tiles in the tile slab. */
    uint64_t tile_num_;
    /** Used in calculations of tile ids. */
    uint64_t* tile_offset_per_dim_;
  };

  /** The state for a tile slab copy. */
  struct TileSlabState {
    /** Keeps track of whether a tile slab copy for an attribute id done. */
    bool* copy_tile_slab_done_;
    /**
     * Applicable only to the sparse case. It holds the current cell position
     * to be considered, per attribute.
     */
    uint64_t* current_cell_pos_;
    /** Current coordinates in tile slab per attribute. */
    void** current_coords_;
    /**
     * The offset in the local buffers of the next cell slab to be copied per
     * attribute. Note that this applies only to fixed-sized attributes
     * because the offsets of the variable-sized attributes can be derived from
     * the buffers that hold the fixed-sized offsets.
     */
    uint64_t* current_offsets_;
    /** The current tile per attribute. */
    uint64_t* current_tile_;
  };

  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /**
   * Constructor.
   *
   * @param array The query this array sorted read state belongs to.
   */
  explicit ArrayOrderedReadState(Query* query);

  /** Destructor. */
  ~ArrayOrderedReadState();

  /* ********************************* */
  /*             ACCESSORS             */
  /* ********************************* */

  /** Returns true if the current slab is finished being copied. */
  bool copy_tile_slab_done() const;

  /** True if read is done for all attributes. */
  bool done() const;

  /**
   * Finalizes the object, and particularly the internal async queries.
   * The async queries will be finalized in the destructor anyway, but
   * this function allows capturing errors upon query finalization.
   */
  Status finalize();

  /**
   * Initializes the array sorted read state.
   *
   * @return Status
   */
  Status init();

  /** Returns true if copying into the user buffers resulted in overflow. */
  bool overflow() const;

  /**
   * Returns true if copying into the user buffers resulted in overflow, for
   * the input attribute id.
   */
  bool overflow(unsigned int attribute_id) const;

  /**
   * The read operation. It will store the results in the input
   * buffers in sorted row- or column-major order.
   *
   * @param buffers The input buffers.
   * @param buffer_sizes The corresponding buffer sizes.
   * @return Status
   */
  Status read(void** buffers, uint64_t* buffer_sizes);

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** Function for advancing a cell slab during a copy operation. */
  void* (*advance_cell_slab_)(void*);

  /** Condition variables used in internal async queries. */
  std::condition_variable async_cv_[2];

  /** Data for the internal async queries. */
  ASRS_Data async_data_[2];

  /** Mutexes used in internal async queries. */
  std::mutex async_mtx_[2];

  /** The internal async queries. */
  Query* async_query_[2];

  /** Wait for async conditions, one for each local buffer. */
  bool async_wait_[2];

  /** The ids of the attributes the array was initialized with. */
  std::vector<unsigned int> attribute_ids_;

  /**
   * The sizes of the attributes. For variable-length attributes,
   * sizeof(uint64_t) is stored.
   */
  std::vector<uint64_t> attribute_sizes_;

  /** Number of allocated buffers. */
  unsigned int buffer_num_;

  /** Allocated sizes for buffers_. */
  uint64_t* buffer_sizes_[2];

  /** Temporary buffer sizes used in internal async queries. */
  uint64_t* buffer_sizes_tmp_[2];

  /**
   * Backup of temporary buffer sizes used in async queries (used when there is
   * overflow).
   */
  uint64_t* buffer_sizes_tmp_bak_[2];

  /** Local buffers. */
  void** buffers_[2];

  /** Function for calculating cell slab info during a copy operation. */
  void* (*calculate_cell_slab_info_)(void*);

  /** Function for calculating tile slab info during a copy operation. */
  void* (*calculate_tile_slab_info_)(void*);

  /**
   * Used only in the sparse case. Holds the sorted positions of the cells
   * for the current tile slab to be copied.
   */
  std::vector<uint64_t> cell_pos_;

  /**
   * Used only in the sparse case. It is the element index in attribute_ids_
   * that represents the coordinates attribute.
   */
  unsigned int coords_attr_i_;

  /**
   * Used only in the sparse case. It is the element index in buffers_
   * that represents the coordinates attribute.
   */
  unsigned int coords_buf_i_;

  /** The coordinates size of the array. */
  uint64_t coords_size_;

  /** The current id of the buffers the next copy will occur from. */
  unsigned int copy_id_;

  /** The copy state. */
  CopyState copy_state_;

  /** The number of dimensions in the array. */
  unsigned int dim_num_;

  /**
   * Used only in the sparse case. It is true if the coordinates are not asked
   * by the user and, thus, TileDB had to append them as an extra attribute
   * to facilitate sorting the cell positions.
   *
   */
  bool extra_coords_;

  /** Overflow flag for each attribute. */
  bool* overflow_;

  /**
   * Overflow flag for each attribute. It starts with *true* for all
   * attributes, and becomes false once an attribute does not overflow any more.
   */
  bool* overflow_still_;

  /** The query the array sorted read state belongs to. */
  Query* query_;

  /** True if no more tile slabs to read. */
  bool read_tile_slabs_done_;

  /** Used to handle overflow. */
  bool resume_copy_;

  /** Used to handle overflow. */
  bool resume_copy_2_;

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

  /* ********************************* */
  /*          STATIC CONSTANTS         */
  /* ********************************* */

  /** Indicates an invalid uint64 value. */
  static const uint64_t INVALID_UINT64;

  /** Indicates an invalid unsigned int value. */
  static const unsigned int INVALID_UNIT;

  /* ********************************* */
  /*           PRIVATE METHODS         */
  /* ********************************* */

  /**
   * Advances a cell slab focusing on column-major order, and updates
   * the CopyState and TileSlabState.
   * Used in copy_tile_slab().
   *
   * @tparam T The domain type.
   * @param data Essentially a pointer to a ASRS_Data object.
   * @return void
   */
  template <class T>
  static void* advance_cell_slab_col_s(void* data);

  /**
   * Advances a cell slab focusing on row-major order, and updates
   * the CopyState and TileSlabState.
   * Used in copy_tile_slab().
   *
   * @tparam T The domain type.
   * @param data Essentially a pointer to a ASRS_Data object.
   * @return void
   */
  template <class T>
  static void* advance_cell_slab_row_s(void* data);

  /**
   * Advances a cell slab when the requested order is column-major.
   *
   * @tparam T The domain type.
   * @param aid The id of the attribute in attribute_ids_ to focus on.
   * @return void
   */
  template <class T>
  void advance_cell_slab_col(unsigned int aid);

  /**
   * Advances a cell slab when the requested order is row-major.
   *
   * @tparam T The domain type.
   * @param aid The id of the attribute in attribute_ids_ to focus on.
   * @return void
   */
  template <class T>
  void advance_cell_slab_row(unsigned int aid);

  /**
   * Called when an AIO completes.
   *
   * @param data A ASRS_Request object.
   * @return void
   */
  static void* async_done(void* data);

  /** Notifies async conditions on the input tile slab id. */
  void async_notify(unsigned int id);

  /**
   * Submits an internal async query.
   *
   * @param id The id of the tile slab the AIO request focuses on.
   * @return Status
   */
  Status async_submit_query(unsigned int id);

  /** Waits for async conditions on the input tile slab id. */
  void async_wait(unsigned int id);

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
   * @tparam T The domain type.
   * @param data Essentially a pointer to a ASRS_Data object.
   * @return void
   */
  template <class T>
  static void* calculate_cell_slab_info_col_col_s(void* data);

  /**
   * Calculates the info used in the copy_tile_slab() function, for the case
   * where the **user** cell order is column-major and the **array** cell
   * order is row-major.
   *
   * @tparam T The domain type.
   * @param data Essentially a pointer to a ASRS_Data object.
   * @return void
   */
  template <class T>
  static void* calculate_cell_slab_info_col_row_s(void* data);

  /**
   * Calculates the info used in the copy_tile_slab() function, for the case
   * where the **user** cell order in row-major and the **array** cell
   * order is column-major.
   *
   * @tparam T The domain type.
   * @param data Essentially a pointer to a ASRS_Data object.
   * @return void
   */
  template <class T>
  static void* calculate_cell_slab_info_row_col_s(void* data);

  /**
   * Calculates the info used in the copy_tile_slab() function, for the case
   * where the **user** cell order is row-major and the **array** cell
   * order is row-major.
   *
   * @tparam T The domain type.
   * @param data Essentially a pointer to a ASRS_Data object.
   * @return void
   */
  template <class T>
  static void* calculate_cell_slab_info_row_row_s(void* data);

  /**
   * Calculates the info used in the copy_tile_slab() function, for the case
   * where the **user** cell order is column-major and the **array** cell
   * order is column-major.
   *
   * @tparam T The domain type.
   * @param id The tile slab id.
   * @param tid The tile id.
   * @return void.
   */
  template <class T>
  void calculate_cell_slab_info_col_col(unsigned int id, uint64_t tid);

  /**
   * Calculates the info used in the copy_tile_slab() function, for the case
   * where the **user** cell order is column-major and the **array** cell
   * order is row-major.
   *
   * @tparam T The domain type.
   * @param id The tile slab id.
   * @param tid The tile id.
   * @return void.
   */
  template <class T>
  void calculate_cell_slab_info_col_row(unsigned int id, uint64_t tid);

  /**
   * Calculates the info used in the copy_tile_slab() function, for the case
   * where the **user** cell order is row-major and the **array** cell
   * order is row-major.
   *
   * @tparam T The domain type.
   * @param id The tile slab id.
   * @param tid The tile id.
   * @return void.
   */
  template <class T>
  void calculate_cell_slab_info_row_row(unsigned int id, uint64_t tid);

  /**
   * Calculates the info used in the copy_tile_slab() function, for the case
   * where the **user** cell order is row-major and the **array** cell
   * order is column-major.
   *
   * @tparam T The domain type.
   * @param id The tile slab id.
   * @param tid The tile id.
   * @return void.
   */
  template <class T>
  void calculate_cell_slab_info_row_col(unsigned int id, uint64_t tid);

  /**
   * Calculates the **normalized** tile domain overlapped by the input tile
   * slab. Note that this domain is the same for all tile slabs
   *
   * @tparam T The domain type.
   * @param id The tile slab id.
   * @return void.
   */
  template <class T>
  void calculate_tile_domain(unsigned int id);

  /**
   * Calculates the info used in the copy_tile_slab() function.
   *
   * @tparam T The domain type.
   * @param id The tile slab id.
   * @return void.
   */
  template <class T>
  void calculate_tile_slab_info(unsigned int id);

  /**
   * Calculates tile slab info for the case where the **array** tile order is
   * column-major
   *
   * @tparam T The domain type.
   * @param data Essentially a pointer to a ASRS_Data object.
   * @return void
   */
  template <class T>
  static void* calculate_tile_slab_info_col(void* data);

  /**
   * Calculates the info used in the copy_tile_slab() function, for the case
   * where the **array** tile order is column-major.
   *
   * @tparam T The domain type.
   * @param id The tile slab id.
   * @return void.
   */
  template <class T>
  void calculate_tile_slab_info_col(unsigned int id);

  /**
   * Calculates tile slab info for the case where the **array** tile order is
   * row-major
   *
   * @tparam T The domain type.
   * @param data Essentially a pointer to a ASRS_Data object.
   * @return void
   */
  template <class T>
  static void* calculate_tile_slab_info_row(void* data);

  /**
   * Calculates the info used in the copy_tile_slab() function, for the case
   * where the **array** tile order is row-major.
   *
   * @tparam T The domain type.
   * @param id The tile slab id.
   * @return void.
   */
  template <class T>
  void calculate_tile_slab_info_row(unsigned int id);

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
  void copy_tile_slab_dense(unsigned int aid, unsigned int bid);

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
  void copy_tile_slab_sparse(unsigned int aid, unsigned int bid);

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
  void copy_tile_slab_dense_var(unsigned int aid, unsigned int bid);

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
  void copy_tile_slab_sparse_var(unsigned int aid, unsigned int bid);

  /**
   * Creates the buffers based on the calculated buffer sizes.
   *
   * @return Status
   */
  Status create_buffers();

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
   * @tparam T The domain type.
   * @param aid The targeted attribute.
   * @return The cell id.
   */
  template <class T>
  uint64_t get_cell_id(unsigned int aid);

  /**
   * Returns the tile id along the **array** order for the current coordinates
   * in the tile slab state for a particular attribute.
   *
   * @tparam T The domain type.
   * @param aid The targeted attribute.
   * @return The tile id.
   */
  template <class T>
  uint64_t get_tile_id(unsigned int aid);

  /** Initializes the copy state. */
  void init_copy_state();

  /** Initializes the tile slab info. */
  void init_tile_slab_info();

  /**
   * Initializes the tile slab info for a particular tile slab, using the
   * input tile number.
   *
   * @tparam T The domain type.
   * @param id The slab id.
   * @return void.
   */
  template <class T>
  void init_tile_slab_info(unsigned int id);

  /** Initializes the tile slab state. */
  void init_tile_slab_state();

  /**
   * Retrieves the next column tile slab to be processed. Applicable to dense
   * arrays.
   *
   * @tparam T The domain type.
   * @return True if the next tile slab was retrieved, and false otherwise.
   */
  template <class T>
  bool next_tile_slab_dense_col();

  /**
   * Retrieves the next row tile slab to be processed. Applicable to dense
   * arrays.
   *
   * @tparam T The domain type.
   * @return True if the next tile slab was retrieved, and false otherwise.
   */
  template <class T>
  bool next_tile_slab_dense_row();

  /**
   * Retrieves the next column tile slab to be processed. Applicable to sparse
   * arrays.
   *
   * @tparam T The domain type.
   * @return True if the next tile slab was retrieved, and false otherwise.
   */
  template <class T>
  bool next_tile_slab_sparse_col();

  /**
   * Retrieves the next row tile slab to be processed. Applicable to sparse
   * arrays.
   *
   * @tparam T The domain type.
   * @return True if the next tile slab was retrieved, and false otherwise.
   */
  template <class T>
  bool next_tile_slab_sparse_row();

  /**
   * The read operation. It will store the results in the input
   * buffers in sorted row- or column-major order.
   *
   * @tparam T The domain type.
   * @return Status
   */
  template <class T>
  Status read();

  /**
   * Same as read(), but the cells are placed in 'buffers' sorted in
   * column-major order with respect to the selected subarray.
   * Applicable only to dense arrays.
   *
   * @tparam T The domain type.
   * @return Status
   */
  template <class T>
  Status read_dense_sorted_col();

  /**
   * Same as read(), but the cells are placed in 'buffers' sorted in
   * row-major order with respect to the selected subarray.
   * Applicable only to dense arrays.
   *
   * @tparam T The domain type.
   * @return Status
   */
  template <class T>
  Status read_dense_sorted_row();

  /**
   * Same as read(), but the cells are placed in 'buffers' sorted in
   * column-major order with respect to the selected subarray.
   * Applicable only to sparse arrays.
   *
   * @tparam T The domain type.
   * @return Status
   */
  template <class T>
  Status read_sparse_sorted_col();

  /**
   * Same as read(), but the cells are placed in 'buffers' sorted in
   * row-major order with respect to the selected subarray.
   * Applicable only to sparse arrays.
   *
   * @tparam T The domain type.
   * @return Status
   */
  template <class T>
  Status read_sparse_sorted_row();

  /** Resets the temporary buffer sizes for the input tile slab id. */
  void reset_buffer_sizes_tmp(unsigned int id);

  /** Resets the copy state using the input buffer info. */
  void reset_copy_state(void** buffers, uint64_t* buffer_sizes);

  /** Resets the oveflow flags to **false**. */
  void reset_overflow();

  /**
   * Resets the tile_coords_ auxiliary variable.
   *
   * @tparam T The domain type.
   * @return void.
   */
  template <class T>
  void reset_tile_coords();

  /**
   * Resets the tile slab state.
   *
   * @tparam T The domain type.
   * @return void.
   */
  template <class T>
  void reset_tile_slab_state();

  /**
   * It sorts the positions of the cells based on the coordinates
   * of the current tile slab to be copied.
   */
  template <class T>
  void sort_cell_pos();

  /**
   * Calculates the new tile and local buffer offset for the new (already
   * computed) current cell coordinates in the tile slab.
   *
   * @tparam T The domain type
   * @param aid The attribute id to focus on.
   * @return void.
   */
  template <class T>
  void update_current_tile_and_offset(unsigned int aid);
};

}  // namespace tiledb

#endif  // TILEDB_ARRAY_ORDERED_READ_STATE_H
