/**
 * @file   array_ordered_write_state.h
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
 * This file defines class ArrayOrderedWriteState.
 */

#ifndef TILEDB_ARRAY_ORDERED_WRITE_STATE_H
#define TILEDB_ARRAY_ORDERED_WRITE_STATE_H

#include "query.h"
#include "status.h"

#include <condition_variable>
#include <mutex>
#include <string>
#include <vector>

namespace tiledb {

class Query;

/**
 * It is responsible for re-arranging the cells ordered in column- or row-major
 * order within the user subarray, such that they are ordered along the array
 * global cell order, and writes them into a new fragment.
 */
class ArrayOrderedWriteState {
 public:
  /* ********************************* */
  /*          TYPE DEFINITIONS         */
  /* ********************************* */

  /** Used in callback functions in an internal async query. */
  struct ASWS_Data {
    /** An id (typically an attribute id or a tile slab id.) */
    unsigned int id_;
    /** Another id (typically a tile id). */
    uint64_t id_2_;
    /** The calling object. */
    ArrayOrderedWriteState* asws_;
  };

  /** Stores local state about the current write/copy request. */
  struct CopyState {
    /** Local buffer offsets. */
    uint64_t* buffer_offsets_[2];
    /** Local buffer sizes. */
    uint64_t* buffer_sizes_[2];
    /** Local buffers. */
    void** buffers_[2];
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
     * Start offsets of each tile in the user buffer, per attribute per tile.
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
   * @param query The query this array sorted read state belongs to.
   */
  explicit ArrayOrderedWriteState(Query* query);

  /** Destructor. */
  ~ArrayOrderedWriteState();

  /* ********************************* */
  /*               API                 */
  /* ********************************* */

  /**
   * Finalizes the object, and particularly the internal async queries.
   * The async queries will be finalized in the destructor anyway, but
   * this function allows capturing errors upon query finalization.
   */
  Status finalize();

  /**
   * Initializes the array sorted write state.
   *
   * @return Status
   */
  Status init();

  /**
   * The write function. The cells are ordered in row- or column-major order
   * and the function will re-order them along the global cell order before
   * writing them to a new fragment.
   *
   * @param buffers The buffers that hold the input cells to be written.
   * @param buffer_sizes The corresponding buffer sizes.
   * @return Status
   */
  Status write(void** buffers, uint64_t* buffer_sizes);

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** Function for advancing a cell slab during a copy operation. */
  void* (*advance_cell_slab_)(void*);

  /** Condition variables used for the internal async queries. */
  std::condition_variable async_cv_[2];

  /** Data for the internal async queries. */
  ASWS_Data async_data_[2];

  /** Mutexes used in async queries. */
  std::mutex async_mtx_[2];

  /** The async queries. */
  Query* async_query_[2];

  /** Wait for async flags, one for each local buffer. */
  bool async_wait_[2];

  /** The ids of the attributes the array was initialized with. */
  const std::vector<unsigned int> attribute_ids_;

  /**
   * The sizes of the attributes. For variable-length attributes,
   * sizeof(uint64_t) is stored.
   */
  std::vector<uint64_t> attribute_sizes_;

  /** Number of allocated buffers. */
  unsigned int buffer_num_;

  /** The user buffer offsets. */
  uint64_t* buffer_offsets_;

  /** The user buffer sizes. */
  uint64_t* buffer_sizes_;

  /** The user buffers. */
  void** buffers_;

  /** Function for calculating cell slab info during a copy operation. */
  void* (*calculate_cell_slab_info_)(void*);

  /** Function for calculating tile slab info during a copy operation. */
  void* (*calculate_tile_slab_info_)(void*);

  /** The coordinates size of the array. */
  uint64_t coords_size_;

  /** The current id of the buffers the next copy will occur from. */
  unsigned int copy_id_;

  /** The copy state, one per tile slab. */
  CopyState copy_state_;

  /** The number of dimensions in the array. */
  unsigned int dim_num_;

  /** The expanded subarray, such that it coincides with tile boundaries. */
  void* expanded_subarray_;

  /** The array this sorted read state belongs to. */
  Query* query_;

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

  /** Indicates an invalid value. */
  static const uint64_t INVALID_UINT64;

  /* ********************************* */
  /*           PRIVATE METHODS         */
  /* ********************************* */

  /**
   * Advances a cell slab focusing on column-major order, and updates
   * the CopyState and TileSlabState.
   * Used in copy_tile_slab().
   *
   * @tparam T The domain type.
   * @param data Essentially a pointer to a ASWS_Data object.
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
   * @param data Essentially a pointer to a ASWS_Data object.
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
   * Called when an async query completes.
   *
   * @param data A ASWS_Request object.
   * @return void
   */
  static void async_done(void* data);

  /** Notifies an async condition on the input tile slab id. */
  void async_notify(unsigned int id);

  /**
   * Sends an async query.
   *
   * @param async_id The id of the tile slab the AIO request focuses on.
   * @return Status
   */
  Status async_submit_query(unsigned int async_id);

  /** Waits on an async condition on the input tile slab id. */
  void async_wait(unsigned int id);

  /**
   * Calculates the number of buffers to be allocated, based on the number
   * of attributes initialized for the array.
   *
   * @return void
   */
  void calculate_buffer_num();

  /**
   * Calculates the info used in the copy_tile_slab() function, for the case
   * where the **user** cell order is column-major and the **array** cell
   * order is column-major.
   *
   * @tparam T The domain type.
   * @param data Essentially a pointer to a ASWS_Data object.
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
   * @param data Essentially a pointer to a ASWS_Data object.
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
   * @param data Essentially a pointer to a AWRS_Data object.
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
   * @param data Essentially a pointer to a AWRS_Data object.
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
   * @param data Essentially a pointer to a ASWS_Data object.
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
   * @param data Essentially a pointer to a ASWS_Data object.
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
   * @tparam T The attribute type.
   * @param aid The index on attribute_ids_ to focus on.
   * @param bid The index on the copy state buffers to focus on.
   * @return void.
   */
  template <class T>
  void copy_tile_slab(unsigned int aid, unsigned int bid);

  /**
   * Copies a tile slab from the local buffers into the user buffers,
   * properly re-organizing the cell order to fit the targeted order,
   * focusing on a particular variable-length attribute.
   *
   * @tparam T The attribute type.
   * @param aid The index on attribute_ids_ to focus on.
   * @param bid The index on the copy state buffers to focus on.
   * @return void.
   */
  template <class T>
  void copy_tile_slab_var(unsigned int aid, unsigned int bid);

  /**
   * Creates the copy state buffers.
   *
   * @return Status
   */
  Status create_copy_state_buffers();

  /**
   * Creates the user buffers.
   *
   * @param buffers The user buffers that hold the input cells to be written.
   * @param buffer_sizes The corresponding buffer sizes.
   * @return void
   */
  void create_user_buffers(void** buffers, uint64_t* buffer_sizes);

  /**
   * Fills the **entire** buffer of the current copy tile slab with the input id
   * with empty values, based on the template type. Applicable only to
   * fixed-sized attributes.
   *
   * @tparam T The attribute type.
   * @param bid The buffer id corresponding to the targeted attribute.
   * @return void
   */
  template <class T>
  void fill_with_empty(unsigned int bid);

  /**
   * Fills the **a single** cell in a variable-sized buffer of the current copy
   * tile slab with the input id with an empty value, based on the template
   * type. Applicable only to variable-sized attributes.
   *
   * @tparam T The attribute type.
   * @param bid The buffer id corresponding to the targeted attribute.
   * @return void
   */
  template <class T>
  void fill_with_empty_var(unsigned int bid);

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
   * Retrieves the next column tile slab to be processed.
   *
   * @tparam T The domain type.
   * @return True if the next tile slab was retrieved, and false otherwise.
   */
  template <class T>
  bool next_tile_slab_col();

  /**
   * Retrieves the next row tile slab to be processed.
   *
   * @tparam T The domain type.
   * @return True if the next tile slab was retrieved, and false otherwise.
   */
  template <class T>
  bool next_tile_slab_row();

  /**
   * Returns true if every async write should create a separate fragment.
   * This happens when the cells in two different writes do not appear
   * contiguous along the global cell order.
   */
  bool separate_fragments() const;

  /**
   * The write function. The cells are ordered in row- or column-major order
   * and the function will re-order them along the global cell order before
   * writing them to a new fragment.
   *
   * @tparam T The domain type.
   * @return Status
   */
  template <class T>
  Status write();

  /**
   * Same as write(), but the cells are provided by the user sorted in
   * column-major order with respect to the selected subarray.
   *
   * @tparam T The domain type.
   * @return Status
   */
  template <class T>
  Status write_sorted_col();

  /**
   * Same as write(), but the cells are provided by the user sorted in
   * row-major order with respect to the selected subarray.
   *
   * @tparam T The domain type.
   * @return Status
   */
  template <class T>
  Status write_sorted_row();

  /** Resets the copy state for the current copy id. */
  void reset_copy_state();

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
   * Calculates the new tile and local buffer offset for the new (already
   * computed) current cell coordinates in the tile slab.
   *
   * @param aid The attribute id to focus on.
   * @return void.
   */
  void update_current_tile_and_offset(unsigned int aid);

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

#endif  // TILEDB_ARRAY_ORDERED_WRITE_STATE_H
