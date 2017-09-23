/**
 * @file   write_state.h
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
 * This file defines class WriteState.
 */

#ifndef TILEDB_WRITE_STATE_H
#define TILEDB_WRITE_STATE_H

#include "fragment.h"
#include "fragment_metadata.h"
#include "tile.h"
#include "tile_io.h"

#include <iostream>
#include <vector>

namespace tiledb {

/** Stores the state necessary when writing cells to a fragment. */
class WriteState {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /**
   * Constructor.
   *
   * @param fragment The fragment the write state belongs to.
   */
  explicit WriteState(const Fragment* fragment);

  /** Destructor. */
  ~WriteState();

  /* ********************************* */
  /*              MUTATORS             */
  /* ********************************* */

  /**
   * Finalizes the fragment.
   *
   * @return Status
   */
  Status finalize();

  /**
   * Syncs all attribute files in the fragment.
   *
   * @return Status
   */
  Status sync();

  /**
   * Performs a write operation in the fragment.
   *
   * @param buffers An array of buffers, one for each attribute (two for a
   *     variable-sized attribute).
   * @param buffer_sizes The sizes (in bytes) of the input buffers (there is
   *     a one-to-one correspondence).
   * @return Status
   */
  Status write(void** buffers, uint64_t* buffer_sizes);

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** The bookkeeping structure of the fragment the write state belongs to. */
  FragmentMetadata* metadata_;

  /** The first and last coordinates of the tile currently being populated. */
  void* bounding_coords_;

  /**
   * The current offsets of the variable-sized attributes in their
   * respective files, or alternatively, the current file size of each
   * variable-sized attribute.
   */
  std::vector<uint64_t> buffer_var_offsets_;

  /** The fragment the write state belongs to. */
  const Fragment* fragment_;

  /** The MBR of the tile currently being populated. */
  void* mbr_;

  /** Auxiliary variable used whenever a tile id needs to be computed. */
  void* tile_coords_aux_;

  /** The number of cells written in the current tile for each attribute. */
  std::vector<uint64_t> tile_cell_num_;

  /** The current tiles, one per attribute. */
  std::vector<Tile*> tiles_;

  /** The current variable-sized tiles, one per attribute. */
  std::vector<Tile*> tiles_var_;

  /**
   * The objects that perform tile I/O, one per attribute and one for
   * the dimensions.
   */
  std::vector<TileIO*> tile_io_;

  /**
   * The objects that perform tile I/O, one per variable-sized attribute.
   */
  std::vector<TileIO*> tile_io_var_;

  /* ********************************* */
  /*           PRIVATE METHODS         */
  /* ********************************* */

  /**
   * Expands the current MBR with the input coordinates.
   *
   * @tparam T The type of the MBR and the input coordinates.
   * @param coords The input coordinates.
   * @return void
   */
  template <class T>
  void expand_mbr(const T* coords);

  /** Initializes the internal tile structures. */
  void init_tiles();

  /** Initializes the internal Tile I/O structures. */
  void init_tile_io();

  /**
   * Sorts the input cell coordinates according to the order specified in the
   * array schema. This is not done in place; the sorted positions are stored
   * in a separate vector.
   *
   * @param buffer The buffer holding the cell coordinates.
   * @param buffer_size The size (in bytes) of *buffer*.
   * @param cell_pos The sorted cell positions.
   * @return void
   */
  void sort_cell_pos(
      const void* buffer,
      uint64_t buffer_size,
      std::vector<uint64_t>* cell_pos) const;

  /**
   * Sorts the input cell coordinates according to the order specified in the
   * array schema. This is not done in place; the sorted positions are stored
   * in a separate vector.
   *
   * @tparam T The type of coordinates stored in *buffer*.
   * @param buffer The buffer holding the cell coordinates.
   * @param buffer_size The size (in bytes) of *buffer*.
   * @param cell_pos The sorted cell positions.
   * @return void
   */
  template <class T>
  void sort_cell_pos(
      const void* buffer,
      uint64_t buffer_size,
      std::vector<uint64_t>* cell_pos) const;

  /**
   * Updates the bookkeeping structures as tiles are written. Specifically, it
   * updates the MBR and bounding coordinates of each tile.
   *
   * @param buffer The buffer storing the cell coordinates.
   * @param buffer_size The size (in bytes) of *buffer*.
   * @return void
   */
  void update_bookkeeping(const void* buffer, uint64_t buffer_size);

  /**
   * Updates the bookkeeping structures as tiles are written. Specifically, it
   * updates the MBR and bounding coordinates of each tile.
   *
   * @tparam T The coordinates type.
   * @param buffer The buffer storing the cell coordinates.
   * @param buffer_size The size (in bytes) of *buffer*.
   * @return void
   */
  template <class T>
  void update_bookkeeping(const void* buffer, uint64_t buffer_size);

  /**
   * Performs the write operation for the case of a dense fragment, focusing
   * on a single fixed-sized attribute.
   *
   * @param attribute_id The id of the attribute this operation focuses on.
   * @param buffer The buffer to write.
   * @param buffer_size The buffer size.
   * @return Status
   */
  Status write_attr(
      unsigned int attribute_id, void* buffer, uint64_t buffer_size);

  /**
   * Writes the last tile with the input id to the disk.
   *
   * @param attribute_id The id of the attribute this operation focuses on.
   * @return Status
   */
  Status write_attr_last(unsigned int attribute_id);

  /**
   * Performs the write operation for the case of a dense fragment, focusing
   * on a single variable-sized attribute.
   *
   * @param attribute_id The id of the attribute this operation focuses on.
   * @param buffer The buffer storing the offsets of the variable-sized cells.
   * @param buffer_size The buffer size.
   * @param buffer_var The buffer storing the variable-sized cells.
   * @param buffer_var_size The size of *buffer_var*.
   * @return Status
   */
  Status write_attr_var(
      unsigned int attribute_id,
      void* buffer,
      uint64_t buffer_size,
      void* buffer_var,
      uint64_t buffer_var_size);

  /**
   * Writes the last variable-sized tile with the input id to the disk.
   *
   * @param attribute_id The id of the attribute this operation focuses on.
   * @return Status
   */
  Status write_attr_var_last(unsigned int attribute_id);

  /**
   * Takes the appropriate actions for writing the very last tile of this write
   * operation. This is done for every attribute.
   *
   * @return Status
   */
  Status write_last_tile();

  /**
   * Performs the write operation for the case of a sparse fragment when the
   * coordinates are unsorted.
   *
   * @param buffers See write().
   * @param buffer_sizes See write().
   * @return Status
   */
  Status write_sparse_unsorted(void** buffers, uint64_t* buffer_sizes);

  /**
   * Performs the write operation for the case of a sparse fragment when the
   * coordinates are unsorted, focusing on a single fixed-sized attribute.
   *
   * @param attribute_id The id of the attribute this operation focuses on.
   * @param buffer The buffer to write.
   * @param buffer_size The buffer size.
   * @param cell_pos The sorted positions of the cells.
   * @return Status
   */
  Status write_sparse_unsorted_attr(
      unsigned int attribute_id,
      void* buffer,
      uint64_t buffer_size,
      const std::vector<uint64_t>& cell_pos);

  /**
   * Performs the write operation for the case of a sparse fragment when the
   * coordinates are unsorted, focusing on a single variable-sized attribute.
   *
   * @param attribute_id The id of the attribute this operation focuses on.
   * @param buffer The buffer storing the offsets of the variable-sized cells.
   * @param buffer_size The buffer size.
   * @param buffer_var The buffer storing the variable-sized cells.
   * @param buffer_var_size The size of *buffer_var*.
   * @param cell_pos The sorted positions of the cells.
   * @return Status
   */
  Status write_sparse_unsorted_attr_var(
      unsigned int attribute_id,
      void* buffer,
      uint64_t buffer_size,
      void* buffer_var,
      uint64_t buffer_var_size,
      const std::vector<uint64_t>& cell_pos);
};

}  // namespace tiledb

#endif  // TILEDB_WRITE_STATE_H
